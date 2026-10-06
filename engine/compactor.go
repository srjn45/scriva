package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/srjn45/scriva/store"
)

// compactLoop runs in a goroutine for the lifetime of a Collection.
// It triggers compaction either when signalled (via compactC) or on a timer.
func (c *Collection) compactLoop() {
	ticker := time.NewTicker(c.cfg.CompactInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			if c.isClosed() {
				return
			}
			_ = c.reapExpired()
			_ = c.compact(false)
		case <-c.compactC:
			if c.isClosed() {
				return
			}
			_ = c.reapExpired()
			_ = c.compact(false)
		}
	}
}

// isClosed reports whether the collection has been closed. Once closed, the
// compactor must never start a new compaction: a select that observes both a
// ready compactC signal and a closed channel picks a case at random, so a
// late compaction could otherwise race with Close() (which closes the active
// segment and persists the index) and corrupt the on-disk segment layout.
func (c *Collection) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// CompactNow runs a compaction pass synchronously and returns only after it has
// completed. Unlike the background compactor it ignores the dirty-ratio gate, so
// operators can force a full merge on demand (e.g. before taking a backup). It
// serializes with the background compactor via compactMu, so a concurrent
// automatic pass cannot race the on-demand one.
func (c *Collection) CompactNow() error {
	if c.isClosed() {
		return fmt.Errorf("compactor: collection %q is closed", c.name)
	}
	if err := c.reapExpired(); err != nil {
		return err
	}
	return c.compact(true)
}

// compact merges and deduplicates sealed segments.
// It operates only on sealed (immutable) segments so writes are never blocked
// except during the brief atomic swap at the end. When force is true the
// dirty-ratio gate is skipped so a caller can compel a full merge on demand.
func (c *Collection) compact(force bool) error {
	// Serialize passes so the background compactor and an on-demand CompactNow
	// never snapshot, remove, and rename the same sealed segments concurrently.
	c.compactMu.Lock()
	defer c.compactMu.Unlock()

	// Re-check after acquiring the lock: Close() holds compactMu while it
	// persists the final index, so a pass that was blocked on the lock during
	// shutdown must not mutate the segment layout afterwards.
	if c.closeDone {
		return nil
	}

	start := time.Now()

	// --- Step 1: Snapshot sealed segments under read lock ---
	c.mu.RLock()
	if len(c.sealed) == 0 {
		c.mu.RUnlock()
		return nil
	}
	toCompact := make([]*Segment, len(c.sealed))
	copy(toCompact, c.sealed)
	c.mu.RUnlock()

	// --- Step 2: Check dirty ratio (skipped for a forced compaction) ---
	// A pending encryption migration (entries below the current policy epoch in
	// these segments) also justifies a pass even when the dirty ratio is low, so
	// background compaction eventually brings every sealed record to the current
	// policy without an operator forcing it.
	if !force && !c.isDirty(toCompact) && !c.migrationPendingIn(toCompact) {
		return nil
	}

	// --- Step 3: Replay all entries, keep latest per id ---
	resolved, err := resolveEntries(toCompact)
	if err != nil {
		return fmt.Errorf("compactor: resolve: %w", err)
	}

	// Re-encrypt surviving entries that predate the current policy epoch, bringing
	// them to the current fields/key. This is fail-closed: a decrypt error (e.g. a
	// key that was retired too early) aborts the pass before anything on disk is
	// mutated, so no data is lost.
	resolved, err = c.reencryptForMigration(resolved)
	if err != nil {
		return fmt.Errorf("compactor: migrate: %w", err)
	}

	// --- Step 4: Write resolved entries into temp segment files (not yet renamed) ---
	tempSegs, err := c.writeCompacted(resolved)
	if err != nil {
		return fmt.Errorf("compactor: write compacted: %w", err)
	}
	// Nothing survived (all deletes) — still need to swap under lock.

	// --- Step 5: Durably record the swap intent, then swap under write lock ---
	//
	// The manifest is fsynced before any segment file is mutated and removed
	// only after the post-swap index persist, so a crash anywhere in between
	// is rolled forward idempotently by recoverCompaction at the next open.
	renames := make(map[string]string, len(tempSegs))
	finals := make(map[string]struct{}, len(tempSegs))
	for i, seg := range tempSegs {
		var final string
		if i < len(toCompact) {
			final = toCompact[i].Path()
		} else {
			final = c.segmentPath(c.segSeq.Add(1))
		}
		renames[seg.Path()] = final
		finals[final] = struct{}{}
	}
	var removals []string
	for _, s := range toCompact {
		if _, reused := finals[s.Path()]; !reused {
			removals = append(removals, s.Path())
		}
	}
	if err := writeCompactManifest(c.dir, compactManifest{Renames: renames, Removals: removals}); err != nil {
		return fmt.Errorf("compactor: write manifest: %w", err)
	}

	if c.cfg.preSwapHook != nil {
		c.cfg.preSwapHook()
	}

	c.mu.Lock()

	// Rename the temp files over their final positions first — when a final
	// name belongs to an old sealed segment the rename replaces it atomically —
	// then delete the old segments whose names were not reused. The reverse
	// order (remove everything, then rename) left a window where the only copy
	// of the sealed data sat in temp files an open would never discover.
	var newSegs []*Segment
	for i, seg := range tempSegs {
		finalPath := c.segmentPath(uint64(i + 1))
		if err := doRename(c.cfg.renameFn, seg.Path(), finalPath); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("compactor: rename %q → %q: %w", seg.Path(), finalPath, err)
		}
		info, _ := os.Stat(finalPath)
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		newSegs = append(newSegs, openSealedSegment(finalPath, size))
	}
	for _, p := range removals {
		_ = os.Remove(p)
	}

	// Keep segments sealed during this compaction pass.
	if len(c.sealed) > len(toCompact) {
		newSegs = append(newSegs, c.sealed[len(toCompact):]...)
	}

	c.sealed = newSegs

	// Rebuild the index from new segments + active.
	all := make([]*Segment, 0, len(c.sealed)+1)
	all = append(all, c.sealed...)
	all = append(all, c.active)
	if err := c.index.Rebuild(all); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("compactor: rebuild index: %w", err)
	}

	c.mu.Unlock()

	// Persist updated primary index. On failure the manifest is deliberately
	// left in place so the next open rebuilds from the segments instead of
	// trusting a stale index.
	if err := c.index.Persist(filepath.Join(c.dir, "index.json")); err != nil {
		return fmt.Errorf("compactor: persist index: %w", err)
	}

	if c.cfg.preSidxRebuildHook != nil {
		c.cfg.preSidxRebuildHook()
	}

	// Rebuild and persist every secondary index from the new segment layout.
	c.sidxMu.RLock()
	sidxCopy := make(map[string]*SecondaryIndex, len(c.sidxMap))
	for f, s := range c.sidxMap {
		sidxCopy[f] = s
	}
	c.sidxMu.RUnlock()

	c.mu.RLock()
	allSegs := make([]*Segment, 0, len(c.sealed)+1)
	allSegs = append(allSegs, c.sealed...)
	allSegs = append(allSegs, c.active)
	c.mu.RUnlock()

	for field, sidx := range sidxCopy {
		if err := sidx.rebuild(allSegs); err != nil {
			return fmt.Errorf("compactor: rebuild secondary index %q: %w", field, err)
		}
		_ = sidx.Persist(sidxFilePath(c.dir, field))
	}

	// The on-disk layout and indexes are consistent again — retire the intent.
	if err := clearCompactManifest(c.dir); err != nil {
		return fmt.Errorf("compactor: %w", err)
	}

	if c.cfg.OnCompaction != nil {
		c.cfg.OnCompaction(c.name, time.Since(start))
	}

	return nil
}

// MigrateNow brings every live record to the current encryption policy and purges
// old-form bytes in one synchronous pass: it seals the active segment so its
// records join the sealed set, then runs a forced re-encrypting compaction. On
// return the collection has reached security completion for the current policy —
// every live record is at the current epoch and no stale old-form bytes remain —
// so an old key can be retired or a de-encrypted field indexed. It is a no-op for
// a collection that is not encrypted. Requires every key still protecting on-disk
// blobs to be resolvable via the provider; a missing key fails the pass without
// mutating anything.
func (c *Collection) MigrateNow(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.enc.Load() == nil {
		return nil // nothing to migrate
	}
	if c.isClosed() {
		return fmt.Errorf("compactor: collection %q is closed", c.name)
	}
	// Move any live records still in the active segment into the sealed set so the
	// compaction pass rewrites them too. Skip an empty active to avoid churning an
	// empty sealed segment.
	c.mu.RLock()
	activeHasData := c.active != nil && c.active.Size() > 0
	c.mu.RUnlock()
	if activeHasData {
		if err := c.rotateSegment(); err != nil {
			return fmt.Errorf("compactor: migrate rotate: %w", err)
		}
	}
	if err := c.reapExpired(); err != nil {
		return err
	}
	return c.compact(true)
}

// migrationPendingIn reports whether any live record located in one of segs was
// written under an epoch older than the current policy epoch, i.e. the segment
// holds records a re-encrypting pass would upgrade. It reads only the in-memory
// index, so the compaction gate stays cheap.
func (c *Collection) migrationPendingIn(segs []*Segment) bool {
	enc := c.enc.Load()
	if enc == nil {
		return false
	}
	epoch := enc.epoch
	paths := make(map[string]struct{}, len(segs))
	for _, s := range segs {
		paths[s.Path()] = struct{}{}
	}
	c.index.mu.RLock()
	defer c.index.mu.RUnlock()
	for _, e := range c.index.entries {
		if e.Epoch != epoch {
			if _, ok := paths[e.SegmentPath]; ok {
				return true
			}
		}
	}
	return false
}

// reencryptForMigration rewrites each surviving entry that predates the current
// policy epoch to the current write policy and key, stamping the current epoch. An
// entry already at the current epoch is left untouched (its ciphertext already
// conforms), so a steady-state compaction with no policy change re-encrypts
// nothing. It is fail-closed: a decrypt failure is returned so the caller aborts
// the pass rather than dropping or corrupting data.
func (c *Collection) reencryptForMigration(entries []store.Entry) ([]store.Entry, error) {
	enc := c.enc.Load()
	if enc == nil {
		return entries, nil
	}
	epoch := enc.epoch
	ctx := context.Background()
	for i := range entries {
		e := &entries[i]
		if e.Op == store.OpDelete || e.Epoch == epoch {
			continue
		}
		stored, err := enc.reencrypt(ctx, e.Data)
		if err != nil {
			return nil, fmt.Errorf("id %d: %w", e.ID, err)
		}
		e.Data = stored
		e.Epoch = epoch
	}
	return entries, nil
}

// isDirty returns true when the proportion of stale entries in the sealed
// segments exceeds the configured threshold.
func (c *Collection) isDirty(segs []*Segment) bool {
	var total, stale int

	// Build a set of live ids from the current index.
	c.index.mu.RLock()
	live := make(map[uint64]string, len(c.index.entries))
	for id, loc := range c.index.entries {
		live[id] = loc.SegmentPath
	}
	c.index.mu.RUnlock()

	for _, seg := range segs {
		entries, err := seg.ScanAll()
		if err != nil {
			continue
		}
		for _, e := range entries {
			total++
			loc, isLive := live[e.ID]
			// Entry is stale if: it's a delete tombstone, or the index points
			// to a different (newer) location for this id.
			if e.Op == store.OpDelete || !isLive || loc != seg.Path() {
				stale++
			}
		}
	}

	if total == 0 {
		return false
	}
	return float64(stale)/float64(total) > c.cfg.CompactDirtyPct
}

// resolveEntries replays all entries from the given segments and returns only
// the latest surviving entry per id (deletes are dropped). Records whose TTL has
// already passed are dropped too, so compaction reclaims expired data even if
// the reaper has not yet tombstoned it.
func resolveEntries(segs []*Segment) ([]store.Entry, error) {
	latest := make(map[uint64]store.Entry)

	for _, seg := range segs {
		entries, err := seg.ScanAll()
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			latest[e.ID] = e // last write wins
		}
	}

	now := time.Now().UnixNano()
	var out []store.Entry
	for _, e := range latest {
		if e.Op == store.OpDelete || expired(e.ExpiresAt, now) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// writeCompacted writes resolved entries into new segment files under c.dir,
// using temp paths that are renamed into place once complete.
func (c *Collection) writeCompacted(entries []store.Entry) ([]*Segment, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	var (
		segs       []*Segment
		current    *Segment
		segIdx     = 1
		tempPrefix = filepath.Join(c.dir, ".compact_")
	)

	newSeg := func() (*Segment, error) {
		path := fmt.Sprintf("%s%06d.ndjson", tempPrefix, segIdx)
		segIdx++
		return openActiveSegmentWith(path, c.cfg.wrapFile)
	}

	var err error
	current, err = newSeg()
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if current.Size() >= c.cfg.SegmentMaxSize {
			if sealErr := current.Seal(); sealErr != nil {
				return nil, sealErr
			}
			segs = append(segs, current)
			current, err = newSeg()
			if err != nil {
				return nil, err
			}
		}
		if _, err := current.Append(e); err != nil {
			return nil, err
		}
	}

	if err := current.Seal(); err != nil {
		return nil, err
	}
	segs = append(segs, current)

	// Rebalance: merge segments that are below 10% of target size into the
	// previous segment where possible.
	segs, err = rebalance(segs, c.cfg.SegmentMaxSize)
	if err != nil {
		return nil, err
	}

	// Return temp-named segments; caller renames them inside the write lock.
	return segs, nil
}

// rebalance merges adjacent segments whose combined size fits within maxSize
// and whose individual sizes are below 10% of maxSize.
func rebalance(segs []*Segment, maxSize int64) ([]*Segment, error) {
	minSize := maxSize / 10
	if len(segs) <= 1 {
		return segs, nil
	}

	var result []*Segment
	i := 0
	for i < len(segs) {
		s := segs[i]
		if s.Size() >= minSize || i == len(segs)-1 {
			result = append(result, s)
			i++
			continue
		}

		// Try to merge s with the next segment.
		next := segs[i+1]
		if s.Size()+next.Size() <= maxSize {
			merged, err := mergeSegments(s, next)
			if err != nil {
				return nil, err
			}
			_ = os.Remove(s.Path())
			_ = os.Remove(next.Path())
			result = append(result, merged)
			i += 2
		} else {
			result = append(result, s)
			i++
		}
	}
	return result, nil
}

// mergeSegments writes all entries from a and b into a new temp file.
func mergeSegments(a, b *Segment) (*Segment, error) {
	tmpPath := a.Path() + ".merge"
	merged, err := openActiveSegment(tmpPath)
	if err != nil {
		return nil, err
	}

	for _, src := range []*Segment{a, b} {
		entries, err := src.ScanAll()
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if _, err := merged.Append(e); err != nil {
				return nil, err
			}
		}
	}

	if err := merged.Seal(); err != nil {
		return nil, err
	}
	return merged, nil
}
