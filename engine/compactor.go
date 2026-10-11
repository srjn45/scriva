package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/srjn45/scriva/store"
)

const (
	compactionRetryDelay    = 100 * time.Millisecond
	compactionRetryMaxDelay = 3 * time.Second
)

// ErrCompactionDeferred reports that CompactNow could not start without
// blocking active full scans. A background pass is rescheduled automatically.
var ErrCompactionDeferred = errors.New("compactor: deferred by active scan")

// rescheduleCompaction retries a pass deferred by an active full scan. The
// flag coalesces retries, avoiding a busy loop when a client consumes a stream
// slowly; the deferral itself is logged at the call site for observability.
func (c *Collection) rescheduleCompaction() {
	if !c.compactRetryPending.CompareAndSwap(false, true) {
		return
	}
	exp := c.compactRetryExp.Add(1) - 1
	if exp > 5 {
		exp = 5
	}
	delay := compactionRetryDelay << exp
	if delay > compactionRetryMaxDelay {
		delay = compactionRetryMaxDelay
	}
	go func() {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-c.closed:
			return
		case <-t.C:
		}
		c.compactRetryPending.Store(false)
		select {
		case c.compactC <- struct{}{}:
		default:
		}
	}()
}

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
	return c.compactLocked(force)
}

// compactLocked is one compaction pass; the caller holds compactMu. Split out
// so a test can arm a fault and run the pass it is meant for under one hold of
// the lock, with no background pass able to slip in between.
func (c *Collection) compactLocked(force bool) error {
	// Re-check after acquiring the lock: Close() holds compactMu while it
	// persists the final index, so a pass that was blocked on the lock during
	// shutdown must not mutate the segment layout afterwards.
	if c.closeDone {
		return nil
	}
	// A swap that failed midway left segment files and the in-memory layout
	// out of step; only a reopen (recoverCompaction + index rebuild) can
	// reconcile them, so refuse to start another pass on top of it.
	if c.swapFailed {
		return fmt.Errorf("compactor: collection %q has an incomplete swap; reopen to recover", c.name)
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

	// Cheap early probe: do not spend a full rewrite when an existing scan is
	// already using the layout. Release it immediately so the rewrite itself
	// never blocks new scans.
	if !c.layoutMu.TryLock() {
		if c.cfg.Logger != nil {
			c.cfg.Logger.Warn("compaction deferred while scans hold layout lease", "collection", c.name)
		}
		c.rescheduleCompaction()
		if force {
			return ErrCompactionDeferred
		}
		return nil
	}
	c.layoutMu.Unlock()
	c.compactRetryExp.Store(0)

	// --- Step 3: Replay all entries, keep latest committed/plain value per id ---
	resolved, err := resolveCompactEntries(toCompact, c.decisions)
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
	// Temps left by an earlier failed pass are never needed (a swap that got as
	// far as the manifest sets swapFailed instead) and must go: segment files
	// are opened in append mode, so reusing a stale temp name would splice old
	// bytes into the new segment.
	if err := discardCompactTemps(c.dir); err != nil {
		return fmt.Errorf("compactor: %w", err)
	}
	tempSegs, err := c.writeCompactEntries(resolved)
	if err != nil {
		_ = discardCompactTemps(c.dir)
		return fmt.Errorf("compactor: write compacted: %w", err)
	}
	// Output segments take the names of the segments they replace, in order, so
	// they stay older than every segment rotated in meanwhile and than the
	// active one. More output segments than inputs would need names that do not
	// exist below the newer segments; fail before touching anything.
	if len(tempSegs) > len(toCompact) {
		_ = discardCompactTemps(c.dir)
		return fmt.Errorf("compactor: compacted output (%d segments) exceeds input (%d); aborting before swap", len(tempSegs), len(toCompact))
	}
	if c.cfg.postWriteCompactedHook != nil {
		c.cfg.postWriteCompactedHook()
	}
	// Take the exclusive lease only for the durable swap. If a scan began while
	// temps were written, discard them: no manifest exists yet, so crash
	// recovery continues to trust the untouched old layout.
	if !c.layoutMu.TryLock() {
		_ = discardCompactTemps(c.dir)
		if c.cfg.Logger != nil {
			c.cfg.Logger.Warn("compaction deferred while scans hold layout lease", "collection", c.name)
		}
		c.rescheduleCompaction()
		if force {
			return ErrCompactionDeferred
		}
		return nil
	}
	layoutLocked := true
	defer func() {
		if layoutLocked {
			c.layoutMu.Unlock()
		}
	}()
	// Nothing survived (all deletes) — still need to swap under lock.

	// --- Step 5: Durably record the swap intent, then swap under write lock ---
	//
	// The manifest is fsynced before any segment file is mutated and removed
	// only after the post-swap index persist, so a crash anywhere in between
	// is rolled forward idempotently by recoverCompaction at the next open.
	renames := make(map[string]string, len(tempSegs))
	finals := make(map[string]struct{}, len(tempSegs))
	finalPaths := make([]string, len(tempSegs))
	for i, seg := range tempSegs {
		final := toCompact[i].Path()
		finalPaths[i] = final
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
		// writeFileAtomic can report an error after the final rename (for
		// example while syncing the directory). Retire any possible durable
		// intent before deleting its sources; otherwise recovery could treat
		// missing temps as already-renamed and remove the old segments.
		if clearErr := clearCompactManifest(c.dir); clearErr != nil {
			return fmt.Errorf("compactor: write manifest: %w", errors.Join(err, fmt.Errorf("clear possible intent: %w", clearErr)))
		}
		_ = discardCompactTemps(c.dir)
		return fmt.Errorf("compactor: write manifest: %w", err)
	}

	if c.cfg.preSwapHook != nil {
		c.cfg.preSwapHook()
	}

	c.mu.Lock()
	// Invalidate lock-free point reads that resolved a location in the layout
	// about to be replaced (see getStored). Bumped before the first rename so no
	// read can observe a replaced file under the old generation.
	c.layoutGen.Add(1)

	// Rename the temp files over their final positions first — when a final
	// name belongs to an old sealed segment the rename replaces it atomically —
	// then delete the old segments whose names were not reused. The reverse
	// order (remove everything, then rename) left a window where the only copy
	// of the sealed data sat in temp files an open would never discover.
	var newSegs []*Segment
	for i, seg := range tempSegs {
		finalPath := finalPaths[i]
		if err := doRename(c.cfg.renameFn, seg.Path(), finalPath); err != nil {
			// Earlier renames already replaced old segments, so the layout can
			// only move forward: the manifest stays and the next open rolls it.
			if i == 0 {
				// Nothing was replaced yet: abandon the pass and keep serving
				// the untouched old layout.
				c.mu.Unlock()
				if clearErr := clearCompactManifest(c.dir); clearErr != nil {
					c.swapFailed = true
					return fmt.Errorf("compactor: rename %q → %q: %w", seg.Path(), finalPath, errors.Join(err, fmt.Errorf("clear manifest: %w", clearErr)))
				}
				_ = discardCompactTemps(c.dir)
				return fmt.Errorf("compactor: rename %q → %q: %w", seg.Path(), finalPath, err)
			}
			c.swapFailed = true
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
	// Make the renames durable before any old segment is unlinked, so a crash
	// cannot persist the removals without the replacements.
	if err := fsyncDir(c.dir); err != nil {
		c.swapFailed = true
		c.mu.Unlock()
		return fmt.Errorf("compactor: fsync dir after rename: %w", err)
	}
	for _, p := range removals {
		_ = os.Remove(p)
	}

	// Keep segments sealed during this compaction pass.
	if len(c.sealed) > len(toCompact) {
		newSegs = append(newSegs, c.sealed[len(toCompact):]...)
	}

	c.sealed = newSegs
	// Segment files were replaced: memoized sealed coverage is invalid.
	clear(c.sealedCov)

	// Rebuild the index from new segments + active.
	all := make([]*Segment, 0, len(c.sealed)+1)
	all = append(all, c.sealed...)
	all = append(all, c.active)
	if err := c.index.Rebuild(all, c.decisions); err != nil {
		c.swapFailed = true
		c.mu.Unlock()
		return fmt.Errorf("compactor: rebuild index: %w", err)
	}
	snap, err := c.index.Snapshot(all)
	if err != nil {
		c.swapFailed = true
		c.mu.Unlock()
		return fmt.Errorf("compactor: snapshot index: %w", err)
	}

	// Rebuild every secondary index and snapshot its buckets under the same
	// write lock. Rebuilding after the unlock left a window in which a write
	// landing between the segment scan and the bucket replacement was dropped
	// from the index; here no write can interleave, and the snapshot's buckets
	// match snap.coverage exactly.
	c.sidxMu.RLock()
	sidxSnaps := make(map[string]*sidxSnapshot, len(c.sidxMap))
	for field, sidx := range c.sidxMap {
		if err := sidx.rebuild(all, false, c.decisions); err != nil {
			c.sidxMu.RUnlock()
			c.swapFailed = true
			c.mu.Unlock()
			return fmt.Errorf("compactor: rebuild secondary index %q: %w", field, err)
		}
		sidxSnaps[field] = sidx.snapshot()
	}
	c.sidxMu.RUnlock()

	c.mu.Unlock()
	c.layoutMu.Unlock()
	layoutLocked = false

	// Persist updated primary index. On failure the manifest is deliberately
	// left in place so the next open rebuilds from the segments instead of
	// trusting a stale index.
	c.persistMu.Lock()
	err = snap.Persist(filepath.Join(c.dir, "index.json"))
	c.persistMu.Unlock()
	if err != nil {
		return fmt.Errorf("compactor: persist index: %w", err)
	}

	if c.cfg.preSidxRebuildHook != nil {
		c.cfg.preSidxRebuildHook()
	}

	// Persist each secondary index with the coverage of the swapped layout (v2),
	// so open can trust it and tail-replay anything written afterwards. As with
	// the primary, a failure leaves the manifest so the next open rebuilds.
	c.persistMu.Lock()
	c.sidxMu.RLock() // excludes DropIndex so a dropped index's file isn't resurrected
	for field, sn := range sidxSnaps {
		if _, live := c.sidxMap[field]; !live {
			continue
		}
		if err := sn.write(sidxFilePath(c.dir, field), snap.coverage, true); err != nil {
			c.sidxMu.RUnlock()
			c.persistMu.Unlock()
			return fmt.Errorf("compactor: persist secondary index %q: %w", field, err)
		}
	}
	c.sidxMu.RUnlock()
	c.persistMu.Unlock()

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
func (c *Collection) reencryptForMigration(entries []compactEntry) ([]compactEntry, error) {
	enc := c.enc.Load()
	if enc == nil {
		return entries, nil
	}
	epoch := enc.epoch
	ctx := context.Background()
	for i := range entries {
		if entries[i].tx != nil {
			continue
		}
		e := &entries[i].entry
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

type compactEntry struct {
	entry store.Entry
	tx    *TxStamp
}

// resolveEntries replays all entries from the given segments and returns the
// latest surviving plain entry per id (deletes are dropped), plus unresolved
// or unretired committed transaction evidence in original log order. Stamped
// committed evidence remains the materialized survivor until the coordinator
// can be atomically checkpointed; aborted stamped entries are omitted.
func resolveCompactEntries(segs []*Segment, decisions map[string]string) ([]compactEntry, error) {
	latest := make(map[uint64]store.Entry)
	var evidence []compactEntry

	for _, seg := range segs {
		err := seg.ScanStampedFromOffset(0, func(_ int64, e store.Entry, tx *TxStamp) error {
			switch {
			case tx == nil || tx.T == "":
				latest[e.ID] = e // last plain write wins
			case entryVisible(tx, decisions):
				stamp := *tx
				evidence = append(evidence, compactEntry{entry: e, tx: &stamp})
				delete(latest, e.ID)
			case decisions == nil || decisions[tx.T] == "":
				stamp := *tx
				evidence = append(evidence, compactEntry{entry: e, tx: &stamp})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	now := time.Now().UnixNano()
	live := make([]store.Entry, 0, len(latest))
	for _, e := range latest {
		if e.Op == store.OpDelete || expired(e.ExpiresAt, now) {
			continue
		}
		live = append(live, e)
	}
	// Map iteration order is random; emit by id so the same input always yields
	// byte-identical output segments (and a reproducible rebalance).
	sort.Slice(live, func(i, j int) bool { return live[i].ID < live[j].ID })
	out := make([]compactEntry, 0, len(evidence)+len(live))
	out = append(out, evidence...)
	for _, e := range live {
		out = append(out, compactEntry{entry: e})
	}
	return out, nil
}

func resolveEntries(segs []*Segment) ([]store.Entry, error) {
	compact, err := resolveCompactEntries(segs, nil)
	if err != nil {
		return nil, err
	}
	out := make([]store.Entry, 0, len(compact))
	for _, ce := range compact {
		out = append(out, ce.entry)
	}
	return out, nil
}

// writeCompacted writes resolved entries into new segment files under c.dir,
// using temp paths that are renamed into place once complete.
func (c *Collection) writeCompacted(entries []store.Entry) ([]*Segment, error) {
	compact := make([]compactEntry, 0, len(entries))
	for _, e := range entries {
		compact = append(compact, compactEntry{entry: e})
	}
	return c.writeCompactEntries(compact)
}

func (c *Collection) writeCompactEntries(entries []compactEntry) ([]*Segment, error) {
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

	for _, ce := range entries {
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
		if ce.tx != nil {
			se := stampedEntry{
				ID:        ce.entry.ID,
				Op:        ce.entry.Op,
				Ts:        ce.entry.Ts.Format(time.RFC3339Nano),
				Rev:       ce.entry.Rev,
				Data:      ce.entry.Data,
				Epoch:     ce.entry.Epoch,
				ExpiresAt: ce.entry.ExpiresAt,
				Tx:        *ce.tx,
			}
			if _, err := current.AppendStamped(se); err != nil {
				return nil, err
			}
		} else if _, err := current.Append(ce.entry); err != nil {
			return nil, err
		}
	}

	if err := current.Seal(); err != nil {
		return nil, err
	}
	segs = append(segs, current)

	// Rebalance: merge segments that are below 10% of target size into the
	// previous segment where possible.
	segs, err = rebalance(segs, c.cfg.SegmentMaxSize, c.cfg.wrapFile)
	if err != nil {
		return nil, err
	}

	// Return temp-named segments; caller renames them inside the write lock.
	return segs, nil
}

// rebalance merges adjacent segments whose combined size fits within maxSize
// and whose individual sizes are below 10% of maxSize.
func rebalance(segs []*Segment, maxSize int64, wrap fileWrapper) ([]*Segment, error) {
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
			merged, err := mergeSegments(s, next, wrap)
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
func mergeSegments(a, b *Segment, wrap fileWrapper) (*Segment, error) {
	tmpPath := a.Path() + ".merge"
	// Start from nothing: a stale file here would be appended to.
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	merged, err := openActiveSegmentWith(tmpPath, wrap)
	if err != nil {
		return nil, err
	}

	for _, src := range []*Segment{a, b} {
		err := src.ScanStampedFromOffset(0, func(_ int64, e store.Entry, tx *TxStamp) error {
			if tx != nil && tx.T != "" {
				se := stampedEntry{
					ID:        e.ID,
					Op:        e.Op,
					Ts:        e.Ts.Format(time.RFC3339Nano),
					Rev:       e.Rev,
					Data:      e.Data,
					Epoch:     e.Epoch,
					ExpiresAt: e.ExpiresAt,
					Tx:        *tx,
				}
				_, err := merged.AppendStamped(se)
				return err
			}
			_, err := merged.Append(e)
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	if err := merged.Seal(); err != nil {
		return nil, err
	}
	return merged, nil
}
