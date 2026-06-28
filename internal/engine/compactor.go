package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/srjn45/filedbv2/internal/store"
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
			_ = c.compact()
		case <-c.compactC:
			if c.isClosed() {
				return
			}
			_ = c.compact()
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

// compact merges and deduplicates sealed segments.
// It operates only on sealed (immutable) segments so writes are never blocked
// except during the brief atomic swap at the end.
func (c *Collection) compact() error {
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

	// --- Step 2: Check dirty ratio ---
	if !c.isDirty(toCompact) {
		return nil
	}

	// --- Step 3: Replay all entries, keep latest per id ---
	resolved, err := resolveEntries(toCompact)
	if err != nil {
		return fmt.Errorf("compactor: resolve: %w", err)
	}

	// --- Step 4: Write resolved entries into temp segment files (not yet renamed) ---
	tempSegs, err := c.writeCompacted(resolved)
	if err != nil {
		return fmt.Errorf("compactor: write compacted: %w", err)
	}
	// Nothing survived (all deletes) — still need to swap under lock.


	// --- Step 5: Acquire write lock, swap segments, rebuild index ---
	c.mu.Lock()

	// Remove old sealed segments from disk, then rename temp files into their
	// permanent positions. Doing both under the lock avoids the race where the
	// rename (step 4) overwrites an original file that the removal (step 5)
	// then deletes, destroying the freshly-compacted data.
	for _, s := range toCompact {
		_ = os.Remove(s.Path())
	}

	var newSegs []*Segment
	for i, seg := range tempSegs {
		finalPath := c.segmentPath(uint64(i + 1))
		if err := os.Rename(seg.Path(), finalPath); err != nil {
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

	// Persist updated primary index.
	_ = c.index.Persist(filepath.Join(c.dir, "index.json"))

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

	if c.cfg.OnCompaction != nil {
		c.cfg.OnCompaction(c.name, time.Since(start))
	}

	return nil
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
// the latest surviving entry per id (deletes are dropped).
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

	var out []store.Entry
	for _, e := range latest {
		if e.Op != store.OpDelete {
			out = append(out, e)
		}
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
		return openActiveSegment(path)
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
