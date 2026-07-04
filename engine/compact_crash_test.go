//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// openCrashTestCollection opens a collection with small segments and a
// background compactor that never fires on its own (rotations signal it, so a
// long interval alone is not enough — the dirty threshold is unreachable too),
// so tests control every compaction-related mutation themselves via the forced
// path.
func openCrashTestCollection(t *testing.T, dir string) *Collection {
	t.Helper()
	col, err := OpenCollection("crash", dir, CollectionConfig{
		SegmentMaxSize:  2048,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 2, // ratio is ≤ 1, so unforced passes never run
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

// seedSealedGarbage inserts n records, updates each one (so the sealed
// segments contain stale versions), and rotates twice so there are at least
// two sealed segments to compact. It returns the ids and the value each
// record must resolve to.
func seedSealedGarbage(t *testing.T, col *Collection, n int) map[uint64]string {
	t.Helper()
	want := make(map[uint64]string, n)
	var ids []uint64
	for i := 0; i < n; i++ {
		id, _, err := col.Insert(map[string]any{"v": "old"})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids = append(ids, id)
	}
	if err := col.rotateSegment(); err != nil {
		t.Fatalf("rotateSegment: %v", err)
	}
	for _, id := range ids {
		if _, err := col.Update(id, map[string]any{"v": "new"}); err != nil {
			t.Fatalf("Update: %v", err)
		}
		want[id] = "new"
	}
	if err := col.rotateSegment(); err != nil {
		t.Fatalf("rotateSegment: %v", err)
	}
	return want
}

// verifyAll reopens nothing — it checks every seeded record resolves to its
// latest value on the given collection.
func verifyAll(t *testing.T, col *Collection, want map[uint64]string) {
	t.Helper()
	for id, v := range want {
		rec, err := col.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got := rec.Data["v"]; got != v {
			t.Fatalf("Get(%d): got %v, want %q", id, got, v)
		}
	}
}

// TestOpenRebuildsDanglingIndex is the regression test for the incident in
// issue #68: a checksum-valid index.json whose entries reference a segment
// file that no longer exists must be rebuilt from the segments at open, not
// trusted.
func TestOpenRebuildsDanglingIndex(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	want := seedSealedGarbage(t, col, 6)
	if err := col.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Rewrite index.json so every entry points at a segment that does not
	// exist — with a checksum that still validates, exactly what a Close()
	// racing a compaction swap used to persist.
	dangling := newIndex()
	col.index.mu.RLock()
	for id, e := range col.index.entries {
		e.SegmentPath = filepath.Join(col.dir, "seg_999999.ndjson")
		dangling.entries[id] = e
	}
	col.index.mu.RUnlock()
	if err := dangling.Persist(filepath.Join(col.dir, "index.json")); err != nil {
		t.Fatalf("Persist dangling index: %v", err)
	}

	re := openCrashTestCollection(t, dir)
	defer re.Close()
	if re.index.Len() != len(want) {
		t.Fatalf("reopened index has %d entries, want %d", re.index.Len(), len(want))
	}
	verifyAll(t, re, want)
}

// TestOpenRebuildsIndexWithStaleOffsets covers the subtler dangling case: the
// referenced segment file exists, but an entry's offset points at or past its
// end (the file was atomically replaced by a shorter compacted one).
func TestOpenRebuildsIndexWithStaleOffsets(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	want := seedSealedGarbage(t, col, 6)
	if err := col.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Point one entry far past the end of a segment that does exist.
	stale := newIndex()
	col.index.mu.RLock()
	first := true
	for id, e := range col.index.entries {
		if first {
			e.Offset = 1 << 40
			first = false
		}
		stale.entries[id] = e
	}
	col.index.mu.RUnlock()
	if err := stale.Persist(filepath.Join(col.dir, "index.json")); err != nil {
		t.Fatalf("Persist stale index: %v", err)
	}

	re := openCrashTestCollection(t, dir)
	defer re.Close()
	verifyAll(t, re, want)
}

// TestCloseWaitsForInflightCompaction verifies the serialization contract:
// Close must not persist the final index while a compaction pass (which holds
// compactMu for its whole segment swap) is still running.
func TestCloseWaitsForInflightCompaction(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	seedSealedGarbage(t, col, 3)

	col.compactMu.Lock() // stand in for an in-flight pass
	done := make(chan error, 1)
	go func() { done <- col.Close() }()

	select {
	case <-done:
		t.Fatal("Close returned while the compaction lock was held")
	case <-time.After(50 * time.Millisecond):
	}

	col.compactMu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the compaction lock was released")
	}
}

// TestCompactAfterCloseIsNoop verifies that a pass which acquires compactMu
// only after Close has finished aborts instead of mutating the segment layout
// (and re-corrupting the index Close just persisted).
func TestCompactAfterCloseIsNoop(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	want := seedSealedGarbage(t, col, 3)
	if err := col.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before, _ := filepath.Glob(filepath.Join(col.dir, "seg_*.ndjson"))
	if err := col.compact(true); err != nil {
		t.Fatalf("compact after close: %v", err)
	}
	after, _ := filepath.Glob(filepath.Join(col.dir, "seg_*.ndjson"))
	if len(before) != len(after) {
		t.Fatalf("compact after close mutated segments: %d → %d files", len(before), len(after))
	}

	re := openCrashTestCollection(t, dir)
	defer re.Close()
	verifyAll(t, re, want)
}

// interruptedSwap fabricates the on-disk state of a compaction pass killed at
// its crash point: the pre-swap index persisted (the same bytes the racy
// shutdown in #68 left behind), temp segments fully written, and the swap
// manifest recorded. Close runs first so the fabricated state cannot be
// disturbed. It returns the manifest so callers can advance the swap to later
// crash points.
func interruptedSwap(t *testing.T, col *Collection) compactManifest {
	t.Helper()

	if err := col.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	toCompact := make([]*Segment, len(col.sealed))
	copy(toCompact, col.sealed)
	if len(toCompact) < 2 {
		t.Fatalf("precondition: want ≥2 sealed segments, got %d", len(toCompact))
	}

	resolved, err := resolveEntries(toCompact)
	if err != nil {
		t.Fatalf("resolveEntries: %v", err)
	}
	tempSegs, err := col.writeCompacted(resolved)
	if err != nil {
		t.Fatalf("writeCompacted: %v", err)
	}

	renames := make(map[string]string, len(tempSegs))
	finals := make(map[string]struct{}, len(tempSegs))
	for i, seg := range tempSegs {
		final := col.segmentPath(uint64(i + 1))
		renames[seg.Path()] = final
		finals[final] = struct{}{}
	}
	var removals []string
	for _, s := range toCompact {
		if _, reused := finals[s.Path()]; !reused {
			removals = append(removals, s.Path())
		}
	}
	m := compactManifest{Renames: renames, Removals: removals}
	if err := writeCompactManifest(col.dir, m); err != nil {
		t.Fatalf("writeCompactManifest: %v", err)
	}
	return m
}

// TestRecoverCompactionRollsForward exercises open-time recovery at each crash
// point of the swap window.
func TestRecoverCompactionRollsForward(t *testing.T) {
	advance := func(t *testing.T, dir string, m compactManifest, renamesToApply int, applyRemovals bool) {
		t.Helper()
		applied := 0
		for src, dst := range m.Renames {
			if applied >= renamesToApply {
				break
			}
			if err := os.Rename(src, dst); err != nil {
				t.Fatalf("advance rename: %v", err)
			}
			applied++
		}
		if applyRemovals {
			for _, p := range m.Removals {
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					t.Fatalf("advance remove: %v", err)
				}
			}
		}
	}

	cases := []struct {
		name    string
		advance func(t *testing.T, dir string, m compactManifest)
	}{
		{"manifest written, nothing applied", func(t *testing.T, dir string, m compactManifest) {}},
		{"partial renames applied", func(t *testing.T, dir string, m compactManifest) {
			advance(t, dir, m, 1, false)
		}},
		{"renames and removals applied, manifest not cleared", func(t *testing.T, dir string, m compactManifest) {
			advance(t, dir, m, len(m.Renames), true)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			col := openCrashTestCollection(t, dir)
			want := seedSealedGarbage(t, col, 8)
			m := interruptedSwap(t, col)
			tc.advance(t, dir, m)

			re := openCrashTestCollection(t, dir)
			defer re.Close()
			verifyAll(t, re, want)

			if _, err := os.Stat(compactManifestPath(re.dir)); !os.IsNotExist(err) {
				t.Fatalf("manifest still present after recovery (stat err: %v)", err)
			}
			temps, _ := filepath.Glob(filepath.Join(re.dir, ".compact_*"))
			if len(temps) != 0 {
				t.Fatalf("temp files still present after recovery: %v", temps)
			}
		})
	}
}

// TestOpenDiscardsTempsWithoutManifest covers a crash while the pass was still
// writing its temp files: no manifest exists, the old segments remain
// authoritative, and the temps must be discarded.
func TestOpenDiscardsTempsWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	want := seedSealedGarbage(t, col, 6)
	if err := col.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	toCompact := make([]*Segment, len(col.sealed))
	copy(toCompact, col.sealed)
	resolved, err := resolveEntries(toCompact)
	if err != nil {
		t.Fatalf("resolveEntries: %v", err)
	}
	if _, err := col.writeCompacted(resolved); err != nil {
		t.Fatalf("writeCompacted: %v", err)
	}

	re := openCrashTestCollection(t, dir)
	defer re.Close()
	verifyAll(t, re, want)
	temps, _ := filepath.Glob(filepath.Join(re.dir, ".compact_*"))
	if len(temps) != 0 {
		t.Fatalf("temp files still present after open: %v", temps)
	}
}

// TestCompactionRestartCycle stress-loops write → compact → close → reopen to
// catch any layout/index inconsistency the deterministic crash-point tests
// might miss.
func TestCompactionRestartCycle(t *testing.T) {
	dir := t.TempDir()
	want := make(map[uint64]string)
	for cycle := 0; cycle < 5; cycle++ {
		col := openCrashTestCollection(t, dir)
		verifyAll(t, col, want)
		for i := 0; i < 4; i++ {
			id, _, err := col.Insert(map[string]any{"v": "old"})
			if err != nil {
				t.Fatalf("cycle %d Insert: %v", cycle, err)
			}
			if _, err := col.Update(id, map[string]any{"v": "new"}); err != nil {
				t.Fatalf("cycle %d Update: %v", cycle, err)
			}
			want[id] = "new"
		}
		if err := col.rotateSegment(); err != nil {
			t.Fatalf("cycle %d rotate: %v", cycle, err)
		}
		if err := col.CompactNow(); err != nil {
			t.Fatalf("cycle %d CompactNow: %v", cycle, err)
		}
		if err := col.Close(); err != nil {
			t.Fatalf("cycle %d Close: %v", cycle, err)
		}
	}
	col := openCrashTestCollection(t, dir)
	defer col.Close()
	verifyAll(t, col, want)
}
