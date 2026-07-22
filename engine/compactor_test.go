//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// openTestCollection creates a collection with a tiny segment size and a
// 30% dirty threshold. The background compactor goroutine is stopped
// immediately so tests can call compact() explicitly without races.
func openTestCollection(t *testing.T) *Collection {
	t.Helper()
	dir := t.TempDir()
	col, err := OpenCollection("test", dir, CollectionConfig{
		SegmentMaxSize:  512,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	// Stop the background goroutine so tests control compaction explicitly.
	// closeOnce is a sync.Once — calling it here is safe; col.Close() (called
	// from cleanup) will be a no-op for the close(closed) step but will still
	// flush the active segment and persist the index.
	col.closeOnce.Do(func() { close(col.closed) })
	// Drain any pending compactor signal produced by the goroutine startup.
	select {
	case <-col.compactC:
	default:
	}

	t.Cleanup(func() { col.Close() })
	return col
}

// ---- isDirty ----------------------------------------------------------------

func TestIsDirty_BelowThreshold(t *testing.T) {
	col := openTestCollection(t)

	// Insert two records — nothing stale yet.
	col.Insert(map[string]any{"x": 1})
	col.Insert(map[string]any{"x": 2})
	col.rotateSegment()

	col.mu.RLock()
	segs := make([]*Segment, len(col.sealed))
	copy(segs, col.sealed)
	col.mu.RUnlock()

	if col.isDirty(segs) {
		t.Error("expected isDirty=false when no stale entries")
	}
}

func TestIsDirty_AboveThreshold(t *testing.T) {
	col := openTestCollection(t)

	// 4 inserts + 4 updates in a sealed segment → 4 stale out of 8 = 50% > 30%.
	for i := 0; i < 4; i++ {
		col.Insert(map[string]any{"v": i})
	}
	col.rotateSegment()
	for id := uint64(1); id <= 4; id++ {
		col.Update(id, map[string]any{"v": 99})
	}
	col.rotateSegment()

	col.mu.RLock()
	segs := make([]*Segment, len(col.sealed))
	copy(segs, col.sealed)
	col.mu.RUnlock()

	if !col.isDirty(segs) {
		t.Error("expected isDirty=true when >30% of entries are stale")
	}
}

// ---- compact reduces segment count -----------------------------------------

func TestCompact_ReducesSegments(t *testing.T) {
	col := openTestCollection(t)

	for i := 0; i < 6; i++ {
		col.Insert(map[string]any{"v": i})
	}
	col.rotateSegment()
	for id := uint64(1); id <= 6; id++ {
		col.Update(id, map[string]any{"v": 99})
	}
	col.rotateSegment()

	col.mu.RLock()
	beforeCount := len(col.sealed)
	col.mu.RUnlock()

	if err := col.compact(false); err != nil {
		t.Fatalf("compact: %v", err)
	}

	col.mu.RLock()
	afterCount := len(col.sealed)
	col.mu.RUnlock()

	if afterCount >= beforeCount {
		t.Errorf("expected fewer sealed segments after compaction: before=%d after=%d",
			beforeCount, afterCount)
	}
}

// ---- records readable after compaction -------------------------------------

func TestCompact_RecordsReadableAfter(t *testing.T) {
	col := openTestCollection(t)

	for i := 1; i <= 4; i++ {
		col.Insert(map[string]any{"n": i})
	}
	col.rotateSegment()
	col.Update(1, map[string]any{"n": 100})
	col.Update(2, map[string]any{"n": 200})
	col.Delete(3)
	col.rotateSegment()

	if err := col.compact(false); err != nil {
		t.Fatalf("compact: %v", err)
	}

	data1, _, err := col.FindByID(1)
	if err != nil || data1["n"] != float64(100) {
		t.Errorf("id 1 after compact: got %v err %v", data1, err)
	}
	data2, _, err := col.FindByID(2)
	if err != nil || data2["n"] != float64(200) {
		t.Errorf("id 2 after compact: got %v err %v", data2, err)
	}
	if _, _, err := col.FindByID(3); err == nil {
		t.Error("expected id 3 to be absent after delete + compact")
	}
	data4, _, err := col.FindByID(4)
	if err != nil || data4["n"] != float64(4) {
		t.Errorf("id 4 after compact: got %v err %v", data4, err)
	}
}

// ---- rebalancer merges tiny segments ---------------------------------------

func TestRebalance_MergesTinySegments(t *testing.T) {
	dir := t.TempDir()

	mkSeg := func(name string, entries []store.Entry) *Segment {
		path := filepath.Join(dir, name)
		seg, _ := openActiveSegment(path)
		for _, e := range entries {
			seg.Append(e)
		}
		seg.Seal()
		info, _ := os.Stat(path)
		return openSealedSegment(path, info.Size())
	}

	a := mkSeg("a.ndjson", []store.Entry{store.NewInsert(1, map[string]any{"x": 1})})
	b := mkSeg("b.ndjson", []store.Entry{store.NewInsert(2, map[string]any{"x": 2})})

	// Both segments are tiny vs a 4 MB max — rebalancer should merge them.
	result, err := rebalance([]*Segment{a, b}, 4*1024*1024)
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 merged segment, got %d", len(result))
	}

	entries, err := result[0].ScanAll()
	if err != nil {
		t.Fatalf("ScanAll after rebalance: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries in merged segment, got %d", len(entries))
	}
}
