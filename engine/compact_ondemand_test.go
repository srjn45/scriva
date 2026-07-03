//nolint:errcheck
package engine

import (
	"testing"
	"time"
)

// openForceTestCollection builds a collection whose segments are large enough
// that several sealed segments' worth of records collapse into a single segment
// once merged. Unlike openTestCollection it leaves the background compactor
// running (CompactNow refuses to run on a closed collection), but with a 24h
// interval and clean data every background pass is a harmless no-op that
// serializes with the on-demand pass via compactMu.
func openForceTestCollection(t *testing.T) *Collection {
	t.Helper()
	dir := t.TempDir()
	col, err := OpenCollection("force", dir, CollectionConfig{
		SegmentMaxSize:  4096,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	t.Cleanup(func() { col.Close() })
	return col
}

// TestCompactNow_ForcesBelowDirtyThreshold verifies that CompactNow runs a full
// pass even when nothing is stale (so the dirty-ratio gate would skip it),
// merging several sealed segments into fewer while keeping every record.
func TestCompactNow_ForcesBelowDirtyThreshold(t *testing.T) {
	col := openForceTestCollection(t)

	// Three sealed segments, three fresh records each, zero stale entries.
	for seg := 0; seg < 3; seg++ {
		for i := 0; i < 3; i++ {
			col.Insert(map[string]any{"seg": seg, "i": i})
		}
		col.rotateSegment()
	}

	col.mu.RLock()
	sealed := make([]*Segment, len(col.sealed))
	copy(sealed, col.sealed)
	beforeCount := len(col.sealed)
	col.mu.RUnlock()

	if col.isDirty(sealed) {
		t.Fatal("precondition: expected isDirty=false with no stale entries")
	}

	// The automatic path is gated on dirtiness, so it must be a no-op here.
	if err := col.compact(false); err != nil {
		t.Fatalf("compact(false): %v", err)
	}
	col.mu.RLock()
	gatedCount := len(col.sealed)
	col.mu.RUnlock()
	if gatedCount != beforeCount {
		t.Fatalf("compact(false) should not merge clean segments: before=%d after=%d",
			beforeCount, gatedCount)
	}

	// CompactNow ignores the gate and merges the segments.
	if err := col.CompactNow(); err != nil {
		t.Fatalf("CompactNow: %v", err)
	}
	col.mu.RLock()
	afterCount := len(col.sealed)
	col.mu.RUnlock()

	if afterCount >= beforeCount {
		t.Errorf("CompactNow should reduce sealed segments: before=%d after=%d",
			beforeCount, afterCount)
	}

	// Every record must survive the forced pass.
	res, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 9 {
		t.Errorf("expected 9 live records after CompactNow, got %d", len(res))
	}
}

// TestCompactNow_ClosedReturnsError verifies CompactNow refuses to run on a
// closed collection rather than racing Close().
func TestCompactNow_ClosedReturnsError(t *testing.T) {
	col := openForceTestCollection(t)
	col.Insert(map[string]any{"x": 1})
	col.rotateSegment()

	if err := col.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := col.CompactNow(); err == nil {
		t.Error("expected error compacting a closed collection")
	}
}
