//nolint:errcheck
package engine

import (
	"context"
	"testing"

	"github.com/srjn45/filedbv2/query"
)

// TestScanStream_StatsFullScan pins the acceptance bar for the slow-query log:
// an unindexed filter reports a full segment sweep — IndexUsed is false and more
// live records are examined (RowsScanned) than are emitted (RowsReturned).
func TestScanStream_StatsFullScan(t *testing.T) {
	col := openTestCollection(t) // no secondary index

	// 20 records; 4 of them ("name" == "alice") match the filter below.
	for i := 0; i < 20; i++ {
		name := "bob"
		if i%5 == 0 {
			name = "alice"
		}
		col.Insert(map[string]any{"name": name})
	}

	f := &query.FieldFilter{Field: "name", Op: query.OpEq, Value: `"alice"`}
	emitted := 0
	stats, err := col.ScanStream(context.Background(), ScanOptions{Filter: f}, func(ScanResult) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}

	if stats.IndexUsed {
		t.Errorf("IndexUsed = true, want false for an unindexed filter")
	}
	if stats.RowsReturned != emitted || emitted != 4 {
		t.Errorf("RowsReturned = %d (emitted %d), want 4", stats.RowsReturned, emitted)
	}
	if stats.RowsScanned != 20 {
		t.Errorf("RowsScanned = %d, want 20 (every live record examined)", stats.RowsScanned)
	}
	if stats.RowsScanned <= stats.RowsReturned {
		t.Errorf("RowsScanned (%d) must exceed RowsReturned (%d) on a full scan",
			stats.RowsScanned, stats.RowsReturned)
	}
}

// TestScanStream_StatsIndexedLookup asserts that an equality lookup served by a
// secondary index reports IndexUsed=true and examines only the indexed
// candidates rather than the whole collection.
func TestScanStream_StatsIndexedLookup(t *testing.T) {
	col := openIndexedCollection(t) // index on "name"

	for i := 0; i < 20; i++ {
		name := "bob"
		if i%5 == 0 {
			name = "alice"
		}
		col.Insert(map[string]any{"name": name})
	}

	f := &query.FieldFilter{Field: "name", Op: query.OpEq, Value: `"alice"`}
	stats, err := col.ScanStream(context.Background(), ScanOptions{Filter: f}, func(ScanResult) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}

	if !stats.IndexUsed {
		t.Errorf("IndexUsed = false, want true for an indexed equality lookup")
	}
	if stats.RowsReturned != 4 {
		t.Errorf("RowsReturned = %d, want 4", stats.RowsReturned)
	}
	// The index narrows candidates to exactly the matching rows, so only those
	// are examined — far fewer than the 20 live records a full scan would read.
	if stats.RowsScanned != 4 {
		t.Errorf("RowsScanned = %d, want 4 (only indexed candidates examined)", stats.RowsScanned)
	}
}
