//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/query"
)

// countViaScan is the ground truth Count must always agree with: the number of
// records a full Scan of the same filter returns.
func countViaScan(t *testing.T, col *Collection, f query.Filter) uint64 {
	t.Helper()
	res, err := col.Scan(f)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return uint64(len(res))
}

// assertCount asserts Count(f) succeeds and equals len(Scan(f)).
func assertCount(t *testing.T, col *Collection, f query.Filter, label string) {
	t.Helper()
	got, err := col.Count(f)
	if err != nil {
		t.Fatalf("Count(%s): %v", label, err)
	}
	if want := countViaScan(t, col, f); got != want {
		t.Errorf("Count(%s) = %d, want len(Scan) = %d", label, got, want)
	}
}

// TestCountMatchesScan checks that Count agrees with len(Scan) across the three
// evaluation paths: match-all/nil (primary index), an eq filter on an indexed
// field (secondary index), and filters that must fall back to a streaming scan
// (non-indexed field, and a compound filter).
func TestCountMatchesScan(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("status")

	// A spread of records over a couple of statuses and priorities. The small
	// SegmentMaxSize of the test collection means these land across several
	// segments, exercising the multi-segment streaming path too.
	statuses := []string{"active", "archived", "active", "pending", "active", "archived"}
	for i, s := range statuses {
		col.Insert(map[string]any{
			"status":   s,
			"priority": float64(i % 3),
		})
	}

	// Match-all and nil are the O(1) primary-index path.
	assertCount(t, col, nil, "nil")
	assertCount(t, col, query.MatchAll, "MatchAll")

	// eq on the indexed "status" field is the secondary-index path.
	assertCount(t, col, &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"active"`}, "status=active")
	assertCount(t, col, &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"archived"`}, "status=archived")
	assertCount(t, col, &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"missing"`}, "status=missing")

	// eq on the non-indexed "priority" field forces the streaming fallback.
	assertCount(t, col, &query.FieldFilter{Field: "priority", Op: query.OpEq, Value: `0`}, "priority=0")

	// A compound filter is not a single FieldFilter, so it also streams.
	compound := &query.AndFilter{Filters: []query.Filter{
		&query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"active"`},
		&query.FieldFilter{Field: "priority", Op: query.OpEq, Value: `0`},
	}}
	assertCount(t, col, compound, "status=active AND priority=0")
}

// TestCountAfterMutations verifies Count stays scan-identical after updates and
// deletes shift records between index buckets and remove them entirely.
func TestCountAfterMutations(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("status")

	ids := make([]uint64, 0, 5)
	for i := 0; i < 5; i++ {
		id, _, err := col.Insert(map[string]any{"status": "active"})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids = append(ids, id)
	}

	// Move two records to a different status and delete one entirely.
	col.Update(ids[0], map[string]any{"status": "archived"})
	col.Update(ids[1], map[string]any{"status": "archived"})
	col.Delete(ids[2])

	active := &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"active"`}
	archived := &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"archived"`}

	assertCount(t, col, nil, "nil after mutations")
	assertCount(t, col, active, "active after mutations")
	assertCount(t, col, archived, "archived after mutations")

	// Sanity-check the absolute numbers, not just agreement with Scan.
	if n, _ := col.Count(nil); n != 4 {
		t.Errorf("total after one delete = %d, want 4", n)
	}
	if n, _ := col.Count(active); n != 2 {
		t.Errorf("active after two moves + one delete = %d, want 2", n)
	}
	if n, _ := col.Count(archived); n != 2 {
		t.Errorf("archived after two moves = %d, want 2", n)
	}
}

// TestExists checks the true/false behaviour of Exists: present keys, absent
// keys, a deleted key, and a collection that has never taken a keyed write.
func TestExists(t *testing.T) {
	col := openTestCollection(t)

	// No keyed write yet: there is no _key index, so every key is absent.
	if ok, err := col.Exists("sess-1"); err != nil || ok {
		t.Errorf("Exists on empty collection = (%v, %v), want (false, nil)", ok, err)
	}

	if _, _, err := col.InsertWithKey("sess-1", map[string]any{"n": float64(1)}); err != nil {
		t.Fatalf("InsertWithKey: %v", err)
	}
	if _, _, err := col.InsertWithKey("sess-2", map[string]any{"n": float64(2)}); err != nil {
		t.Fatalf("InsertWithKey: %v", err)
	}

	if ok, err := col.Exists("sess-1"); err != nil || !ok {
		t.Errorf("Exists(sess-1) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := col.Exists("nope"); err != nil || ok {
		t.Errorf("Exists(nope) = (%v, %v), want (false, nil)", ok, err)
	}

	// A deleted key must report absent.
	if err := col.DeleteByKey("sess-2"); err != nil {
		t.Fatalf("DeleteByKey: %v", err)
	}
	if ok, err := col.Exists("sess-2"); err != nil || ok {
		t.Errorf("Exists(sess-2) after delete = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestExistsNoSegmentReads proves Exists performs no segment I/O: after removing
// every segment file from disk, Exists still answers correctly because it only
// consults the in-memory _key index. For contrast, GetByKey — which does read a
// segment — fails once the files are gone.
func TestExistsNoSegmentReads(t *testing.T) {
	col := openTestCollection(t)

	for i := 0; i < 50; i++ {
		key := "sess-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if _, _, err := col.InsertWithKey(key, map[string]any{"n": float64(i)}); err != nil {
			t.Fatalf("InsertWithKey: %v", err)
		}
	}

	// Delete every segment file. IndexLookup works off the in-memory secondary
	// index, so a read that touches disk would now fail — which is exactly what we
	// use to detect any accidental segment read.
	segs, err := filepath.Glob(filepath.Join(col.dir, "seg_*.ndjson"))
	if err != nil {
		t.Fatalf("glob segments: %v", err)
	}
	if len(segs) == 0 {
		t.Fatal("expected at least one segment file")
	}
	for _, p := range segs {
		if err := os.Remove(p); err != nil {
			t.Fatalf("remove %q: %v", p, err)
		}
	}

	// Exists still answers from memory — no segment read.
	if ok, err := col.Exists("sess-a0"); err != nil || !ok {
		t.Errorf("Exists(sess-a0) after removing segments = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := col.Exists("sess-zzz"); err != nil || ok {
		t.Errorf("Exists(sess-zzz) after removing segments = (%v, %v), want (false, nil)", ok, err)
	}

	// Count(nil) is likewise index-only and must still work.
	if n, err := col.Count(nil); err != nil || n != 50 {
		t.Errorf("Count(nil) after removing segments = (%d, %v), want (50, nil)", n, err)
	}

	// Contrast: GetByKey does read a segment, so it now fails — confirming the
	// files really are gone and Exists genuinely avoided the read above.
	if _, err := col.GetByKey("sess-a0"); err == nil {
		t.Error("GetByKey after removing segments unexpectedly succeeded — the no-read proof is moot")
	}
}
