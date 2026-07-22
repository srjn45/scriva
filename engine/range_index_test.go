//nolint:errcheck
package engine

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/srjn45/scriva/query"
)

// scanIDs runs a filter through Scan and returns the matching ids, sorted.
func scanIDs(t *testing.T, col *Collection, f query.Filter) []uint64 {
	t.Helper()
	res, err := col.Scan(f)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids := make([]uint64, len(res))
	for i, r := range res {
		ids[i] = r.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func rangeFilter(field string, op query.Op, jsonVal string) *query.FieldFilter {
	return &query.FieldFilter{Field: field, Op: op, Value: jsonVal}
}

// TestRangeIndex_IdenticalToFullScan is the acceptance bar: for the same data,
// a range query served by a secondary index returns exactly the ids a full scan
// (no index) returns, across all four operators and many random values.
func TestRangeIndex_IdenticalToFullScan(t *testing.T) {
	indexed := openTestCollection(t)
	if err := indexed.EnsureIndex("score"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	plain := openTestCollection(t) // no index → full scan

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 400; i++ {
		v := float64(rng.Intn(200))
		indexed.Insert(map[string]any{"score": v})
		plain.Insert(map[string]any{"score": v})
	}

	for _, op := range []query.Op{query.OpGt, query.OpGte, query.OpLt, query.OpLte} {
		for _, bound := range []int{0, 1, 50, 100, 199, 200} {
			f := rangeFilter("score", op, fmt.Sprintf("%d", bound))
			got := scanIDs(t, indexed, f)
			want := scanIDs(t, plain, f)
			if !equalIDs(got, want) {
				t.Errorf("op=%s bound=%d: indexed=%v full=%v", op, bound, got, want)
			}
		}
	}
}

// TestRangeIndex_NumericNotLexical pins that a range on a numeric field orders
// numerically: gt 9 matches 10 and 100, not the lexical surprise where "10" < "9".
func TestRangeIndex_NumericNotLexical(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("n")
	ids := map[float64]uint64{}
	for _, v := range []float64{2, 9, 10, 100, 5} {
		id, _, _ := col.Insert(map[string]any{"n": v})
		ids[v] = id
	}

	got := scanIDs(t, col, rangeFilter("n", query.OpGt, "9"))
	want := []uint64{ids[10], ids[100]}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !equalIDs(got, want) {
		t.Errorf("gt 9: got %v want %v (numeric order)", got, want)
	}

	// lte 10 → 2, 5, 9, 10.
	got = scanIDs(t, col, rangeFilter("n", query.OpLte, "10"))
	if len(got) != 4 {
		t.Errorf("lte 10: got %d ids %v want 4", len(got), got)
	}
}

// TestRangeIndex_Strings covers the lexical branch of a string-typed index.
func TestRangeIndex_Strings(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("name")
	for _, s := range []string{"alice", "bob", "carol", "dave", "erin"} {
		col.Insert(map[string]any{"name": s})
	}
	// gte "carol" → carol, dave, erin.
	got := scanIDs(t, col, rangeFilter("name", query.OpGte, `"carol"`))
	if len(got) != 3 {
		t.Errorf("gte carol: got %d ids %v want 3", len(got), got)
	}
	// lt "carol" → alice, bob.
	got = scanIDs(t, col, rangeFilter("name", query.OpLt, `"carol"`))
	if len(got) != 2 {
		t.Errorf("lt carol: got %d ids %v want 2", len(got), got)
	}
}

// TestRangeIndex_SurvivesUpdateDelete verifies the ordered view is maintained
// when records move buckets or disappear.
func TestRangeIndex_SurvivesUpdateDelete(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("v")
	id1, _, _ := col.Insert(map[string]any{"v": float64(10)})
	id2, _, _ := col.Insert(map[string]any{"v": float64(20)})
	id3, _, _ := col.Insert(map[string]any{"v": float64(30)})

	// Move id1 above the others, delete id2.
	col.Update(id1, map[string]any{"v": float64(100)})
	col.Delete(id2)

	// gt 25 → id3 (30) and id1 (100); id2 gone.
	got := scanIDs(t, col, rangeFilter("v", query.OpGt, "25"))
	want := []uint64{id1, id3}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !equalIDs(got, want) {
		t.Errorf("gt 25 after update/delete: got %v want %v", got, want)
	}
	// lt 25 → nothing (id1 moved up, id2 deleted, id3 is 30).
	if got := scanIDs(t, col, rangeFilter("v", query.OpLt, "25")); len(got) != 0 {
		t.Errorf("lt 25: got %v want empty", got)
	}
}

// TestRangeIndex_MixedTypesFallBack verifies that a field mixing numbers and
// strings disables the ordered index (range serving), yet the scan path still
// returns correct, scan-identical results via full-scan fallback.
func TestRangeIndex_MixedTypesFallBack(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("x")
	col.Insert(map[string]any{"x": float64(5)})
	col.Insert(map[string]any{"x": "hello"}) // makes the field heterogeneous

	sidx := col.sidxMap["x"]
	if sidx.kind != indexMixed {
		t.Fatalf("expected indexMixed after number+string, got %v", sidx.kind)
	}
	if _, ok := sidx.LookupRange(query.OpGt, float64(1)); ok {
		t.Error("mixed index should not serve range queries")
	}

	// The scan path must still work (full-scan fallback): gt 1 matches the
	// numeric 5 (and "hello" degrades to a string comparison, same as scan).
	plain := openTestCollection(t)
	plain.Insert(map[string]any{"x": float64(5)})
	plain.Insert(map[string]any{"x": "hello"})
	got := scanIDs(t, col, rangeFilter("x", query.OpGt, "1"))
	want := scanIDs(t, plain, rangeFilter("x", query.OpGt, "1"))
	if !equalIDs(got, want) {
		t.Errorf("mixed-type fallback: indexed=%v full=%v", got, want)
	}
}

// TestRangeIndex_TypeMismatchFallsBack checks that a numeric index asked with a
// string bound (or vice versa) declines to serve, so the caller full-scans.
func TestRangeIndex_TypeMismatchFallsBack(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("n")
	col.Insert(map[string]any{"n": float64(10)})

	sidx := col.sidxMap["n"]
	if _, ok := sidx.LookupRange(query.OpGt, "5"); ok {
		t.Error("numeric index should not serve a string-typed bound")
	}
	if _, ok := sidx.LookupRange(query.OpGt, float64(5)); !ok {
		t.Error("numeric index should serve a numeric bound")
	}
}

// TestRangeIndex_EmptyIndex confirms an index with no values answers a range
// query with zero candidates rather than forcing a scan.
func TestRangeIndex_EmptyIndex(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("n")
	sidx := col.sidxMap["n"]
	ids, ok := sidx.LookupRange(query.OpGt, float64(0))
	if !ok || len(ids) != 0 {
		t.Errorf("empty index: got ids=%v ok=%v, want [] true", ids, ok)
	}
}

// TestRangeIndex_PushdownBounded proves the range lookup returns only in-range
// candidates, not the whole collection.
func TestRangeIndex_PushdownBounded(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("n")
	for i := 0; i < 1000; i++ {
		col.Insert(map[string]any{"n": float64(i)})
	}
	sidx := col.sidxMap["n"]
	ids, ok := sidx.LookupRange(query.OpGte, float64(995)) // 995..999 → 5 candidates
	if !ok {
		t.Fatal("expected index to serve the range")
	}
	if len(ids) != 5 {
		t.Errorf("gte 995 over 1000 rows: %d candidates, want 5 (push-down not bounding)", len(ids))
	}
}

// TestRangeIndex_SurvivesReopen checks the ordered view is restored from disk so
// range queries stay accelerated and correct after a restart.
func TestRangeIndex_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.SegmentMaxSize = 512

	col, err := OpenCollection("nums", dir, cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	col.EnsureIndex("score")
	for i := 0; i < 50; i++ {
		col.Insert(map[string]any{"score": float64(i)})
	}
	col.Close()

	reopened, err := OpenCollection("nums", dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	sidx := reopened.sidxMap["score"]
	if sidx == nil {
		t.Fatal("index not restored on reopen")
	}
	if sidx.kind != indexNumeric {
		t.Errorf("kind after reopen: got %v want numeric", sidx.kind)
	}
	// gte 45 → 45..49 = 5 records.
	got := scanIDs(t, reopened, rangeFilter("score", query.OpGte, "45"))
	if len(got) != 5 {
		t.Errorf("gte 45 after reopen: got %d ids %v want 5", len(got), got)
	}
	ids, ok := sidx.LookupRange(query.OpGte, float64(45))
	if !ok || len(ids) != 5 {
		t.Errorf("index not serving range after reopen: ids=%v ok=%v", ids, ok)
	}
}

// TestRangeIndex_SurvivesCompaction verifies the ordered view is rebuilt after a
// compaction pass (which replays segments) and still answers ranges correctly.
func TestRangeIndex_SurvivesCompaction(t *testing.T) {
	col := newCompactableCollection(t)
	col.EnsureIndex("v")
	for i := 0; i < 8; i++ {
		col.Insert(map[string]any{"v": float64(i)})
	}
	col.rotateSegment()
	// Push the first four out of the >3 range and delete one.
	for id := uint64(1); id <= 4; id++ {
		col.Update(id, map[string]any{"v": float64(100 + id)})
	}
	col.rotateSegment()
	if err := col.compact(false); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// After compaction: ids 5..8 have v=4..7; ids 1..4 have v=101..104.
	got := scanIDs(t, col, rangeFilter("v", query.OpGte, "100"))
	if len(got) != 4 {
		t.Errorf("gte 100 after compaction: got %d ids %v want 4", len(got), got)
	}
	// Values below 100 must still be reachable too (ordered view intact).
	if got := scanIDs(t, col, rangeFilter("v", query.OpLt, "100")); len(got) != 4 {
		t.Errorf("lt 100 after compaction: got %d ids %v want 4", len(got), got)
	}
}

// newCompactableCollection builds a collection with a stopped background
// compactor so tests can drive compaction deterministically.
func newCompactableCollection(t *testing.T) *Collection {
	t.Helper()
	col := &Collection{
		name:     "test",
		dir:      t.TempDir(),
		cfg:      CollectionConfig{SegmentMaxSize: 512, CompactInterval: defaultConfig().CompactInterval, CompactDirtyPct: 0.30},
		index:    newIndex(),
		sidxMap:  make(map[string]*SecondaryIndex),
		watchers: make(map[uint64]*watcher),
		compactC: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	if err := col.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	col.closeOnce.Do(func() { close(col.closed) })
	select {
	case <-col.compactC:
	default:
	}
	t.Cleanup(func() { col.Close() })
	return col
}
