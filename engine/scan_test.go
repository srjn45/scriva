//nolint:errcheck
package engine

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"testing"

	"github.com/srjn45/scriva/query"
)

// collectStream runs ScanStream with the given options and returns the emitted
// ids in order.
func collectStream(t *testing.T, col *Collection, opts ScanOptions) []uint64 {
	t.Helper()
	var ids []uint64
	_, err := col.ScanStream(context.Background(), opts, func(r ScanResult) error {
		ids = append(ids, r.ID)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}
	return ids
}

// countingFilter wraps a filter and counts how many records it examines.
type countingFilter struct {
	n     *int
	inner query.Filter
}

func (c countingFilter) Match(r map[string]any) bool {
	*c.n++
	return c.inner.Match(r)
}

func TestScanStream_UnorderedLimitOffset(t *testing.T) {
	col := openTestCollection(t)
	for i := 1; i <= 10; i++ {
		col.Insert(map[string]any{"n": float64(i)})
	}

	// Natural order is insertion (ascending id). offset 2, limit 3 → ids 3,4,5.
	got := collectStream(t, col, ScanOptions{Offset: 2, Limit: 3})
	want := []uint64{3, 4, 5}
	if !equalIDs(got, want) {
		t.Errorf("offset/limit page: got %v want %v", got, want)
	}

	// Limit alone.
	got = collectStream(t, col, ScanOptions{Limit: 4})
	if !equalIDs(got, []uint64{1, 2, 3, 4}) {
		t.Errorf("limit page: got %v", got)
	}

	// Offset past the end → empty.
	got = collectStream(t, col, ScanOptions{Offset: 100})
	if len(got) != 0 {
		t.Errorf("offset beyond end: got %v want empty", got)
	}
}

// TestScanStream_LimitBoundsExamination proves push-down: a limited unordered
// query examines only ~Offset+Limit records, not the whole collection.
func TestScanStream_LimitBoundsExamination(t *testing.T) {
	col := openTestCollection(t)
	const total = 300
	for i := 1; i <= total; i++ {
		col.Insert(map[string]any{"n": float64(i)})
	}

	examined := 0
	f := countingFilter{n: &examined, inner: query.MatchAll}
	var emitted int
	_, err := col.ScanStream(context.Background(), ScanOptions{Filter: f, Limit: 5}, func(r ScanResult) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}
	if emitted != 5 {
		t.Errorf("emitted %d want 5", emitted)
	}
	// With every record matching, exactly 5 should have been examined — and in
	// any case far fewer than the whole collection.
	if examined > 20 {
		t.Errorf("examined %d records for limit=5 over %d — push-down not bounding reads", examined, total)
	}
}

func TestScanStream_OrderByAscendingDescending(t *testing.T) {
	col := openTestCollection(t)
	scores := []float64{30, 10, 50, 20, 40}
	for _, s := range scores {
		col.Insert(map[string]any{"score": s})
	}

	asc := collectScores(t, col, ScanOptions{OrderBy: "score"})
	if !sortedFloatsEqual(asc, []float64{10, 20, 30, 40, 50}) {
		t.Errorf("ascending: got %v", asc)
	}

	desc := collectScores(t, col, ScanOptions{OrderBy: "score", Descending: true})
	if !sortedFloatsEqual(desc, []float64{50, 40, 30, 20, 10}) {
		t.Errorf("descending: got %v", desc)
	}
}

// TestScanStream_OrderByNumericNotLexical pins the acceptance bar: ordering by a
// numeric field sorts 2 before 10 (numeric), not the lexical "10" < "2" that a
// naive string sort would produce — and descending reverses it.
func TestScanStream_OrderByNumericNotLexical(t *testing.T) {
	col := openTestCollection(t)
	for _, n := range []float64{10, 2, 100, 9, 1} {
		col.Insert(map[string]any{"score": n})
	}
	asc := collectScores(t, col, ScanOptions{OrderBy: "score"})
	if !sortedFloatsEqual(asc, []float64{1, 2, 9, 10, 100}) {
		t.Errorf("ascending numeric: got %v want [1 2 9 10 100]", asc)
	}
	desc := collectScores(t, col, ScanOptions{OrderBy: "score", Descending: true})
	if !sortedFloatsEqual(desc, []float64{100, 10, 9, 2, 1}) {
		t.Errorf("descending numeric: got %v want [100 10 9 2 1]", desc)
	}
}

// TestScanStream_OrderByStrings exercises the lexical branch of the shared
// comparison: string fields sort in byte order, ascending and descending.
func TestScanStream_OrderByStrings(t *testing.T) {
	col := openTestCollection(t)
	for _, name := range []string{"charlie", "alice", "bob"} {
		col.Insert(map[string]any{"name": name})
	}
	collect := func(opts ScanOptions) []string {
		t.Helper()
		var out []string
		if _, err := col.ScanStream(context.Background(), opts, func(r ScanResult) error {
			out = append(out, r.Data["name"].(string))
			return nil
		}); err != nil {
			t.Fatalf("ScanStream: %v", err)
		}
		return out
	}
	asc := collect(ScanOptions{OrderBy: "name"})
	if len(asc) != 3 || asc[0] != "alice" || asc[1] != "bob" || asc[2] != "charlie" {
		t.Errorf("ascending strings: got %v want [alice bob charlie]", asc)
	}
	desc := collect(ScanOptions{OrderBy: "name", Descending: true})
	if len(desc) != 3 || desc[0] != "charlie" || desc[1] != "bob" || desc[2] != "alice" {
		t.Errorf("descending strings: got %v want [charlie bob alice]", desc)
	}
}

// TestScanStream_TopKMatchesFullSort checks that a bounded top-K query returns
// exactly the same page a full sort would.
func TestScanStream_TopKMatchesFullSort(t *testing.T) {
	col := openTestCollection(t)
	rng := rand.New(rand.NewSource(42))
	const n = 80
	all := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		s := float64(rng.Intn(1000))
		all = append(all, s)
		col.Insert(map[string]any{"score": s})
	}

	for _, desc := range []bool{false, true} {
		full := append([]float64(nil), all...)
		sort.Slice(full, func(i, j int) bool {
			if desc {
				return full[i] > full[j]
			}
			return full[i] < full[j]
		})
		wantPage := full[10:25] // offset 10, limit 15

		got := collectScores(t, col, ScanOptions{OrderBy: "score", Descending: desc, Offset: 10, Limit: 15})
		if len(got) != len(wantPage) {
			t.Fatalf("desc=%v: page len %d want %d", desc, len(got), len(wantPage))
		}
		for i := range got {
			if got[i] != wantPage[i] {
				t.Errorf("desc=%v: page[%d]=%v want %v (full=%v got=%v)", desc, i, got[i], wantPage[i], wantPage, got)
				break
			}
		}
	}
}

// TestScanStream_TieBreakById ensures equal sort keys are ordered by id so
// pagination is deterministic and top-K agrees with a full sort.
func TestScanStream_TieBreakById(t *testing.T) {
	col := openTestCollection(t)
	// All the same score → order must fall back to id ascending.
	for i := 0; i < 12; i++ {
		col.Insert(map[string]any{"score": float64(7)})
	}
	got := collectStream(t, col, ScanOptions{OrderBy: "score", Limit: 4})
	if !equalIDs(got, []uint64{1, 2, 3, 4}) {
		t.Errorf("tie-break by id: got %v want [1 2 3 4]", got)
	}
}

func TestScanStream_EqIndexWithLimit(t *testing.T) {
	col := openIndexedCollection(t) // index on "name"
	for i := 0; i < 6; i++ {
		col.Insert(map[string]any{"name": "alice", "k": float64(i)})
	}
	col.Insert(map[string]any{"name": "bob"})

	got := collectStream(t, col, ScanOptions{
		Filter: &query.FieldFilter{Field: "name", Op: query.OpEq, Value: `"alice"`},
		Limit:  3,
	})
	if len(got) != 3 {
		t.Errorf("eq-index limit: got %d ids (%v) want 3", len(got), got)
	}
}

func TestScanStream_ContextCancelBefore(t *testing.T) {
	col := openTestCollection(t)
	for i := 0; i < 20; i++ {
		col.Insert(map[string]any{"n": float64(i)})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel up front

	_, err := col.ScanStream(ctx, ScanOptions{}, func(r ScanResult) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestScanStream_ContextCancelMidStream(t *testing.T) {
	col := openTestCollection(t)
	for i := 0; i < 200; i++ {
		col.Insert(map[string]any{"n": float64(i)})
	}
	ctx, cancel := context.WithCancel(context.Background())

	seen := 0
	_, err := col.ScanStream(ctx, ScanOptions{}, func(r ScanResult) error {
		seen++
		if seen == 5 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled after cancel, got %v", err)
	}
	if seen >= 200 {
		t.Errorf("scan did not stop after cancel: saw %d records", seen)
	}
}

// TestScanStream_SurvivesUpdatesAndDeletes verifies the index-liveness check:
// stale (overwritten) versions and tombstones are never emitted.
func TestScanStream_SurvivesUpdatesAndDeletes(t *testing.T) {
	col := openTestCollection(t)
	id1, _, _ := col.Insert(map[string]any{"v": float64(1)})
	id2, _, _ := col.Insert(map[string]any{"v": float64(2)})
	col.Insert(map[string]any{"v": float64(3)})

	col.Update(id1, map[string]any{"v": float64(100)}) // supersede first version
	col.Delete(id2)                                     // tombstone second

	var results []ScanResult
	col.ScanStream(context.Background(), ScanOptions{OrderBy: "v"}, func(r ScanResult) error {
		results = append(results, r)
		return nil
	})
	if len(results) != 2 {
		t.Fatalf("expected 2 live records, got %d: %v", len(results), results)
	}
	// id2 deleted; id1 now v=100, id3 v=3 → ascending by v: id3 then id1.
	if results[0].ID != 3 || results[1].ID != id1 {
		t.Errorf("unexpected order/liveness: %+v", results)
	}
	if results[1].Data["v"].(float64) != 100 {
		t.Errorf("expected updated value 100, got %v", results[1].Data["v"])
	}
}

// ---- helpers ----------------------------------------------------------------

func collectScores(t *testing.T, col *Collection, opts ScanOptions) []float64 {
	t.Helper()
	var out []float64
	_, err := col.ScanStream(context.Background(), opts, func(r ScanResult) error {
		out = append(out, r.Data["score"].(float64))
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}
	return out
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedFloatsEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestProjectData exercises the field-projection helper in isolation: empty
// fields returns the record unchanged, a projection keeps only the requested
// keys, an unknown key is silently skipped, and the reserved key field always
// survives so a record's caller-supplied string key is never lost.
func TestProjectData(t *testing.T) {
	data := map[string]any{"name": "ada", "role": "eng", KeyField: "u1"}

	// Empty projection returns the same map (full record, backward compatible).
	if got := ProjectData(data, nil); len(got) != 3 {
		t.Errorf("empty projection should return full record, got %v", got)
	}

	// Projecting to a known and an unknown field keeps only the known one, plus
	// the reserved key field, and never mutates the input.
	got := ProjectData(data, []string{"name", "missing"})
	if got["name"] != "ada" {
		t.Errorf("projected name = %v, want ada", got["name"])
	}
	if _, ok := got["role"]; ok {
		t.Errorf("projection should drop role: %v", got)
	}
	if _, ok := got["missing"]; ok {
		t.Errorf("unknown field should be silently absent: %v", got)
	}
	if got[KeyField] != "u1" {
		t.Errorf("projection must retain the reserved key field: %v", got)
	}
	if len(data) != 3 {
		t.Errorf("ProjectData mutated its input: %v", data)
	}
}

// TestScanStream_FieldProjection drives projection through the scan path: only
// requested fields reach the caller, and an order-by field need not be projected
// because projection is applied after ordering.
func TestScanStream_FieldProjection(t *testing.T) {
	col := openTestCollection(t)
	for i := 1; i <= 3; i++ {
		col.Insert(map[string]any{"name": "n", "rank": float64(i), "extra": "x"})
	}

	var seen []map[string]any
	_, err := col.ScanStream(context.Background(), ScanOptions{
		OrderBy: "rank",
		Fields:  []string{"rank"}, // order-by field is projected; "extra" is dropped
	}, func(r ScanResult) error {
		seen = append(seen, r.Data)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("got %d records, want 3", len(seen))
	}
	for i, d := range seen {
		if len(d) != 1 {
			t.Errorf("record %d not projected to a single field: %v", i, d)
		}
		if d["rank"] != float64(i+1) {
			t.Errorf("record %d rank = %v, want %d (order preserved under projection)", i, d["rank"], i+1)
		}
		if _, ok := d["extra"]; ok {
			t.Errorf("record %d should not carry extra: %v", i, d)
		}
	}
}
