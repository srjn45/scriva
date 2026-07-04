//nolint:errcheck
package engine

import (
	"context"
	"math"
	"testing"

	"github.com/srjn45/filedbv2/query"
)

// collectGroups runs an aggregation and returns its emitted groups in stream order.
func collectGroups(t *testing.T, col *Collection, spec AggregateSpec) []GroupResult {
	t.Helper()
	var out []GroupResult
	if err := col.Aggregate(context.Background(), spec, func(g GroupResult) error {
		out = append(out, g)
		return nil
	}); err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	return out
}

// TestAggregateCountMatchesScan checks that an ungrouped count equals the number
// of records the equivalent Scan returns, across a match-all and a filtered case.
func TestAggregateCountMatchesScan(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("status")

	statuses := []string{"active", "archived", "active", "pending", "active", "archived"}
	for _, s := range statuses {
		col.Insert(map[string]any{"status": s})
	}

	// Whole-set count == len(Scan(match-all)).
	all := collectGroups(t, col, AggregateSpec{})
	if len(all) != 1 {
		t.Fatalf("ungrouped aggregation emitted %d groups, want 1", len(all))
	}
	if want := countViaScan(t, col, nil); all[0].Count != want {
		t.Errorf("count = %d, want len(Scan) = %d", all[0].Count, want)
	}

	// Filtered count == len(Scan(filter)).
	f := &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"active"`}
	got := collectGroups(t, col, AggregateSpec{Filter: f})
	if want := countViaScan(t, col, f); got[0].Count != want {
		t.Errorf("filtered count = %d, want len(Scan) = %d", got[0].Count, want)
	}
}

// TestAggregateNumericWholeSet checks sum/avg/min/max over the whole set equal a
// manual reduction over the same data.
func TestAggregateNumericWholeSet(t *testing.T) {
	col := openTestCollection(t)

	amounts := []float64{10, 20, 30, 5, 100}
	var sum, min, max float64 = 0, math.Inf(1), math.Inf(-1)
	for _, a := range amounts {
		col.Insert(map[string]any{"amount": a})
		sum += a
		min, max = math.Min(min, a), math.Max(max, a)
	}
	wantAvg := sum / float64(len(amounts))

	groups := collectGroups(t, col, AggregateSpec{Field: "amount"})
	if len(groups) != 1 {
		t.Fatalf("emitted %d groups, want 1", len(groups))
	}
	g := groups[0]
	if !g.Numeric {
		t.Fatal("expected Numeric=true over numeric data")
	}
	if g.Count != uint64(len(amounts)) {
		t.Errorf("count = %d, want %d", g.Count, len(amounts))
	}
	if g.Sum != sum || g.Min != min || g.Max != max || g.Avg != wantAvg {
		t.Errorf("got sum=%g avg=%g min=%g max=%g; want sum=%g avg=%g min=%g max=%g",
			g.Sum, g.Avg, g.Min, g.Max, sum, wantAvg, min, max)
	}
}

// TestAggregateGroupByMatchesManualReduction checks grouped count/sum/avg/min/max
// equal a manual per-group reduction computed independently over the same data.
func TestAggregateGroupByMatchesManualReduction(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("region")

	type row struct {
		region string
		amount float64
	}
	rows := []row{
		{"us", 10}, {"eu", 5}, {"us", 30}, {"eu", 25},
		{"us", 20}, {"apac", 7}, {"eu", 5}, {"apac", 3},
	}
	// Manual reduction: per-region count, sum, min, max.
	type acc struct {
		count         uint64
		sum, min, max float64
	}
	want := map[string]*acc{}
	for _, r := range rows {
		col.Insert(map[string]any{"region": r.region, "amount": r.amount})
		a := want[r.region]
		if a == nil {
			a = &acc{min: math.Inf(1), max: math.Inf(-1)}
			want[r.region] = a
		}
		a.count++
		a.sum += r.amount
		a.min, a.max = math.Min(a.min, r.amount), math.Max(a.max, r.amount)
	}

	groups := collectGroups(t, col, AggregateSpec{GroupBy: "region", Field: "amount"})
	if len(groups) != len(want) {
		t.Fatalf("emitted %d groups, want %d", len(groups), len(want))
	}
	for _, g := range groups {
		region, _ := g.Key.(string)
		a, ok := want[region]
		if !ok {
			t.Fatalf("unexpected group %v", g.Key)
		}
		wantAvg := a.sum / float64(a.count)
		if g.Count != a.count || g.Sum != a.sum || g.Min != a.min || g.Max != a.max || g.Avg != wantAvg {
			t.Errorf("group %q: got count=%d sum=%g avg=%g min=%g max=%g; want count=%d sum=%g avg=%g min=%g max=%g",
				region, g.Count, g.Sum, g.Avg, g.Min, g.Max, a.count, a.sum, wantAvg, a.min, a.max)
		}
	}

	// Groups are emitted in ascending key order: apac < eu < us.
	gotOrder := []string{groups[0].Key.(string), groups[1].Key.(string), groups[2].Key.(string)}
	wantOrder := []string{"apac", "eu", "us"}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Errorf("group order = %v, want %v", gotOrder, wantOrder)
			break
		}
	}
}

// TestAggregateFilterHonored verifies aggregating a filtered subset excludes
// non-matching records: the grouped totals cover only records passing the filter.
func TestAggregateFilterHonored(t *testing.T) {
	col := openTestCollection(t)

	// Two regions; only "open" status records should contribute.
	col.Insert(map[string]any{"region": "us", "status": "open", "amount": float64(10)})
	col.Insert(map[string]any{"region": "us", "status": "closed", "amount": float64(1000)})
	col.Insert(map[string]any{"region": "eu", "status": "open", "amount": float64(20)})
	col.Insert(map[string]any{"region": "eu", "status": "closed", "amount": float64(9000)})

	f := &query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"open"`}
	groups := collectGroups(t, col, AggregateSpec{Filter: f, GroupBy: "region", Field: "amount"})
	if len(groups) != 2 {
		t.Fatalf("emitted %d groups, want 2", len(groups))
	}
	byRegion := map[string]GroupResult{}
	for _, g := range groups {
		byRegion[g.Key.(string)] = g
	}
	if g := byRegion["us"]; g.Count != 1 || g.Sum != 10 {
		t.Errorf("us group = count %d sum %g, want count 1 sum 10 (closed excluded)", g.Count, g.Sum)
	}
	if g := byRegion["eu"]; g.Count != 1 || g.Sum != 20 {
		t.Errorf("eu group = count %d sum %g, want count 1 sum 20 (closed excluded)", g.Count, g.Sum)
	}
}

// TestAggregateNonNumericField checks that records whose numeric field is absent or
// non-numeric still count but do not contribute to the numeric aggregates, and AVG
// divides by the numeric count (not the total count).
func TestAggregateNonNumericField(t *testing.T) {
	col := openTestCollection(t)

	col.Insert(map[string]any{"g": "x", "amount": float64(10)})
	col.Insert(map[string]any{"g": "x", "amount": "not-a-number"})
	col.Insert(map[string]any{"g": "x"}) // amount absent
	col.Insert(map[string]any{"g": "x", "amount": float64(30)})

	groups := collectGroups(t, col, AggregateSpec{GroupBy: "g", Field: "amount"})
	if len(groups) != 1 {
		t.Fatalf("emitted %d groups, want 1", len(groups))
	}
	g := groups[0]
	if g.Count != 4 {
		t.Errorf("count = %d, want 4 (all records counted)", g.Count)
	}
	// Only the two numeric values (10, 30) contribute.
	if !g.Numeric || g.Sum != 40 || g.Min != 10 || g.Max != 30 || g.Avg != 20 {
		t.Errorf("got numeric=%v sum=%g avg=%g min=%g max=%g; want sum=40 avg=20 min=10 max=30",
			g.Numeric, g.Sum, g.Avg, g.Min, g.Max)
	}
}

// TestAggregateEmptyCollection checks an empty collection yields a single count-0
// group for an ungrouped request and no groups for a grouped one.
func TestAggregateEmptyCollection(t *testing.T) {
	col := openTestCollection(t)

	all := collectGroups(t, col, AggregateSpec{})
	if len(all) != 1 || all[0].Count != 0 {
		t.Errorf("ungrouped over empty = %+v, want a single count-0 group", all)
	}

	grouped := collectGroups(t, col, AggregateSpec{GroupBy: "region", Field: "amount"})
	if len(grouped) != 0 {
		t.Errorf("grouped over empty emitted %d groups, want 0", len(grouped))
	}
}
