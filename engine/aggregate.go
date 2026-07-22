package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/srjn45/scriva/query"
)

// AggregateSpec parameterises an aggregation over a collection's live records.
// It is plain data with no grpc/proto dependency, so the engine stays embeddable;
// the server maps its GroupResult output onto the wire.
type AggregateSpec struct {
	// Filter selects the records that contribute; nil (or query.MatchAll) matches
	// every live record, exactly as it does for a scan or Count.
	Filter query.Filter
	// GroupBy is the field to bucket records by. Empty aggregates the whole filtered
	// set into a single group whose Key is nil.
	GroupBy string
	// Field is the numeric field summed/averaged/min/maxed. Empty computes only the
	// per-group count. Only records whose Field value is numeric (per query.AsNumber)
	// contribute to the numeric aggregates.
	Field string
}

// GroupResult is one emitted group of an aggregation. For an ungrouped request
// there is exactly one (with Key nil); for a grouped request there is one per
// distinct GroupBy value.
type GroupResult struct {
	// Key is the group's GroupBy value (number, string, bool), or nil for the
	// whole-set group and for records that lacked the group field.
	Key any
	// Count is the number of live records in the group after filtering.
	Count uint64
	// Sum/Avg/Min/Max aggregate the numeric Field across the group's records that
	// carried a numeric value. Avg divides Sum by that numeric count (SQL AVG
	// semantics: non-numeric/absent values are ignored, not treated as zero). They
	// are meaningful only when Numeric is true.
	Sum, Avg, Min, Max float64
	// Numeric reports whether at least one record in the group contributed a numeric
	// Field value. When false, Sum/Avg/Min/Max are zero and meaningless.
	Numeric bool
}

// aggAcc accumulates one group's running aggregates during a single scan pass.
// Only per-group state is held — never the records themselves — so memory is
// bounded by the number of distinct groups, not the collection size.
type aggAcc struct {
	key      any
	count    uint64
	numCount uint64 // records that contributed a numeric Field value
	sum      float64
	min, max float64
	hasNum   bool
}

// Aggregate streams per-group aggregates of the live records matching spec.Filter
// to emit, in ascending group order. It never materialises the collection: the
// scan reads records one at a time (using a secondary index when the filter can be
// served by one, exactly as Count and Find do) and folds each into its group's
// accumulator, so only per-group state is retained. emit is invoked once per group;
// returning an error from it aborts the aggregation and that error is returned.
//
// ctx cancellation aborts the scan between reads and is returned as
// context.Canceled / context.DeadlineExceeded.
func (c *Collection) Aggregate(ctx context.Context, spec AggregateSpec, emit func(GroupResult) error) error {
	f := spec.Filter
	if f == nil {
		f = query.MatchAll
	}

	// Fast path: a whole-set count with no numeric field reuses Count, which answers
	// from the primary/secondary index without reading segments where it can.
	if spec.GroupBy == "" && spec.Field == "" {
		n, err := c.Count(f)
		if err != nil {
			return err
		}
		return emit(GroupResult{Count: n})
	}

	groups := make(map[string]*aggAcc)
	err := c.forEachMatch(ctx, f, &ScanStats{}, func(r ScanResult) error {
		var key any
		if spec.GroupBy != "" {
			key = r.Data[spec.GroupBy]
		}
		ck := canonicalGroupKey(key)
		acc := groups[ck]
		if acc == nil {
			acc = &aggAcc{key: key}
			groups[ck] = acc
		}
		acc.count++
		if spec.Field != "" {
			if n, ok := query.AsNumber(r.Data[spec.Field]); ok {
				acc.sum += n
				acc.numCount++
				if !acc.hasNum {
					acc.min, acc.max, acc.hasNum = n, n, true
				} else {
					if n < acc.min {
						acc.min = n
					}
					if n > acc.max {
						acc.max = n
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Emit in a deterministic order (ascending group key, via the same type-aware
	// comparison the filter/sort use) so the stream is stable across runs and a
	// client sees groups in a predictable sequence.
	accs := make([]*aggAcc, 0, len(groups))
	for _, a := range groups {
		accs = append(accs, a)
	}
	sort.Slice(accs, func(i, j int) bool { return query.Compare(accs[i].key, accs[j].key) < 0 })

	for _, a := range accs {
		g := GroupResult{Key: a.key, Count: a.count, Numeric: a.hasNum}
		if a.hasNum {
			g.Sum, g.Min, g.Max = a.sum, a.min, a.max
			g.Avg = a.sum / float64(a.numCount)
		}
		if err := emit(g); err != nil {
			return err
		}
	}
	return nil
}

// canonicalGroupKey maps a group value to a map key that never collides across
// types: the numeric 1, the string "1", and the bool true bucket separately even
// though they render alike. The original value is preserved on the accumulator for
// emission; this string is only the bucket identity.
func canonicalGroupKey(v any) string {
	if v == nil {
		return "\x00null"
	}
	return fmt.Sprintf("%T\x00%v", v, v)
}
