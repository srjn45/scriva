package engine

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/srjn45/filedbv2/query"
	"github.com/srjn45/filedbv2/store"
)

// ScanResult holds a single matched record from a scan.
type ScanResult struct {
	ID   uint64
	Rev  uint64
	Data map[string]any
	Ts   time.Time
}

// ScanOptions parameterises a streaming scan.
type ScanOptions struct {
	Filter     query.Filter // nil = match all
	Limit      int          // 0 = no limit
	Offset     int          // number of leading matches to skip
	OrderBy    string       // "" = natural (insertion) order
	Descending bool         // reverse the ordering (only meaningful with OrderBy)
}

// ScanStats reports the cost of a completed scan: how many live records were
// examined, how many were emitted to the caller, and whether a secondary index
// produced the candidate set instead of a full segment sweep. It is plain data
// with no external dependencies, so the engine stays embeddable; the server
// layer turns it into a slow-query log line and a metric (never the reverse —
// the engine imports no logger or metrics package).
type ScanStats struct {
	RowsScanned  int  // live records examined against the filter
	RowsReturned int  // records emitted to yield
	IndexUsed    bool // a secondary index produced the candidate set
}

// errStopScan is an internal sentinel used to terminate a scan early once a
// limit has been satisfied. It never escapes ScanStream.
var errStopScan = errors.New("engine: scan limit reached")

// ScanStream streams the live records matching opts.Filter to yield, in the
// requested order, honouring Offset and Limit. yield is invoked once per
// emitted result, in order; returning an error from it aborts the scan and that
// error is returned to the caller.
//
// Cost profile:
//
//   - Unordered (OrderBy == "") with a positive Limit: records are read lazily
//     and the scan stops after Offset+Limit matches, so both the rows read and
//     the memory held are bounded by Offset+Limit regardless of collection size.
//   - Ordered with a positive Limit: every candidate must be examined, but only
//     a bounded top-(Offset+Limit) buffer is retained.
//   - Ordered with no Limit: all matches are buffered and fully sorted (the
//     inherent cost of returning every row in sorted order).
//
// ctx cancellation aborts the scan between reads and is returned as
// context.Canceled / context.DeadlineExceeded.
//
// The returned ScanStats describe the cost of the scan (rows examined vs
// emitted, and whether an index served it) and are valid even when a non-nil
// error is returned — they reflect the work done up to the point of failure.
func (c *Collection) ScanStream(ctx context.Context, opts ScanOptions, yield func(ScanResult) error) (ScanStats, error) {
	f := opts.Filter
	if f == nil {
		f = query.MatchAll
	}

	// counting wraps the caller's yield so every successfully emitted record is
	// tallied once, regardless of ordered/unordered path.
	stats := &ScanStats{}
	counting := func(r ScanResult) error {
		if err := yield(r); err != nil {
			return err
		}
		stats.RowsReturned++
		return nil
	}

	var err error
	if opts.OrderBy == "" {
		err = c.scanUnordered(ctx, f, opts, stats, counting)
	} else {
		err = c.scanOrdered(ctx, f, opts, stats, counting)
	}
	return *stats, err
}

// scanUnordered streams matches in natural (insertion) order, applying offset
// and limit as it goes and stopping as early as possible.
func (c *Collection) scanUnordered(ctx context.Context, f query.Filter, opts ScanOptions, stats *ScanStats, yield func(ScanResult) error) error {
	skipped, emitted := 0, 0
	err := c.forEachMatch(ctx, f, stats, func(r ScanResult) error {
		if skipped < opts.Offset {
			skipped++
			return nil
		}
		if err := yield(r); err != nil {
			return err
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		return nil
	}
	return err
}

// scanOrdered buffers matches, sorts them by the requested field, then emits the
// requested page. With a positive limit only a bounded top-K buffer is kept.
func (c *Collection) scanOrdered(ctx context.Context, f query.Filter, opts ScanOptions, stats *ScanStats, yield func(ScanResult) error) error {
	less := orderLess(opts.OrderBy, opts.Descending)

	var page []ScanResult
	if opts.Limit > 0 {
		// Keep only the smallest K = Offset+Limit results under the ordering.
		k := opts.Offset + opts.Limit
		th := &topK{less: less, cap: k}
		if err := c.forEachMatch(ctx, f, stats, func(r ScanResult) error {
			th.push(r)
			return nil
		}); err != nil {
			return err
		}
		page = th.sorted()
	} else {
		if err := c.forEachMatch(ctx, f, stats, func(r ScanResult) error {
			page = append(page, r)
			return nil
		}); err != nil {
			return err
		}
		sort.SliceStable(page, func(i, j int) bool { return less(page[i], page[j]) })
	}

	if opts.Offset < len(page) {
		page = page[opts.Offset:]
	} else {
		page = nil
	}
	if opts.Limit > 0 && opts.Limit < len(page) {
		page = page[:opts.Limit]
	}

	for _, r := range page {
		if err := yield(r); err != nil {
			return err
		}
	}
	return nil
}

// forEachMatch invokes visit for every live record matching f. For an eq or
// range (gt/gte/lt/lte) filter on an indexed field it walks only the indexed
// candidate ids; otherwise it streams all segments sequentially, using the
// primary index to skip stale (overwritten or deleted) versions without
// materialising the whole collection.
func (c *Collection) forEachMatch(ctx context.Context, f query.Filter, stats *ScanStats, visit func(ScanResult) error) error {
	if ids, hit := c.indexCandidates(f); hit {
		stats.IndexUsed = true
		return c.visitCandidates(ctx, f, ids, stats, visit)
	}
	return c.streamLive(ctx, f, stats, visit)
}

// indexCandidates returns a candidate id set from a secondary index when f is a
// single field predicate an index can serve (eq, or a range), and whether the
// index answered. Candidates are a superset that visitCandidates re-filters, so
// results stay identical to a full scan.
func (c *Collection) indexCandidates(f query.Filter) ([]uint64, bool) {
	ff, ok := f.(*query.FieldFilter)
	if !ok {
		return nil, false
	}
	switch ff.Op {
	case query.OpEq:
		return c.IndexLookup(ff.Field, filterValueToIndexKey(ff.Value))
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		return c.indexRangeLookup(ff.Field, ff.Op, filterValueTyped(ff.Value))
	default:
		return nil, false
	}
}

// visitCandidates re-validates indexed candidate ids against the live store and
// the filter, emitting matches in ascending id (insertion) order.
func (c *Collection) visitCandidates(ctx context.Context, f query.Filter, ids []uint64, stats *ScanStats, visit func(ScanResult) error) error {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := c.Get(id)
		if err != nil {
			continue // deleted since the index was consulted
		}
		stats.RowsScanned++ // a live candidate examined against the filter
		if !f.Match(rec.Data) {
			continue
		}
		if err := visit(ScanResult{ID: id, Rev: rec.Rev, Data: rec.Data, Ts: rec.Ts}); err != nil {
			return err
		}
	}
	return nil
}

// streamLive reads every segment in insertion order and invokes visit for each
// live, matching record. An entry is live only when the primary index still
// points at exactly its segment and offset; stale versions and tombstones are
// skipped. No per-id map of the whole collection is built.
func (c *Collection) streamLive(ctx context.Context, f query.Filter, stats *ScanStats, visit func(ScanResult) error) error {
	c.mu.RLock()
	segs := make([]*Segment, 0, len(c.sealed)+1)
	segs = append(segs, c.sealed...)
	segs = append(segs, c.active)
	c.mu.RUnlock()

	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := seg.Path()
		err := seg.ScanFrom(func(offset int64, e store.Entry) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			loc, ok := c.index.Get(e.ID)
			if !ok || loc.SegmentPath != path || loc.Offset != offset {
				return nil // stale version, or the record was deleted
			}
			if e.Op == store.OpDelete {
				return nil
			}
			if c.isExpired(loc) {
				return nil // TTL passed; hidden until the reaper reclaims it
			}
			stats.RowsScanned++ // a live record examined against the filter
			if !f.Match(e.Data) {
				return nil
			}
			return visit(ScanResult{ID: e.ID, Rev: loc.Rev, Data: e.Data, Ts: e.Ts})
		})
		if err != nil {
			return fmt.Errorf("collection: scan %q: %w", path, err)
		}
	}
	return nil
}

// Scan iterates all live records and returns those matching f in natural order.
// It is a convenience wrapper over ScanStream that buffers the full result set;
// prefer ScanStream directly to bound memory for large collections.
func (c *Collection) Scan(f query.Filter) ([]ScanResult, error) {
	var out []ScanResult
	_, err := c.ScanStream(context.Background(), ScanOptions{Filter: f}, func(r ScanResult) error {
		out = append(out, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// orderLess returns a total ordering over ScanResults by the given field. The
// primary comparison is query.Compare — the exact same type-aware comparison
// the filter operators (gt/gte/lt/lte) use, so a sort and a query never
// disagree: numbers order numerically (2 < 10, not the lexical "10" < "2") and
// strings lexically. The result is reversed when desc is set. Ties break on
// ascending id so results are deterministic and a bounded top-K agrees with a
// full sort.
func orderLess(field string, desc bool) func(a, b ScanResult) bool {
	return func(a, b ScanResult) bool {
		c := query.Compare(a.Data[field], b.Data[field])
		if desc {
			c = -c
		}
		if c != 0 {
			return c < 0
		}
		return a.ID < b.ID
	}
}

// topK keeps the smallest cap results under less, discarding larger ones as it
// goes so memory stays bounded by cap. It is backed by a max-heap (its root is
// the largest retained result) so the worst survivor can be evicted in O(log n).
type topK struct {
	less func(a, b ScanResult) bool
	cap  int
	buf  []ScanResult
}

func (t *topK) push(r ScanResult) {
	if t.cap <= 0 {
		return
	}
	if len(t.buf) < t.cap {
		heap.Push(t, r)
		return
	}
	// Full: replace the current max if r is smaller than it.
	if t.less(r, t.buf[0]) {
		t.buf[0] = r
		heap.Fix(t, 0)
	}
}

// sorted drains the buffer into an ascending slice under less.
func (t *topK) sorted() []ScanResult {
	out := make([]ScanResult, len(t.buf))
	copy(out, t.buf)
	sort.SliceStable(out, func(i, j int) bool { return t.less(out[i], out[j]) })
	return out
}

// heap.Interface — ordered as a max-heap so buf[0] is the largest retained
// result (the eviction candidate). Less is therefore reversed.
func (t *topK) Len() int           { return len(t.buf) }
func (t *topK) Less(i, j int) bool { return t.less(t.buf[j], t.buf[i]) }
func (t *topK) Swap(i, j int)      { t.buf[i], t.buf[j] = t.buf[j], t.buf[i] }
func (t *topK) Push(x any)         { t.buf = append(t.buf, x.(ScanResult)) }
func (t *topK) Pop() any {
	old := t.buf
	n := len(old)
	r := old[n-1]
	t.buf = old[:n-1]
	return r
}
