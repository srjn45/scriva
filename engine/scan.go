package engine

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
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

// SortField names one sort key of a multi-field ordering and its direction. A
// scan applies a slice of these lexicographically (first field dominant); the
// record id is always the implicit final tiebreaker so the ordering is total.
type SortField struct {
	Field string
	Desc  bool // false = ascending, true = descending
}

// ScanOptions parameterises a streaming scan.
type ScanOptions struct {
	Filter query.Filter // nil = match all
	Limit  int          // 0 = no limit
	Offset int          // number of leading matches to skip
	// OrderBy/Descending are the legacy single-field sort. Sort supersedes them
	// when non-empty; otherwise a non-empty OrderBy is promoted to a one-element
	// Sort. Empty both = natural (insertion) order.
	OrderBy    string
	Descending bool
	// Sort is the multi-field, per-field-directional ordering (N3). When
	// non-empty it takes precedence over OrderBy/Descending. The record id breaks
	// any remaining tie, so the ordering is total and a keyset cursor is stable.
	Sort []SortField
	// PageToken is an opaque keyset cursor (N3). Empty (the default) starts at the
	// first page; otherwise it must be a token returned by a previous scan of the
	// same collection under the same ordering, and the scan seeks strictly past
	// the row it encodes instead of counting past it (O(page), not O(offset)).
	// A page token requires an ordering (Sort or OrderBy); a malformed token
	// yields ErrInvalidPageToken.
	PageToken string
	// Fields narrows each emitted record's Data to the named top-level keys
	// (field projection). Empty (the default) emits the full record. The
	// reserved key field is always retained so a record's string key survives,
	// and id/rev live outside Data so they are unaffected. Projection is applied
	// after filtering and ordering, so an order-by field need not be projected.
	Fields []string
}

// ErrInvalidPageToken is returned by ScanStream when ScanOptions.PageToken is not
// a token this engine produced (bad base64, bad payload, or a key count that does
// not match the requested ordering). The server maps it to InvalidArgument.
var ErrInvalidPageToken = errors.New("engine: invalid page token")

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
	// NextPageToken is the opaque keyset cursor for the following page (N3). It is
	// set only for an ordered, limited scan that left more matching rows beyond the
	// returned page; feed it back as ScanOptions.PageToken to continue. Empty means
	// the last page was reached (or the scan was unordered/unlimited).
	NextPageToken string
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
	// Report scan cost through the optional hook (the server turns it into a
	// tracing span). Timing the whole scan keeps the engine dependency-free.
	if c.cfg.OnScan != nil {
		start := time.Now()
		defer func() { c.cfg.OnScan(ctx, c.name, time.Since(start)) }()
	}

	f := opts.Filter
	if f == nil {
		f = query.MatchAll
	}

	// counting wraps the caller's yield so every successfully emitted record is
	// tallied once, regardless of ordered/unordered path. Field projection is
	// applied here — after filtering and ordering — so it narrows only what
	// reaches the caller (and thus the wire) without affecting order-by.
	stats := &ScanStats{}
	counting := func(r ScanResult) error {
		r.Data = ProjectData(r.Data, opts.Fields)
		if err := yield(r); err != nil {
			return err
		}
		stats.RowsReturned++
		return nil
	}

	// Resolve the effective ordering: an explicit multi-field Sort wins; otherwise
	// a non-empty legacy OrderBy is promoted to a single-field Sort. A page token
	// implies an ordering even when none was named (id-only, the always-present
	// tiebreak), so keyset seeking still has a total order to seek within.
	sortFields := opts.Sort
	if len(sortFields) == 0 && opts.OrderBy != "" {
		sortFields = []SortField{{Field: opts.OrderBy, Desc: opts.Descending}}
	}
	ordered := len(sortFields) > 0 || opts.PageToken != ""

	var err error
	if !ordered {
		err = c.scanUnordered(ctx, f, opts, stats, counting)
	} else {
		err = c.scanOrdered(ctx, f, opts, sortFields, stats, counting)
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

// scanOrdered buffers matches, sorts them lexicographically by sortFields (id as
// the final tiebreak), then emits the requested page. With a positive limit only
// a bounded top-K buffer is kept. When opts.PageToken is set the scan first drops
// every row at or before the cursor under the ordering, so a follow-up page seeks
// past the rows already returned instead of counting past them; and when the
// page leaves more matching rows behind it sets stats.NextPageToken to the cursor
// that resumes after the last emitted row.
func (c *Collection) scanOrdered(ctx context.Context, f query.Filter, opts ScanOptions, sortFields []SortField, stats *ScanStats, yield func(ScanResult) error) error {
	less := sortLess(sortFields)

	// A page token restricts the scan to rows strictly after the encoded cursor.
	// Because the ordering is total (id breaks every remaining tie), "strictly
	// after" is unambiguous and never re-emits or skips a boundary row.
	afterCursor, err := cursorAfter(opts.PageToken, sortFields, less)
	if err != nil {
		return err
	}

	matched := 0 // rows passing the filter and the cursor — the pageable universe
	var page []ScanResult
	collect := func(r ScanResult) error {
		if afterCursor != nil && !afterCursor(r) {
			return nil
		}
		matched++
		page = append(page, r)
		return nil
	}
	if opts.Limit > 0 {
		// Keep only the smallest K = Offset+Limit results under the ordering.
		k := opts.Offset + opts.Limit
		th := &topK{less: less, cap: k}
		collect = func(r ScanResult) error {
			if afterCursor != nil && !afterCursor(r) {
				return nil
			}
			matched++
			th.push(r)
			return nil
		}
		if err := c.forEachMatch(ctx, f, stats, collect); err != nil {
			return err
		}
		page = th.sorted()
	} else {
		if err := c.forEachMatch(ctx, f, stats, collect); err != nil {
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

	// More rows remain beyond this page iff the limit truncated the pageable
	// universe (offset + limit < matched). Encode the last emitted row's sort-key
	// tuple + id as the resume cursor. Read the keys from the pre-projection Data
	// so a token survives even when the sort field was not among Fields.
	if opts.Limit > 0 && len(page) > 0 && matched > opts.Offset+opts.Limit {
		last := page[len(page)-1]
		keys := make([]any, len(sortFields))
		for i, sf := range sortFields {
			keys[i] = last.Data[sf.Field]
		}
		tok, err := encodeCursor(keys, last.ID)
		if err != nil {
			return err
		}
		stats.NextPageToken = tok
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

// ProjectData returns a view of data limited to the named top-level fields
// (field projection). An empty fields slice returns data unchanged — the full
// record, which keeps the default read path backward compatible. When fields is
// non-empty a fresh map is built (data is never mutated) containing only the
// requested keys that exist; a requested key absent from data is silently
// skipped rather than erroring. The reserved key field is always retained so a
// record's caller-supplied string key survives projection — id and rev are
// carried outside the data map and so are unaffected.
func ProjectData(data map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		return data
	}
	out := make(map[string]any, len(fields)+1)
	for _, f := range fields {
		if v, ok := data[f]; ok {
			out[f] = v
		}
	}
	if v, ok := data[KeyField]; ok {
		out[KeyField] = v
	}
	return out
}

// sortLess returns a total ordering over ScanResults for a multi-field sort. Each
// field is compared with query.Compare — the exact same type-aware comparison the
// filter operators (gt/gte/lt/lte) use, so a sort and a query never disagree:
// numbers order numerically (2 < 10, not the lexical "10" < "2") and strings
// lexically. Fields are applied lexicographically (the first is dominant), each
// reversed when its Desc is set. Any remaining tie — including the zero-field case
// — breaks on ascending id, so the ordering is total: results are deterministic, a
// bounded top-K agrees with a full sort, and a keyset cursor is unambiguous.
func sortLess(fields []SortField) func(a, b ScanResult) bool {
	return func(a, b ScanResult) bool {
		for _, sf := range fields {
			c := query.Compare(a.Data[sf.Field], b.Data[sf.Field])
			if sf.Desc {
				c = -c
			}
			if c != 0 {
				return c < 0
			}
		}
		return a.ID < b.ID
	}
}

// pageCursor is the decoded form of a keyset page token: the sort-key tuple and
// id of the last row emitted on the previous page. Keys are stored in sort-field
// order; JSON round-trips numbers as float64, which query.Compare treats
// identically to the numeric types decoded from a segment.
type pageCursor struct {
	Keys []any  `json:"k"`
	ID   uint64 `json:"i"`
}

// encodeCursor packs a sort-key tuple and id into an opaque, URL-safe token. The
// encoding is a base64 of compact JSON: self-describing enough to survive numbers
// and strings, and free of any grpc/proto dependency so the engine stays
// embeddable. Callers treat the result as opaque bytes.
func encodeCursor(keys []any, id uint64) (string, error) {
	b, err := json.Marshal(pageCursor{Keys: keys, ID: id})
	if err != nil {
		return "", fmt.Errorf("engine: encode page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// decodeCursor reverses encodeCursor. Any malformed token — bad base64 or bad
// payload — is reported as ErrInvalidPageToken so the server can return a stable
// InvalidArgument regardless of the underlying cause.
func decodeCursor(tok string) (pageCursor, error) {
	var c pageCursor
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return c, fmt.Errorf("%w: %s", ErrInvalidPageToken, err.Error())
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("%w: %s", ErrInvalidPageToken, err.Error())
	}
	return c, nil
}

// cursorAfter builds the "strictly after the cursor" predicate for a keyset page.
// An empty token yields a nil predicate (no seek — the first page). Otherwise it
// decodes the token, rebuilds a synthetic ScanResult carrying the cursor's
// sort-key values under their field names plus its id, and returns a predicate
// that is true for exactly the rows that sort strictly after it under less — so
// the boundary row itself (same id) is excluded and no row is dropped or dupled.
// A key count that disagrees with the ordering is ErrInvalidPageToken.
func cursorAfter(token string, sortFields []SortField, less func(a, b ScanResult) bool) (func(ScanResult) bool, error) {
	if token == "" {
		return nil, nil
	}
	cur, err := decodeCursor(token)
	if err != nil {
		return nil, err
	}
	if len(cur.Keys) != len(sortFields) {
		return nil, fmt.Errorf("%w: key count %d does not match order (%d fields)", ErrInvalidPageToken, len(cur.Keys), len(sortFields))
	}
	boundary := ScanResult{ID: cur.ID, Data: make(map[string]any, len(sortFields))}
	for i, sf := range sortFields {
		boundary.Data[sf.Field] = cur.Keys[i]
	}
	return func(r ScanResult) bool { return less(boundary, r) }, nil
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
