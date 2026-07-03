package engine

import (
	"context"

	"github.com/srjn45/filedbv2/query"
)

// Count returns the number of live records matching f. It is computed as cheaply
// as the filter allows and never buffers the matched records' data:
//
//   - A nil or match-all filter is answered from the primary index in O(1): the
//     index already tracks exactly the live records, so no segment is read.
//   - A single equality predicate on an indexed field is answered from that
//     secondary index — the value maps directly to its live id set, so the count
//     is the set's size (O(matches), still no segment reads).
//   - Any other filter falls back to a streaming scan that evaluates the filter
//     against each live record and counts the matches, without materialising a
//     result slice or a whole-collection data map.
//
// The result equals len(Scan(f)) for every filter; Count merely avoids the
// allocation of the result set (and, for the fast paths above, the reads).
func (c *Collection) Count(f query.Filter) (uint64, error) {
	// Whole-collection count: the primary index is exactly the set of live
	// records, so its length is the answer without touching a segment.
	if f == nil || f == query.MatchAll {
		return uint64(c.index.Len()), nil
	}

	// Single equality predicate on an indexed field: the secondary index maps the
	// value to its live id set, so the match count is that set's size. The bucket
	// membership is exactly the records the filter would accept (see the eq case
	// in indexCandidates), so this is scan-identical without any segment read.
	if ff, ok := f.(*query.FieldFilter); ok && ff.Op == query.OpEq {
		if ids, hit := c.IndexLookup(ff.Field, filterValueToIndexKey(ff.Value)); hit {
			return uint64(len(ids)), nil
		}
	}

	// General case: stream live records and count matches. forEachMatch uses a
	// range index when one can serve the filter and otherwise scans segments
	// sequentially, skipping stale versions via the primary index — either way it
	// never builds a per-record map of the whole collection. We only keep a
	// running counter, so nothing is materialised.
	var n uint64
	err := c.forEachMatch(context.Background(), f, func(ScanResult) error {
		n++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Exists reports whether a live record carries the caller-supplied string key.
// It is O(1): the lookup goes through the unique _key secondary index and never
// reads a segment, so it is safe on a dashboard hot path regardless of how large
// the collection is. A collection that has never taken a keyed write has no _key
// index, so Exists reports false for every key. It returns an error only to keep
// the signature stable for future implementations; today it never fails.
func (c *Collection) Exists(key string) (bool, error) {
	ids, ok := c.IndexLookup(KeyField, key)
	return ok && len(ids) > 0, nil
}
