//nolint:errcheck
package engine

import (
	"context"
	"errors"
	"testing"
)

// paginate walks a collection in pages of the given size under opts' ordering,
// feeding each page's NextPageToken back as the next page's cursor, and returns
// the concatenated ids across every page. It stops when a page reports no next
// token (the last page). base carries the ordering/filter; Limit and PageToken
// are driven by the helper.
func paginate(t *testing.T, col *Collection, base ScanOptions, size int, mutate func(page int)) []uint64 {
	t.Helper()
	var all []uint64
	token := ""
	for page := 0; page < 10000; page++ {
		opts := base
		opts.Limit = size
		opts.Offset = 0
		opts.PageToken = token
		var pageIDs []uint64
		stats, err := col.ScanStream(context.Background(), opts, func(r ScanResult) error {
			pageIDs = append(pageIDs, r.ID)
			return nil
		})
		if err != nil {
			t.Fatalf("ScanStream page %d: %v", page, err)
		}
		if len(pageIDs) > size {
			t.Fatalf("page %d returned %d rows > limit %d", page, len(pageIDs), size)
		}
		all = append(all, pageIDs...)
		if stats.NextPageToken == "" {
			return all
		}
		token = stats.NextPageToken
		if mutate != nil {
			mutate(page)
		}
	}
	t.Fatalf("pagination did not terminate")
	return nil
}

// TestKeyset_CursorRoundTrip pins the opaque codec: a tuple encodes to URL-safe
// bytes and decodes back to the same keys (numbers as float64) and id.
func TestKeyset_CursorRoundTrip(t *testing.T) {
	tok, err := encodeCursor([]any{float64(42), "bob"}, 7)
	if err != nil {
		t.Fatalf("encodeCursor: %v", err)
	}
	got, err := decodeCursor(tok)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if got.ID != 7 || len(got.Keys) != 2 || got.Keys[0] != float64(42) || got.Keys[1] != "bob" {
		t.Errorf("round-trip mismatch: %+v", got)
	}

	if _, err := decodeCursor("!!!not base64!!!"); !errors.Is(err, ErrInvalidPageToken) {
		t.Errorf("bad base64: want ErrInvalidPageToken, got %v", err)
	}
	if _, err := decodeCursor("bm90anNvbg"); !errors.Is(err, ErrInvalidPageToken) {
		t.Errorf("bad payload: want ErrInvalidPageToken, got %v", err)
	}
}

// TestKeyset_InvalidTokenRejected proves a malformed cursor surfaces as
// ErrInvalidPageToken from ScanStream, and that a key count disagreeing with the
// ordering is likewise rejected rather than silently mis-seeking.
func TestKeyset_InvalidTokenRejected(t *testing.T) {
	col := openTestCollection(t)
	for i := 1; i <= 5; i++ {
		col.Insert(map[string]any{"score": float64(i)})
	}

	_, err := col.ScanStream(context.Background(), ScanOptions{
		Sort: []SortField{{Field: "score"}}, Limit: 2, PageToken: "garbage",
	}, func(ScanResult) error { return nil })
	if !errors.Is(err, ErrInvalidPageToken) {
		t.Errorf("garbage token: want ErrInvalidPageToken, got %v", err)
	}

	// A two-key cursor against a one-field ordering must be rejected.
	tok, _ := encodeCursor([]any{float64(1), float64(2)}, 1)
	_, err = col.ScanStream(context.Background(), ScanOptions{
		Sort: []SortField{{Field: "score"}}, Limit: 2, PageToken: tok,
	}, func(ScanResult) error { return nil })
	if !errors.Is(err, ErrInvalidPageToken) {
		t.Errorf("key-count mismatch: want ErrInvalidPageToken, got %v", err)
	}
}

// TestKeyset_MultiFieldTieBreak checks a two-field sort applies each key in order
// with its own direction, and that rows tying on both keys fall back to ascending
// id so the ordering is total and deterministic.
func TestKeyset_MultiFieldTieBreak(t *testing.T) {
	col := openTestCollection(t)
	// Same team repeated so the second key (and then id) decides ties.
	rows := []map[string]any{
		{"team": "b", "score": float64(10)},
		{"team": "a", "score": float64(20)},
		{"team": "a", "score": float64(10)},
		{"team": "b", "score": float64(20)},
		{"team": "a", "score": float64(10)},
	}
	ids := make([]uint64, len(rows))
	for i, r := range rows {
		id, _, _ := col.Insert(r)
		ids[i] = id
	}

	// Sort by team ASC, then score DESC, then id ASC (implicit).
	var got []struct {
		team  string
		score float64
		id    uint64
	}
	_, err := col.ScanStream(context.Background(), ScanOptions{
		Sort: []SortField{{Field: "team"}, {Field: "score", Desc: true}},
	}, func(r ScanResult) error {
		got = append(got, struct {
			team  string
			score float64
			id    uint64
		}{r.Data["team"].(string), r.Data["score"].(float64), r.ID})
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}

	// Expected: team a {score20}, a{score10,id=ids[2]}, a{score10,id=ids[4]},
	// then team b {score20}, b{score10}. The two a/score=10 rows tie and must
	// appear in ascending id order.
	want := []struct {
		team  string
		score float64
		id    uint64
	}{
		{"a", 20, ids[1]},
		{"a", 10, ids[2]},
		{"a", 10, ids[4]},
		{"b", 20, ids[3]},
		{"b", 10, ids[0]},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestKeyset_PaginationCoversEveryRowOnce paginates a large collection under a
// tie-heavy ordering and asserts the concatenation of pages hits every id exactly
// once — no dupes, no gaps — which only holds if the cursor seek respects the same
// total order the sort does.
func TestKeyset_PaginationCoversEveryRowOnce(t *testing.T) {
	col := openTestCollection(t)
	const total = 250
	want := map[uint64]bool{}
	for i := 0; i < total; i++ {
		// bucket has many duplicates so the id tiebreak is exercised heavily.
		id, _, _ := col.Insert(map[string]any{"bucket": float64(i % 7), "n": float64(i)})
		want[id] = true
	}

	got := paginate(t, col, ScanOptions{Sort: []SortField{{Field: "bucket"}}}, 13, nil)

	seen := map[uint64]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("id %d returned twice", id)
		}
		seen[id] = true
	}
	if len(seen) != total {
		t.Fatalf("covered %d ids, want %d", len(seen), total)
	}
	for id := range want {
		if !seen[id] {
			t.Errorf("id %d never returned (gap)", id)
		}
	}
}

// TestKeyset_StableUnderConcurrentInserts inserts fresh rows between page fetches
// and asserts every row that existed before pagination started is still returned
// exactly once, with no duplicates anywhere — the keyset guarantee under writes.
func TestKeyset_StableUnderConcurrentInserts(t *testing.T) {
	col := openTestCollection(t)
	const total = 120
	original := map[uint64]bool{}
	for i := 0; i < total; i++ {
		id, _, _ := col.Insert(map[string]any{"bucket": float64(i % 5), "n": float64(i)})
		original[id] = true
	}

	// After each page, insert a new row. New rows sort by (bucket, id); because
	// their ids exceed every original id, they may or may not appear, but must
	// never displace or duplicate an original row.
	extra := 0
	got := paginate(t, col, ScanOptions{Sort: []SortField{{Field: "bucket"}}}, 10, func(page int) {
		col.Insert(map[string]any{"bucket": float64(page % 5), "n": float64(1000 + page)})
		extra++
	})

	seen := map[uint64]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("id %d returned twice under concurrent inserts", id)
		}
		seen[id] = true
	}
	for id := range original {
		if !seen[id] {
			t.Errorf("original id %d dropped under concurrent inserts (gap)", id)
		}
	}
}

// TestKeyset_LastPageHasNoToken checks the terminating condition: a page that
// exhausts the matching rows reports an empty NextPageToken, and a full page with
// more rows behind it reports a non-empty one.
func TestKeyset_LastPageHasNoToken(t *testing.T) {
	col := openTestCollection(t)
	for i := 1; i <= 5; i++ {
		col.Insert(map[string]any{"score": float64(i)})
	}

	// First page of 2 over 5 rows: token present.
	stats, err := col.ScanStream(context.Background(), ScanOptions{
		Sort: []SortField{{Field: "score"}}, Limit: 2,
	}, func(ScanResult) error { return nil })
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}
	if stats.NextPageToken == "" {
		t.Fatal("first page of 2/5: want a next token")
	}

	// A limit covering everything leaves nothing behind: no token.
	stats, err = col.ScanStream(context.Background(), ScanOptions{
		Sort: []SortField{{Field: "score"}}, Limit: 5,
	}, func(ScanResult) error { return nil })
	if err != nil {
		t.Fatalf("ScanStream: %v", err)
	}
	if stats.NextPageToken != "" {
		t.Errorf("limit=5 over 5 rows: want no token, got %q", stats.NextPageToken)
	}
}

// TestKeyset_ScalarOrderByStillPaginates proves the deprecated single-field
// OrderBy path still produces and consumes a cursor, so the back-compat sort is a
// first-class pagination key too.
func TestKeyset_ScalarOrderByStillPaginates(t *testing.T) {
	col := openTestCollection(t)
	const total = 40
	want := map[uint64]bool{}
	for i := 0; i < total; i++ {
		id, _, _ := col.Insert(map[string]any{"n": float64(i)})
		want[id] = true
	}

	got := paginate(t, col, ScanOptions{OrderBy: "n", Descending: true}, 7, nil)
	if len(got) != total {
		t.Fatalf("scalar order_by pagination covered %d ids, want %d", len(got), total)
	}
	seen := map[uint64]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("id %d returned twice", id)
		}
		seen[id] = true
	}
	for id := range want {
		if !seen[id] {
			t.Errorf("id %d missing", id)
		}
	}
}
