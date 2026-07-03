//nolint:errcheck
package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// assertIndexConsistent verifies the live primary index agrees with a fresh
// rebuild from the collection's segments: the same set of live ids, and every
// indexed offset reading back the record it claims. A failed bulk load must leave
// the collection in this state — no dangling or missing index entries.
func assertIndexConsistent(t *testing.T, c *Collection) {
	t.Helper()

	c.mu.RLock()
	all := make([]*Segment, 0, len(c.sealed)+1)
	all = append(all, c.sealed...)
	all = append(all, c.active)
	c.mu.RUnlock()

	fresh := newIndex()
	if err := fresh.Rebuild(all); err != nil {
		t.Fatalf("rebuild for consistency check: %v", err)
	}

	c.index.mu.RLock()
	live := make(map[uint64]IndexEntry, len(c.index.entries))
	for id, e := range c.index.entries {
		live[id] = e
	}
	c.index.mu.RUnlock()

	fresh.mu.RLock()
	rebuilt := fresh.entries
	fresh.mu.RUnlock()

	if len(live) != len(rebuilt) {
		t.Fatalf("index inconsistent: live has %d entries, rebuild has %d", len(live), len(rebuilt))
	}
	for id := range live {
		if _, ok := rebuilt[id]; !ok {
			t.Fatalf("index inconsistent: live id %d absent from rebuild", id)
		}
		// The record the live index points at must still be readable.
		if _, err := c.Get(id); err != nil {
			t.Fatalf("index inconsistent: live id %d unreadable: %v", id, err)
		}
	}
}

// ---- Happy path: unkeyed ----------------------------------------------------

func TestLoadJSONL_UnkeyedLoadsAllRecords(t *testing.T) {
	col := openTestCollection(t)

	const n = 25
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"i":%d,"name":"rec-%d"}`+"\n", i, i)
	}

	got, err := col.LoadJSONL(strings.NewReader(sb.String()), "")
	if err != nil {
		t.Fatalf("LoadJSONL: %v", err)
	}
	if got != n {
		t.Fatalf("loaded %d records, want %d", got, n)
	}

	results, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != n {
		t.Fatalf("scan found %d live records, want %d", len(results), n)
	}
	cnt, err := col.Count(nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != n {
		t.Fatalf("Count = %d, want %d", cnt, n)
	}
	assertIndexConsistent(t, col)
}

// Blank and whitespace-only lines are skipped; a trailing newline is not counted.
func TestLoadJSONL_SkipsBlankLines(t *testing.T) {
	col := openTestCollection(t)

	input := "\n{\"a\":1}\n   \n{\"a\":2}\n\n"
	got, err := col.LoadJSONL(strings.NewReader(input), "")
	if err != nil {
		t.Fatalf("LoadJSONL: %v", err)
	}
	if got != 2 {
		t.Fatalf("loaded %d records, want 2", got)
	}
}

func TestLoadJSONL_EmptyReader(t *testing.T) {
	col := openTestCollection(t)
	got, err := col.LoadJSONL(strings.NewReader(""), "")
	if err != nil {
		t.Fatalf("LoadJSONL: %v", err)
	}
	if got != 0 {
		t.Fatalf("loaded %d records, want 0", got)
	}
}

// ---- Happy path: keyed ------------------------------------------------------

func TestLoadJSONL_KeyedRecordsAddressable(t *testing.T) {
	col := openTestCollection(t)

	input := strings.Join([]string{
		`{"id":"sess-1","status":"open"}`,
		`{"id":"sess-2","status":"closed"}`,
		`{"id":"sess-3","status":"open"}`,
	}, "\n")

	got, err := col.LoadJSONL(strings.NewReader(input), "id")
	if err != nil {
		t.Fatalf("LoadJSONL: %v", err)
	}
	if got != 3 {
		t.Fatalf("loaded %d records, want 3", got)
	}

	for _, key := range []string{"sess-1", "sess-2", "sess-3"} {
		data, _, err := col.FindByKey(key)
		if err != nil {
			t.Fatalf("FindByKey(%q): %v", key, err)
		}
		if data[KeyField] != key {
			t.Errorf("record %q lost its key: %v", key, data[KeyField])
		}
		if _, ok := data["status"]; !ok {
			t.Errorf("record %q missing status field", key)
		}
	}
	assertIndexConsistent(t, col)
}

// ---- Malformed line: aborts with line number, no partial corruption ---------

func TestLoadJSONL_MalformedLineErrorsWithLineNumber(t *testing.T) {
	col := openTestCollection(t)

	// Seed a couple of pre-existing records; the failed load must not disturb them.
	col.Insert(map[string]any{"seed": 1})
	col.Insert(map[string]any{"seed": 2})
	before, _ := col.Count(nil)

	input := strings.Join([]string{
		`{"a":1}`,
		`{"a":2}`,
		`{oops not json`, // line 3
		`{"a":4}`,
	}, "\n")

	got, err := col.LoadJSONL(strings.NewReader(input), "")
	if err == nil {
		t.Fatalf("expected error for malformed line, got nil (n=%d)", got)
	}
	if got != 0 {
		t.Fatalf("failed load reported %d records, want 0", got)
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Errorf("error should name the offending line 3, got: %v", err)
	}

	// Nothing from the batch was applied: only the seeded records remain.
	after, _ := col.Count(nil)
	if after != before {
		t.Fatalf("count changed after failed load: before=%d after=%d", before, after)
	}
	assertIndexConsistent(t, col)
}

// ---- Reserved field rejected ------------------------------------------------

func TestLoadJSONL_UnkeyedRejectsReservedField(t *testing.T) {
	col := openTestCollection(t)

	input := `{"a":1}` + "\n" + `{"_key":"smuggled"}` + "\n"
	got, err := col.LoadJSONL(strings.NewReader(input), "")
	if !errors.Is(err, ErrReservedField) {
		t.Fatalf("expected ErrReservedField, got %v", err)
	}
	if got != 0 {
		t.Fatalf("n=%d, want 0", got)
	}
	if c, _ := col.Count(nil); c != 0 {
		t.Fatalf("reserved-field violation applied records: count=%d", c)
	}
}

func TestLoadJSONL_KeyedMissingOrNonStringKey(t *testing.T) {
	col := openTestCollection(t)

	// Missing key field.
	if _, err := col.LoadJSONL(strings.NewReader(`{"other":1}`), "id"); err == nil ||
		!strings.Contains(err.Error(), "missing key field") {
		t.Fatalf("expected missing-key error, got %v", err)
	}
	// Non-string key value.
	if _, err := col.LoadJSONL(strings.NewReader(`{"id":42}`), "id"); err == nil ||
		!strings.Contains(err.Error(), "must be a string") {
		t.Fatalf("expected non-string-key error, got %v", err)
	}
	if c, _ := col.Count(nil); c != 0 {
		t.Fatalf("rejected keyed load applied records: count=%d", c)
	}
}

// ---- Duplicate key: rejected, does not partially apply ----------------------

func TestLoadJSONL_DuplicateKeyWithinBatchRejected(t *testing.T) {
	col := openTestCollection(t)

	input := strings.Join([]string{
		`{"id":"k1","v":1}`,
		`{"id":"k2","v":2}`,
		`{"id":"k1","v":3}`, // duplicate of k1
	}, "\n")

	got, err := col.LoadJSONL(strings.NewReader(input), "id")
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", err)
	}
	if got != 0 {
		t.Fatalf("n=%d, want 0", got)
	}
	// Nothing applied: not even the records preceding the duplicate.
	if c, _ := col.Count(nil); c != 0 {
		t.Fatalf("duplicate load partially applied: count=%d", c)
	}
	if _, _, err := col.FindByKey("k1"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("k1 should not exist after rejected load, got %v", err)
	}
	assertIndexConsistent(t, col)
}

func TestLoadJSONL_DuplicateKeyAgainstExistingRejected(t *testing.T) {
	col := openTestCollection(t)

	// Pre-existing keyed record.
	if _, _, err := col.InsertWithKey("k1", map[string]any{"v": "original"}); err != nil {
		t.Fatalf("InsertWithKey: %v", err)
	}

	input := strings.Join([]string{
		`{"id":"k2","v":"new"}`,
		`{"id":"k1","v":"collision"}`, // collides with the existing record
	}, "\n")

	got, err := col.LoadJSONL(strings.NewReader(input), "id")
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", err)
	}
	if got != 0 {
		t.Fatalf("n=%d, want 0", got)
	}
	// The pre-existing record is untouched and k2 was not applied.
	data, _, err := col.FindByKey("k1")
	if err != nil {
		t.Fatalf("FindByKey(k1): %v", err)
	}
	if data["v"] != "original" {
		t.Errorf("existing record mutated by rejected load: %v", data["v"])
	}
	if _, _, err := col.FindByKey("k2"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("k2 should not exist after rejected load, got %v", err)
	}
	assertIndexConsistent(t, col)
}

// ---- Watch events fire on the normal write path -----------------------------

func TestLoadJSONL_EmitsWatchEvents(t *testing.T) {
	col := openTestCollection(t)

	_, events, cancel := col.Subscribe()
	defer cancel()

	const n = 5
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"i":%d}`+"\n", i)
	}
	if _, err := col.LoadJSONL(strings.NewReader(sb.String()), ""); err != nil {
		t.Fatalf("LoadJSONL: %v", err)
	}

	for i := 0; i < n; i++ {
		ev := <-events
		if ev.Op != "insert" {
			t.Fatalf("event %d: op=%q, want insert", i, ev.Op)
		}
	}
}
