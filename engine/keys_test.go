//nolint:errcheck
package engine

import (
	"errors"
	"testing"
)

// ---- CRUD by string key -----------------------------------------------------

func TestKeyedCRUD(t *testing.T) {
	col := openTestCollection(t)

	id, _, err := col.InsertWithKey("sess-abc123", map[string]any{"status": "open"})
	if err != nil {
		t.Fatalf("InsertWithKey: %v", err)
	}

	// FindByKey resolves the same record.
	data, _, err := col.FindByKey("sess-abc123")
	if err != nil {
		t.Fatalf("FindByKey: %v", err)
	}
	if data["status"] != "open" {
		t.Errorf("expected status=open, got %v", data["status"])
	}
	if data[KeyField] != "sess-abc123" {
		t.Errorf("expected _key stamped into data, got %v", data[KeyField])
	}
	// FindByKey and FindByID must agree on the same record.
	if byID, _, err := col.FindByID(id); err != nil || byID["status"] != "open" {
		t.Errorf("FindByID(%d) disagreed with FindByKey: %v, %v", id, byID, err)
	}

	// UpdateByKey overwrites, preserving the key.
	if _, err := col.UpdateByKey("sess-abc123", map[string]any{"status": "closed"}); err != nil {
		t.Fatalf("UpdateByKey: %v", err)
	}
	data, _, err = col.FindByKey("sess-abc123")
	if err != nil {
		t.Fatalf("FindByKey after update: %v", err)
	}
	if data["status"] != "closed" {
		t.Errorf("expected status=closed after update, got %v", data["status"])
	}
	if data[KeyField] != "sess-abc123" {
		t.Errorf("update dropped the _key, got %v", data[KeyField])
	}

	// DeleteByKey removes the record.
	if err := col.DeleteByKey("sess-abc123"); err != nil {
		t.Fatalf("DeleteByKey: %v", err)
	}
	if _, _, err := col.FindByKey("sess-abc123"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestKeyedMissingKeyErrors(t *testing.T) {
	col := openTestCollection(t)

	// Before any keyed write the _key index does not exist yet; still ErrKeyNotFound.
	if _, _, err := col.FindByKey("nope"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("FindByKey on empty collection: expected ErrKeyNotFound, got %v", err)
	}

	col.InsertWithKey("present", map[string]any{"x": 1})

	if _, _, err := col.FindByKey("absent"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("FindByKey absent: expected ErrKeyNotFound, got %v", err)
	}
	if _, err := col.UpdateByKey("absent", map[string]any{"x": 2}); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("UpdateByKey absent: expected ErrKeyNotFound, got %v", err)
	}
	if err := col.DeleteByKey("absent"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("DeleteByKey absent: expected ErrKeyNotFound, got %v", err)
	}
}

// ---- Duplicate key rejection ------------------------------------------------

func TestKeyedDuplicateRejected(t *testing.T) {
	col := openTestCollection(t)

	if _, _, err := col.InsertWithKey("dup", map[string]any{"n": 1}); err != nil {
		t.Fatalf("first InsertWithKey: %v", err)
	}
	_, _, err := col.InsertWithKey("dup", map[string]any{"n": 2})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey on duplicate key, got %v", err)
	}

	// The rejected insert must not have added a record.
	if got := col.Stats().RecordCount; got != 1 {
		t.Errorf("expected 1 record after rejected duplicate, got %d", got)
	}
	// The surviving record keeps its original data.
	data, _, err := col.FindByKey("dup")
	if err != nil {
		t.Fatalf("FindByKey: %v", err)
	}
	if data["n"] != float64(1) && data["n"] != 1 {
		t.Errorf("expected n=1 preserved, got %v", data["n"])
	}
}

// A key freed by delete can be reused.
func TestKeyedReuseAfterDelete(t *testing.T) {
	col := openTestCollection(t)

	col.InsertWithKey("k", map[string]any{"v": 1})
	if err := col.DeleteByKey("k"); err != nil {
		t.Fatalf("DeleteByKey: %v", err)
	}
	if _, _, err := col.InsertWithKey("k", map[string]any{"v": 2}); err != nil {
		t.Fatalf("re-insert after delete should succeed, got %v", err)
	}
	data, _, _ := col.FindByKey("k")
	if data["v"] != float64(2) && data["v"] != 2 {
		t.Errorf("expected reused key to hold new value, got %v", data["v"])
	}
}

// ---- Reserved-field enforcement on the plain API ----------------------------

func TestReservedKeyFieldRejected(t *testing.T) {
	col := openTestCollection(t)

	// Plain Insert may not set _key directly.
	if _, _, err := col.Insert(map[string]any{"_key": "sneaky", "x": 1}); !errors.Is(err, ErrReservedField) {
		t.Errorf("Insert with _key: expected ErrReservedField, got %v", err)
	}
	// The rejected insert wrote nothing.
	if got := col.Stats().RecordCount; got != 0 {
		t.Errorf("expected 0 records after rejected insert, got %d", got)
	}

	// A normal insert, then a plain Update that tries to set _key.
	id, _, err := col.Insert(map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("plain Insert: %v", err)
	}
	if _, err := col.Update(id, map[string]any{"_key": "sneaky"}); !errors.Is(err, ErrReservedField) {
		t.Errorf("Update with _key: expected ErrReservedField, got %v", err)
	}

	// InsertWithKey/UpdateByKey also reject _key smuggled inside data.
	if _, _, err := col.InsertWithKey("k", map[string]any{"_key": "other"}); !errors.Is(err, ErrReservedField) {
		t.Errorf("InsertWithKey with _key in data: expected ErrReservedField, got %v", err)
	}
	col.InsertWithKey("real", map[string]any{"x": 1})
	if _, err := col.UpdateByKey("real", map[string]any{"_key": "other"}); !errors.Is(err, ErrReservedField) {
		t.Errorf("UpdateByKey with _key in data: expected ErrReservedField, got %v", err)
	}
}

// ---- _key visible in Watch events -------------------------------------------

func TestKeyedWatchCarriesKey(t *testing.T) {
	col := openTestCollection(t)

	_, ch, cancel := col.Subscribe()
	defer cancel()

	if _, _, err := col.InsertWithKey("watch-key", map[string]any{"status": "open"}); err != nil {
		t.Fatalf("InsertWithKey: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Op != "insert" {
			t.Fatalf("expected insert event, got %v", ev.Op)
		}
		if ev.Data[KeyField] != "watch-key" {
			t.Errorf("expected _key in watch event data, got %v", ev.Data[KeyField])
		}
	default:
		t.Fatal("expected a watch event, got none")
	}
}

// ---- Round-trip: compaction + reopen ----------------------------------------

func TestKeyedSurvivesCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write several keyed records, update and delete some, then force a
	// compaction so stale entries are physically dropped.
	func() {
		col := openUniqueCollection(t, dir)

		for _, k := range []string{"a", "b", "c", "d"} {
			if _, _, err := col.InsertWithKey(k, map[string]any{"v": k}); err != nil {
				t.Fatalf("InsertWithKey %q: %v", k, err)
			}
		}
		// Mutate the live set so compaction has stale versions to reclaim.
		if _, err := col.UpdateByKey("b", map[string]any{"v": "b2"}); err != nil {
			t.Fatalf("UpdateByKey b: %v", err)
		}
		if err := col.DeleteByKey("c"); err != nil {
			t.Fatalf("DeleteByKey c: %v", err)
		}

		// Rotate then compact so the sealed segments are rewritten.
		col.rotateSegment()
		if err := col.compact(); err != nil {
			t.Fatalf("compact: %v", err)
		}

		// Still correct in-process after compaction (O(1) key lookups).
		if data, _, err := col.FindByKey("a"); err != nil || data["v"] != "a" {
			t.Errorf("after compaction FindByKey(a) = %v, %v", data, err)
		}
		if data, _, err := col.FindByKey("b"); err != nil || data["v"] != "b2" {
			t.Errorf("after compaction FindByKey(b) = %v, %v", data, err)
		}
		if _, _, err := col.FindByKey("c"); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("after compaction FindByKey(c): expected ErrKeyNotFound, got %v", err)
		}
		col.Close()
	}()

	// Phase 2: reopen from disk and confirm the keyed records survive, the _key
	// index still enforces uniqueness, and lookups still resolve.
	col := openUniqueCollection(t, dir)

	if data, _, err := col.FindByKey("a"); err != nil || data["v"] != "a" {
		t.Errorf("after reopen FindByKey(a) = %v, %v", data, err)
	}
	if data, _, err := col.FindByKey("b"); err != nil || data["v"] != "b2" {
		t.Errorf("after reopen FindByKey(b) = %v, %v", data, err)
	}
	if data, _, err := col.FindByKey("d"); err != nil || data["v"] != "d" {
		t.Errorf("after reopen FindByKey(d) = %v, %v", data, err)
	}
	if _, _, err := col.FindByKey("c"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("after reopen FindByKey(c): expected ErrKeyNotFound, got %v", err)
	}

	// Uniqueness is still enforced after reopen (index + unique flag survived).
	if _, _, err := col.InsertWithKey("a", map[string]any{"v": "dup"}); !errors.Is(err, ErrDuplicateKey) {
		t.Errorf("expected duplicate key rejected after reopen, got %v", err)
	}
	// The freed key from the delete can be reused after reopen.
	if _, _, err := col.InsertWithKey("c", map[string]any{"v": "c-again"}); err != nil {
		t.Errorf("expected reuse of deleted key after reopen, got %v", err)
	}
}
