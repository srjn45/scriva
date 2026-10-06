package engine

import (
	"errors"
	"testing"

	"github.com/srjn45/scriva/query"
)

func TestGet_VerifyIdentity_CorruptOffset(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCollection("items", dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer c.Close()

	id1, _, err := c.Insert(map[string]any{"name": "record-1"})
	if err != nil {
		t.Fatalf("Insert 1: %v", err)
	}
	id2, _, err := c.Insert(map[string]any{"name": "record-2"})
	if err != nil {
		t.Fatalf("Insert 2: %v", err)
	}

	loc1, ok := c.index.Get(id1)
	if !ok {
		t.Fatalf("index missing id1")
	}
	loc2, ok := c.index.Get(id2)
	if !ok {
		t.Fatalf("index missing id2")
	}

	// Deliberately corrupt id1's index entry to point at id2's valid in-range offset.
	corrupted := loc1
	corrupted.Offset = loc2.Offset
	c.index.Set(id1, corrupted)

	// Get(id1) must fail with ErrIndexCorrupt and must NEVER return record-2's data.
	got, err := c.Get(id1)
	if err == nil {
		t.Fatalf("Get(id1) expected error, got record: %+v", got)
	}
	if !errors.Is(err, ErrIndexCorrupt) {
		t.Fatalf("Get(id1) expected ErrIndexCorrupt, got: %v", err)
	}

	var ie *IntegrityError
	if !errors.As(err, &ie) {
		t.Fatalf("expected *IntegrityError, got: %T (%v)", err, err)
	}
	if ie.ID != id1 {
		t.Errorf("IntegrityError ID = %d, want %d", ie.ID, id1)
	}
	if ie.FoundID != id2 {
		t.Errorf("IntegrityError FoundID = %d, want %d", ie.FoundID, id2)
	}
	if ie.Offset != loc2.Offset {
		t.Errorf("IntegrityError Offset = %d, want %d", ie.Offset, loc2.Offset)
	}
}

func TestScan_VerifyIdentity_CorruptOffset(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCollection("items", dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer c.Close()

	id1, _, err := c.Insert(map[string]any{"name": "record-1"})
	if err != nil {
		t.Fatalf("Insert 1: %v", err)
	}
	id2, _, err := c.Insert(map[string]any{"name": "record-2"})
	if err != nil {
		t.Fatalf("Insert 2: %v", err)
	}

	loc1, _ := c.index.Get(id1)
	loc2, _ := c.index.Get(id2)

	// Corrupt id1 to point to id2's offset
	corrupted := loc1
	corrupted.Offset = loc2.Offset
	c.index.Set(id1, corrupted)

	// Scan must surface the integrity error rather than silently omitting id1 or shortening results
	results, err := c.Scan(query.MatchAll)
	if err == nil {
		t.Fatalf("Scan expected error, got %d results: %+v", len(results), results)
	}
	if !errors.Is(err, ErrIndexCorrupt) {
		t.Fatalf("Scan expected ErrIndexCorrupt, got: %v", err)
	}
}

func TestScan_SecondaryIndex_VerifyIdentity_CorruptOffset(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCollection("items", dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer c.Close()

	if err := c.EnsureIndex("tag"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}

	id1, _, err := c.Insert(map[string]any{"tag": "alpha", "val": 1})
	if err != nil {
		t.Fatalf("Insert 1: %v", err)
	}
	id2, _, err := c.Insert(map[string]any{"tag": "alpha", "val": 2})
	if err != nil {
		t.Fatalf("Insert 2: %v", err)
	}

	loc1, _ := c.index.Get(id1)
	loc2, _ := c.index.Get(id2)

	// Corrupt id1's index entry to point to id2's offset
	corrupted := loc1
	corrupted.Offset = loc2.Offset
	c.index.Set(id1, corrupted)

	// Scan via secondary index must not silently drop candidate id1; it must return ErrIndexCorrupt
	filter := &query.FieldFilter{Field: "tag", Op: query.OpEq, Value: "alpha"}
	results, err := c.Scan(filter)
	if err == nil {
		t.Fatalf("Scan(secondary) expected error, got %d results: %+v", len(results), results)
	}
	if !errors.Is(err, ErrIndexCorrupt) {
		t.Fatalf("Scan(secondary) expected ErrIndexCorrupt, got: %v", err)
	}
}

func TestGetByKey_VerifyIdentity_CorruptOffset(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCollection("items", dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer c.Close()

	id1, _, err := c.InsertWithKey("key-1", map[string]any{"name": "record-1"})
	if err != nil {
		t.Fatalf("InsertWithKey 1: %v", err)
	}
	id2, _, err := c.InsertWithKey("key-2", map[string]any{"name": "record-2"})
	if err != nil {
		t.Fatalf("InsertWithKey 2: %v", err)
	}

	loc1, _ := c.index.Get(id1)
	loc2, _ := c.index.Get(id2)

	corrupted := loc1
	corrupted.Offset = loc2.Offset
	c.index.Set(id1, corrupted)

	got, err := c.GetByKey("key-1")
	if err == nil {
		t.Fatalf("GetByKey expected error, got: %+v", got)
	}
	if !errors.Is(err, ErrIndexCorrupt) {
		t.Fatalf("GetByKey expected ErrIndexCorrupt, got: %v", err)
	}
}
