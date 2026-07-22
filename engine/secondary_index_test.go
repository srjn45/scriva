//nolint:errcheck
package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

// openIndexedCollection creates a test collection and ensures a secondary index
// on "name". The background compactor is stopped so tests control timing.
func openIndexedCollection(t *testing.T) *Collection {
	t.Helper()
	col := openTestCollection(t)
	if err := col.EnsureIndex("name"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	return col
}

// ---- EnsureIndex / ListIndexes / DropIndex ---------------------------------

func TestEnsureIndex_Idempotent(t *testing.T) {
	col := openTestCollection(t)
	if err := col.EnsureIndex("email"); err != nil {
		t.Fatalf("first EnsureIndex: %v", err)
	}
	if err := col.EnsureIndex("email"); err != nil {
		t.Fatalf("second EnsureIndex (idempotent) returned error: %v", err)
	}
	fields := col.ListIndexes()
	if len(fields) != 1 || fields[0] != "email" {
		t.Errorf("expected [email], got %v", fields)
	}
}

func TestDropIndex(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("city")

	if err := col.DropIndex("city"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if fields := col.ListIndexes(); len(fields) != 0 {
		t.Errorf("expected no indexes after drop, got %v", fields)
	}
	// Dropping again should return an error.
	if err := col.DropIndex("city"); err == nil {
		t.Error("expected error dropping non-existent index")
	}
}

func TestListIndexes_Sorted(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("zzz")
	col.EnsureIndex("aaa")
	col.EnsureIndex("mmm")

	fields := col.ListIndexes()
	if len(fields) != 3 || fields[0] != "aaa" || fields[1] != "mmm" || fields[2] != "zzz" {
		t.Errorf("expected sorted [aaa mmm zzz], got %v", fields)
	}
}

// ---- Insert / Update / Delete maintain the index ----------------------------

func TestSecondaryIndex_InsertAndLookup(t *testing.T) {
	col := openIndexedCollection(t)

	col.Insert(map[string]any{"name": "alice", "age": 30})
	col.Insert(map[string]any{"name": "bob", "age": 25})
	col.Insert(map[string]any{"name": "alice", "age": 40})

	ids, ok := col.IndexLookup("name", "alice")
	if !ok {
		t.Fatal("expected index hit for 'alice'")
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 ids for 'alice', got %d", len(ids))
	}

	ids, ok = col.IndexLookup("name", "bob")
	if !ok || len(ids) != 1 {
		t.Errorf("expected 1 id for 'bob', got %v (ok=%v)", ids, ok)
	}
}

func TestSecondaryIndex_UpdateMovesID(t *testing.T) {
	col := openIndexedCollection(t)

	id, _, _ := col.Insert(map[string]any{"name": "alice"})
	col.Update(id, map[string]any{"name": "carol"})

	if ids, _ := col.IndexLookup("name", "alice"); len(ids) != 0 {
		t.Errorf("expected alice bucket empty after update, got %v", ids)
	}
	if ids, _ := col.IndexLookup("name", "carol"); len(ids) != 1 || ids[0] != id {
		t.Errorf("expected id %d in carol bucket, got %v", id, ids)
	}
}

func TestSecondaryIndex_DeleteRemovesID(t *testing.T) {
	col := openIndexedCollection(t)

	id, _, _ := col.Insert(map[string]any{"name": "dave"})
	col.Delete(id)

	if ids, _ := col.IndexLookup("name", "dave"); len(ids) != 0 {
		t.Errorf("expected dave bucket empty after delete, got %v", ids)
	}
}

// ---- Scan uses the index for eq-filter -------------------------------------

func TestScan_UsesSecondaryIndex(t *testing.T) {
	col := openIndexedCollection(t)

	col.Insert(map[string]any{"name": "eve", "score": 10})
	col.Insert(map[string]any{"name": "eve", "score": 20})
	col.Insert(map[string]any{"name": "frank", "score": 30})

	results, err := col.Scan(&query.FieldFilter{Field: "name", Op: query.OpEq, Value: `"eve"`})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for name=eve, got %d", len(results))
	}
}

func TestScan_FallsBackWithoutIndex(t *testing.T) {
	col := openTestCollection(t) // no index on "role"

	col.Insert(map[string]any{"role": "admin"})
	col.Insert(map[string]any{"role": "user"})
	col.Insert(map[string]any{"role": "admin"})

	results, err := col.Scan(&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`})
	if err != nil {
		t.Fatalf("Scan (full): %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 admin results from full scan, got %d", len(results))
	}
}

// ---- Persist / Load round-trip ----------------------------------------------

func TestSecondaryIndex_PersistLoad(t *testing.T) {
	dir := t.TempDir()
	sidx := newSecondaryIndex("city", false)
	sidx.add("london", 1)
	sidx.add("london", 2)
	sidx.add("paris", 3)

	path := filepath.Join(dir, "sidx_city.json")
	if err := sidx.Persist(path); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	sidx2 := newSecondaryIndex("city", false)
	if err := sidx2.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	londonIDs := sidx2.Lookup("london")
	if len(londonIDs) != 2 {
		t.Errorf("expected 2 london ids after load, got %d", len(londonIDs))
	}
	parisIDs := sidx2.Lookup("paris")
	if len(parisIDs) != 1 || parisIDs[0] != 3 {
		t.Errorf("expected [3] for paris, got %v", parisIDs)
	}
}

// ---- EnsureIndex rebuilds from existing data --------------------------------

func TestEnsureIndex_RebuildsFromExistingData(t *testing.T) {
	col := openTestCollection(t) // no index yet

	col.Insert(map[string]any{"tag": "go"})
	col.Insert(map[string]any{"tag": "rust"})
	col.Insert(map[string]any{"tag": "go"})

	// Create the index after data already exists.
	if err := col.EnsureIndex("tag"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}

	ids, ok := col.IndexLookup("tag", "go")
	if !ok || len(ids) != 2 {
		t.Errorf("expected 2 ids for tag=go, got %v (ok=%v)", ids, ok)
	}
}

// ---- Index survives compaction ----------------------------------------------

func TestSecondaryIndex_SurvivesCompaction(t *testing.T) {
	col := &Collection{
		name:    "test",
		dir:     t.TempDir(),
		cfg:     CollectionConfig{SegmentMaxSize: 512, CompactInterval: 24 * time.Hour, CompactDirtyPct: 0.30},
		index:   newIndex(),
		sidxMap: make(map[string]*SecondaryIndex),
		watchers: make(map[uint64]*watcher),
		compactC: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	if err := col.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	col.closeOnce.Do(func() { close(col.closed) })
	select {
	case <-col.compactC:
	default:
	}
	t.Cleanup(func() { col.Close() })

	col.EnsureIndex("name")

	for i := 0; i < 4; i++ {
		col.Insert(map[string]any{"name": "alpha"})
	}
	col.rotateSegment()
	for id := uint64(1); id <= 4; id++ {
		col.Update(id, map[string]any{"name": "beta"})
	}
	col.rotateSegment()

	if err := col.compact(false); err != nil {
		t.Fatalf("compact: %v", err)
	}

	ids, ok := col.IndexLookup("name", "beta")
	if !ok || len(ids) != 4 {
		t.Errorf("expected 4 beta ids after compact, got %v (ok=%v)", ids, ok)
	}
	if alphaIDs, _ := col.IndexLookup("name", "alpha"); len(alphaIDs) != 0 {
		t.Errorf("expected alpha bucket empty after compact, got %v", alphaIDs)
	}
}
