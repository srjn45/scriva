//nolint:errcheck
package engine

import (
	"testing"
	"time"
)

func testCfg() CollectionConfig {
	cfg := defaultConfig()
	cfg.CompactInterval = 24 * time.Hour // disable automatic compaction in tests
	return cfg
}

func TestCollectionInsertFindByID(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("users", dir, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	id, _, err := col.Insert(map[string]any{"name": "alice", "age": float64(30)})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	data, _, err := col.FindByID(id)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if data["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", data["name"])
	}
}

func TestCollectionUpdate(t *testing.T) {
	dir := t.TempDir()
	col, _ := OpenCollection("users", dir, testCfg())
	defer col.Close()

	id, _, _ := col.Insert(map[string]any{"name": "bob"})
	if _, err := col.Update(id, map[string]any{"name": "bob2"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	data, _, err := col.FindByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if data["name"] != "bob2" {
		t.Errorf("expected bob2, got %v", data["name"])
	}
}

func TestCollectionDelete(t *testing.T) {
	dir := t.TempDir()
	col, _ := OpenCollection("users", dir, testCfg())
	defer col.Close()

	id, _, _ := col.Insert(map[string]any{"name": "carol"})
	if err := col.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, _, err := col.FindByID(id)
	if err == nil {
		t.Error("expected error after delete, got nil")
	}
}

func TestCollectionScan(t *testing.T) {
	dir := t.TempDir()
	col, _ := OpenCollection("items", dir, testCfg())
	defer col.Close()

	for _, name := range []string{"apple", "banana", "apricot", "cherry"} {
		col.Insert(map[string]any{"name": name})
	}

	results, err := col.Scan(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 results, got %d", len(results))
	}
}

func TestCollectionPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	col, _ := OpenCollection("users", dir, testCfg())
	id, _, _ := col.Insert(map[string]any{"name": "dave"})
	col.Close()

	// Re-open.
	col2, err := OpenCollection("users", dir, testCfg())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer col2.Close()

	data, _, err := col2.FindByID(id)
	if err != nil {
		t.Fatalf("FindByID after reopen: %v", err)
	}
	if data["name"] != "dave" {
		t.Errorf("expected dave, got %v", data["name"])
	}
}

func TestCollectionConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	col, _ := OpenCollection("concurrent", dir, testCfg())
	defer col.Close()

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func(n int) {
			col.Insert(map[string]any{"n": float64(n)})
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 20; i++ {
		<-done
	}

	results, _ := col.Scan(nil)
	if len(results) != 20 {
		t.Errorf("expected 20 records after concurrent inserts, got %d", len(results))
	}
}

func TestCollectionWatcher(t *testing.T) {
	dir := t.TempDir()
	col, _ := OpenCollection("watched", dir, testCfg())
	defer col.Close()

	_, ch, cancel := col.Subscribe()
	defer cancel()

	col.Insert(map[string]any{"x": float64(1)})

	select {
	case ev := <-ch:
		if ev.Data["x"] != float64(1) {
			t.Errorf("unexpected event data: %v", ev.Data)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for watch event")
	}
}
