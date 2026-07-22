//nolint:errcheck
package engine

import (
	"errors"
	"strings"
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

// TestCollectionInsertOversizeRecord covers issue #80 on the embedded path: an
// Insert whose encoded record exceeds the 16 MiB scan-buffer ceiling is
// rejected with ErrRecordTooLarge, and the collection stays usable afterward.
// (The gRPC surface can't reach this — its 4 MiB message limit sits below the
// ceiling — so this guard exists for the embedded façade and internal
// re-append paths.)
func TestCollectionInsertOversizeRecord(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("big", dir, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	// ~17 MiB value, comfortably over the ceiling once encoded.
	huge := strings.Repeat("a", 17*1024*1024)
	if _, _, err := col.Insert(map[string]any{"blob": huge}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Insert oversize: got %v, want ErrRecordTooLarge", err)
	}

	// The rejected write left nothing behind: a normal insert still works and
	// reads back.
	id, _, err := col.Insert(map[string]any{"name": "ok"})
	if err != nil {
		t.Fatalf("Insert after reject: %v", err)
	}
	data, _, err := col.FindByID(id)
	if err != nil || data["name"] != "ok" {
		t.Fatalf("FindByID after reject: data=%v err=%v", data, err)
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
