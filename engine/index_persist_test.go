package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func loadedCoverage(t *testing.T, dir string) *Index {
	t.Helper()
	idx := newIndex()
	if err := idx.Load(filepath.Join(dir, "index.json")); err != nil {
		t.Fatalf("load index: %v", err)
	}
	return idx
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestIndexPersist_IntervalPersistsWithoutClose(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	dir := filepath.Join(data, "c")
	c, err := OpenCollection("c", data, CollectionConfig{IndexPersistInterval: 20 * time.Millisecond, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := 0; i < 5; i++ {
		if _, _, err := c.Insert(map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "periodic index persist", func() bool {
		idx := newIndex()
		if err := idx.Load(filepath.Join(dir, "index.json")); err != nil {
			return false
		}
		return idx.Len() == 5
	})
}

func TestIndexPersist_RotationPersistsSealedSegment(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	dir := filepath.Join(data, "c")
	c, err := OpenCollection("c", data, CollectionConfig{
		SegmentMaxSize:       256,
		IndexPersistInterval: -1, // only rotation may trigger a persist
		CompactInterval:      time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := 0; i < 20; i++ {
		if _, _, err := c.Insert(map[string]any{"payload": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "rotation-triggered persist", func() bool {
		idx := newIndex()
		if err := idx.Load(filepath.Join(dir, "index.json")); err != nil {
			return false
		}
		return len(idx.coverage) > 1
	})
}

func TestIndexPersist_DisabledAndCloseIdempotentWithLoop(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	dir := filepath.Join(data, "c")
	c, err := OpenCollection("c", data, CollectionConfig{IndexPersistInterval: 5 * time.Millisecond, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // let the loop run concurrently with Close
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "index.json"))
	time.Sleep(30 * time.Millisecond)
	after, _ := os.ReadFile(filepath.Join(dir, "index.json"))
	if string(before) != string(after) {
		t.Fatal("index.json changed after Close")
	}
	if got := loadedCoverage(t, dir).Len(); got != 1 {
		t.Fatalf("index entries = %d, want 1", got)
	}
	if n := c.persistErr.Load(); n != 0 {
		t.Fatalf("background persist errors = %d", n)
	}
}

func TestIndexPersist_CrashAfterPersistReplaysBoundedTail(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	dir := filepath.Join(data, "c")
	c, err := OpenCollection("c", data, CollectionConfig{IndexPersistInterval: 20 * time.Millisecond, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, _, err := c.Insert(map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "persist", func() bool {
		idx := newIndex()
		return idx.Load(filepath.Join(dir, "index.json")) == nil && idx.Len() == 10
	})
	// Simulate a crash: stop background work without Close's final persist.
	c.closeOnce.Do(func() { close(c.closed) })
	c.persistWG.Wait()
	for i := 0; i < 3; i++ {
		if _, _, err := c.Insert(map[string]any{"i": 100 + i}); err != nil {
			t.Fatal(err)
		}
	}
	c2, err := OpenCollection("c", data, CollectionConfig{IndexPersistInterval: -1, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if got := c2.index.Len(); got != 13 {
		t.Fatalf("recovered %d entries, want 13", got)
	}
	if c2.indexRebuilds.Load() != 0 {
		t.Fatal("expected tail replay, got full rebuild")
	}
}
