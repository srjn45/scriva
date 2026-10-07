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
		IndexPersistInterval: -1, IndexPersistMinInterval: -1, // only rotation may trigger a persist
		CompactInterval: time.Hour,
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

func TestIndexPersist_RotationsCoalesce(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	c, err := OpenCollection("c", data, CollectionConfig{
		SegmentMaxSize:          256,
		IndexPersistInterval:    -1,
		IndexPersistMinInterval: time.Hour, // rotations may only request; none may run a pass
		CompactInterval:         time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := 0; i < 200; i++ {
		if _, _, err := c.Insert(map[string]any{"payload": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}); err != nil {
			t.Fatal(err)
		}
	}
	c.mu.RLock()
	rotations := len(c.sealed)
	c.mu.RUnlock()
	if rotations < 10 {
		t.Fatalf("expected many rotations, got %d", rotations)
	}
	time.Sleep(100 * time.Millisecond)
	if n := c.persistPasses.Load(); n != 0 {
		t.Fatalf("%d rotations ran %d persist passes within the debounce window, want 0", rotations, n)
	}
}

func TestIndexPersist_RotationDebounceBoundsPasses(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	c, err := OpenCollection("c", data, CollectionConfig{
		SegmentMaxSize:          256,
		IndexPersistInterval:    -1,
		IndexPersistMinInterval: 150 * time.Millisecond,
		CompactInterval:         time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	start := time.Now()
	for i := 0; i < 300; i++ {
		if _, _, err := c.Insert(map[string]any{"payload": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "deferred pass", func() bool { return c.persistPasses.Load() >= 1 })
	elapsed := time.Since(start)
	max := int64(elapsed/(150*time.Millisecond)) + 1
	if n := c.persistPasses.Load(); n > max {
		t.Fatalf("%d passes in %v, want <= %d", n, elapsed, max)
	}
}

// A crash right after a rotation whose persist was still debounced must
// recover exactly the acknowledged state, including deletes.
func TestIndexPersist_CrashWithDebouncedPersistRecoversExactState(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	cfg := CollectionConfig{
		SegmentMaxSize:          256,
		IndexPersistInterval:    -1,
		IndexPersistMinInterval: time.Hour,
		CompactInterval:         time.Hour,
	}
	c, err := OpenCollection("c", data, cfg)
	if err != nil {
		t.Fatal(err)
	}
	live := map[uint64]bool{}
	var ids []uint64
	for i := 0; i < 60; i++ {
		id, _, err := c.Insert(map[string]any{"payload": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "i": i})
		if err != nil {
			t.Fatal(err)
		}
		live[id] = true
		ids = append(ids, id)
	}
	// Persist once (as a prior pass would have), then mutate across rotations.
	if err := c.persistIndexes(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		id, _, err := c.Insert(map[string]any{"payload": "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", "i": 100 + i})
		if err != nil {
			t.Fatal(err)
		}
		live[id] = true
	}
	for _, id := range ids[:20] { // deletes after the persisted snapshot must not resurrect
		if err := c.Delete(id); err != nil {
			t.Fatal(err)
		}
		delete(live, id)
	}
	// Crash: stop background work without Close's final persist.
	c.closeOnce.Do(func() { close(c.closed) })
	c.persistWG.Wait()
	if c.persistPasses.Load() != 0 {
		t.Fatal("debounced persist ran; test premise broken")
	}

	c2, err := OpenCollection("c", data, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if got := c2.index.Len(); got != len(live) {
		t.Fatalf("recovered %d entries, want %d", got, len(live))
	}
	for id := range live {
		if _, ok := c2.index.Get(id); !ok {
			t.Fatalf("acknowledged id %d missing", id)
		}
	}
	for _, id := range ids[:20] {
		if _, ok := c2.index.Get(id); ok {
			t.Fatalf("deleted id %d resurrected", id)
		}
	}
}

func TestIndexLoad_LegacyCoverageWithoutTailStillVerified(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	dir := filepath.Join(data, "c")
	c, err := OpenCollection("c", data, CollectionConfig{SegmentMaxSize: 256, IndexPersistInterval: -1, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, _, err := c.Insert(map[string]any{"payload": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Sealed segments carry a tail fingerprint and no full hash.
	idx := loadedCoverage(t, dir)
	if len(idx.coverage) < 2 || idx.coverage[0].Tail == "" || idx.coverage[0].Checksum != "" {
		t.Fatalf("unexpected coverage %+v", idx.coverage)
	}
	// A flipped byte in a sealed segment's tail forces a rebuild, not a trust.
	p := filepath.Join(dir, idx.coverage[0].Segment)
	b, _ := os.ReadFile(p)
	b[len(b)-2] ^= 1
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	c2, err := OpenCollection("c", data, CollectionConfig{IndexPersistInterval: -1, CompactInterval: time.Hour})
	if err != nil {
		t.Skipf("open refused corrupted segment (acceptable): %v", err)
	}
	defer func() { _ = c2.Close() }()
	if c2.indexRebuilds.Load() == 0 {
		t.Fatal("tail mismatch did not force a rebuild")
	}
}
