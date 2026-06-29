//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestIDCounterReconcileFromActiveSegment verifies that a stale meta.json (as
// can happen after a crash, now that meta is not rewritten per insert) does not
// cause id reuse: the counter is reconciled against the highest id present in
// the active segment on load.
func TestIDCounterReconcileFromActiveSegment(t *testing.T) {
	dir := t.TempDir()

	col, err := OpenCollection("users", dir, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	var lastID uint64
	for i := 0; i < 5; i++ {
		id, _, err := col.Insert(map[string]any{"n": float64(i)})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		lastID = id
	}
	if lastID != 5 {
		t.Fatalf("expected last id 5, got %d", lastID)
	}
	col.Close()

	// Simulate a crash that left meta.json trailing the true counter.
	metaPath := filepath.Join(dir, "users", metaFilename)
	if err := persistMeta(metaPath, collectionMeta{IDCounter: 2, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("rewrite stale meta: %v", err)
	}

	col2, err := OpenCollection("users", dir, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer col2.Close()

	// The next insert must not reuse an existing id.
	id, _, err := col2.Insert(map[string]any{"n": float64(99)})
	if err != nil {
		t.Fatalf("Insert after reopen: %v", err)
	}
	if id <= lastID {
		t.Errorf("id reuse after stale meta: got %d, want > %d", id, lastID)
	}
}

// TestMetaPersistedOnRotation verifies the id counter survives a reopen when a
// segment boundary was crossed (meta is persisted on rotation), without relying
// on a clean Close.
func TestMetaPersistedOnRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.SegmentMaxSize = 256 // tiny, so a few inserts force rotation

	col, err := OpenCollection("c", dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var lastID uint64
	for i := 0; i < 20; i++ {
		id, _, err := col.Insert(map[string]any{"name": "padding-to-cross-segment-size"})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		lastID = id
	}

	// Confirm a rotation actually happened.
	col.mu.RLock()
	sealed := len(col.sealed)
	col.mu.RUnlock()
	if sealed == 0 {
		t.Fatalf("expected at least one rotation, got %d sealed segments", sealed)
	}

	// meta.json should exist and carry a counter purely from rotation, with no
	// clean Close yet. It may trail lastID by the inserts made after the final
	// rotation — reconcile-on-load (asserted below) covers that gap.
	meta, err := loadMeta(filepath.Join(dir, "c", metaFilename))
	if err != nil {
		t.Fatalf("meta should be persisted on rotation: %v", err)
	}
	if meta.IDCounter == 0 {
		t.Errorf("meta counter should be set by rotation, got 0")
	}

	col.Close()
	col2, err := OpenCollection("c", dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer col2.Close()
	id, _, err := col2.Insert(map[string]any{"name": "next"})
	if err != nil {
		t.Fatalf("Insert after reopen: %v", err)
	}
	if id <= lastID {
		t.Errorf("id reuse after rotation reopen: got %d, want > %d", id, lastID)
	}
}

// TestWriteFileAtomicDurable verifies writeFileAtomic leaves no temp file behind
// and writes the exact bytes via the atomic rename path.
func TestWriteFileAtomicDurable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.json")

	if err := writeFileAtomic(path, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != `{"a":1}` {
		t.Errorf("content mismatch: %q", got)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file should not remain after atomic write")
	}

	// Overwrite must replace atomically.
	if err := writeFileAtomic(path, []byte(`{"a":2}`), 0o644); err != nil {
		t.Fatalf("writeFileAtomic overwrite: %v", err)
	}
	got, _ = os.ReadFile(path)
	if string(got) != `{"a":2}` {
		t.Errorf("overwrite mismatch: %q", got)
	}
}
