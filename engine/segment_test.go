//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/filedbv2/store"
)

func TestSegmentAppendAndReadAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_000001.ndjson")

	seg, err := openActiveSegment(path)
	if err != nil {
		t.Fatalf("openActiveSegment: %v", err)
	}
	defer seg.Close()

	entries := []store.Entry{
		store.NewInsert(1, map[string]any{"name": "alice"}),
		store.NewInsert(2, map[string]any{"name": "bob"}),
		store.NewUpdate(1, map[string]any{"name": "alice2"}),
	}

	offsets := make([]int64, len(entries))
	for i, e := range entries {
		off, err := seg.Append(e)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		offsets[i] = off
	}

	for i, e := range entries {
		got, err := seg.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("ReadAt[%d]: %v", i, err)
		}
		if got.ID != e.ID || got.Op != e.Op {
			t.Errorf("[%d] got {%d %s} want {%d %s}", i, got.ID, got.Op, e.ID, e.Op)
		}
	}
}

func TestSegmentScanAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_000001.ndjson")

	seg, err := openActiveSegment(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 5; i++ {
		if _, err := seg.Append(store.NewInsert(i, map[string]any{"i": i})); err != nil {
			t.Fatal(err)
		}
	}
	seg.Close()

	sealed := openSealedSegment(path, seg.Size())
	entries, err := sealed.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}
}

func TestSegmentCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_crash.ndjson")

	// Write two valid entries then append a partial line (simulates crash).
	seg, err := openActiveSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	seg.Append(store.NewInsert(1, map[string]any{"x": 1}))
	seg.Append(store.NewInsert(2, map[string]any{"x": 2}))
	seg.Close()

	// Corrupt the file by appending half a JSON object.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"id":3,"op":"insert","ts":"2026-01`) // no closing brace or newline
	f.Close()

	// Re-open: crash recovery should truncate the partial line.
	seg2, err := openActiveSegment(path)
	if err != nil {
		t.Fatalf("openActiveSegment after crash: %v", err)
	}
	defer seg2.Close()

	entries, err := seg2.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll after recovery: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries after recovery, got %d", len(entries))
	}
}

func TestSegmentSeal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_000001.ndjson")

	seg, _ := openActiveSegment(path)
	seg.Append(store.NewInsert(1, map[string]any{"a": 1}))

	if err := seg.Seal(); err != nil {
		t.Fatal(err)
	}

	// Append after seal must fail.
	_, err := seg.Append(store.NewInsert(2, map[string]any{"a": 2}))
	if err == nil {
		t.Error("expected error appending to sealed segment")
	}
}
