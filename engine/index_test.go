//nolint:errcheck
package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/store"
)

func TestIndexSetGetDelete(t *testing.T) {
	idx := newIndex()

	// Not present initially.
	if _, ok := idx.Get(1); ok {
		t.Fatal("expected id 1 to be absent")
	}

	// Set and retrieve.
	entry := IndexEntry{SegmentPath: "seg_000001.ndjson", Offset: 42}
	idx.Set(1, entry)
	got, ok := idx.Get(1)
	if !ok {
		t.Fatal("expected id 1 to be present after Set")
	}
	if got != entry {
		t.Fatalf("got %+v, want %+v", got, entry)
	}

	// Delete and confirm gone.
	idx.Delete(1)
	if _, ok := idx.Get(1); ok {
		t.Fatal("expected id 1 to be absent after Delete")
	}

	if idx.Len() != 0 {
		t.Fatalf("expected Len 0, got %d", idx.Len())
	}
}

func TestIndexLen(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 5; i++ {
		idx.Set(i, IndexEntry{SegmentPath: "s", Offset: int64(i)})
	}
	if idx.Len() != 5 {
		t.Fatalf("expected Len 5, got %d", idx.Len())
	}
	idx.Delete(3)
	if idx.Len() != 4 {
		t.Fatalf("expected Len 4 after delete, got %d", idx.Len())
	}
}

func TestIndexPersistAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")

	idx := newIndex()
	idx.Set(1, IndexEntry{SegmentPath: filepath.Join(dir, "seg_000001.ndjson"), Offset: 0})
	idx.Set(2, IndexEntry{SegmentPath: filepath.Join(dir, "seg_000001.ndjson"), Offset: 100})

	if err := idx.Persist(path); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	idx2 := newIndex()
	if err := idx2.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	for _, id := range []uint64{1, 2} {
		got, ok := idx2.Get(id)
		want, _ := idx.Get(id)
		if !ok {
			t.Fatalf("id %d missing after load", id)
		}
		if got != want {
			t.Fatalf("id %d: got %+v, want %+v", id, got, want)
		}
	}
}

func TestIndexLoadChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")

	idx := newIndex()
	idx.Set(1, IndexEntry{SegmentPath: "s", Offset: 0})
	idx.Persist(path)

	// Corrupt the file.
	b, _ := os.ReadFile(path)
	b[len(b)-5] ^= 0xFF
	os.WriteFile(path, b, 0o644)

	idx2 := newIndex()
	err := idx2.Load(path)
	if !errors.Is(err, ErrIndexStale) {
		t.Fatalf("expected ErrIndexStale, got %v", err)
	}
}

func TestIndexLoadNotExist(t *testing.T) {
	idx := newIndex()
	err := idx.Load("/nonexistent/path/index.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	// Should NOT be ErrIndexStale — it's a file-not-found error.
	if errors.Is(err, ErrIndexStale) {
		t.Fatal("expected os.ErrNotExist-type error, not ErrIndexStale")
	}
}

func TestIndexRebuildFromSegments(t *testing.T) {
	dir := t.TempDir()

	// Build a segment with insert(1), insert(2), update(1), delete(2).
	segPath := filepath.Join(dir, "seg_000001.ndjson")
	seg, err := openActiveSegment(segPath)
	if err != nil {
		t.Fatal(err)
	}
	seg.Append(store.NewInsert(1, map[string]any{"name": "Alice"}))
	seg.Append(store.NewInsert(2, map[string]any{"name": "Bob"}))
	seg.Append(store.NewUpdate(1, map[string]any{"name": "Alice2"}))
	seg.Append(store.NewDelete(2))
	seg.Close()

	// Reopen as a sealed segment for rebuild.
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatal(err)
	}
	sealed := openSealedSegment(segPath, info.Size())

	idx := newIndex()
	if err := idx.Rebuild([]*Segment{sealed}); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// id 1 should be present (updated), id 2 should be absent (deleted).
	if _, ok := idx.Get(1); !ok {
		t.Error("expected id 1 to be in index after rebuild")
	}
	if _, ok := idx.Get(2); ok {
		t.Error("expected id 2 to be absent after delete in rebuild")
	}
	if idx.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", idx.Len())
	}
}

func TestIndexV2RelativePathsAndCoverage(t *testing.T) {
	dir := t.TempDir()
	segPath := filepath.Join(dir, "seg_000001.ndjson")
	if err := os.WriteFile(segPath, []byte("hello\nworld\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	seg, err := openActiveSegment(segPath)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	idx := newIndex()
	idx.Set(1, IndexEntry{SegmentPath: segPath, Offset: 0, Rev: 3})
	snap, err := idx.Snapshot([]*Segment{seg})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "index.json")
	if err := snap.Persist(path); err != nil {
		t.Fatal(err)
	}

	raw, _ := os.ReadFile(path)
	var f indexFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.Version != indexFormatV2 || f.Entries[1].SegmentPath != "seg_000001.ndjson" {
		t.Fatalf("not v2/relative: %s", raw)
	}

	// Move the directory; entries must resolve under the new location.
	dst := filepath.Join(t.TempDir(), "moved")
	if err := os.Rename(dir, dst); err != nil {
		t.Fatal(err)
	}
	idx2 := newIndex()
	if err := idx2.Load(filepath.Join(dst, "index.json")); err != nil {
		t.Fatal(err)
	}
	e, _ := idx2.Get(1)
	if e.SegmentPath != filepath.Join(dst, "seg_000001.ndjson") || e.Rev != 3 {
		t.Fatalf("bad entry %+v", e)
	}
	sum := sha256.Sum256([]byte("hello\nworld\n"))
	cov := idx2.Coverage()
	if len(cov) != 1 || cov[0].Segment != "seg_000001.ndjson" || cov[0].Size != 12 || cov[0].Checksum != hex.EncodeToString(sum[:]) {
		t.Fatalf("bad coverage %+v", cov)
	}
}

func TestIndexV2TamperedChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")
	idx := newIndex()
	idx.Set(1, IndexEntry{SegmentPath: filepath.Join(dir, "s"), Offset: 5})
	if err := idx.Persist(path); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	raw = bytes.Replace(raw, []byte(`"offset":5`), []byte(`"offset":6`), 1)
	_ = os.WriteFile(path, raw, 0o644)
	if err := newIndex().Load(path); !errors.Is(err, ErrIndexStale) {
		t.Fatalf("want ErrIndexStale, got %v", err)
	}
	// Unknown version is rejected as stale.
	_ = os.WriteFile(path, []byte(`{"version":99,"entries":{},"checksum":"x"}`), 0o644)
	if err := newIndex().Load(path); !errors.Is(err, ErrIndexStale) {
		t.Fatalf("want ErrIndexStale for unknown version, got %v", err)
	}
}

func TestIndexLoadsV1AbsolutePaths(t *testing.T) {
	dir := t.TempDir()
	entries := map[uint64]IndexEntry{7: {SegmentPath: "/old/location/seg_000002.ndjson", Offset: 9, Rev: 2}}
	payload, _ := json.Marshal(entries)
	sum := sha256.Sum256(payload)
	b, _ := json.Marshal(map[string]any{"entries": entries, "checksum": hex.EncodeToString(sum[:])})
	path := filepath.Join(dir, "index.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	idx := newIndex()
	if err := idx.Load(path); err != nil {
		t.Fatal(err)
	}
	e, _ := idx.Get(7)
	if e.SegmentPath != filepath.Join(dir, "seg_000002.ndjson") || e.Offset != 9 || e.Rev != 2 {
		t.Fatalf("bad entry %+v", e)
	}
	if idx.Coverage() != nil && len(idx.Coverage()) != 0 {
		t.Fatalf("v1 should have no coverage")
	}
}
