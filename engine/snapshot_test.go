//nolint:errcheck
package engine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// extractTarGz unpacks a gzip-compressed tar archive into dir.
func extractTarGz(t *testing.T, data []byte, dir string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		target := filepath.Join(dir, hdr.Name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		f, err := os.Create(target)
		if err != nil {
			t.Fatalf("create %q: %v", target, err)
		}
		if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // trusted test archive
			t.Fatalf("copy: %v", err)
		}
		f.Close()
	}
}

func TestSnapshot_RoundTrip(t *testing.T) {
	src := t.TempDir()
	db, err := Open(src, CollectionConfig{
		SegmentMaxSize:  256, // tiny, so multiple sealed segments form
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	users, _ := db.CreateCollection("users")
	orders, _ := db.CreateCollection("orders")

	// Populate enough to roll several segments, plus updates and a delete so the
	// snapshot exercises multi-segment + stale-entry state.
	ids := make([]uint64, 0, 20)
	for i := 0; i < 20; i++ {
		id, _, _ := users.Insert(map[string]any{"n": i, "name": "user"})
		ids = append(ids, id)
	}
	users.Update(ids[0], map[string]any{"n": 999, "name": "updated"})
	users.Delete(ids[1])
	orders.Insert(map[string]any{"item": "widget"})

	// An index should survive too.
	users.EnsureIndex("name")

	// Take the snapshot into a buffer.
	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	db.Close()

	// Restore into a fresh data dir and reopen.
	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)

	restored, err := Open(dst, CollectionConfig{
		SegmentMaxSize:  256,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer restored.Close()

	// Both collections present.
	got := restored.ListCollections()
	if len(got) != 2 {
		t.Fatalf("restored collections: got %v, want users+orders", got)
	}

	ru, err := restored.Collection("users")
	if err != nil {
		t.Fatalf("restored users: %v", err)
	}

	// The updated record reflects the latest value.
	rec, _, err := ru.FindByID(ids[0])
	if err != nil {
		t.Fatalf("FindByID updated: %v", err)
	}
	if rec["n"] != float64(999) {
		t.Errorf("updated record n = %v, want 999", rec["n"])
	}

	// The deleted record stays gone.
	if _, _, err := ru.FindByID(ids[1]); err == nil {
		t.Error("deleted record visible after restore")
	}

	// Live count matches: 20 inserted, 1 deleted = 19.
	res, err := ru.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 19 {
		t.Errorf("restored users live count = %d, want 19", len(res))
	}

	// The secondary index was captured and still answers lookups.
	if idxs := ru.ListIndexes(); len(idxs) == 0 {
		t.Error("expected the 'name' secondary index to survive the snapshot")
	}
}

func TestSnapshot_EmptyDB(t *testing.T) {
	db, err := Open(t.TempDir(), CollectionConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo empty: %v", err)
	}

	// A valid, empty gzip tar must still extract cleanly.
	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)
}
