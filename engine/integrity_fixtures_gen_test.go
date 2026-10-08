package engine

//go:generate go test -run ^TestGenerateIntegrityFixtures$ -count=1 . -update-integrity-fixtures

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// The synthetic incident fixtures under testdata/integrity are generated, never
// hand-edited: `go generate ./engine` rewrites them from the builders below,
// and TestIntegrityFixturesUpToDate fails if the committed bytes drift. They
// contain only invented records (v1, v2, ...), fixed timestamps and no host
// paths, so regeneration is byte-for-byte reproducible.

var updateIntegrityFixtures = flag.Bool("update-integrity-fixtures", false, "rewrite testdata/integrity from the builders")

const integrityFixtureRoot = "testdata/integrity"

// fixtureBuilder lays out one data directory (collection "c") under root.
type fixtureBuilder func(t testing.TB, colDir string)

var integrityFixtureBuilders = map[string]fixtureBuilder{
	"stale-crash-index": buildStaleCrashIndex,
	"orphan-segment":    buildOrphanSegment,
	"wrong-offset":      buildWrongOffset,
	"glued-corrupt":     buildGluedCorrupt,
	"conflicting-dupes": buildConflictingDupes,
	"relocated-v1":      buildRelocatedV1,
}

var fixtureEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func fxEntry(t testing.TB, id uint64, op store.Op, rev uint64, v string) []byte {
	t.Helper()
	e := store.Entry{ID: id, Op: op, Rev: rev, Ts: fixtureEpoch.Add(time.Duration(id) * time.Second)}
	if op != store.OpDelete {
		e.Data = map[string]any{"v": v}
	}
	b, err := store.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fxCat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func fxWrite(t testing.TB, path string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func fxMeta(t testing.TB, colDir string, counter uint64) {
	t.Helper()
	b, err := json.MarshalIndent(collectionMeta{IDCounter: counter, CreatedAt: fixtureEpoch}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fxWrite(t, filepath.Join(colDir, metaFilename), append(b, '\n'))
}

// fxIndex persists a v2 index (with coverage) describing exactly the bytes now
// on disk in the named segments.
func fxIndex(t testing.TB, colDir string, names ...string) *Index {
	t.Helper()
	var segs []*Segment
	for _, n := range names {
		p := filepath.Join(colDir, n)
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		segs = append(segs, openSealedSegment(p, fi.Size()))
	}
	idx := newIndex()
	if err := idx.Rebuild(segs); err != nil {
		t.Fatal(err)
	}
	snap, err := idx.Snapshot(segs)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Persist(filepath.Join(colDir, "index.json")); err != nil {
		t.Fatal(err)
	}
	return idx
}

func fxAppend(t testing.TB, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func fxBase(t testing.TB) []byte {
	return fxCat(
		fxEntry(t, 1, store.OpInsert, 1, "v1"),
		fxEntry(t, 2, store.OpInsert, 1, "v2"),
		fxEntry(t, 3, store.OpInsert, 1, "v3"),
		fxEntry(t, 4, store.OpInsert, 1, "v4"),
	)
}

// Index persisted after v1..v4; the process then crashed after appending an
// insert (5), an update (1) and a delete (4). The id counter on disk is stale.
func buildStaleCrashIndex(t testing.TB, colDir string) {
	seg := filepath.Join(colDir, "seg_000001.ndjson")
	fxWrite(t, seg, fxBase(t))
	fxIndex(t, colDir, "seg_000001.ndjson")
	fxMeta(t, colDir, 4)
	fxAppend(t, seg, fxCat(
		fxEntry(t, 5, store.OpInsert, 1, "late"),
		fxEntry(t, 1, store.OpUpdate, 2, "updated"),
		fxEntry(t, 4, store.OpDelete, 2, ""),
	))
}

// Three segments; the persisted index omits the oldest, as after a rotation race.
func buildOrphanSegment(t testing.TB, colDir string) {
	fxWrite(t, filepath.Join(colDir, "seg_000001.ndjson"), fxCat(
		fxEntry(t, 1, store.OpInsert, 1, "v1"), fxEntry(t, 2, store.OpInsert, 1, "v2")))
	fxWrite(t, filepath.Join(colDir, "seg_000002.ndjson"), fxCat(
		fxEntry(t, 3, store.OpInsert, 1, "v3"), fxEntry(t, 2, store.OpDelete, 2, "")))
	fxWrite(t, filepath.Join(colDir, "seg_000003.ndjson"), fxCat(
		fxEntry(t, 4, store.OpInsert, 1, "v4")))
	fxIndex(t, colDir, "seg_000002.ndjson", "seg_000003.ndjson")
	fxMeta(t, colDir, 4)
}

// A well-formed index whose offsets point at the wrong place: id 1 at id 2's
// record (a valid line boundary, wrong identity), id 3 into the middle of a line.
func buildWrongOffset(t testing.TB, colDir string) {
	fxWrite(t, filepath.Join(colDir, "seg_000001.ndjson"), fxBase(t))
	idx := fxIndex(t, colDir, "seg_000001.ndjson")
	segs := []*Segment{openSealedSegment(filepath.Join(colDir, "seg_000001.ndjson"), int64(len(fxBase(t))))}
	idx.entries[1] = idx.entries[2]
	mid := idx.entries[3]
	mid.Offset += 3
	idx.entries[3] = mid
	snap, err := idx.Snapshot(segs)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Persist(filepath.Join(colDir, "index.json")); err != nil {
		t.Fatal(err)
	}
	fxMeta(t, colDir, 4)
}

// A partial write for id 50 was never terminated, and the next writer's good
// record (id 5) landed on the same line: one corrupt region with a valid
// entry glued behind it. Nothing indexes it (no index.json).
func buildGluedCorrupt(t testing.TB, colDir string) {
	fxWrite(t, filepath.Join(colDir, "seg_000001.ndjson"), fxCat(
		fxBase(t),
		[]byte(`{"id":50,"op":"insert","data":{"v":"par`),
		fxEntry(t, 5, store.OpInsert, 1, "glued"),
		fxEntry(t, 6, store.OpInsert, 1, "v6"),
	))
	fxMeta(t, colDir, 6)
}

// Conflicting history: id 2 inserted twice with different data, id 3 deleted
// and then re-inserted (id reuse), id 4 deleted and left deleted.
func buildConflictingDupes(t testing.TB, colDir string) {
	fxWrite(t, filepath.Join(colDir, "seg_000001.ndjson"), fxCat(
		fxBase(t),
		fxEntry(t, 2, store.OpInsert, 1, "v2-conflict"),
		fxEntry(t, 3, store.OpDelete, 2, ""),
		fxEntry(t, 3, store.OpInsert, 1, "v3-reused"),
		fxEntry(t, 4, store.OpDelete, 2, ""),
	))
	fxMeta(t, colDir, 4)
}

// An index.json written by an old server (v1: bare entries, absolute segment
// paths) for a directory that has since been moved to a different location.
func buildRelocatedV1(t testing.TB, colDir string) {
	fxWrite(t, filepath.Join(colDir, "seg_000001.ndjson"), fxBase(t))
	idx := fxIndex(t, colDir, "seg_000001.ndjson")
	entries := map[uint64]IndexEntry{}
	for id, e := range idx.entries {
		e.SegmentPath = "/srv/old-host/scriva/c/seg_000001.ndjson"
		entries[id] = e
	}
	pb, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(pb)
	b, err := json.Marshal(indexFile{Entries: entries, Checksum: hex.EncodeToString(sum[:])})
	if err != nil {
		t.Fatal(err)
	}
	fxWrite(t, filepath.Join(colDir, "index.json"), b)
	fxMeta(t, colDir, 4)
}

func buildFixtureTree(t testing.TB, root string) {
	for name, b := range integrityFixtureBuilders {
		b(t, filepath.Join(root, name, "c"))
	}
}

func TestGenerateIntegrityFixtures(t *testing.T) {
	if !*updateIntegrityFixtures {
		t.Skip("run `go generate ./engine` to rewrite the fixtures")
	}
	// Absolute, so the persisted index stores clean base-relative segment paths.
	root, err := filepath.Abs(integrityFixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	buildFixtureTree(t, root)
}

// The committed fixtures must be exactly what the builders produce.
func TestIntegrityFixturesUpToDate(t *testing.T) {
	fresh := t.TempDir()
	buildFixtureTree(t, fresh)
	if got, want := treeDigest(t, integrityFixtureRoot), treeDigest(t, fresh); got != want {
		t.Fatalf("testdata/integrity is stale; run `go generate ./engine`\ncommitted: %s\nbuilders:  %s", got, want)
	}
}

func treeDigest(t testing.TB, root string) string {
	t.Helper()
	h := sha256.New()
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		fmt.Fprintf(h, "%s:%d:", filepath.ToSlash(rel), len(b))
		h.Write(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
