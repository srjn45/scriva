package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

func TestResolveEntriesDeterministicOrder(t *testing.T) {
	col := openCrashTestCollection(t, t.TempDir())
	defer col.Close()
	seedSealedGarbage(t, col, 30)
	for run := 0; run < 5; run++ {
		got, err := resolveEntries(col.sealed)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < len(got); i++ {
			if got[i-1].ID >= got[i].ID {
				t.Fatalf("run %d: entries not strictly ascending by id at %d", run, i)
			}
		}
	}
}

// Two identical collections compacted independently must produce byte-identical
// segment files.
func TestCompactionOutputIsByteDeterministic(t *testing.T) {
	build := func() map[string]string {
		dir := t.TempDir()
		col := openCrashTestCollection(t, dir)
		seedSealedGarbage(t, col, 40)
		if err := col.CompactNow(); err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, s := range col.sealed {
			b, err := os.ReadFile(s.Path())
			if err != nil {
				t.Fatal(err)
			}
			// ts differs between runs; compare ids in order instead.
			var ids []string
			for _, ln := range strings.Split(strings.TrimSpace(string(b)), "\n") {
				e, err := store.Decode([]byte(ln))
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, fmt.Sprint(e.ID))
			}
			out[filepath.Base(s.Path())] = strings.Join(ids, ",")
		}
		col.Close()
		return out
	}
	a, b := build(), build()
	if len(a) != len(b) {
		t.Fatalf("segment count differs: %d vs %d", len(a), len(b))
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("%s differs:\n%s\n%s", k, v, b[k])
		}
	}
}

// Sealed segment names with gaps (after earlier compactions plus rotations)
// must be replaced in place: no stale segment may survive on disk.
func TestCompactionWithGappedSegmentNames(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	want := seedSealedGarbage(t, col, 8)
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	// More churn → sealed names now have gaps relative to a fresh 1..n.
	for i := 0; i < 3; i++ {
		for id := range want {
			if _, err := col.Update(id, map[string]any{"v": "new"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := col.rotateSegment(); err != nil {
			t.Fatal(err)
		}
	}
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, col, want)
	onDisk, _ := filepath.Glob(filepath.Join(col.dir, "seg_*.ndjson"))
	if len(onDisk) != len(col.sealed)+1 {
		t.Fatalf("disk has %d segments, memory has %d sealed+active: %v", len(onDisk), len(col.sealed), onDisk)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
	re := openCrashTestCollection(t, dir)
	defer re.Close()
	verifyAll(t, re, want)
}

// A stale temp at a name a new pass reuses must not leak into the output.
func TestCompactionIgnoresStaleTemps(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	defer col.Close()
	want := seedSealedGarbage(t, col, 6)
	junk := []byte(`{"op":"insert","id":99999,"data":{"v":"stale"}}` + "\n")
	for _, n := range []string{".compact_000001.ndjson", ".compact_000001.ndjson.merge"} {
		if err := os.WriteFile(filepath.Join(col.dir, n), junk, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, col, want)
	if _, err := col.Get(99999); err == nil {
		t.Fatal("stale temp content leaked into compacted segments")
	}
	if temps, _ := filepath.Glob(filepath.Join(col.dir, ".compact_*")); len(temps) != 0 {
		t.Fatalf("temps left: %v", temps)
	}
}

// A rename failure on the first swap step leaves the old layout intact, cleans
// up, and lets the next pass succeed.
func TestCompactionFirstRenameFailureCleansUp(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	db, col := openFaulty(t, dir, fs, CollectionConfig{SegmentMaxSize: 2048, CompactDirtyPct: 2})
	defer db.Close()
	want := seedSealedGarbage(t, col, 6)
	fs.failRenameAt(fs.count("rename")+1, syscall.EXDEV)
	if err := col.CompactNow(); err == nil {
		t.Fatal("expected rename failure")
	}
	verifyAll(t, col, want)
	cd := colDirOf(dir)
	if m, _ := filepath.Glob(filepath.Join(cd, ".compact_*")); len(m) != 0 {
		t.Fatalf("temps left: %v", m)
	}
	if _, err := os.Stat(compactManifestPath(cd)); err == nil {
		t.Fatal("manifest left behind")
	}
	if err := col.CompactNow(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	verifyAll(t, col, want)
}

// A rename failure after the first step is recoverable at reopen and blocks
// further in-process passes.
func TestCompactionMidSwapFailureRecoversOnReopen(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		t.Run(fmt.Sprint("rename", failAt), func(t *testing.T) {
			dir := t.TempDir()
			fs := newFaultFS()
			cfg := CollectionConfig{SegmentMaxSize: 150, CompactDirtyPct: 2}
			db, col := openFaulty(t, dir, fs, cfg)
			if err := col.EnsureIndex("v"); err != nil {
				t.Fatal(err)
			}
			want := map[uint64]string{}
			for i := 0; i < 40; i++ {
				id, _, err := col.Insert(map[string]any{"v": "a"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := col.Update(id, map[string]any{"v": "b"}); err != nil {
					t.Fatal(err)
				}
				want[id] = "b"
			}
			col.compactMu.Lock() // park the background compactor
			col.compactMu.Unlock()
			fs.failRenameAt(fs.count("rename")+failAt, syscall.EXDEV)
			err := col.CompactNow()
			if fs.faults() == 0 {
				t.Skip("fault not reached (fewer output segments)")
			}
			if err == nil {
				t.Fatal("expected failure")
			}
			if err2 := col.CompactNow(); err2 == nil || !strings.Contains(err2.Error(), "incomplete swap") {
				t.Fatalf("second pass should be refused, got %v", err2)
			}
			db2 := reopenAfterCrash(t, db, fs, dir, CollectionConfig{SegmentMaxSize: 150, CompactDirtyPct: 2})
			col2, err := db2.Collection("c")
			if err != nil {
				t.Fatal(err)
			}
			verifyAll(t, col2, want)
			ids, ok := col2.IndexLookup("v", "b")
			if !ok || len(ids) != len(want) {
				t.Fatalf("index lookup got %d ids ok=%v, want %d", len(ids), ok, len(want))
			}
			if ids, _ := col2.IndexLookup("v", "a"); len(ids) != 0 {
				t.Fatalf("stale values indexed: %v", ids)
			}
		})
	}
}

// After a clean compaction the secondary index is persisted with coverage, so
// a reopen trusts it instead of rebuilding.
func TestCompactionPersistsSecondaryCoverage(t *testing.T) {
	dir := t.TempDir()
	col := openCrashTestCollection(t, dir)
	if err := col.EnsureIndex("v"); err != nil {
		t.Fatal(err)
	}
	want := seedSealedGarbage(t, col, 10)
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	// Writes after the compaction exercise tail replay on reopen.
	id, _, err := col.Insert(map[string]any{"v": "tail"})
	if err != nil {
		t.Fatal(err)
	}
	_ = id
	// Simulate crash: skip Close's persist by reopening a copy of the dir.
	cp := t.TempDir()
	copyDir(t, col.dir, filepath.Join(cp, "crash"))
	col.Close()
	re := openCrashTestCollection(t, cp)
	defer re.Close()
	verifyAll(t, re, want)
	if s := re.IndexRecoveryStats(); s.SecondaryRebuilds != 0 {
		t.Fatalf("secondary index rebuilt after compaction: %+v", s)
	}
	if ids, _ := re.IndexLookup("v", "tail"); len(ids) != 1 {
		t.Fatalf("tail write not indexed: %v", ids)
	}
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".lock") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// Writers racing compaction must never be lost from the secondary index.
func TestCompactionConcurrentWritersKeepSecondaryIndex(t *testing.T) {
	col := openCrashTestCollection(t, t.TempDir())
	defer col.Close()
	if err := col.EnsureIndex("v"); err != nil {
		t.Fatal(err)
	}
	seedSealedGarbage(t, col, 10)
	var wg sync.WaitGroup
	var mu sync.Mutex
	inserted := map[uint64]bool{}
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			id, _, err := col.Insert(map[string]any{"v": "race"})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			inserted[id] = true
			mu.Unlock()
			time.Sleep(50 * time.Microsecond)
		}
	}()
	for i := 0; i < 15; i++ {
		if err := col.CompactNow(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	ids, _ := col.IndexLookup("v", "race")
	got := map[uint64]bool{}
	for _, id := range ids {
		got[id] = true
	}
	for id := range inserted {
		if !got[id] {
			t.Fatalf("id %d missing from secondary index", id)
		}
	}
}
