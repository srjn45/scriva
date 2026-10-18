package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/srjn45/scriva/store"
)

func colDirOf(dir string) string { return filepath.Join(dir, "c") }

func TestFaultFS_ShortWriteIsRolledBack(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	_, col := openFaulty(t, dir, fs, CollectionConfig{})
	if _, _, err := col.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(colDirOf(dir), "seg_000001.ndjson")
	before, _ := os.ReadFile(segPath)

	fs.failWriteAt(fs.count("write")+1, 7, syscall.ENOSPC)
	_, _, err := col.Insert(map[string]any{"b": 2})
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC, got %v", err)
	}
	after, _ := os.ReadFile(segPath)
	// The injected 7-byte torn prefix must have been rolled back by Append.
	if string(after) != string(before) {
		t.Fatalf("torn bytes not rolled back, before=%d after=%d", len(before), len(after))
	}
	rep := diskState(t, colDirOf(dir))
	if len(rep.Bad) != 0 || len(rep.Live) != 1 {
		t.Fatalf("want 1 live and no torn line, got live=%d bad=%v", len(rep.Live), rep.Bad)
	}
}

func TestFaultFS_FailSync(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	_, col := openFaulty(t, dir, fs, CollectionConfig{SyncMode: SyncModeAlways})
	fs.failSyncAt(fs.count("sync")+1, nil)
	if _, _, err := col.Insert(map[string]any{"a": 1}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("want EIO from fsync, got %v", err)
	}
}

func TestFaultFS_FailRename(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	_, col := openFaulty(t, dir, fs, CollectionConfig{SegmentMaxSize: 200})
	for i := 0; i < 20; i++ {
		id, _, err := col.Insert(map[string]any{"i": i, "pad": strings.Repeat("x", 40)})
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if err := col.Delete(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The first rename of a pass failing replaces nothing: the pass is
	// abandoned, the old layout keeps serving and a later pass may run.
	err := compactWithRenameFault(col, fs, 1, syscall.EXDEV)
	if !errors.Is(err, syscall.EXDEV) {
		t.Fatalf("want EXDEV from the armed rename, got %v", err)
	}
	if fs.faults() != 1 {
		t.Fatalf("fault not injected exactly once: faults=%d", fs.faults())
	}
	if err := col.CompactNow(); err != nil {
		t.Fatalf("compaction after an abandoned pass: %v", err)
	}
}

func TestFaultFS_CrashAfterAndReopen(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	db, col := openFaulty(t, dir, fs, CollectionConfig{})
	id1, _, err := col.Insert(map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	fs.crashAfter(fs.totalSteps()) // freeze: the next op never reaches disk
	if _, _, err := col.Insert(map[string]any{"b": 2}); !errors.Is(err, errCrash) {
		t.Fatalf("want errCrash, got %v", err)
	}
	if !fs.crashed() {
		t.Fatal("fs should be crashed")
	}
	// Even Close on the abandoned handle's file must not flush anything.
	size := func() int64 {
		st, _ := os.Stat(filepath.Join(colDirOf(dir), "seg_000001.ndjson"))
		return st.Size()
	}
	sz := size()

	db2 := reopenAfterCrash(t, db, fs, dir, CollectionConfig{})
	if size() != sz {
		t.Fatalf("segment changed across abandon/reopen: %d -> %d", sz, size())
	}
	col2, err := db2.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	rep := diskState(t, colDirOf(dir))
	if len(rep.Bad) != 0 {
		t.Fatalf("unexpected bad lines: %v", rep.Bad)
	}
	_ = col2 // see TestFaultFS_CleanReopenAgrees for lookup/scan agreement
	if _, ok := rep.Live[id1]; !ok || len(rep.Live) != 1 {
		t.Fatalf("want only id %d live, got %v", id1, rep.Live)
	}
}

func TestDiskState_ReportsTombstonesAndBadLines(t *testing.T) {
	dir := t.TempDir()
	enc := func(e store.Entry) string { b, _ := store.Encode(e); return string(b) }
	body := enc(store.NewInsert(1, map[string]any{"v": 1})) +
		enc(store.NewInsert(2, map[string]any{"v": 2})) +
		enc(store.NewDelete(2)) +
		"{garbage\n" +
		`{"id":3,"op":"ins` // torn tail
	if err := os.WriteFile(filepath.Join(dir, "seg_000001.ndjson"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	rep := diskState(t, dir)
	if len(rep.Live) != 1 || len(rep.Tombstones) != 1 || len(rep.Bad) != 2 {
		t.Fatalf("live=%d tomb=%d bad=%d", len(rep.Live), len(rep.Tombstones), len(rep.Bad))
	}
}

func TestFaultFS_CleanReopenAgrees(t *testing.T) {
	dir := t.TempDir()
	db, col := openFaulty(t, dir, newFaultFS(), CollectionConfig{})
	var del uint64
	for i := 0; i < 5; i++ {
		id, _, err := col.Insert(map[string]any{"i": i})
		if err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			del = id
		}
	}
	if err := col.Delete(del); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	col2, err := db2.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	rep := diskState(t, colDirOf(dir))
	if len(rep.Bad) != 0 || len(rep.Live) != 4 || len(rep.Tombstones) != 1 {
		t.Fatalf("disk: live=%d tomb=%d bad=%v", len(rep.Live), len(rep.Tombstones), rep.Bad)
	}
	assertLookupScanAgree(t, col2, rep.model(), del)
}

func TestFaultFS_FailTruncatePoisons(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	_, col := openFaulty(t, dir, fs, CollectionConfig{})
	if _, _, err := col.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	fs.failWriteAt(fs.count("write")+1, 7, syscall.ENOSPC)
	fs.failTruncateAt(fs.count("truncate")+1, nil)
	if _, _, err := col.Insert(map[string]any{"b": 2}); !errors.Is(err, ErrSegmentPoisoned) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("want poisoned segment wrapping EIO, got %v", err)
	}
	if fs.faults() != 2 {
		t.Fatalf("faults=%d, want 2 (write + truncate)", fs.faults())
	}
	// The failed truncate left the torn prefix in place.
	if rep := diskState(t, colDirOf(dir)); len(rep.Bad) != 1 {
		t.Fatalf("want the torn tail preserved, bad=%v", rep.Bad)
	}
}

func TestFaultFS_ScopeAndDurableSizes(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	_, col := openFaulty(t, dir, fs, CollectionConfig{SyncMode: SyncModeAlways})
	if _, _, err := col.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	col.mu.RLock()
	active := col.active.Path()
	col.mu.RUnlock()

	fs.scopeTo(filepath.Join(dir, "elsewhere"))
	fs.failSyncAt(1, nil)
	if _, _, err := col.Insert(map[string]any{"a": 2}); err != nil {
		t.Fatalf("a fault scoped to another file fired: %v", err)
	}
	fs.scopeTo(active)
	fs.failSyncAt(fs.scopedCount("sync")+1, nil)
	if _, _, err := col.Insert(map[string]any{"a": 3}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("want EIO from the scoped fsync, got %v", err)
	}
	// Two synced records survive a power loss; the third (unsynced) does not.
	img := t.TempDir()
	powerLossImage(t, fs, dir, img)
	if rep := diskState(t, colDirOf(img)); len(rep.Live) != 2 || len(rep.Bad) != 0 {
		t.Fatalf("power-loss image: live=%d bad=%v, want 2 live", len(rep.Live), rep.Bad)
	}
	if rep := diskState(t, colDirOf(dir)); len(rep.Live) != 3 {
		t.Fatalf("live dir: live=%d, want 3", len(rep.Live))
	}
}
