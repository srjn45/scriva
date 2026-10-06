package engine

// Phase-0 failing-first regression repros for the index/data integrity
// hardening (issue #107, phase 2). Tests that reproduce a confirmed gap are
// t.Skip'd with the task that fixes them; remove the skip when that task lands.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestCrashStaleIndex: records written after a clean Close/reopen, then a
// crash (no Close), must all be visible after the next open. Today the stale
// but checksum-valid index.json from the first Close is trusted.
func TestCrashStaleIndex(t *testing.T) {
	t.Skip("gap: stale-but-valid index.json trusted after crash; fixed by the index-vs-segment reconciliation task (index data-integrity phase 2, #107)")
	dir := t.TempDir()

	db0, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	col0, err := db0.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	a, _, _ := col0.Insert(map[string]any{"v": "a1"})
	d, _, _ := col0.Insert(map[string]any{"v": "d1"})
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	}

	fs := newFaultFS()
	db, err := Open(dir, CollectionConfig{SyncMode: SyncModeAlways, wrapFile: fs.wrap})
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := col.Insert(map[string]any{"v": "b1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Update(a, map[string]any{"v": "a2"}); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(d); err != nil {
		t.Fatal(err)
	}

	db2 := reopenAfterCrash(t, db, fs, dir, CollectionConfig{})
	col2, err := db2.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	assertLookupScanAgree(t, col2, map[uint64]map[string]any{
		a: {"v": "a2"},
		b: {"v": "b1"},
	}, d)
}

// TestPartialWriteKeepsOffsetsCorrect: after an Append that persisted a partial
// line and errored, later writes must stay decodable and reopen must succeed.
func TestPartialWriteKeepsOffsetsCorrect(t *testing.T) {
	t.Skip("gap: failed partial Append leaves torn bytes and desyncs later offsets; fixed by the append-rollback/torn-tail task (index data-integrity phase 2, #107)")
	dir := t.TempDir()
	fs := newFaultFS()
	db, col := openFaulty(t, dir, fs, CollectionConfig{})
	first, _, err := col.Insert(map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	fs.failWriteAt(fs.count("write")+1, 9, syscall.ENOSPC)
	if _, _, err := col.Insert(map[string]any{"v": "torn"}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC, got %v", err)
	}
	later, _, err := col.Insert(map[string]any{"v": 2})
	if err != nil {
		t.Fatalf("insert after partial write: %v", err)
	}
	if rec, err := col.Get(later); err != nil || !equalAny(rec.Data["v"], 2) {
		t.Fatalf("get later: %v %v", rec, err)
	}
	if _, err := col.Get(first); err != nil {
		t.Fatalf("get first: %v", err)
	}
	assertLookupScanAgree(t, col, map[uint64]map[string]any{first: {"v": 1}, later: {"v": 2}})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	col2, err := db2.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	assertLookupScanAgree(t, col2, map[uint64]map[string]any{first: {"v": 1}, later: {"v": 2}})
}

// TestIndexRelativePaths: a cleanly closed data dir that is moved must reopen
// without a full index rebuild. index.json stores absolute segment paths today.
func TestIndexRelativePaths(t *testing.T) {
	t.Skip("gap: index.json stores absolute segment paths, so a moved data dir forces a full rebuild; fixed by the relative-path index task (index data-integrity phase 2, #107)")
	src := filepath.Join(t.TempDir(), "src")
	cfg := CollectionConfig{SegmentMaxSize: 512, CompactInterval: time.Hour}
	db, err := Open(src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if _, _, err := col.Insert(map[string]any{"i": i, "pad": "xxxxxxxxxxxxxxxxxxxx"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "dst")
	if err := os.Rename(src, dst); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dst, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	col2, err := db2.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	if n := col2.indexRebuilds.Load(); n != 0 {
		t.Fatalf("moved data dir triggered %d full index rebuild(s), want 0", n)
	}
	if got := col2.Stats().RecordCount; got != 30 {
		t.Fatalf("record count %d, want 30", got)
	}
}

// TestSecondaryIndexSurvivesCompactionWrites: inserts landing between the
// primary index rebuild and the secondary index rebuild in compact() must all
// be findable via IndexLookup. (Unverified gap; guard test, enabled.)
func TestSecondaryIndexSurvivesCompactionWrites(t *testing.T) {
	cfg := CollectionConfig{SegmentMaxSize: 200, CompactInterval: time.Hour, CompactDirtyPct: 0.01}
	var col *Collection
	var mu sync.Mutex
	var injected []uint64
	fire := false
	cfg.preSidxRebuildHook = func() {
		mu.Lock()
		on := fire
		fire = false
		mu.Unlock()
		if !on {
			return
		}
		for i := 0; i < 5; i++ {
			id, _, err := col.Insert(map[string]any{"name": fmt.Sprintf("late%d", i)})
			if err != nil {
				t.Errorf("insert in hook: %v", err)
				return
			}
			injected = append(injected, id)
		}
	}
	db, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, err = db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureIndex("name"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		id, _, err := col.Insert(map[string]any{"name": "early"})
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if _, err := col.Update(id, map[string]any{"name": "early"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	mu.Lock()
	fire = true
	mu.Unlock()
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	if len(injected) != 5 {
		t.Fatalf("hook did not run (injected=%d)", len(injected))
	}
	for i, id := range injected {
		ids, ok := col.IndexLookup("name", fmt.Sprintf("late%d", i))
		if !ok || len(ids) != 1 || ids[0] != id {
			t.Errorf("late%d: IndexLookup = %v ok=%v, want [%d]", i, ids, ok, id)
		}
	}
}

// TestCommitTxAllOrNothingOnIOError: a write failure on the Kth append inside
// CommitTx must leave no op of the batch visible and no stray bytes on disk.
func TestCommitTxAllOrNothingOnIOError(t *testing.T) {
	t.Skip("gap: CommitTx applies ops one by one with no rollback on append failure; fixed by the atomic-batch-append task (index data-integrity phase 2, #107)")
	dir := t.TempDir()
	fs := newFaultFS()
	_, col := openFaulty(t, dir, fs, CollectionConfig{})
	if err := col.EnsureIndex("name"); err != nil {
		t.Fatal(err)
	}
	base, _, err := col.Insert(map[string]any{"name": "base"})
	if err != nil {
		t.Fatal(err)
	}
	ids := []uint64{col.ReserveID(), col.ReserveID(), col.ReserveID()}
	ops := make([]txOp, len(ids))
	for i, id := range ids {
		ops[i] = txOp{kind: txOpInsert, id: id, data: map[string]any{"name": fmt.Sprintf("tx%d", i)}, ts: time.Now().UTC()}
	}
	fs.failWriteAt(fs.count("write")+2, 6, syscall.ENOSPC) // K=2: partial line, then error
	if err := col.CommitTx(ops); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC, got %v", err)
	}
	for i, id := range ids {
		if _, err := col.Get(id); err == nil {
			t.Errorf("op %d (id %d) visible via Get after failed commit", i, id)
		}
		if got, _ := col.IndexLookup("name", fmt.Sprintf("tx%d", i)); len(got) != 0 {
			t.Errorf("op %d visible via secondary index: %v", i, got)
		}
	}
	assertLookupScanAgree(t, col, map[uint64]map[string]any{base: {"name": "base"}}, ids...)
	rep := diskState(t, colDirOf(dir))
	if len(rep.Bad) != 0 {
		t.Errorf("stray partial bytes in segment: %v", rep.Bad)
	}
	for _, id := range ids {
		if _, ok := rep.Live[id]; ok {
			t.Errorf("id %d of the failed batch is durable in the segment", id)
		}
	}
}
