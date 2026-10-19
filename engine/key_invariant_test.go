//nolint:errcheck
package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

func keyedCol(t *testing.T) (*DB, *Collection, string, uint64) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := col.InsertWithKey("k", map[string]any{"x": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	return db, col, dir, id
}

// assertKeyHeld checks the key still resolves to id and cannot be re-inserted,
// then again after compaction.
func assertKeyHeld(t *testing.T, col *Collection, id uint64, wantX any) {
	t.Helper()
	check := func(stage string) {
		d, _, err := col.FindByKey("k")
		if err != nil {
			t.Fatalf("%s: FindByKey: %v", stage, err)
		}
		if d[KeyField] != "k" || d["x"] != wantX {
			t.Fatalf("%s: data = %v", stage, d)
		}
		if _, _, err := col.InsertWithKey("k", map[string]any{}); !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("%s: re-insert err = %v, want ErrDuplicateKey", stage, err)
		}
	}
	check("live")
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	check("compacted")
}

func TestKeyPreservedByUpdate(t *testing.T) {
	_, col, _, id := keyedCol(t)
	if _, err := col.Update(id, map[string]any{"x": 2.0}); err != nil {
		t.Fatal(err)
	}
	assertKeyHeld(t, col, id, 2.0)
	if _, err := col.Update(id, map[string]any{"x": 3.0, KeyField: "other"}); !errors.Is(err, ErrReservedField) {
		t.Fatalf("Update with _key err = %v", err)
	}
}

func TestKeyPreservedByUpdateWithExpiry(t *testing.T) {
	_, col, _, id := keyedCol(t)
	if _, err := col.UpdateWithExpiry(id, map[string]any{"x": 2.0}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertKeyHeld(t, col, id, 2.0)
}

func TestKeyPreservedByUpsertAndCAS(t *testing.T) {
	_, col, _, _ := keyedCol(t)
	rec, err := col.Upsert("k", map[string]any{"x": 2.0})
	if err != nil {
		t.Fatal(err)
	}
	assertKeyHeld(t, col, rec.ID, 2.0)
	if ok, err := col.UpdateIfRev("k", rec.Rev, map[string]any{"x": 3.0}); err != nil || !ok {
		t.Fatalf("UpdateIfRev = %v, %v", ok, err)
	}
	assertKeyHeld(t, col, rec.ID, 3.0)
	if _, err := col.Upsert("k", map[string]any{KeyField: "z"}); !errors.Is(err, ErrReservedField) {
		t.Fatalf("Upsert with _key err = %v", err)
	}
}

func TestKeyPreservedAcrossReopen(t *testing.T) {
	db, col, dir, id := keyedCol(t)
	if _, err := col.Update(id, map[string]any{"x": 2.0}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	col2, err := db2.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	assertKeyHeld(t, col2, id, 2.0)
}

func TestKeyNotAddedWithoutKeyIndex(t *testing.T) {
	db, err := Open(t.TempDir(), vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.CreateCollection("plain")
	id, _, _ := col.Insert(map[string]any{"x": 1.0})
	if _, err := col.Update(id, map[string]any{"x": 2.0}); err != nil {
		t.Fatal(err)
	}
	d, _, _ := col.FindByID(id)
	if _, has := d[KeyField]; has || len(d) != 1 {
		t.Fatalf("data = %v", d)
	}
}

func TestKeyPreservedByCommitTx(t *testing.T) {
	_, col, _, id := keyedCol(t)
	if err := col.CommitTx([]txOp{{kind: txOpUpdate, id: id, data: map[string]any{"x": 2.0}, ts: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	assertKeyHeld(t, col, id, 2.0)
}

func TestKeyPreservedByXTx(t *testing.T) {
	db, col, _, id := keyedCol(t)
	other, _ := db.CreateCollection("other")
	oid, _, _ := other.Insert(map[string]any{"y": 1.0})

	x, err := db.BeginXTx(context.Background(), []string{"c", "other"}, XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Update("c", id, map[string]any{"x": 2.0}); err != nil {
		t.Fatal(err)
	}
	if err := x.Update("other", oid, map[string]any{"y": 2.0}); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertKeyHeld(t, col, id, 2.0)
	if d, _, _ := other.FindByID(oid); len(d) != 1 {
		t.Fatalf("unkeyed collection gained fields: %v", d)
	}
}

func TestXTxStageRejectsKey(t *testing.T) {
	db, _, _, id := keyedCol(t)
	x, err := db.BeginXTx(context.Background(), []string{"c"}, XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer x.Rollback()
	if _, err := x.Stage(XTxOp{Collection: "c", Op: store.OpInsert, Data: map[string]any{KeyField: "z"}}); !errors.Is(err, ErrReservedField) {
		t.Fatalf("insert err = %v", err)
	}
	if err := x.Update("c", id, map[string]any{KeyField: "z"}); !errors.Is(err, ErrReservedField) {
		t.Fatalf("update err = %v", err)
	}
}

// dupKeyDir builds a collection whose segment holds two live records with the
// same string _key plus one with a non-string _key (legacy data).
func dupKeyDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.CreateCollection("c")
	if _, _, err := col.InsertWithKey("k", map[string]any{"x": 1.0}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "c", "seg_000001.ndjson")
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 50, Op: store.OpInsert, Rev: 1, Data: map[string]any{KeyField: "k", "x": 2.0}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 51, Op: store.OpInsert, Rev: 1, Data: map[string]any{KeyField: 5.0}}))
	return dir
}

func TestKeyDuplicateOwnershipDetected(t *testing.T) {
	dir := dupKeyDir(t)
	cr := verifyCol(t, dir, VerifyFull)
	wantCodes(t, cr, CodeKeyDuplicate)

	db, err := Open(dir, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.Collection("c")

	var kc *KeyIndexConflictError
	_, _, err = col.FindByKey("k")
	if !errors.Is(err, ErrKeyIndexConflict) || !errors.As(err, &kc) {
		t.Fatalf("FindByKey err = %v", err)
	}
	if kc.Key != "k" || len(kc.IDs) != 2 || kc.IDs[0] >= kc.IDs[1] {
		t.Fatalf("conflict = %+v", kc)
	}
	if _, _, err := col.InsertWithKey("fresh", nil); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("InsertWithKey err = %v", err)
	}
	if _, err := col.Upsert("k", map[string]any{}); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("Upsert err = %v", err)
	}
	if _, err := col.Exists("k"); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("Exists err = %v", err)
	}
	if _, err := col.UpdateIfRev("k", 1, map[string]any{}); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("UpdateIfRev err = %v", err)
	}
	if err := col.DeleteByKey("k"); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("DeleteByKey err = %v", err)
	}
	if _, err := col.LoadJSONL(strings.NewReader(`{"id":"n"}`), "id"); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("LoadJSONL err = %v", err)
	}
	// Id-addressed operations keep working, and keep both keys intact.
	if _, err := col.Update(50, map[string]any{"x": 9.0}); err != nil {
		t.Fatal(err)
	}
	if d, _, err := col.FindByID(50); err != nil || d[KeyField] != "k" {
		t.Fatalf("FindByID(50) = %v, %v", d, err)
	}
	// Non-string legacy _key is ignored by the index and left intact.
	if d, _, err := col.FindByID(51); err != nil || d[KeyField] != 5.0 {
		t.Fatalf("FindByID(51) = %v, %v", d, err)
	}

	// Resolution is by id: once one holder is deleted the keyed API recovers.
	if err := col.Delete(50); err != nil {
		t.Fatal(err)
	}
	if d, _, err := col.FindByKey("k"); err != nil || d["x"] != 1.0 {
		t.Fatalf("after resolve FindByKey = %v, %v", d, err)
	}
}

func TestKeyDuplicateSurvivesRebuild(t *testing.T) {
	dir := dupKeyDir(t)
	// Corrupt the persisted _key index so open must rebuild it from segments.
	if err := os.WriteFile(sidxFilePath(filepath.Join(dir, "c"), KeyField), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.Collection("c")
	if _, _, err := col.FindByKey("k"); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("FindByKey after rebuild err = %v", err)
	}
}

func TestReplicatedKeyConflictStopsApply(t *testing.T) {
	_, col, _, id := keyedCol(t)
	en := store.NewInsert(id+100, map[string]any{KeyField: "k"})
	en.Rev = 1
	en.Ts = time.Now().UTC()
	if err := col.applyEntry(en); !errors.Is(err, ErrKeyIndexConflict) {
		t.Fatalf("apply err = %v", err)
	}
	if _, ok := col.index.Get(id + 100); ok {
		t.Fatal("conflicting entry was applied")
	}
	// The same key on the same id (a replicated update) is fine.
	up := store.NewUpdate(id, map[string]any{KeyField: "k", "x": 5.0})
	up.Rev, up.Ts = 2, time.Now().UTC()
	if err := col.applyEntry(up); err != nil {
		t.Fatal(err)
	}
}
