package engine

import (
	"errors"
	"testing"

	"github.com/srjn45/scriva/store"
)

func xtxInsertOps(t *testing.T, db *DB, names ...string) ([]XTxOp, map[string]uint64) {
	t.Helper()
	var ops []XTxOp
	ids := map[string]uint64{}
	for _, n := range names {
		c, err := db.CreateCollection(n)
		if err != nil {
			t.Fatal(err)
		}
		id := c.ReserveID()
		ids[n] = id
		ops = append(ops, XTxOp{Collection: n, Op: store.OpInsert, ID: id, Data: map[string]any{"v": n}})
	}
	return ops, ids
}

func TestDropCollection_RefusesNonRetiredParticipant(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ops, ids := xtxInsertOps(t, db, "a", "b")
	res, err := db.CommitXTx("k1", ops)
	if err != nil {
		t.Fatal(err)
	}

	before := snapshotTree(t, dir)
	err = db.DropCollection("a")
	if !errors.Is(err, ErrParticipantRetained) {
		t.Fatalf("Drop(a) = %v, want ErrParticipantRetained", err)
	}
	var pr *ParticipantRetainedError
	if !errors.As(err, &pr) || pr.Collection != "a" || len(pr.TxIDs) != 1 || pr.TxIDs[0] != res.TxID {
		t.Fatalf("bad error detail: %#v", pr)
	}
	sameTree(t, "refused drop", before, snapshotTree(t, dir))
	if _, err := db.Collection("a"); err != nil {
		t.Fatalf("a no longer open after refused drop: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	for _, n := range []string{"a", "b"} {
		c, err := db.Collection(n)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(ids[n]); err != nil {
			t.Errorf("%s: committed record not readable after reopen", n)
		}
	}
}

func TestDropCollection_UnreferencedStillDrops(t *testing.T) {
	db := openTestDB(t, defaultConfig())
	ops, _ := xtxInsertOps(t, db, "a", "b")
	if _, err := db.CommitXTx("k1", ops); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("lonely"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("lonely"); err != nil {
		t.Fatalf("drop unreferenced: %v", err)
	}
	if _, err := db.Collection("lonely"); err == nil {
		t.Fatal("lonely still present")
	}
}

func TestDropCollection_AbortedOnlyDoesNotBlock(t *testing.T) {
	db := openTestDB(t, defaultConfig())
	ops, _ := xtxInsertOps(t, db, "x", "y") // creates the journal
	if _, err := db.CommitXTx("k0", ops); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("a"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.xtxJournal.allocateTxID("k")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.xtxJournal.abort(tx, "k", "client"); err != nil {
		t.Fatal(err)
	}
	if got := db.xtxJournal.retainedFor("a"); len(got) != 0 {
		t.Fatalf("retainedFor = %v", got)
	}
	if err := db.DropCollection("a"); err != nil {
		t.Fatalf("drop: %v", err)
	}
}

func TestDropCollection_RetiredCommitDoesNotBlock(t *testing.T) {
	db := openTestDB(t, defaultConfig())
	ops, _ := xtxInsertOps(t, db, "a", "b")
	res, err := db.CommitXTx("k1", ops)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.xtxJournal.retire(res.TxID); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("a"); err != nil {
		t.Fatalf("drop after retire: %v", err)
	}
}
