package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/store"
)

func encRepl(id uint64) store.Entry {
	return store.Entry{ID: id, Op: store.OpInsert, Rev: 1, Epoch: 1, Data: map[string]any{"password": "\x00scrivaenc:fake"}}
}

// A streamed encrypted entry for a collection the follower lacks (or holds
// without an encryption block) is refused before anything is written, and the
// directory still reopens.
func TestApplyReplication_EncryptedWithoutBlockHalts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openFollower(t, dir)

	// Auto-create path: collection does not exist.
	err := db.ApplyReplication("users", encRepl(1))
	if !errors.Is(err, ErrReplicationNeedsBootstrap) {
		t.Fatalf("missing collection: got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "users")); !os.IsNotExist(err) {
		t.Fatalf("collection dir created by refused apply: %v", err)
	}

	// Existing plaintext collection.
	col, err := db.CreateCollection("plain")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyReplication("plain", encRepl(1)); !errors.Is(err, ErrReplicationNeedsBootstrap) {
		t.Fatalf("plaintext collection: got %v", err)
	}
	if n := col.index.Len(); n != 0 {
		t.Fatalf("index has %d entries after refused apply", n)
	}
	// Envelope marker without an epoch is also refused.
	e := encRepl(2)
	e.Epoch = 0
	if err := db.ApplyReplication("plain", e); !errors.Is(err, ErrReplicationNeedsBootstrap) {
		t.Fatalf("marker only: got %v", err)
	}
	db.Close()

	db2, err := Open(dir, followerCfg())
	if err != nil {
		t.Fatalf("reopen after refused apply: %v", err)
	}
	db2.Close()
}

func TestApplyReplication_PlaintextUnchanged(t *testing.T) {
	t.Parallel()
	db := openFollower(t, t.TempDir())
	if err := db.ApplyReplication("users", store.Entry{ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	col, err := db.Collection("users")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := col.index.Get(1); !ok {
		t.Fatal("plaintext entry not applied")
	}
}

// An XTx with one encrypted participant halts atomically: the plaintext
// participant is not applied, no collection is created and the LSN is unchanged.
func TestApplyReplicatedTx_EncryptedParticipantHaltsAtomically(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openFollower(t, dir)
	plain, err := db.CreateCollection("plain")
	if err != nil {
		t.Fatal(err)
	}
	env, err := NewTxEnvelope("t1", "", []TxEnvelopeOp{
		{Collection: "plain", Entry: store.Entry{ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"a": 1}}},
		{Collection: "secret", Entry: encRepl(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplyReplicatedTx(7, env); !errors.Is(err, ErrReplicationNeedsBootstrap) {
		t.Fatalf("got %v", err)
	}
	if _, ok := plain.index.Get(1); ok {
		t.Fatal("plaintext participant partially applied")
	}
	if _, err := os.Stat(filepath.Join(dir, "secret")); !os.IsNotExist(err) {
		t.Fatalf("encrypted participant collection created: %v", err)
	}
	if got := db.AppliedLSN(); got != 0 {
		t.Fatalf("applied LSN advanced to %d", got)
	}
}
