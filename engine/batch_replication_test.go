package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

func TestReplication_SingleParticipantBatchEnvelope(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	cfg.ReplicationRingSize = 64
	leader, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open leader: %v", err)
	}
	defer leader.Close()

	col, err := leader.CreateCollection("bcol")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	beforeLSN := leader.CurrentLSN()
	ids, ts, err := col.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids len = %d, want 3", len(ids))
	}
	if ts.IsZero() {
		t.Fatal("expected non-zero batch timestamp")
	}

	// Must consume exactly ONE LSN for the whole batch
	afterLSN := leader.CurrentLSN()
	if afterLSN != beforeLSN+1 {
		t.Fatalf("batch consumed %d LSNs, want 1", afterLSN-beforeLSN)
	}

	// Read replication entry from broker
	_, backlog, ok := leader.broker.subscribe(beforeLSN, "test-sub")
	if !ok || len(backlog) != 1 {
		t.Fatalf("subscribe: ok=%v len=%d, want 1", ok, len(backlog))
	}
	rentry := backlog[0]
	if !rentry.IsTx() {
		t.Fatal("replication record is not a transaction; batch was flattened!")
	}
	env := rentry.Tx
	if err := env.Verify(); err != nil {
		t.Fatalf("envelope verify: %v", err)
	}
	if parts := env.Participants(); len(parts) != 1 || parts[0].Collection != "bcol" {
		t.Fatalf("participants = %v, want [bcol]", parts)
	}
	if len(env.Ops) != 3 {
		t.Fatalf("envelope ops = %d, want 3", len(env.Ops))
	}
	for i, op := range env.Ops {
		if op.Collection != "bcol" {
			t.Fatalf("op %d collection = %q, want bcol", i, op.Collection)
		}
		if op.Entry.ID != ids[i] {
			t.Fatalf("op %d id = %d, want %d", i, op.Entry.ID, ids[i])
		}
		if op.Entry.Ts.IsZero() {
			t.Fatalf("op %d has zero timestamp", i)
		}
	}

	// Apply envelope on follower
	fdir := t.TempDir()
	fcfg := defaultConfig()
	fcfg.Follower = true
	follower, err := Open(fdir, fcfg)
	if err != nil {
		t.Fatalf("Open follower: %v", err)
	}
	defer follower.Close()

	applied, err := follower.ApplyReplicatedTx(afterLSN, env)
	if err != nil {
		t.Fatalf("ApplyReplicatedTx: %v", err)
	}
	if !applied {
		t.Fatal("expected applied=true")
	}
	if follower.AppliedLSN() != afterLSN {
		t.Fatalf("follower applied LSN = %d, want %d", follower.AppliedLSN(), afterLSN)
	}

	// All 3 entries visible on follower
	fcol, err := follower.Collection("bcol")
	if err != nil {
		t.Fatalf("follower collection: %v", err)
	}
	if n, _ := fcol.Count(nil); n != 3 {
		t.Fatalf("follower count = %d, want 3", n)
	}
	for _, id := range ids {
		if _, _, err := fcol.FindByID(id); err != nil {
			t.Fatalf("follower FindByID(%d): %v", id, err)
		}
	}

	// Local follower xtx journal has the decision
	fj := follower.journalOrNil()
	if fj == nil {
		t.Fatal("follower has no xtx journal")
	}
	d, ok := fj.boundDecision(afterLSN, env.TxID)
	if !ok || d.Outcome != xtxKindCommit {
		t.Fatalf("follower boundDecision(%d, %s) = %+v, ok=%v; want committed", afterLSN, env.TxID, d, ok)
	}
	st, _ := follower.XTxStatus(d.Tx)
	if st != XTxCommitted {
		t.Fatalf("follower local tx %s status %v, want XTxCommitted", d.Tx, st)
	}

	// Redelivery of the exact same envelope is a no-op
	reapplied, err := follower.ApplyReplicatedTx(afterLSN, env)
	if err != nil {
		t.Fatalf("reapply error: %v", err)
	}
	if reapplied {
		t.Fatal("expected reapplied=false for idempotent duplicate")
	}
}

func TestReplication_SingleParticipantFollowerCrashAtomicity(t *testing.T) {
	withAtomicBatchWriter(t, true)
	fdir := t.TempDir()
	fcfg := defaultConfig()
	fcfg.Follower = true
	follower, err := Open(fdir, fcfg)
	if err != nil {
		t.Fatal(err)
	}

	fcol, err := follower.ensureReplCollection("crashcol")
	if err != nil {
		t.Fatal(err)
	}
	fj, err := follower.ensureXTxJournal()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate crash during ApplyReplicatedTx:
	// stamped entries appended and synced to segment, but journal COMMIT never appended.
	txid, err := fj.allocateTxID("crash-batch")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		line := stampedEntry{
			ID:   uint64(i + 1),
			Op:   store.OpInsert,
			Rev:  1,
			Data: map[string]any{"v": float64(i)},
			Tx:   TxStamp{T: txid, I: uint32(i), N: 3},
		}
		if _, err := fcol.active.AppendStamped(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := fcol.active.Sync(); err != nil {
		t.Fatal(err)
	}
	follower.Close()

	// Reopen follower: recovery must fail closed / presume abort on the undecided run.
	f2, err := Open(fdir, fcfg)
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer f2.Close()

	c2, err := f2.Collection("crashcol")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := c2.Count(nil); n != 0 {
		t.Fatalf("crashed batch visible on follower reopen: count = %d, want 0", n)
	}
	st, _ := f2.XTxStatus(txid)
	if st != XTxAborted {
		t.Fatalf("status = %v, want XTxAborted", st)
	}
}

func TestReplication_SingleParticipantDivergenceFailsClosed(t *testing.T) {
	withAtomicBatchWriter(t, true)
	fdir := t.TempDir()
	fcfg := defaultConfig()
	fcfg.Follower = true
	follower, err := Open(fdir, fcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	env1 := envOf(t, "tx-div-1", 2)
	applied, err := follower.ApplyReplicatedTx(10, env1)
	if err != nil || !applied {
		t.Fatalf("Apply 10: %v %v", applied, err)
	}

	// Same LSN 10, different envelope: must return ErrReplicationDivergence
	env2 := envOf(t, "tx-div-2", 2)
	_, err = follower.ApplyReplicatedTx(10, env2)
	if !errors.Is(err, ErrReplicationDivergence) {
		t.Fatalf("got err = %v, want ErrReplicationDivergence", err)
	}
	if follower.AppliedLSN() != 10 {
		t.Fatalf("applied LSN moved on divergence: %d, want 10", follower.AppliedLSN())
	}
}

func TestReplication_SingleParticipantDigestMismatchFailsClosed(t *testing.T) {
	withAtomicBatchWriter(t, true)
	env := envOf(t, "tx-tamper", 2)
	// Tamper with data without updating digest
	env.Ops[0].Entry.Data = map[string]any{"tampered": true}

	fdir := t.TempDir()
	fcfg := defaultConfig()
	fcfg.Follower = true
	follower, err := Open(fdir, fcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	_, err = follower.ApplyReplicatedTx(1, env)
	if !errors.Is(err, ErrEnvelopeDigest) {
		t.Fatalf("got err = %v, want ErrEnvelopeDigest", err)
	}
	if follower.AppliedLSN() != 0 {
		t.Fatalf("applied LSN moved on digest mismatch: %d, want 0", follower.AppliedLSN())
	}
}
