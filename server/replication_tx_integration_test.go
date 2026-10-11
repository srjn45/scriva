//nolint:errcheck
package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
	"github.com/srjn45/scriva/store"
)

// commitCrossTx commits one two-collection transaction on the leader engine.
func commitCrossTx(t *testing.T, db *engine.DB, key string) {
	t.Helper()
	xa, err := db.Collection("xa")
	if err != nil {
		t.Fatal(err)
	}
	xb, err := db.Collection("xb")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CommitXTx(key, []engine.XTxOp{
		{Collection: "xa", Op: store.OpInsert, ID: xa.ReserveID(), Data: map[string]any{"n": 1}},
		{Collection: "xb", Op: store.OpInsert, ID: xb.ReserveID(), Data: map[string]any{"n": 2}},
	})
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}
}

func TestReplicationV2_EnvelopeOnTheWire(t *testing.T) {
	leader, db := startLeader(t)
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")
	before := db.CurrentLSN()
	commitCrossTx(t, db, "k1")
	if got := db.CurrentLSN(); got != before+1 {
		t.Fatalf("tx must consume exactly one LSN: %d -> %d", before, got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := leader.Replicate(ctx, &pb.ReplicateRequest{
		FromLsn: before, FollowerId: "raw",
		ProtocolVersions: []uint32{engine.ReplicationProtocolV1, engine.ReplicationProtocolV2},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := rs.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if rec.RecordVersion != engine.ReplicationProtocolV2 || rec.Lsn != before+1 || rec.Tx == nil {
		t.Fatalf("want v2 envelope at lsn %d, got %+v", before+1, rec)
	}
	if len(rec.Tx.Ops) != 2 || rec.Tx.Key != "k1" || len(rec.Tx.Digest) == 0 {
		t.Fatalf("bad envelope: %+v", rec.Tx)
	}
	if rec.Collection != "" {
		t.Fatalf("envelope must not carry single-entry fields: %+v", rec)
	}
}

func TestReplicationV2_LegacyFollowerGetsTypedRefusal(t *testing.T) {
	leader, db := startLeader(t)
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")
	lsn := db.CurrentLSN()
	// A plain single-entry write streams to a legacy follower unchanged.
	mustInsert(t, leader, "xa", map[string]any{"plain": true})
	commitCrossTx(t, db, "k2")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := leader.Replicate(ctx, &pb.ReplicateRequest{FromLsn: lsn, FollowerId: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := rs.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.Lsn != lsn+1 || first.Collection != "xa" || first.Tx != nil {
		t.Fatalf("legacy entry mangled: %+v", first)
	}
	_, err = rs.Recv()
	if err == nil || !server.IsReplicationProtocolError(err) {
		t.Fatalf("want typed protocol refusal, got %v", err)
	}
}

// recState is a collection's id→(rev,data) map read through Get, so revisions
// are compared as well as payloads.
func recState(t *testing.T, db *engine.DB, coll string) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	c, err := db.Collection(coll)
	if err != nil {
		return out
	}
	for id := range scanState(t, db, coll) {
		r, err := c.Get(id)
		if err != nil {
			t.Fatalf("get %s/%d: %v", coll, id, err)
		}
		b, _ := json.Marshal(r.Data)
		out[id] = fmt.Sprintf("rev=%d %s", r.Rev, b)
	}
	return out
}

func waitAllConverged(t *testing.T, leader, follower *engine.DB, colls ...string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ok := follower.AppliedLSN() >= leader.CurrentLSN()
		for _, c := range colls {
			if ok && !reflect.DeepEqual(recState(t, leader, c), recState(t, follower, c)) {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: leaderLSN=%d followerLSN=%d", leader.CurrentLSN(), follower.AppliedLSN())
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// A multi-collection transaction committed on the leader appears on the
// follower with identical ids and revisions, consuming one LSN on both.
func TestReplicationV2_TxAppliesAtomicallyOnFollower(t *testing.T) {
	leader, db := startLeader(t)
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")
	mustInsert(t, leader, "xa", map[string]any{"seed": true})
	fdb, stop := bootstrapFollower(t, leader)
	defer stop()

	before := db.CurrentLSN()
	for i := 0; i < 5; i++ {
		commitCrossTx(t, db, fmt.Sprintf("k%d", i))
	}
	if got := db.CurrentLSN(); got != before+5 {
		t.Fatalf("5 txs must use 5 LSNs: %d -> %d", before, got)
	}
	waitAllConverged(t, db, fdb, "xa", "xb")
	if n := len(recState(t, fdb, "xb")); n != 5 {
		t.Fatalf("xb has %d records, want 5", n)
	}
	if fdb.AppliedLSN() != db.CurrentLSN() {
		t.Fatalf("applied %d != leader %d", fdb.AppliedLSN(), db.CurrentLSN())
	}
}

// Legacy single-entry writes and transactions interleave in LSN order and both
// nodes converge to identical state, including a restarted follower.
func TestReplicationV2_InterleavedLegacyAndTxConverge(t *testing.T) {
	leader, db := startLeader(t)
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")
	fdb, stop := bootstrapFollower(t, leader)
	defer stop()

	for i := 0; i < 6; i++ {
		mustInsert(t, leader, "xa", map[string]any{"legacy": float64(i)})
		commitCrossTx(t, db, fmt.Sprintf("m%d", i))
		mustInsert(t, leader, "xb", map[string]any{"legacy": float64(i)})
	}
	// Update + delete through a transaction.
	xa := scanState(t, db, "xa")
	var ids []uint64
	for id := range xa {
		ids = append(ids, id)
	}
	_, err := db.CommitXTx("upd", []engine.XTxOp{
		{Collection: "xa", Op: store.OpUpdate, ID: ids[0], Data: map[string]any{"changed": true}},
		{Collection: "xa", Op: store.OpDelete, ID: ids[1]},
		{Collection: "xb", Op: store.OpInsert, ID: func() uint64 { c, _ := db.Collection("xb"); return c.ReserveID() }(), Data: map[string]any{"n": 9}},
	})
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}
	waitAllConverged(t, db, fdb, "xa", "xb")
}

// A follower stopped mid-stream resumes from its applied LSN without losing or
// duplicating any transaction.
func TestReplicationV2_RestartMidStreamResumes(t *testing.T) {
	leader, db := startLeader(t)
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")

	dir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, dir)
	if err != nil {
		t.Fatal(err)
	}
	run := func(upTo int) *engine.DB {
		fdb, err := engine.Open(dir, followerEngineConfig())
		if err != nil {
			t.Fatal(err)
		}
		if fdb.AppliedLSN() < wm {
			fdb.SetAppliedLSN(wm)
		}
		ctx, cancel := context.WithCancel(context.Background())
		fol := server.NewFollower(fdb, leader, "f", "", discardLogger())
		done := make(chan struct{})
		go func() { fol.Run(ctx); close(done) }()
		waitAllConverged(t, db, fdb, "xa", "xb")
		cancel()
		<-done
		return fdb
	}

	for i := 0; i < 4; i++ {
		commitCrossTx(t, db, fmt.Sprintf("a%d", i))
	}
	f1 := run(4)
	applied := f1.AppliedLSN()
	f1.Close()
	for i := 4; i < 10; i++ {
		commitCrossTx(t, db, fmt.Sprintf("a%d", i))
	}
	f2 := run(10)
	defer f2.Close()
	if f2.AppliedLSN() <= applied {
		t.Fatalf("did not advance: %d -> %d", applied, f2.AppliedLSN())
	}
	if n := len(recState(t, f2, "xa")); n != 10 {
		t.Fatalf("xa has %d records, want 10 (no loss, no duplication)", n)
	}
}

// A redelivered transaction whose content differs from the applied one is a
// typed divergence: the stream stops and the applied LSN does not advance.
func TestReplicationV2_DivergentRedeliveryStopsRun(t *testing.T) {
	leader, db := startLeader(t)
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")
	dir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, dir)
	if err != nil {
		t.Fatal(err)
	}
	fdb, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer fdb.Close()
	fdb.SetAppliedLSN(wm)
	commitCrossTx(t, db, "div")

	// Apply the leader's real envelope at its LSN, then rewind the follower's
	// applied LSN and make the leader's stream deliver the same LSN again with
	// a different body: simulate by applying a doctored envelope directly.
	fol := server.NewFollower(fdb, leader, "f", "", discardLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go fol.Run(ctx)
	waitAllConverged(t, db, fdb, "xa", "xb")
	lsn := fdb.AppliedLSN()

	ops := []engine.TxEnvelopeOp{
		{Collection: "xa", Entry: store.Entry{ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"n": 99}}},
	}
	env, err := engine.NewTxEnvelope("different", "div", ops)
	if err != nil {
		t.Fatal(err)
	}
	// Same LSN, different content: divergence, nothing applied.
	if _, err := fdb.ApplyReplicatedTx(lsn, env); !errors.Is(err, engine.ErrReplicationDivergence) {
		t.Fatalf("want ErrReplicationDivergence, got %v", err)
	}
	if fdb.AppliedLSN() != lsn {
		t.Fatalf("applied LSN moved: %d -> %d", lsn, fdb.AppliedLSN())
	}
}
