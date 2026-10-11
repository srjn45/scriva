//nolint:errcheck
package server_test

import (
	"context"
	"errors"
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
	_, err := db.CommitXTx(key, []engine.XTxOp{
		{Collection: "xa", Op: store.OpInsert, Data: map[string]any{"n": 1}},
		{Collection: "xb", Op: store.OpInsert, Data: map[string]any{"n": 2}},
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

// TestReplicationV2_FollowerSeamRefusesWithoutAdvancing: the v2 follower decodes
// and verifies the envelope, then stops at the apply seam without applying it or
// advancing the applied LSN.
func TestReplicationV2_FollowerSeamRefusesWithoutAdvancing(t *testing.T) {
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
	if err := fdb.SetAppliedLSN(wm); err != nil {
		t.Fatal(err)
	}
	commitCrossTx(t, db, "k3")

	fol := server.NewFollower(fdb, leader, "f-v2", "", discardLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = fol.Run(ctx)
	if !errors.Is(err, server.ErrEnvelopeApplyUnsupported) {
		t.Fatalf("want ErrEnvelopeApplyUnsupported, got %v", err)
	}
	if got := fdb.AppliedLSN(); got != wm {
		t.Fatalf("applied LSN advanced past an unapplied tx: %d -> %d", wm, got)
	}
}
