package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

func startAtomicLeader(t *testing.T) (pb.ScrivaClient, *engine.DB) {
	t.Helper()
	dir := t.TempDir()
	cfg := leaderEngineConfig()
	cfg.EnableAtomicBatches = true
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatalf("engine.Open leader: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	srv := grpc.NewServer()
	pb.RegisterScrivaServer(srv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewScrivaClient(conn), db
}

func TestReplication_SingleParticipantBatch_EndToEnd(t *testing.T) {
	engine.SetAtomicBatchWriterReady(t, true)
	leader, db := startAtomicLeader(t)
	mustCreate(t, leader, "batchcol")

	// Set up follower
	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, fdir)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	fdb, err := engine.Open(fdir, followerEngineConfig())
	if err != nil {
		t.Fatalf("Open follower: %v", err)
	}
	defer fdb.Close()
	if fdb.AppliedLSN() < wm {
		fdb.SetAppliedLSN(wm)
	}

	fol := server.NewFollower(fdb, leader, "f-batch", "", discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fol.Run(ctx)

	// Write batch via leader engine InsertMany
	col, err := db.Collection("batchcol")
	if err != nil {
		t.Fatal(err)
	}
	beforeLSN := db.CurrentLSN()
	records := []map[string]any{
		{"k": "one", "v": 1.0},
		{"k": "two", "v": 2.0},
		{"k": "three", "v": 3.0},
	}
	ids, _, err := col.InsertMany(records, time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids len = %d, want 3", len(ids))
	}
	afterLSN := db.CurrentLSN()
	if afterLSN != beforeLSN+1 {
		t.Fatalf("single-participant batch should consume 1 LSN: %d -> %d", beforeLSN, afterLSN)
	}

	// Wait for follower to converge
	waitAllConverged(t, db, fdb, "batchcol")

	if fdb.AppliedLSN() != afterLSN {
		t.Fatalf("follower applied LSN = %d, want %d", fdb.AppliedLSN(), afterLSN)
	}

	fcol, err := fdb.Collection("batchcol")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := fcol.Count(nil); n != 3 {
		t.Fatalf("follower record count = %d, want 3", n)
	}
	for _, id := range ids {
		if _, _, err := fcol.FindByID(id); err != nil {
			t.Fatalf("follower missing id %d: %v", id, err)
		}
	}
}

func TestReplication_MixedVersion_SingleParticipantBatchRefusesV1Follower(t *testing.T) {
	engine.SetAtomicBatchWriterReady(t, true)
	leader, db := startAtomicLeader(t)
	mustCreate(t, leader, "batch_mix")

	col, err := db.Collection("batch_mix")
	if err != nil {
		t.Fatal(err)
	}
	beforeLSN := db.CurrentLSN()

	// Commit single-collection batch with InsertMany
	records := []map[string]any{
		{"foo": "bar"},
		{"foo": "baz"},
	}
	_, _, err = col.InsertMany(records, time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Connect as a legacy v1-only follower (ProtocolVersions = [1])
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := leader.Replicate(ctx, &pb.ReplicateRequest{
		FromLsn:          beforeLSN,
		FollowerId:       "legacy-v1-follower",
		ProtocolVersions: []uint32{engine.ReplicationProtocolV1}, // V1 only!
	})
	if err != nil {
		t.Fatalf("Replicate call failed: %v", err)
	}

	// Attempting to stream the single-participant batch envelope must fail with REPLICATION_PROTOCOL_UNSUPPORTED
	// The leader must never flatten or downgrade the transaction into per-entry records.
	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatal("expected error receiving v2 transaction on v1 stream, but got success")
	}
	if !server.IsReplicationProtocolError(recvErr) {
		t.Fatalf("expected replication protocol error (REPLICATION_PROTOCOL_UNSUPPORTED), got: %v", recvErr)
	}
}

func TestReplication_SingleParticipantBatch_InsertManyViaRPC(t *testing.T) {
	engine.SetAtomicBatchWriterReady(t, true)
	leader, db := startAtomicLeader(t)
	mustCreate(t, leader, "batch_rpc")

	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, fdir)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	fdb, err := engine.Open(fdir, followerEngineConfig())
	if err != nil {
		t.Fatalf("Open follower: %v", err)
	}
	defer fdb.Close()
	if fdb.AppliedLSN() < wm {
		fdb.SetAppliedLSN(wm)
	}

	fol := server.NewFollower(fdb, leader, "f-rpc", "", discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fol.Run(ctx)

	// Call InsertMany RPC on leader
	s1, _ := structpb.NewStruct(map[string]any{"v": 1.0})
	s2, _ := structpb.NewStruct(map[string]any{"v": 2.0})
	resp, err := leader.InsertMany(context.Background(), &pb.InsertManyRequest{
		Collection: "batch_rpc",
		Records:    []*structpb.Struct{s1, s2},
	})
	if err != nil {
		t.Fatalf("InsertMany RPC: %v", err)
	}
	if len(resp.Ids) != 2 {
		t.Fatalf("len(Ids) = %d, want 2", len(resp.Ids))
	}

	// Wait for follower to converge
	waitAllConverged(t, db, fdb, "batch_rpc")

	fcol, err := fdb.Collection("batch_rpc")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := fcol.Count(nil); n != 2 {
		t.Fatalf("follower record count = %d, want 2", n)
	}
}
