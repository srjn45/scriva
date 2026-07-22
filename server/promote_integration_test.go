//nolint:errcheck
package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
)

// startReplicaServer serves fdb behind the R2/R3 read-only guard — the same
// wiring cmd/filedb installs in --replicate-from mode — with the given promotion
// lag ceiling, and returns a client to it. The read-only guard is dynamic, so a
// successful Promote lifts it live.
func startReplicaServer(t *testing.T, fdb *engine.DB, maxLag uint64) pb.ScrivaClient {
	t.Helper()
	roUnary, roStream := server.ReadOnlyInterceptors(fdb)
	gs := server.NewGRPCServer(fdb, 5*time.Minute, server.WithPromoteMaxLag(maxLag))
	t.Cleanup(gs.Close)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(roUnary), grpc.ChainStreamInterceptor(roStream))
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
	return pb.NewScrivaClient(conn)
}

// TestPromote_CaughtUpFollowerAcceptsWrites: a write refused with
// FailedPrecondition on a read-only follower succeeds after the follower is
// promoted through the Promote RPC — the read-only guard is lifted live.
func TestPromote_CaughtUpFollowerAcceptsWrites(t *testing.T) {
	t.Parallel()
	client, ldb := startLeader(t)
	mustCreate(t, client, "c")
	mustInsert(t, client, "c", map[string]any{"i": float64(1)})

	fdb, stop := bootstrapFollower(t, client)
	defer stop()
	waitConverged(t, ldb, fdb, "c")

	ro := startReplicaServer(t, fdb, engine.DefaultPromoteMaxLag)
	ctx := context.Background()

	// Before promotion the follower rejects writes with FailedPrecondition.
	_, err := ro.Insert(ctx, &pb.InsertRequest{Collection: "c", Data: mustStruct(t, map[string]any{"i": float64(2)})})
	if err == nil {
		t.Fatalf("write on follower succeeded, want FailedPrecondition")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("pre-promote write code = %s, want FailedPrecondition", code)
	}

	// Promote the caught-up follower.
	resp, err := ro.Promote(ctx, &pb.PromoteRequest{})
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if resp.Role != "leader" {
		t.Fatalf("promoted role = %q, want leader", resp.Role)
	}
	if resp.Lag != 0 {
		t.Fatalf("promoted lag = %d, want 0 (caught up)", resp.Lag)
	}
	if fdb.IsFollower() {
		t.Fatalf("engine still reports follower after Promote")
	}

	// The same write now succeeds against the promoted leader.
	if _, err := ro.Insert(ctx, &pb.InsertRequest{Collection: "c", Data: mustStruct(t, map[string]any{"i": float64(2)})}); err != nil {
		t.Fatalf("write after promotion failed: %v", err)
	}
}

// TestPromote_LaggingFollowerRefusedUnlessForced: a follower whose lag exceeds
// the server threshold is refused with FailedPrecondition, but force=true wins.
func TestPromote_LaggingFollowerRefusedUnlessForced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fdb, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	t.Cleanup(func() { fdb.Close() })
	if _, err := fdb.CreateCollection("c"); err != nil {
		t.Fatalf("seed collection: %v", err)
	}

	// The follower knows the leader reached LSN 100 but has applied only 10.
	fdb.NoteLeaderLSN(100)
	if err := fdb.SetAppliedLSN(10); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}

	ro := startReplicaServer(t, fdb, engine.DefaultPromoteMaxLag)
	ctx := context.Background()

	// Unforced promotion of the lagging follower is refused, and the node stays
	// a read-only follower.
	_, err = ro.Promote(ctx, &pb.PromoteRequest{})
	if err == nil {
		t.Fatalf("promote lagging follower succeeded, want FailedPrecondition")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("lagging promote code = %s, want FailedPrecondition", code)
	}
	if !fdb.IsFollower() {
		t.Fatalf("refused promotion still flipped role to leader")
	}
	// Writes are still refused (guard intact).
	if _, err := ro.Insert(ctx, &pb.InsertRequest{Collection: "c", Data: mustStruct(t, map[string]any{"i": float64(1)})}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("write after refused promotion code = %s, want FailedPrecondition", status.Code(err))
	}

	// Forcing overrides the guard and promotes the lagging follower.
	resp, err := ro.Promote(ctx, &pb.PromoteRequest{Force: true})
	if err != nil {
		t.Fatalf("forced promote: %v", err)
	}
	if resp.Role != "leader" || resp.Lag != 90 {
		t.Fatalf("forced promote role=%q lag=%d, want leader/90", resp.Role, resp.Lag)
	}
	if _, err := ro.Insert(ctx, &pb.InsertRequest{Collection: "c", Data: mustStruct(t, map[string]any{"i": float64(1)})}); err != nil {
		t.Fatalf("write after forced promotion failed: %v", err)
	}
}

// TestPromote_LeaderRefused: Promote against a leader is refused with
// FailedPrecondition (nothing to promote).
func TestPromote_LeaderRefused(t *testing.T) {
	t.Parallel()
	client, _ := startLeader(t)
	_, err := client.Promote(context.Background(), &pb.PromoteRequest{})
	if err == nil {
		t.Fatalf("promote on leader succeeded, want FailedPrecondition")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("leader promote code = %s, want FailedPrecondition", code)
	}
}
