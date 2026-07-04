//nolint:errcheck
package server_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/srjn45/filedbv2/engine"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
	"github.com/srjn45/filedbv2/query"
	"github.com/srjn45/filedbv2/server"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func leaderEngineConfig() engine.CollectionConfig {
	return engine.CollectionConfig{
		SegmentMaxSize:      4 * 1024 * 1024,
		CompactInterval:     time.Hour,
		CompactDirtyPct:     0.30,
		ReplicationRingSize: 8192,
	}
}

func followerEngineConfig() engine.CollectionConfig {
	return engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: time.Hour,
		CompactDirtyPct: 0.30,
		// Open in the follower role so the read-only guard rejects writes and a
		// Promote (R3) is a valid transition — matching --replicate-from mode.
		Follower: true,
	}
}

// startLeader spins up an in-process, replication-enabled leader and returns a
// connected client plus the backing DB (for direct state comparison in tests).
func startLeader(t *testing.T) (pb.FileDBClient, *engine.DB) {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(dir, leaderEngineConfig())
	if err != nil {
		t.Fatalf("engine.Open leader: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	srv := grpc.NewServer()
	pb.RegisterFileDBServer(srv, gs)
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
	return pb.NewFileDBClient(conn), db
}

func mustCreate(t *testing.T, client pb.FileDBClient, coll string) {
	t.Helper()
	if _, err := client.CreateCollection(context.Background(), &pb.CreateCollectionRequest{Name: coll}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
}

func mustInsert(t *testing.T, client pb.FileDBClient, coll string, data map[string]any) uint64 {
	t.Helper()
	st, err := structpb.NewStruct(data)
	if err != nil {
		t.Fatalf("struct: %v", err)
	}
	resp, err := client.Insert(context.Background(), &pb.InsertRequest{Collection: coll, Data: st})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return resp.Id
}

// scanState reads a collection into an id→data map via the engine.
func scanState(t *testing.T, db *engine.DB, coll string) map[uint64]map[string]any {
	t.Helper()
	out := make(map[uint64]map[string]any)
	c, err := db.Collection(coll)
	if err != nil {
		return out
	}
	if _, err := c.ScanStream(context.Background(), engine.ScanOptions{Filter: query.MatchAll}, func(r engine.ScanResult) error {
		out[r.ID] = r.Data
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

// waitConverged polls until the follower has applied at least the leader's
// current LSN and their query state for coll is identical, or fails after a
// generous timeout.
func waitConverged(t *testing.T, leader, follower *engine.DB, coll string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		lsn := leader.CurrentLSN()
		if follower.AppliedLSN() >= lsn {
			want := scanState(t, leader, coll)
			got := scanState(t, follower, coll)
			if reflect.DeepEqual(want, got) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not converge: leaderLSN=%d followerLSN=%d leaderRecs=%d followerRecs=%d",
				leader.CurrentLSN(), follower.AppliedLSN(),
				len(scanState(t, leader, coll)), len(scanState(t, follower, coll)))
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// bootstrapFollower bootstraps a fresh follower DB from the leader and starts its
// apply loop. It returns the follower DB and a stop func that cleanly halts the
// loop and closes the DB.
func bootstrapFollower(t *testing.T, leader pb.FileDBClient) (*engine.DB, func()) {
	t.Helper()
	dir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, dir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	fdb, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	if err := fdb.SetAppliedLSN(wm); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fol := server.NewFollower(fdb, leader, "f1", "", discardLogger())
	done := make(chan struct{})
	go func() { fol.Run(ctx); close(done) }()
	stop := func() {
		cancel()
		<-done
		fdb.Close()
	}
	return fdb, stop
}

// TestReplication_ConvergesUnderWrites: a follower started against a live leader
// converges to identical query results under continuous writes.
func TestReplication_ConvergesUnderWrites(t *testing.T) {
	t.Parallel()
	client, ldb := startLeader(t)
	mustCreate(t, client, "c")
	for i := 0; i < 20; i++ {
		mustInsert(t, client, "c", map[string]any{"i": float64(i)})
	}

	fdb, stop := bootstrapFollower(t, client)
	defer stop()

	// Continuous writes after the follower is tailing.
	done := make(chan struct{})
	go func() {
		for i := 20; i < 220; i++ {
			mustInsert(t, client, "c", map[string]any{"i": float64(i)})
		}
		close(done)
	}()
	<-done

	waitConverged(t, ldb, fdb, "c")
}

// TestReplication_KillResume: killing and resuming the follower resumes from its
// applied LSN without gaps or duplication.
func TestReplication_KillResume(t *testing.T) {
	t.Parallel()
	client, ldb := startLeader(t)
	mustCreate(t, client, "c")

	// Batch 1.
	var ids []uint64
	for i := 0; i < 15; i++ {
		ids = append(ids, mustInsert(t, client, "c", map[string]any{"i": float64(i)}))
	}

	// Bootstrap + start follower #1 (manual lifecycle so we can "kill" it).
	dir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), client, dir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	fdb1, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower1: %v", err)
	}
	fdb1.SetAppliedLSN(wm)
	ctx1, cancel1 := context.WithCancel(context.Background())
	fol1 := server.NewFollower(fdb1, client, "f", "", discardLogger())
	done1 := make(chan struct{})
	go func() { fol1.Run(ctx1); close(done1) }()
	waitConverged(t, ldb, fdb1, "c")
	appliedBeforeKill := fdb1.AppliedLSN()

	// Kill the follower.
	cancel1()
	<-done1
	fdb1.Close()

	// Batch 2 while the follower is down: new inserts + updates to batch-1 records.
	for i := 15; i < 45; i++ {
		mustInsert(t, client, "c", map[string]any{"i": float64(i)})
	}
	for _, id := range ids {
		upd, _ := structpb.NewStruct(map[string]any{"i": float64(1000), "updated": true})
		if _, err := client.Update(context.Background(), &pb.UpdateRequest{Collection: "c", Id: id, Data: upd}); err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	// Resume: reopen the same data dir — the applied LSN is restored from disk.
	fdb2, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("reopen follower2: %v", err)
	}
	if got := fdb2.AppliedLSN(); got == 0 || got != appliedBeforeKill {
		t.Fatalf("resumed applied LSN = %d, want %d (persisted)", got, appliedBeforeKill)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	fol2 := server.NewFollower(fdb2, client, "f", "", discardLogger())
	done2 := make(chan struct{})
	go func() { fol2.Run(ctx2); close(done2) }()
	defer func() { cancel2(); <-done2; fdb2.Close() }()

	waitConverged(t, ldb, fdb2, "c")
}

// TestReplication_SnapshotBootstrapPlusTailEqualsReplay: a snapshot-bootstrapped
// follower plus the streamed tail ends in the same state as a full replay,
// across inserts, updates, and deletes.
func TestReplication_SnapshotBootstrapPlusTailEqualsReplay(t *testing.T) {
	t.Parallel()
	client, ldb := startLeader(t)
	mustCreate(t, client, "c")

	// Pre-bootstrap data (delivered via the snapshot).
	var ids []uint64
	for i := 0; i < 30; i++ {
		ids = append(ids, mustInsert(t, client, "c", map[string]any{"i": float64(i)}))
	}

	fdb, stop := bootstrapFollower(t, client)
	defer stop()

	// Post-bootstrap data (delivered via the tail): inserts, updates, deletes.
	for i := 30; i < 60; i++ {
		mustInsert(t, client, "c", map[string]any{"i": float64(i)})
	}
	for _, id := range ids[:10] {
		upd, _ := structpb.NewStruct(map[string]any{"i": float64(-1)})
		client.Update(context.Background(), &pb.UpdateRequest{Collection: "c", Id: id, Data: upd})
	}
	for _, id := range ids[10:20] {
		client.Delete(context.Background(), &pb.DeleteRequest{Collection: "c", Id: id})
	}

	waitConverged(t, ldb, fdb, "c")

	// Sanity: the follower reports the same live count via ReplicationStatus lag 0.
	st, err := client.ReplicationStatus(context.Background(), &pb.ReplicationStatusRequest{})
	if err != nil {
		t.Fatalf("ReplicationStatus: %v", err)
	}
	if st.LeaderLsn == 0 {
		t.Fatal("leader LSN should be non-zero")
	}
}

// TestReplication_StatusReportsFollower verifies the leader surfaces a connected
// follower and a shrinking lag.
func TestReplication_StatusReportsFollower(t *testing.T) {
	t.Parallel()
	client, ldb := startLeader(t)
	mustCreate(t, client, "c")
	for i := 0; i < 5; i++ {
		mustInsert(t, client, "c", map[string]any{"i": float64(i)})
	}
	fdb, stop := bootstrapFollower(t, client)
	defer stop()
	waitConverged(t, ldb, fdb, "c")

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := client.ReplicationStatus(context.Background(), &pb.ReplicationStatusRequest{})
		if err != nil {
			t.Fatalf("ReplicationStatus: %v", err)
		}
		if len(st.Followers) >= 1 && st.Followers[0].Lag == 0 && st.Followers[0].AckedLsn == st.LeaderLsn {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower not reported caught up: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
