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
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
)

// startReadOnlyReplica serves the follower DB behind the R2 read-only
// interceptors — the same wiring cmd/filedb installs in --replicate-from mode —
// and returns a client to it.
func startReadOnlyReplica(t *testing.T, fdb *engine.DB) pb.ScrivaClient {
	t.Helper()
	roUnary, roStream := server.ReadOnlyInterceptors(fdb)
	gs := server.NewGRPCServer(fdb, 5*time.Minute)
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

// drainFind collects a streamed Find into an id→record map.
func drainFind(t *testing.T, client pb.ScrivaClient, coll string) map[uint64]*pb.Record {
	t.Helper()
	stream, err := client.Find(context.Background(), &pb.FindRequest{Collection: coll})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	out := make(map[uint64]*pb.Record)
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if resp.Record != nil {
			out[resp.Record.Id] = resp.Record
		}
	}
	return out
}

// TestReadReplica_ServesReadsConsistentWithLSN: a follower behind the read-only
// interceptors answers Find/FindById/FindByKey/Aggregate from its applied state,
// consistent with a write made on the leader, and reports its applied LSN via
// ReplicationStatus so a client can bound staleness.
func TestReadReplica_ServesReadsConsistentWithLSN(t *testing.T) {
	t.Parallel()
	client, ldb := startLeader(t)
	mustCreate(t, client, "c")

	// A keyless record and a keyed record, so FindById and FindByKey both apply.
	id := mustInsert(t, client, "c", map[string]any{"i": float64(1), "name": "alice"})
	if _, err := client.Insert(context.Background(), &pb.InsertRequest{
		Collection: "c", Key: "k1", Data: mustStruct(t, map[string]any{"i": float64(2), "name": "bob"}),
	}); err != nil {
		t.Fatalf("keyed insert: %v", err)
	}

	fdb, stop := bootstrapFollower(t, client)
	defer stop()

	// A write on the leader after the follower is tailing must become visible.
	id3 := mustInsert(t, client, "c", map[string]any{"i": float64(3), "name": "carol"})
	waitConverged(t, ldb, fdb, "c")

	ro := startReadOnlyReplica(t, fdb)

	// FindById reflects the post-tail leader write.
	got, err := ro.FindById(context.Background(), &pb.FindByIdRequest{Collection: "c", Id: id3})
	if err != nil {
		t.Fatalf("FindById on replica: %v", err)
	}
	if got.Record.Data.Fields["name"].GetStringValue() != "carol" {
		t.Fatalf("FindById returned %v, want name=carol", got.Record.Data)
	}

	// FindByKey resolves the keyed record.
	gk, err := ro.FindByKey(context.Background(), &pb.FindByKeyRequest{Collection: "c", Key: "k1"})
	if err != nil {
		t.Fatalf("FindByKey on replica: %v", err)
	}
	if gk.Record.Data.Fields["name"].GetStringValue() != "bob" {
		t.Fatalf("FindByKey returned %v, want name=bob", gk.Record.Data)
	}

	// Find streams every record the leader holds.
	recs := drainFind(t, ro, "c")
	if len(recs) != 3 {
		t.Fatalf("Find on replica returned %d records, want 3", len(recs))
	}
	if _, ok := recs[id]; !ok {
		t.Fatalf("Find on replica missing keyless record %d", id)
	}

	// Aggregate counts the whole set.
	astream, err := ro.Aggregate(context.Background(), &pb.AggregateRequest{
		Collection: "c", Aggregations: []pb.AggregateOp{pb.AggregateOp_AGG_COUNT},
	})
	if err != nil {
		t.Fatalf("Aggregate on replica: %v", err)
	}
	var total uint64
	for {
		resp, err := astream.Recv()
		if err != nil {
			break
		}
		total += resp.Count
	}
	if total != 3 {
		t.Fatalf("Aggregate count on replica = %d, want 3", total)
	}

	// ReplicationStatus surfaces the follower's applied LSN (the observable
	// staleness bound), matching the engine watermark and the leader's LSN.
	st, err := ro.ReplicationStatus(context.Background(), &pb.ReplicationStatusRequest{})
	if err != nil {
		t.Fatalf("ReplicationStatus on replica: %v", err)
	}
	if st.AppliedLsn == 0 || st.AppliedLsn != fdb.AppliedLSN() {
		t.Fatalf("replica applied_lsn = %d, want engine watermark %d (non-zero)", st.AppliedLsn, fdb.AppliedLSN())
	}
	if st.AppliedLsn != ldb.CurrentLSN() {
		t.Fatalf("replica applied_lsn = %d, want caught up to leader LSN %d", st.AppliedLsn, ldb.CurrentLSN())
	}
}

// TestReadReplica_RejectsWrites: every mutating RPC issued against a read-only
// follower is refused with FailedPrecondition and the documented message, while
// the underlying data is left untouched.
func TestReadReplica_RejectsWrites(t *testing.T) {
	t.Parallel()
	// A standalone follower DB (no tailing needed) with a pre-existing collection,
	// so write RPCs reach the guard rather than failing on a missing collection.
	dir := t.TempDir()
	fdb, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	t.Cleanup(func() { fdb.Close() })
	if _, err := fdb.CreateCollection("c"); err != nil {
		t.Fatalf("seed collection: %v", err)
	}

	ro := startReadOnlyReplica(t, fdb)
	ctx := context.Background()
	data := mustStruct(t, map[string]any{"i": float64(1)})

	writes := map[string]func() error{
		"CreateCollection": func() error {
			_, err := ro.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "x"})
			return err
		},
		"DropCollection": func() error {
			_, err := ro.DropCollection(ctx, &pb.DropCollectionRequest{Name: "c"})
			return err
		},
		"Insert": func() error {
			_, err := ro.Insert(ctx, &pb.InsertRequest{Collection: "c", Data: data})
			return err
		},
		"InsertMany": func() error {
			_, err := ro.InsertMany(ctx, &pb.InsertManyRequest{Collection: "c", Records: []*structpb.Struct{data}})
			return err
		},
		"Update": func() error {
			_, err := ro.Update(ctx, &pb.UpdateRequest{Collection: "c", Id: 1, Data: data})
			return err
		},
		"Delete": func() error {
			_, err := ro.Delete(ctx, &pb.DeleteRequest{Collection: "c", Id: 1})
			return err
		},
		"Upsert": func() error {
			_, err := ro.Upsert(ctx, &pb.UpsertRequest{Collection: "c", Key: "k", Data: data})
			return err
		},
		"UpdateByKey": func() error {
			_, err := ro.UpdateByKey(ctx, &pb.UpdateByKeyRequest{Collection: "c", Key: "k", Data: data})
			return err
		},
		"DeleteByKey": func() error {
			_, err := ro.DeleteByKey(ctx, &pb.DeleteByKeyRequest{Collection: "c", Key: "k"})
			return err
		},
		"UpdateIfRev": func() error {
			_, err := ro.UpdateIfRev(ctx, &pb.UpdateIfRevRequest{Collection: "c", Key: "k", ExpectedRev: 1, Data: data})
			return err
		},
		"EnsureIndex": func() error {
			_, err := ro.EnsureIndex(ctx, &pb.EnsureIndexRequest{Collection: "c", Field: "i"})
			return err
		},
		"DropIndex": func() error {
			_, err := ro.DropIndex(ctx, &pb.DropIndexRequest{Collection: "c", Field: "i"})
			return err
		},
		"BeginTx": func() error {
			_, err := ro.BeginTx(ctx, &pb.BeginTxRequest{Collection: "c"})
			return err
		},
		"CommitTx": func() error {
			_, err := ro.CommitTx(ctx, &pb.CommitTxRequest{TxId: "t"})
			return err
		},
		"RollbackTx": func() error {
			_, err := ro.RollbackTx(ctx, &pb.RollbackTxRequest{TxId: "t"})
			return err
		},
		"Compact": func() error {
			_, err := ro.Compact(ctx, &pb.CompactRequest{Collection: "c"})
			return err
		},
	}

	for name, call := range writes {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatalf("%s on read-only replica succeeded, want FailedPrecondition", name)
			}
			st, _ := status.FromError(err)
			if st.Code() != codes.FailedPrecondition {
				t.Fatalf("%s: code = %s, want FailedPrecondition", name, st.Code())
			}
			if st.Message() != server.ReadOnlyReplicaMessage {
				t.Fatalf("%s: message = %q, want %q", name, st.Message(), server.ReadOnlyReplicaMessage)
			}
		})
	}

	// The guard runs before any handler, so nothing was mutated: the seeded
	// collection is still empty and still present.
	col, err := fdb.Collection("c")
	if err != nil {
		t.Fatalf("collection gone after rejected writes: %v", err)
	}
	if got := col.Stats().RecordCount; got != 0 {
		t.Fatalf("record count = %d after rejected writes, want 0", got)
	}
}

// TestReadReplica_ReadsNotBlocked confirms the guard lets read and observability
// RPCs through unchanged (a regression guard on the writeMethods set).
func TestReadReplica_ReadsNotBlocked(t *testing.T) {
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

	ro := startReadOnlyReplica(t, fdb)
	ctx := context.Background()

	if _, err := ro.ListCollections(ctx, &pb.ListCollectionsRequest{}); err != nil {
		t.Fatalf("ListCollections blocked: %v", err)
	}
	if _, err := ro.CollectionStats(ctx, &pb.CollectionStatsRequest{Collection: "c"}); err != nil {
		t.Fatalf("CollectionStats blocked: %v", err)
	}
	if _, err := ro.ListIndexes(ctx, &pb.ListIndexesRequest{Collection: "c"}); err != nil {
		t.Fatalf("ListIndexes blocked: %v", err)
	}
	if _, err := ro.ReplicationStatus(ctx, &pb.ReplicationStatusRequest{}); err != nil {
		t.Fatalf("ReplicationStatus blocked: %v", err)
	}
}
