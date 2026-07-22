//nolint:errcheck
package server_test

import (
	"context"
	"net"
	"sync/atomic"
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

// newQuotaServer spins up an in-process gRPC server whose "capped" collection is
// limited to maxRecords live records, and returns the client plus a counter that
// the quota-rejection observer increments.
func newQuotaServer(t *testing.T, collection string, maxRecords uint64) (pb.ScrivaClient, *atomic.Int64) {
	t.Helper()

	dir := t.TempDir()
	cfg := engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
		Quotas:          map[string]engine.Quota{collection: {MaxRecords: maxRecords}},
	}
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	var rejects atomic.Int64
	gs := server.NewGRPCServer(db, 5*time.Minute,
		server.WithQuotaObserver(func(string) { rejects.Add(1) }))
	t.Cleanup(gs.Close)

	grpcSrv := grpc.NewServer()
	pb.RegisterScrivaServer(grpcSrv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewScrivaClient(conn), &rejects
}

func TestIntegration_QuotaResourceExhausted(t *testing.T) {
	c, rejects := newQuotaServer(t, "capped", 2)
	bg := context.Background()

	if _, err := c.CreateCollection(bg, &pb.CreateCollectionRequest{Name: "capped"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	rec := func(v float64) *structpb.Struct {
		s, _ := structpb.NewStruct(map[string]any{"n": v})
		return s
	}

	// The first two inserts fit the MaxRecords=2 budget.
	for i := 0; i < 2; i++ {
		if _, err := c.Insert(bg, &pb.InsertRequest{Collection: "capped", Data: rec(float64(i))}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	// The third breaches the quota → ResourceExhausted.
	_, err := c.Insert(bg, &pb.InsertRequest{Collection: "capped", Data: rec(2)})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("over-quota insert: got code %v (%v), want ResourceExhausted", got, err)
	}
	if got := rejects.Load(); got != 1 {
		t.Fatalf("quota-reject metric = %d, want 1", got)
	}

	// A batch that would breach the quota is rejected atomically with the same code.
	recs := []*structpb.Struct{rec(10), rec(11)}
	_, err = c.InsertMany(bg, &pb.InsertManyRequest{Collection: "capped", Records: recs})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("over-quota InsertMany: got code %v (%v), want ResourceExhausted", got, err)
	}
	if got := rejects.Load(); got != 2 {
		t.Fatalf("quota-reject metric = %d, want 2 after batch rejection", got)
	}
}
