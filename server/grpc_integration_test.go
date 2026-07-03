//nolint:errcheck
package server_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/srjn45/filedbv2/engine"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
	"github.com/srjn45/filedbv2/server"
)

// newTestServer spins up an in-process gRPC server backed by a real engine.DB
// and returns a connected client. The server is stopped when the test ends.
func newTestServer(t *testing.T) pb.FileDBClient {
	t.Helper()

	dir := t.TempDir()
	db, err := engine.Open(dir, engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	grpcSrv := grpc.NewServer()
	pb.RegisterFileDBServer(grpcSrv, gs)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewFileDBClient(conn)
}

// ctx returns a background context (no auth needed — server created without interceptors).
func ctx() context.Context { return context.Background() }

// txCtx returns a context with the x-tx-id metadata header set.
func txCtx(txID string) context.Context {
	return metadata.NewOutgoingContext(ctx(), metadata.Pairs("x-tx-id", txID))
}

// ---- Collection management --------------------------------------------------

func TestIntegration_CollectionLifecycle(t *testing.T) {
	c := newTestServer(t)

	// Create.
	cr, err := c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "users"})
	if err != nil || cr.Name != "users" {
		t.Fatalf("CreateCollection: got (%v, %v)", cr, err)
	}

	// Duplicate must fail.
	if _, err := c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "users"}); err == nil {
		t.Error("expected error creating duplicate collection")
	}

	// List.
	lr, err := c.ListCollections(ctx(), &pb.ListCollectionsRequest{})
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(lr.Names) != 1 || lr.Names[0] != "users" {
		t.Errorf("ListCollections: got %v", lr.Names)
	}

	// Drop.
	dr, err := c.DropCollection(ctx(), &pb.DropCollectionRequest{Name: "users"})
	if err != nil || !dr.Ok {
		t.Fatalf("DropCollection: got (%v, %v)", dr, err)
	}

	// List again — empty.
	lr2, _ := c.ListCollections(ctx(), &pb.ListCollectionsRequest{})
	if len(lr2.Names) != 0 {
		t.Errorf("expected empty list after drop, got %v", lr2.Names)
	}
}

// ---- Insert / FindById / Update / Delete ------------------------------------

func TestIntegration_CRUDBasic(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "things"})

	data, _ := structpb.NewStruct(map[string]any{"name": "apple", "count": float64(3)})
	ir, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "things", Data: data})
	if err != nil || ir.Id == 0 {
		t.Fatalf("Insert: got (%v, %v)", ir, err)
	}

	// FindById.
	fr, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "things", Id: ir.Id})
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if fr.Record.Data.Fields["name"].GetStringValue() != "apple" {
		t.Errorf("FindById name: got %v", fr.Record.Data.Fields["name"])
	}

	// Update.
	upData, _ := structpb.NewStruct(map[string]any{"name": "pear", "count": float64(5)})
	ur, err := c.Update(ctx(), &pb.UpdateRequest{Collection: "things", Id: ir.Id, Data: upData})
	if err != nil || ur.Id != ir.Id {
		t.Fatalf("Update: got (%v, %v)", ur, err)
	}

	// Confirm updated value.
	fr2, _ := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "things", Id: ir.Id})
	if fr2.Record.Data.Fields["name"].GetStringValue() != "pear" {
		t.Errorf("after update name: got %v", fr2.Record.Data.Fields["name"])
	}

	// Delete.
	delR, err := c.Delete(ctx(), &pb.DeleteRequest{Collection: "things", Id: ir.Id})
	if err != nil || !delR.Ok {
		t.Fatalf("Delete: got (%v, %v)", delR, err)
	}

	// FindById must return NotFound.
	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "things", Id: ir.Id}); err == nil {
		t.Error("expected NotFound after delete")
	}
}

// ---- InsertMany -------------------------------------------------------------

func TestIntegration_InsertMany(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "batch"})

	recs := make([]*structpb.Struct, 5)
	for i := range recs {
		recs[i], _ = structpb.NewStruct(map[string]any{"i": float64(i)})
	}
	imr, err := c.InsertMany(ctx(), &pb.InsertManyRequest{Collection: "batch", Records: recs})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(imr.Ids) != 5 {
		t.Errorf("expected 5 ids, got %d", len(imr.Ids))
	}
	// IDs must be distinct.
	seen := make(map[uint64]bool)
	for _, id := range imr.Ids {
		if seen[id] {
			t.Errorf("duplicate id %d", id)
		}
		seen[id] = true
	}
}

// ---- Find with filter and order_by ------------------------------------------

func collectFind(t *testing.T, stream pb.FileDB_FindClient) []*pb.Record {
	t.Helper()
	var out []*pb.Record
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Find.Recv: %v", err)
		}
		out = append(out, resp.Record)
	}
	return out
}

func TestIntegration_Find_FilterAndOrder(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "nums"})

	for i := 1; i <= 5; i++ {
		d, _ := structpb.NewStruct(map[string]any{"v": float64(i)})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "nums", Data: d})
	}

	// Find all, ordered descending.
	stream, err := c.Find(ctx(), &pb.FindRequest{
		Collection: "nums",
		OrderBy:    "v",
		Descending: true,
	})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recs := collectFind(t, stream)
	if len(recs) != 5 {
		t.Fatalf("expected 5 records, got %d", len(recs))
	}
	// First record should have v=5.
	if recs[0].Data.Fields["v"].GetNumberValue() != 5 {
		t.Errorf("first record v: want 5, got %v", recs[0].Data.Fields["v"])
	}

	// Find with eq filter — only v=3.
	stream2, err := c.Find(ctx(), &pb.FindRequest{
		Collection: "nums",
		Filter: &pb.Filter{Kind: &pb.Filter_Field{Field: &pb.FieldFilter{
			Field: "v",
			Op:    pb.FilterOp_EQ,
			Value: "3",
		}}},
	})
	if err != nil {
		t.Fatalf("Find with filter: %v", err)
	}
	recs2 := collectFind(t, stream2)
	if len(recs2) != 1 || recs2[0].Data.Fields["v"].GetNumberValue() != 3 {
		t.Errorf("filtered find: got %v", recs2)
	}
}

// TestIntegration_Find_OrderNumericNotLexical drives order_by end-to-end over
// gRPC with values that span the numeric/lexical boundary (2 vs 10 vs 100). A
// naive string sort would put "10" and "100" before "2"; the typed comparison
// must sort them numerically.
func TestIntegration_Find_OrderNumericNotLexical(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "spread"})

	for _, n := range []float64{10, 2, 100, 9, 1} {
		d, _ := structpb.NewStruct(map[string]any{"score": n})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "spread", Data: d})
	}

	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: "spread", OrderBy: "score"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recs := collectFind(t, stream)
	want := []float64{1, 2, 9, 10, 100}
	if len(recs) != len(want) {
		t.Fatalf("got %d records, want %d", len(recs), len(want))
	}
	for i, w := range want {
		if got := recs[i].Data.Fields["score"].GetNumberValue(); got != w {
			t.Errorf("ascending[%d] = %v, want %v (numeric, not lexical)", i, got, w)
		}
	}
}

func TestIntegration_Find_LimitOffset(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "paged"})

	for i := 1; i <= 10; i++ {
		d, _ := structpb.NewStruct(map[string]any{"n": float64(i)})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "paged", Data: d})
	}

	stream, err := c.Find(ctx(), &pb.FindRequest{
		Collection: "paged",
		OrderBy:    "n",
		Offset:     3,
		Limit:      4,
	})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recs := collectFind(t, stream)
	if len(recs) != 4 {
		t.Errorf("expected 4 records with limit=4, got %d", len(recs))
	}
	// offset 3, limit 4 over 1..10 ordered by n → 4,5,6,7.
	want := []float64{4, 5, 6, 7}
	for i, r := range recs {
		if got := r.Data.Fields["n"].GetNumberValue(); got != want[i] {
			t.Errorf("page[%d]=%v want %v (full page %v)", i, got, want[i], want)
		}
	}
}

// TestIntegration_Find_UnorderedLimit checks the push-down streaming path:
// an unordered limited query returns exactly the requested number of records.
func TestIntegration_Find_UnorderedLimit(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "big"})
	for i := 1; i <= 500; i++ {
		d, _ := structpb.NewStruct(map[string]any{"n": float64(i)})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "big", Data: d})
	}

	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: "big", Limit: 7})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recs := collectFind(t, stream)
	if len(recs) != 7 {
		t.Errorf("unordered limit=7: got %d records", len(recs))
	}
}

// TestIntegration_Find_CancelStops verifies that cancelling the client context
// mid-stream aborts the Find with a Canceled status.
func TestIntegration_Find_CancelStops(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "cancelme"})
	for i := 1; i <= 500; i++ {
		d, _ := structpb.NewStruct(map[string]any{"n": float64(i)})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "cancelme", Data: d})
	}

	cctx, cancel := context.WithCancel(ctx())
	defer cancel()
	stream, err := c.Find(cctx, &pb.FindRequest{Collection: "cancelme"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	// Read a couple, then cancel and confirm the stream ends with an error.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	cancel()
	sawErr := false
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Error("expected a stream error after cancellation")
	}
}

// ---- CollectionStats --------------------------------------------------------

func TestIntegration_CollectionStats(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "stat_col"})

	for i := 0; i < 3; i++ {
		d, _ := structpb.NewStruct(map[string]any{"x": float64(i)})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "stat_col", Data: d})
	}

	sr, err := c.CollectionStats(ctx(), &pb.CollectionStatsRequest{Collection: "stat_col"})
	if err != nil {
		t.Fatalf("CollectionStats: %v", err)
	}
	if sr.RecordCount != 3 {
		t.Errorf("RecordCount: want 3, got %d", sr.RecordCount)
	}
}

// ---- Transactions -----------------------------------------------------------

func TestIntegration_Tx_CommitVisible(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "tx_col"})

	// Pre-insert a record we'll update inside the tx.
	d0, _ := structpb.NewStruct(map[string]any{"val": "before"})
	ir, _ := c.Insert(ctx(), &pb.InsertRequest{Collection: "tx_col", Data: d0})

	// Begin tx.
	btr, err := c.BeginTx(ctx(), &pb.BeginTxRequest{Collection: "tx_col"})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	txID := btr.TxId

	// Stage insert inside tx — not yet visible.
	d1, _ := structpb.NewStruct(map[string]any{"val": "new"})
	txIR, err := c.Insert(txCtx(txID), &pb.InsertRequest{Collection: "tx_col", Data: d1})
	if err != nil {
		t.Fatalf("tx Insert: %v", err)
	}

	// Stage update inside tx.
	d2, _ := structpb.NewStruct(map[string]any{"val": "updated"})
	_, err = c.Update(txCtx(txID), &pb.UpdateRequest{Collection: "tx_col", Id: ir.Id, Data: d2})
	if err != nil {
		t.Fatalf("tx Update: %v", err)
	}

	// Before commit: new record should not exist.
	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "tx_col", Id: txIR.Id}); err == nil {
		t.Error("expected staged insert to be invisible before commit")
	}

	// Commit.
	cr, err := c.CommitTx(ctx(), &pb.CommitTxRequest{TxId: txID})
	if err != nil || !cr.Ok {
		t.Fatalf("CommitTx: got (%v, %v)", cr, err)
	}

	// After commit: new record visible.
	fr, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "tx_col", Id: txIR.Id})
	if err != nil {
		t.Fatalf("FindById after commit: %v", err)
	}
	if fr.Record.Data.Fields["val"].GetStringValue() != "new" {
		t.Errorf("post-commit val: got %v", fr.Record.Data.Fields["val"])
	}

	// Update also committed.
	fr2, _ := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "tx_col", Id: ir.Id})
	if fr2.Record.Data.Fields["val"].GetStringValue() != "updated" {
		t.Errorf("post-commit update val: got %v", fr2.Record.Data.Fields["val"])
	}
}

func TestIntegration_Tx_RollbackInvisible(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "rollback_col"})

	btr, _ := c.BeginTx(ctx(), &pb.BeginTxRequest{Collection: "rollback_col"})
	txID := btr.TxId

	d, _ := structpb.NewStruct(map[string]any{"val": "ghost"})
	txIR, _ := c.Insert(txCtx(txID), &pb.InsertRequest{Collection: "rollback_col", Data: d})

	// Rollback.
	rr, err := c.RollbackTx(ctx(), &pb.RollbackTxRequest{TxId: txID})
	if err != nil || !rr.Ok {
		t.Fatalf("RollbackTx: got (%v, %v)", rr, err)
	}

	// Staged insert must not be visible.
	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "rollback_col", Id: txIR.Id}); err == nil {
		t.Error("expected rolled-back insert to be invisible")
	}

	// tx_id reuse must fail.
	if _, err := c.CommitTx(ctx(), &pb.CommitTxRequest{TxId: txID}); err == nil {
		t.Error("expected error committing after rollback")
	}
}

func TestIntegration_Tx_DeleteInTx(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "del_tx"})

	d, _ := structpb.NewStruct(map[string]any{"x": "gone"})
	ir, _ := c.Insert(ctx(), &pb.InsertRequest{Collection: "del_tx", Data: d})

	btr, _ := c.BeginTx(ctx(), &pb.BeginTxRequest{Collection: "del_tx"})
	c.Delete(txCtx(btr.TxId), &pb.DeleteRequest{Collection: "del_tx", Id: ir.Id})
	c.CommitTx(ctx(), &pb.CommitTxRequest{TxId: btr.TxId})

	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "del_tx", Id: ir.Id}); err == nil {
		t.Error("expected record to be gone after tx delete + commit")
	}
}

// ---- Error paths ------------------------------------------------------------

func TestIntegration_Errors_UnknownCollection(t *testing.T) {
	c := newTestServer(t)

	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "noexist", Id: 1}); err == nil {
		t.Error("expected error for unknown collection in FindById")
	}
	d, _ := structpb.NewStruct(map[string]any{"x": 1})
	if _, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "noexist", Data: d}); err == nil {
		t.Error("expected error for unknown collection in Insert")
	}
	if _, err := c.BeginTx(ctx(), &pb.BeginTxRequest{Collection: "noexist"}); err == nil {
		t.Error("expected error for unknown collection in BeginTx")
	}
}

func TestIntegration_Errors_UnknownTx(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "err_col"})

	if _, err := c.CommitTx(ctx(), &pb.CommitTxRequest{TxId: "fake-tx-id"}); err == nil {
		t.Error("expected error committing unknown tx")
	}
	if _, err := c.RollbackTx(ctx(), &pb.RollbackTxRequest{TxId: "fake-tx-id"}); err == nil {
		t.Error("expected error rolling back unknown tx")
	}
}
