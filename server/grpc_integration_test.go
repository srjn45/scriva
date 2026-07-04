//nolint:errcheck
package server_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/srjn45/filedbv2/engine"
	"github.com/srjn45/filedbv2/internal/auth"
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

// TestIntegration_Find_Projection drives field projection (N2) end-to-end over
// gRPC: a projected Find returns only the requested fields, an empty projection
// returns the full record, and an unknown requested field is silently absent.
func TestIntegration_Find_Projection(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "fruit"})
	d, _ := structpb.NewStruct(map[string]any{"name": "apple", "color": "red", "price": float64(3)})
	if _, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "fruit", Data: d}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Projected to name + an unknown field: only name survives, id/rev are set,
	// and the unknown field is silently omitted (not an error).
	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: "fruit", Fields: []string{"name", "nope"}})
	if err != nil {
		t.Fatalf("Find projected: %v", err)
	}
	recs := collectFind(t, stream)
	if len(recs) != 1 {
		t.Fatalf("projected find: got %d records, want 1", len(recs))
	}
	got := recs[0].Data.Fields
	if len(got) != 1 {
		t.Errorf("projected data has %d fields, want only name: %v", len(got), got)
	}
	if got["name"].GetStringValue() != "apple" {
		t.Errorf("projected name = %v, want apple", got["name"])
	}
	if _, ok := got["color"]; ok {
		t.Errorf("projected data should not carry color: %v", got)
	}
	if _, ok := got["nope"]; ok {
		t.Errorf("unknown requested field should be silently absent: %v", got)
	}
	if recs[0].Id == 0 {
		t.Errorf("projected record must still carry its id, got 0")
	}
	if recs[0].Rev == 0 {
		t.Errorf("projected record must still carry its rev, got 0")
	}

	// Empty projection returns the full record (backward compatible).
	stream2, err := c.Find(ctx(), &pb.FindRequest{Collection: "fruit"})
	if err != nil {
		t.Fatalf("Find full: %v", err)
	}
	full := collectFind(t, stream2)
	if len(full) != 1 || len(full[0].Data.Fields) != 3 {
		t.Errorf("empty projection should return full 3-field record, got %v", full)
	}
}

// TestIntegration_FindById_Projection checks projection on the point-lookup read
// path and that a record's caller-supplied string key survives projection.
func TestIntegration_FindById_Projection(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "people"})
	d, _ := structpb.NewStruct(map[string]any{"name": "ada", "role": "eng"})
	ins, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "people", Data: d, Key: "u1"})
	if err != nil {
		t.Fatalf("Insert keyed: %v", err)
	}

	resp, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "people", Id: ins.Id, Fields: []string{"name"}})
	if err != nil {
		t.Fatalf("FindById projected: %v", err)
	}
	if got := resp.Record.Data.Fields["name"].GetStringValue(); got != "ada" {
		t.Errorf("projected name = %v, want ada", got)
	}
	if _, ok := resp.Record.Data.Fields["role"]; ok {
		t.Errorf("projected FindById should not carry role: %v", resp.Record.Data.Fields)
	}
	// key is always included regardless of projection.
	if resp.Record.Key != "u1" {
		t.Errorf("projected record dropped its key: got %q, want u1", resp.Record.Key)
	}
}

// TestIntegration_FindByKey_Projection checks projection on the keyed lookup.
func TestIntegration_FindByKey_Projection(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "kv"})
	d, _ := structpb.NewStruct(map[string]any{"name": "grace", "role": "eng"})
	if _, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "kv", Data: d, Key: "k1"}); err != nil {
		t.Fatalf("Insert keyed: %v", err)
	}

	resp, err := c.FindByKey(ctx(), &pb.FindByKeyRequest{Collection: "kv", Key: "k1", Fields: []string{"name"}})
	if err != nil {
		t.Fatalf("FindByKey projected: %v", err)
	}
	if got := resp.Record.Data.Fields["name"].GetStringValue(); got != "grace" {
		t.Errorf("projected name = %v, want grace", got)
	}
	if _, ok := resp.Record.Data.Fields["role"]; ok {
		t.Errorf("projected FindByKey should not carry role: %v", resp.Record.Data.Fields)
	}
	if resp.Record.Key != "k1" {
		t.Errorf("projected record dropped its key: got %q, want k1", resp.Record.Key)
	}
}

// TestIntegration_Find_RangeIndexed drives a range predicate end-to-end against
// a collection with a secondary index on the queried field, and asserts the
// results match the numeric predicate (2 < 10, not the lexical order).
func TestIntegration_Find_RangeIndexed(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "ages"})
	if _, err := c.EnsureIndex(ctx(), &pb.EnsureIndexRequest{Collection: "ages", Field: "age"}); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	for _, age := range []float64{2, 9, 10, 25, 100} {
		d, _ := structpb.NewStruct(map[string]any{"age": age})
		c.Insert(ctx(), &pb.InsertRequest{Collection: "ages", Data: d})
	}

	// age >= 10 → 10, 25, 100 (numeric, so "2" and "9" are excluded).
	stream, err := c.Find(ctx(), &pb.FindRequest{
		Collection: "ages",
		Filter: &pb.Filter{Kind: &pb.Filter_Field{Field: &pb.FieldFilter{
			Field: "age", Op: pb.FilterOp_GTE, Value: "10",
		}}},
		OrderBy: "age",
	})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recs := collectFind(t, stream)
	got := make([]float64, len(recs))
	for i, r := range recs {
		got[i] = r.Data.Fields["age"].GetNumberValue()
	}
	want := []float64{10, 25, 100}
	if len(got) != len(want) {
		t.Fatalf("gte 10: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("gte 10 ordered[%d] = %v, want %v", i, got[i], want[i])
		}
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

// ---- Compact ----------------------------------------------------------------

func TestIntegration_Compact(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "comp_col"})

	// Insert and churn a few records so there is something to compact.
	ids := make([]uint64, 0, 4)
	for i := 0; i < 4; i++ {
		d, _ := structpb.NewStruct(map[string]any{"x": float64(i)})
		ir, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "comp_col", Data: d})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids = append(ids, ir.Id)
	}
	for _, id := range ids {
		u, _ := structpb.NewStruct(map[string]any{"x": float64(99)})
		if _, err := c.Update(ctx(), &pb.UpdateRequest{Collection: "comp_col", Id: id, Data: u}); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}

	// Compact returns only after the forced pass completes.
	cr, err := c.Compact(ctx(), &pb.CompactRequest{Collection: "comp_col"})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if !cr.Ok {
		t.Error("Compact: expected Ok=true")
	}

	// Every record must still be readable at its latest value.
	for _, id := range ids {
		fr, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "comp_col", Id: id})
		if err != nil {
			t.Fatalf("FindById(%d) after compact: %v", id, err)
		}
		if fr.Record.Data.Fields["x"].GetNumberValue() != 99 {
			t.Errorf("id %d after compact: x=%v, want 99", id, fr.Record.Data.Fields["x"])
		}
	}

	// Unknown collection is a NotFound.
	if _, err := c.Compact(ctx(), &pb.CompactRequest{Collection: "nope"}); err == nil {
		t.Error("expected error compacting unknown collection")
	}
}

// ---- Snapshot / backup ------------------------------------------------------

func TestIntegration_Snapshot(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "snap"})

	for i := 0; i < 5; i++ {
		d, _ := structpb.NewStruct(map[string]any{"i": float64(i)})
		if _, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "snap", Data: d}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Stream the snapshot over gRPC into a buffer.
	stream, err := c.Snapshot(ctx(), &pb.SnapshotRequest{})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		buf.Write(chunk.Data)
	}
	if buf.Len() == 0 {
		t.Fatal("snapshot produced no bytes")
	}

	// Extract into a fresh data dir ("untar into --data") and open it directly.
	dst := t.TempDir()
	gz, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		target := filepath.Join(dst, hdr.Name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		f, err := os.Create(target)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // trusted test archive
			t.Fatalf("copy: %v", err)
		}
		f.Close()
	}

	db, err := engine.Open(dst, engine.CollectionConfig{})
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db.Close()

	col, err := db.Collection("snap")
	if err != nil {
		t.Fatalf("restored collection: %v", err)
	}
	res, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 5 {
		t.Errorf("restored record count = %d, want 5", len(res))
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

func TestIntegration_TTL(t *testing.T) {
	c := newTestServer(t)

	// A per-collection default TTL is accepted at create time.
	if _, err := c.CreateCollection(ctx(), &pb.CreateCollectionRequest{
		Name: "ttlcol", DefaultTtlSeconds: 3600,
	}); err != nil {
		t.Fatalf("CreateCollection with default TTL: %v", err)
	}

	// A record inserted without an explicit TTL is still readable (its deadline
	// is an hour out); an explicit per-record TTL is accepted too.
	d, _ := structpb.NewStruct(map[string]any{"k": "v"})
	ir, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "ttlcol", Data: d})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "ttlcol", Id: ir.Id}); err != nil {
		t.Errorf("FindById on live record: %v", err)
	}
	if _, err := c.Insert(ctx(), &pb.InsertRequest{Collection: "ttlcol", Data: d, TtlSeconds: 1800}); err != nil {
		t.Errorf("Insert with explicit TTL: %v", err)
	}

	// Negative TTLs are rejected on every write surface.
	if got := status.Code(mustErr(c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "bad", DefaultTtlSeconds: -1}))); got != codes.InvalidArgument {
		t.Errorf("CreateCollection negative default TTL: got %v, want InvalidArgument", got)
	}
	if got := status.Code(mustErr(c.Insert(ctx(), &pb.InsertRequest{Collection: "ttlcol", Data: d, TtlSeconds: -1}))); got != codes.InvalidArgument {
		t.Errorf("Insert negative TTL: got %v, want InvalidArgument", got)
	}
	recs := []*structpb.Struct{d}
	if got := status.Code(mustErr(c.InsertMany(ctx(), &pb.InsertManyRequest{Collection: "ttlcol", Records: recs, TtlSeconds: -1}))); got != codes.InvalidArgument {
		t.Errorf("InsertMany negative TTL: got %v, want InvalidArgument", got)
	}
	if got := status.Code(mustErr(c.Update(ctx(), &pb.UpdateRequest{Collection: "ttlcol", Id: ir.Id, Data: d, TtlSeconds: -1}))); got != codes.InvalidArgument {
		t.Errorf("Update negative TTL: got %v, want InvalidArgument", got)
	}

	// A resetting TTL on Update is accepted.
	if _, err := c.Update(ctx(), &pb.UpdateRequest{Collection: "ttlcol", Id: ir.Id, Data: d, TtlSeconds: 60}); err != nil {
		t.Errorf("Update with TTL: %v", err)
	}

	// Per-record TTL is not (yet) supported inside a transaction.
	btr, err := c.BeginTx(ctx(), &pb.BeginTxRequest{Collection: "ttlcol"})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if got := status.Code(mustErr(c.Insert(txCtx(btr.TxId), &pb.InsertRequest{Collection: "ttlcol", Data: d, TtlSeconds: 60}))); got != codes.InvalidArgument {
		t.Errorf("Insert TTL inside tx: got %v, want InvalidArgument", got)
	}
	if got := status.Code(mustErr(c.Update(txCtx(btr.TxId), &pb.UpdateRequest{Collection: "ttlcol", Id: ir.Id, Data: d, TtlSeconds: 60}))); got != codes.InvalidArgument {
		t.Errorf("Update TTL inside tx: got %v, want InvalidArgument", got)
	}
}

// mustErr returns the error from a (result, error) pair, ignoring the result.
// It lets a status-code assertion read as one line regardless of the RPC's
// response type.
func mustErr[T any](_ T, err error) error { return err }

// ---- O1: structured logging interceptor -------------------------------------

// newInstrumentedServer spins up an in-process gRPC server with the auth and
// (optionally) logging interceptors chained exactly as cmd/filedb wires them,
// plus an optional health service. It returns a connected client and the raw
// connection (so callers can build a health client).
func newInstrumentedServer(t *testing.T, logger *slog.Logger, keys []auth.Key, healthSvc *server.HealthService) *grpc.ClientConn {
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

	authn, err := auth.New(keys)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	au, as := authn.Interceptors()
	var opts []grpc.ServerOption
	if logger != nil {
		lu, ls := server.LoggingInterceptors(logger)
		opts = append(opts,
			grpc.ChainUnaryInterceptor(au, lu),
			grpc.ChainStreamInterceptor(as, ls),
		)
	} else {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(au),
			grpc.ChainStreamInterceptor(as),
		)
	}
	grpcSrv := grpc.NewServer(opts...)
	pb.RegisterFileDBServer(grpcSrv, gs)
	if healthSvc != nil {
		healthSvc.Register(grpcSrv)
	}

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
	return conn
}

// keyCtx returns a context carrying the x-api-key header.
func keyCtx(key string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-api-key", key))
}

func TestIntegration_LoggingInterceptor_OneRecordPerCall(t *testing.T) {
	var buf bytes.Buffer
	logger, err := server.NewLogger(&buf, "info", "json")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	keys := []auth.Key{{Key: "sekret", Name: "tester", Scope: auth.ScopeReadWrite}}
	conn := newInstrumentedServer(t, logger, keys, nil)
	c := pb.NewFileDBClient(conn)

	if _, err := c.CreateCollection(keyCtx("sekret"), &pb.CreateCollectionRequest{Name: "logs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Exactly one structured record for the single RPC, with all fields.
	lines := logLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 log record, got %d: %v", len(lines), lines)
	}
	rec := lines[0]
	if rec["msg"] != "grpc request" {
		t.Errorf("msg = %v, want %q", rec["msg"], "grpc request")
	}
	if rec["method"] != "/filedb.v1.FileDB/CreateCollection" {
		t.Errorf("method = %v", rec["method"])
	}
	if rec["principal"] != "tester" {
		t.Errorf("principal = %v, want tester", rec["principal"])
	}
	if rec["code"] != "OK" {
		t.Errorf("code = %v, want OK", rec["code"])
	}
	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", rec["level"])
	}
	if _, ok := rec["duration"]; !ok {
		t.Errorf("record missing duration field: %v", rec)
	}
}

func TestIntegration_LoggingInterceptor_LevelFiltering(t *testing.T) {
	// At warn level the info-level success record is suppressed, while a failing
	// RPC (logged at error) still comes through.
	var buf bytes.Buffer
	logger, err := server.NewLogger(&buf, "warn", "json")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	conn := newInstrumentedServer(t, logger, nil, nil) // auth disabled
	c := pb.NewFileDBClient(conn)

	// Success → info → suppressed at warn.
	if _, err := c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "ok"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if lines := logLines(t, &buf); len(lines) != 0 {
		t.Fatalf("info record should be filtered at warn level, got %d: %v", len(lines), lines)
	}

	// Failure → error → emitted even at warn.
	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "missing", Id: 1}); err == nil {
		t.Fatal("expected error for missing collection")
	}
	lines := logLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 error record, got %d: %v", len(lines), lines)
	}
	if lines[0]["level"] != "ERROR" {
		t.Errorf("level = %v, want ERROR", lines[0]["level"])
	}
	if lines[0]["code"] != "NotFound" {
		t.Errorf("code = %v, want NotFound", lines[0]["code"])
	}
	if lines[0]["principal"] != "anonymous" {
		t.Errorf("principal = %v, want anonymous (auth disabled)", lines[0]["principal"])
	}
}

// logLines parses newline-delimited JSON log records out of buf, draining it.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("parse log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	buf.Reset()
	return out
}

// ---- O2: health & readiness -------------------------------------------------

func TestIntegration_Health_ServingLifecycle(t *testing.T) {
	healthSvc := server.NewHealthService()
	conn := newInstrumentedServer(t, nil, nil, healthSvc)
	hc := healthpb.NewHealthClient(conn)

	// Starts NOT_SERVING until listeners are marked up.
	resp, err := hc.Check(ctx(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check (initial): %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("initial status = %v, want NOT_SERVING", resp.Status)
	}

	healthSvc.SetServing()
	resp, err = hc.Check(ctx(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check (serving): %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status after SetServing = %v, want SERVING", resp.Status)
	}

	// Graceful shutdown flips to NOT_SERVING so in-flight drains.
	healthSvc.SetNotServing()
	resp, err = hc.Check(ctx(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check (draining): %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("status after SetNotServing = %v, want NOT_SERVING", resp.Status)
	}
}

func TestReadinessHandler_DataDirWritable(t *testing.T) {
	dir := t.TempDir()

	// Writable dir → 200 ready.
	rr := httptest.NewRecorder()
	server.ReadinessHandler(func() error { return server.CheckDataDirWritable(dir) })(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("writable dir: status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}

	// Liveness is unconditional.
	rr = httptest.NewRecorder()
	server.LivenessHandler()(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz: status = %d, want 200", rr.Code)
	}
}

func TestReadinessHandler_UnwritableDataDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions do not restrict writes")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) }) // restore so TempDir cleanup works

	if err := server.CheckDataDirWritable(dir); err == nil {
		t.Fatal("CheckDataDirWritable: want error for read-only dir, got nil")
	}

	rr := httptest.NewRecorder()
	server.ReadinessHandler(func() error { return server.CheckDataDirWritable(dir) })(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("unwritable dir: status = %d, want 503 (body %q)", rr.Code, rr.Body.String())
	}
}

// ---- O3: request backpressure & rate limiting -------------------------------

// newLimitedServer spins up an in-process gRPC server backed by a real engine,
// chaining the auth and limiter interceptors exactly as cmd/filedb wires them
// (auth resolves the principal, then the limiter reads it) — the limiter is
// chained only when it is enabled, mirroring production. An optional extraUnary
// interceptor is chained after the limiter so a test can hold an RPC in-flight
// and deterministically saturate the semaphore.
func newLimitedServer(t *testing.T, limiter *server.Limiter, keys []auth.Key, extraUnary grpc.UnaryServerInterceptor) pb.FileDBClient {
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

	authn, err := auth.New(keys)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	au, as := authn.Interceptors()
	unary := []grpc.UnaryServerInterceptor{au}
	stream := []grpc.StreamServerInterceptor{as}
	if limiter.Enabled() {
		lu, ls := limiter.Interceptors()
		unary = append(unary, lu)
		stream = append(stream, ls)
	}
	if extraUnary != nil {
		unary = append(unary, extraUnary)
	}
	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	)
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

// TestIntegration_InflightSemaphore_ShedsExcess saturates the in-flight
// semaphore with concurrent calls and asserts that a further call is shed with
// ResourceExhausted, while the calls occupying the ceiling still complete
// successfully once released.
func TestIntegration_InflightSemaphore_ShedsExcess(t *testing.T) {
	const ceiling = 2
	entered := make(chan struct{}, ceiling)
	release := make(chan struct{})

	// A blocking interceptor chained after the limiter holds each admitted call
	// in-flight (occupying its semaphore slot) until release is closed.
	blocker := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		entered <- struct{}{}
		<-release
		return handler(ctx, req)
	}

	limiter := server.NewLimiter(ceiling, 0) // in-flight cap only, no rate limiting
	c := newLimitedServer(t, limiter, nil, blocker)

	// Occupy every slot with concurrent calls.
	errs := make(chan error, ceiling)
	for i := 0; i < ceiling; i++ {
		go func() {
			_, err := c.ListCollections(ctx(), &pb.ListCollectionsRequest{})
			errs <- err
		}()
	}
	for i := 0; i < ceiling; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for calls to saturate the semaphore")
		}
	}

	// With the ceiling saturated, a further call is shed with ResourceExhausted
	// and never reaches the (blocking) handler.
	if _, err := c.ListCollections(ctx(), &pb.ListCollectionsRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("over-ceiling call: code = %v, want ResourceExhausted (err %v)", status.Code(err), err)
	}

	// Releasing the slots lets the admitted calls finish cleanly.
	close(release)
	for i := 0; i < ceiling; i++ {
		if err := <-errs; err != nil {
			t.Errorf("in-flight call failed: %v", err)
		}
	}
}

// TestIntegration_RateLimit_PerPrincipalIsolation asserts that the per-principal
// token bucket throttles one API-key principal without affecting another.
func TestIntegration_RateLimit_PerPrincipalIsolation(t *testing.T) {
	keys := []auth.Key{
		{Key: "alice-key", Name: "alice", Scope: auth.ScopeReadWrite},
		{Key: "bob-key", Name: "bob", Scope: auth.ScopeReadWrite},
	}
	// 5 rps → burst 5. Firing well past the burst back-to-back guarantees some
	// calls are throttled regardless of scheduling.
	limiter := server.NewLimiter(0, 5.0)
	c := newLimitedServer(t, limiter, keys, nil)

	aliceOK, aliceThrottled := 0, 0
	for i := 0; i < 25; i++ {
		_, err := c.ListCollections(keyCtx("alice-key"), &pb.ListCollectionsRequest{})
		switch {
		case err == nil:
			aliceOK++
		case status.Code(err) == codes.ResourceExhausted:
			aliceThrottled++
		default:
			t.Fatalf("alice call %d: unexpected error: %v", i, err)
		}
	}
	if aliceOK == 0 {
		t.Fatal("alice: expected the initial burst of calls to succeed")
	}
	if aliceThrottled == 0 {
		t.Fatal("alice: expected calls past the burst to be throttled with ResourceExhausted")
	}

	// Bob has an independent bucket: a handful of calls (under the burst) must
	// all succeed even though alice is being throttled.
	for i := 0; i < 3; i++ {
		if _, err := c.ListCollections(keyCtx("bob-key"), &pb.ListCollectionsRequest{}); err != nil {
			t.Fatalf("bob call %d was affected by alice's throttling: %v", i, err)
		}
	}
}

// TestIntegration_Limits_DisabledByDefault confirms that with every limit at its
// zero value the limiter is a no-op: it is not even chained, and a burst of
// rapid calls all succeed with no throttling.
func TestIntegration_Limits_DisabledByDefault(t *testing.T) {
	limiter := server.NewLimiter(0, 0)
	if limiter.Enabled() {
		t.Fatal("NewLimiter(0, 0) should be disabled")
	}
	c := newLimitedServer(t, limiter, nil, nil)

	for i := 0; i < 50; i++ {
		if _, err := c.ListCollections(ctx(), &pb.ListCollectionsRequest{}); err != nil {
			t.Fatalf("call %d failed with limits disabled: %v", i, err)
		}
	}
}

// ---- Slow-query log (O5) ----------------------------------------------------

// lockedBuffer is a bytes.Buffer safe for the concurrent writes the slog handler
// performs from the server's RPC goroutine while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newSlowQueryTestServer is newTestServer with the slow-query log wired to a
// captured JSON logger at the given threshold. It returns the client and the
// buffer the WARN slow-query lines land in.
func newSlowQueryTestServer(t *testing.T, threshold time.Duration) (pb.FileDBClient, *lockedBuffer) {
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

	logBuf := &lockedBuffer{}
	logger, err := server.NewLogger(logBuf, "info", "json")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}

	gs := server.NewGRPCServer(db, 5*time.Minute, server.WithSlowQueryLog(logger, threshold))
	t.Cleanup(gs.Close)
	grpcSrv := grpc.NewServer()
	pb.RegisterFileDBServer(grpcSrv, gs)

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

	return pb.NewFileDBClient(conn), logBuf
}

// waitForLogLine polls the buffer for up to two seconds and returns the first
// log line once one appears. The slow-query line is written by the server's RPC
// goroutine, so a brief poll avoids racing its flush.
func waitForLogLine(t *testing.T, b *lockedBuffer) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := strings.TrimSpace(b.String()); s != "" {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				return s[:i]
			}
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no slow-query log line within timeout")
	return ""
}

func seedRoles(t *testing.T, c pb.FileDBClient, collection string) {
	t.Helper()
	for i := 0; i < 10; i++ {
		role := "user"
		if i%5 == 0 { // ids ...→ 2 of 10 records are admins
			role = "admin"
		}
		d, _ := structpb.NewStruct(map[string]any{"role": role})
		if _, err := c.Insert(ctx(), &pb.InsertRequest{Collection: collection, Data: d}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
}

// TestIntegration_SlowQuery_FullScanLogsOnce drives a full-scan Find that
// exceeds the threshold and asserts exactly one WARN slow-query line reporting
// index_used=false with rows_scanned > rows_returned.
func TestIntegration_SlowQuery_FullScanLogsOnce(t *testing.T) {
	// A 1ns threshold makes every query "slow" — deterministic, no timing flake.
	c, logBuf := newSlowQueryTestServer(t, time.Nanosecond)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "users"})
	seedRoles(t, c, "users")

	// No index on "role" → full scan; 2 of 10 records match.
	stream, err := c.Find(ctx(), &pb.FindRequest{
		Collection: "users",
		Filter: &pb.Filter{Kind: &pb.Filter_Field{Field: &pb.FieldFilter{
			Field: "role", Op: pb.FilterOp_EQ, Value: `"admin"`,
		}}},
	})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if recs := collectFind(t, stream); len(recs) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(recs))
	}

	line := waitForLogLine(t, logBuf)
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("parse log line %q: %v", line, err)
	}
	if rec["msg"] != "slow query" {
		t.Errorf("msg = %v, want \"slow query\"", rec["msg"])
	}
	if rec["level"] != "WARN" {
		t.Errorf("level = %v, want WARN", rec["level"])
	}
	if rec["collection"] != "users" {
		t.Errorf("collection = %v, want users", rec["collection"])
	}
	if rec["index_used"] != false {
		t.Errorf("index_used = %v, want false", rec["index_used"])
	}
	if rec["filter"] != "role EQ" {
		t.Errorf("filter = %v, want \"role EQ\"", rec["filter"])
	}
	scanned, _ := rec["rows_scanned"].(float64)
	returned, _ := rec["rows_returned"].(float64)
	if returned != 2 {
		t.Errorf("rows_returned = %v, want 2", returned)
	}
	if scanned != 10 {
		t.Errorf("rows_scanned = %v, want 10", scanned)
	}
	if scanned <= returned {
		t.Errorf("rows_scanned (%v) must exceed rows_returned (%v)", scanned, returned)
	}

	// Exactly one slow-query line was emitted.
	if n := strings.Count(logBuf.String(), "slow query"); n != 1 {
		t.Errorf("expected exactly one slow-query line, got %d\n%s", n, logBuf.String())
	}
}

// TestIntegration_SlowQuery_IndexedUnderThresholdNoLog asserts an indexed
// equality lookup that stays under the threshold produces no slow-query log.
func TestIntegration_SlowQuery_IndexedUnderThresholdNoLog(t *testing.T) {
	// A 1h threshold: no realistic query is "slow", so nothing is logged.
	c, logBuf := newSlowQueryTestServer(t, time.Hour)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "users"})
	if _, err := c.EnsureIndex(ctx(), &pb.EnsureIndexRequest{Collection: "users", Field: "role"}); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	seedRoles(t, c, "users")

	stream, err := c.Find(ctx(), &pb.FindRequest{
		Collection: "users",
		Filter: &pb.Filter{Kind: &pb.Filter_Field{Field: &pb.FieldFilter{
			Field: "role", Op: pb.FilterOp_EQ, Value: `"admin"`,
		}}},
	})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if recs := collectFind(t, stream); len(recs) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(recs))
	}

	// Allow any erroneous async log a moment to surface, then assert none did.
	time.Sleep(20 * time.Millisecond)
	if s := logBuf.String(); s != "" {
		t.Errorf("expected no slow-query log under threshold, got:\n%s", s)
	}
}

// ---- Tracing (O4) -----------------------------------------------------------

// newTracedServer spins up an in-process gRPC server wired exactly as
// cmd/filedb does when --otlp-endpoint is set: the tracing interceptor is
// chained outermost and the engine's OnScan hook turns scans into child spans.
// Spans are captured by an in-memory exporter (no live collector) via a
// synchronous span processor, so they are readable the moment an RPC returns.
func newTracedServer(t *testing.T) (pb.FileDBClient, *tracetest.InMemoryExporter) {
	t.Helper()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	dir := t.TempDir()
	db, err := engine.Open(dir, engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
		OnScan:          server.ScanTraceHook(tp),
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)

	tu, ts := server.TracingInterceptors(tp)
	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tu),
		grpc.ChainStreamInterceptor(ts),
	)
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

	return pb.NewFileDBClient(conn), exp
}

// spanByName returns the first captured span with the given name, or nil.
func spanByName(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	return nil
}

// spanAttr returns the value of a span attribute as a string, or "" if absent.
func spanAttr(s *tracetest.SpanStub, key string) (attribute.Value, bool) {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestIntegration_TracingInterceptor_SpanPerRPC(t *testing.T) {
	c, exp := newTracedServer(t)

	// A unary RPC: exactly one span named after the method, tagged with the
	// method and an OK status code.
	if _, err := c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "traced"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	spans := exp.GetSpans()
	rpc := spanByName(spans, "/filedb.v1.FileDB/CreateCollection")
	if rpc == nil {
		t.Fatalf("no span for CreateCollection; got %v", spanNames(spans))
	}
	if v, ok := spanAttr(rpc, "rpc.method"); !ok || v.AsString() != "/filedb.v1.FileDB/CreateCollection" {
		t.Errorf("rpc.method attr = %v (present=%v), want the full method", v.AsString(), ok)
	}
	if v, ok := spanAttr(rpc, "rpc.grpc.status_code"); !ok || v.AsInt64() != int64(codes.OK) {
		t.Errorf("rpc.grpc.status_code attr = %d (present=%v), want %d", v.AsInt64(), ok, codes.OK)
	}
	exp.Reset()

	if _, err := c.Insert(ctx(), &pb.InsertRequest{
		Collection: "traced",
		Data:       mustStruct(t, map[string]any{"n": 1}),
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	exp.Reset()

	// A streaming Find drives an engine scan, which the OnScan hook records as an
	// "engine.scan" child span of the Find RPC span.
	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: "traced"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Find recv: %v", err)
		}
	}

	spans = exp.GetSpans()
	find := spanByName(spans, "/filedb.v1.FileDB/Find")
	if find == nil {
		t.Fatalf("no span for Find; got %v", spanNames(spans))
	}
	scan := spanByName(spans, "engine.scan")
	if scan == nil {
		t.Fatalf("no engine.scan span; got %v", spanNames(spans))
	}
	// The engine span must nest under the RPC span (same trace, parent = RPC).
	if scan.Parent.SpanID() != find.SpanContext.SpanID() {
		t.Errorf("engine.scan parent = %v, want Find span %v", scan.Parent.SpanID(), find.SpanContext.SpanID())
	}
	if scan.SpanContext.TraceID() != find.SpanContext.TraceID() {
		t.Errorf("engine.scan trace = %v, want Find trace %v", scan.SpanContext.TraceID(), find.SpanContext.TraceID())
	}
}

// TestIntegration_Tracing_DisabledIsNoop verifies the default path: with no
// --otlp-endpoint the server wires no tracing interceptor and no exporter, and
// RPCs still succeed. newTestServer builds exactly that (no tracing) server.
func TestIntegration_Tracing_DisabledIsNoop(t *testing.T) {
	c := newTestServer(t)

	if _, err := c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "plain"}); err != nil {
		t.Fatalf("CreateCollection with tracing disabled: %v", err)
	}
	if _, err := c.Insert(ctx(), &pb.InsertRequest{
		Collection: "plain",
		Data:       mustStruct(t, map[string]any{"n": 1}),
	}); err != nil {
		t.Fatalf("Insert with tracing disabled: %v", err)
	}
	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: "plain"})
	if err != nil {
		t.Fatalf("Find with tracing disabled: %v", err)
	}
	n := 0
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Find recv with tracing disabled: %v", err)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("Find returned %d records, want 1", n)
	}
}

// ---- N1: keyed CRUD, Upsert, CAS & Rev over the wire ------------------------

// TestIntegration_N1_UpsertInsertThenReplace verifies that Upsert inserts on a
// fresh key at rev 1, then replaces on the same key returning the same id with
// an incremented rev and the new data.
func TestIntegration_N1_UpsertInsertThenReplace(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "kv"})

	ins, err := c.Upsert(ctx(), &pb.UpsertRequest{
		Collection: "kv", Key: "u1", Data: mustStruct(t, map[string]any{"n": float64(1)}),
	})
	if err != nil {
		t.Fatalf("Upsert insert: %v", err)
	}
	if ins.Record.Key != "u1" || ins.Record.Rev != 1 {
		t.Fatalf("Upsert insert: got key=%q rev=%d, want key=u1 rev=1", ins.Record.Key, ins.Record.Rev)
	}
	if got := ins.Record.Data.Fields["n"].GetNumberValue(); got != 1 {
		t.Fatalf("Upsert insert n: got %v, want 1", got)
	}

	rep, err := c.Upsert(ctx(), &pb.UpsertRequest{
		Collection: "kv", Key: "u1", Data: mustStruct(t, map[string]any{"n": float64(2)}),
	})
	if err != nil {
		t.Fatalf("Upsert replace: %v", err)
	}
	if rep.Record.Id != ins.Record.Id {
		t.Errorf("Upsert replace id: got %d, want %d (same record)", rep.Record.Id, ins.Record.Id)
	}
	if rep.Record.Rev != 2 {
		t.Errorf("Upsert replace rev: got %d, want 2 (incremented)", rep.Record.Rev)
	}
	if got := rep.Record.Data.Fields["n"].GetNumberValue(); got != 2 {
		t.Errorf("Upsert replace n: got %v, want 2", got)
	}

	// FindByKey observes the replaced record.
	fr, err := c.FindByKey(ctx(), &pb.FindByKeyRequest{Collection: "kv", Key: "u1"})
	if err != nil {
		t.Fatalf("FindByKey: %v", err)
	}
	if fr.Record.Rev != 2 || fr.Record.Key != "u1" {
		t.Errorf("FindByKey: got key=%q rev=%d, want key=u1 rev=2", fr.Record.Key, fr.Record.Rev)
	}
	if got := fr.Record.Data.Fields["n"].GetNumberValue(); got != 2 {
		t.Errorf("FindByKey n: got %v, want 2", got)
	}
}

// TestIntegration_N1_CAS verifies UpdateIfRev: a stale revision is a clean
// no-op (swapped=false, no error) that leaves the record untouched, and the
// current revision applies and bumps the rev.
func TestIntegration_N1_CAS(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "cas"})

	if _, err := c.Upsert(ctx(), &pb.UpsertRequest{
		Collection: "cas", Key: "c1", Data: mustStruct(t, map[string]any{"v": "a"}),
	}); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	// Stale rev → clean no-op, no error, no record.
	stale, err := c.UpdateIfRev(ctx(), &pb.UpdateIfRevRequest{
		Collection: "cas", Key: "c1", ExpectedRev: 99, Data: mustStruct(t, map[string]any{"v": "stale"}),
	})
	if err != nil {
		t.Fatalf("UpdateIfRev stale: unexpected error %v", err)
	}
	if stale.Swapped {
		t.Error("UpdateIfRev stale: got swapped=true, want false")
	}
	if stale.Record != nil {
		t.Errorf("UpdateIfRev stale: got record %v, want nil", stale.Record)
	}
	fr, _ := c.FindByKey(ctx(), &pb.FindByKeyRequest{Collection: "cas", Key: "c1"})
	if got := fr.Record.Data.Fields["v"].GetStringValue(); got != "a" {
		t.Errorf("after stale CAS: value=%q, want unchanged \"a\"", got)
	}

	// Current rev → applies and bumps rev.
	ok, err := c.UpdateIfRev(ctx(), &pb.UpdateIfRevRequest{
		Collection: "cas", Key: "c1", ExpectedRev: 1, Data: mustStruct(t, map[string]any{"v": "b"}),
	})
	if err != nil {
		t.Fatalf("UpdateIfRev current: %v", err)
	}
	if !ok.Swapped {
		t.Fatal("UpdateIfRev current: got swapped=false, want true")
	}
	if ok.Record == nil || ok.Record.Rev != 2 {
		t.Fatalf("UpdateIfRev current: got record %v, want rev 2", ok.Record)
	}
	if got := ok.Record.Data.Fields["v"].GetStringValue(); got != "b" {
		t.Errorf("UpdateIfRev current value: got %q, want b", got)
	}
}

// TestIntegration_N1_DuplicateKey verifies a keyed insert of an already-held key
// is rejected with ALREADY_EXISTS.
func TestIntegration_N1_DuplicateKey(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "dup"})

	if _, err := c.Insert(ctx(), &pb.InsertRequest{
		Collection: "dup", Key: "k1", Data: mustStruct(t, map[string]any{"n": float64(1)}),
	}); err != nil {
		t.Fatalf("first keyed insert: %v", err)
	}
	_, err := c.Insert(ctx(), &pb.InsertRequest{
		Collection: "dup", Key: "k1", Data: mustStruct(t, map[string]any{"n": float64(2)}),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate keyed insert: got code %v (err %v), want AlreadyExists", status.Code(err), err)
	}
}

// TestIntegration_N1_MissingKeyNotFound verifies FindByKey/UpdateByKey/
// DeleteByKey on an absent key each return NOT_FOUND.
func TestIntegration_N1_MissingKeyNotFound(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "miss"})
	// A keyed write creates the _key index so resolveKey has a real index to miss.
	c.Insert(ctx(), &pb.InsertRequest{Collection: "miss", Key: "present", Data: mustStruct(t, map[string]any{"n": float64(1)})})

	if _, err := c.FindByKey(ctx(), &pb.FindByKeyRequest{Collection: "miss", Key: "ghost"}); status.Code(err) != codes.NotFound {
		t.Errorf("FindByKey missing: got %v, want NotFound", status.Code(err))
	}
	if _, err := c.UpdateByKey(ctx(), &pb.UpdateByKeyRequest{
		Collection: "miss", Key: "ghost", Data: mustStruct(t, map[string]any{"n": float64(9)}),
	}); status.Code(err) != codes.NotFound {
		t.Errorf("UpdateByKey missing: got %v, want NotFound", status.Code(err))
	}
	if _, err := c.DeleteByKey(ctx(), &pb.DeleteByKeyRequest{Collection: "miss", Key: "ghost"}); status.Code(err) != codes.NotFound {
		t.Errorf("DeleteByKey missing: got %v, want NotFound", status.Code(err))
	}
}

// TestIntegration_N1_KeyRevPopulated verifies key/rev are populated on the
// record-bearing responses: keyed Insert, FindById, Find stream, and UpdateByKey.
func TestIntegration_N1_KeyRevPopulated(t *testing.T) {
	c := newTestServer(t)
	c.CreateCollection(ctx(), &pb.CreateCollectionRequest{Name: "kr"})

	ins, err := c.Insert(ctx(), &pb.InsertRequest{
		Collection: "kr", Key: "kr1", Data: mustStruct(t, map[string]any{"n": float64(1)}),
	})
	if err != nil {
		t.Fatalf("keyed Insert: %v", err)
	}
	if ins.Key != "kr1" || ins.Rev != 1 {
		t.Errorf("InsertResponse: got key=%q rev=%d, want key=kr1 rev=1", ins.Key, ins.Rev)
	}

	// FindById surfaces key/rev.
	fb, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "kr", Id: ins.Id})
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if fb.Record.Key != "kr1" || fb.Record.Rev != 1 {
		t.Errorf("FindById record: got key=%q rev=%d, want key=kr1 rev=1", fb.Record.Key, fb.Record.Rev)
	}

	// Find stream surfaces key/rev.
	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: "kr"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recs := collectFind(t, stream)
	if len(recs) != 1 || recs[0].Key != "kr1" || recs[0].Rev != 1 {
		t.Errorf("Find record: got %+v, want one record key=kr1 rev=1", recs)
	}

	// UpdateByKey surfaces the id, key, and bumped rev.
	ub, err := c.UpdateByKey(ctx(), &pb.UpdateByKeyRequest{
		Collection: "kr", Key: "kr1", Data: mustStruct(t, map[string]any{"n": float64(2)}),
	})
	if err != nil {
		t.Fatalf("UpdateByKey: %v", err)
	}
	if ub.Id != ins.Id || ub.Key != "kr1" || ub.Rev != 2 {
		t.Errorf("UpdateByKey response: got id=%d key=%q rev=%d, want id=%d key=kr1 rev=2", ub.Id, ub.Key, ub.Rev, ins.Id)
	}
}

// mustStruct builds a *structpb.Struct or fails the test.
func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	return s
}

// spanNames lists captured span names for failure messages.
func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}
