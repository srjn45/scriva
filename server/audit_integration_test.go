//nolint:errcheck
package server_test

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/auth"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
)

// startAuditServer serves an authenticated in-process gRPC server whose audit
// interceptor is chained *outside* the auth interceptor — the same wiring
// cmd/filedb installs for --audit-log — writing NDJSON to a file under a temp
// dir. It returns a client, the backing DB (so a test can seed a collection
// without generating its own audit records), and the audit file path.
func startAuditServer(t *testing.T, keys []auth.Key) (pb.ScrivaClient, *engine.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(filepath.Join(dir, "data"), engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	auditPath := filepath.Join(dir, "audit.log")
	f, err := os.OpenFile(auditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	auditLogger, err := server.NewAuditLogger(f)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	auditUnary, auditStream := server.AuditInterceptors(auditLogger)

	authn, err := auth.New(keys)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	au, as := authn.Interceptors()

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(auditUnary, au),
		grpc.ChainStreamInterceptor(auditStream, as),
	)
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
	return pb.NewScrivaClient(conn), db, auditPath
}

// readAuditLines parses the audit NDJSON file into one map per record. The
// audit interceptor writes each record before the RPC returns to the client, so
// reading the file after a completed call observes the record.
func readAuditLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse audit line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// TestIntegration_Audit_MutatingRPC_OneRecord: a single mutating RPC by an
// authenticated principal appends exactly one audit record carrying that
// principal, the RPC method, the target (collection + key), and an ok outcome.
func TestIntegration_Audit_MutatingRPC_OneRecord(t *testing.T) {
	t.Parallel()
	keys := []auth.Key{{Key: "sekret", Name: "writer", Scope: auth.ScopeReadWrite}}
	c, db, path := startAuditServer(t, keys)

	// Seed the collection directly on the engine so the only audited RPC is the
	// Insert under test.
	if _, err := db.CreateCollection("c"); err != nil {
		t.Fatalf("seed collection: %v", err)
	}

	if _, err := c.Insert(keyCtx("sekret"), &pb.InsertRequest{
		Collection: "c", Key: "k1", Data: mustStruct(t, map[string]any{"a": float64(1)}),
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	lines := readAuditLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("want exactly 1 audit record, got %d: %v", len(lines), lines)
	}
	rec := lines[0]
	if rec["msg"] != "audit" {
		t.Errorf("msg = %v, want audit", rec["msg"])
	}
	if rec["method"] != pb.Scriva_Insert_FullMethodName {
		t.Errorf("method = %v, want %s", rec["method"], pb.Scriva_Insert_FullMethodName)
	}
	if rec["principal"] != "writer" {
		t.Errorf("principal = %v, want writer", rec["principal"])
	}
	if rec["collection"] != "c" {
		t.Errorf("collection = %v, want c", rec["collection"])
	}
	if rec["key"] != "k1" {
		t.Errorf("key = %v, want k1", rec["key"])
	}
	if rec["outcome"] != "ok" {
		t.Errorf("outcome = %v, want ok", rec["outcome"])
	}
	if _, ok := rec["auth_failure"]; ok {
		t.Errorf("ok RPC should not carry auth_failure: %v", rec)
	}
}

// TestIntegration_Audit_AuthFailure_OneRecord: a call with an invalid API key is
// rejected before any handler runs, and produces exactly one audit record marked
// as an auth failure, attributed to an unauthenticated caller with the rejecting
// status code as its outcome.
func TestIntegration_Audit_AuthFailure_OneRecord(t *testing.T) {
	t.Parallel()
	keys := []auth.Key{{Key: "sekret", Name: "writer", Scope: auth.ScopeReadWrite}}
	c, _, path := startAuditServer(t, keys)

	if _, err := c.Insert(keyCtx("wrong-key"), &pb.InsertRequest{
		Collection: "c", Data: mustStruct(t, map[string]any{"a": float64(1)}),
	}); err == nil {
		t.Fatal("Insert with invalid key succeeded, want Unauthenticated")
	}

	lines := readAuditLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("want exactly 1 audit record, got %d: %v", len(lines), lines)
	}
	rec := lines[0]
	if rec["method"] != pb.Scriva_Insert_FullMethodName {
		t.Errorf("method = %v, want %s", rec["method"], pb.Scriva_Insert_FullMethodName)
	}
	if rec["principal"] != "unauthenticated" {
		t.Errorf("principal = %v, want unauthenticated", rec["principal"])
	}
	if rec["outcome"] != "Unauthenticated" {
		t.Errorf("outcome = %v, want Unauthenticated", rec["outcome"])
	}
	if af, _ := rec["auth_failure"].(bool); !af {
		t.Errorf("auth_failure = %v, want true", rec["auth_failure"])
	}
}

// TestIntegration_Audit_ScopeDenied_NamesPrincipal: a caller with a valid but
// read-only key that attempts a write is rejected with PermissionDenied. The
// caller *is* authenticated, so the audit record is attributed to the real
// principal (not "unauthenticated") and still marked as an auth failure.
func TestIntegration_Audit_ScopeDenied_NamesPrincipal(t *testing.T) {
	t.Parallel()
	keys := []auth.Key{{Key: "ro", Name: "reader", Scope: auth.ScopeRead}}
	c, db, path := startAuditServer(t, keys)
	if _, err := db.CreateCollection("c"); err != nil {
		t.Fatalf("seed collection: %v", err)
	}

	if _, err := c.Insert(keyCtx("ro"), &pb.InsertRequest{
		Collection: "c", Data: mustStruct(t, map[string]any{"a": float64(1)}),
	}); err == nil {
		t.Fatal("Insert with read-only key succeeded, want PermissionDenied")
	}

	lines := readAuditLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("want exactly 1 audit record, got %d: %v", len(lines), lines)
	}
	rec := lines[0]
	if rec["principal"] != "reader" {
		t.Errorf("principal = %v, want reader", rec["principal"])
	}
	if rec["outcome"] != "PermissionDenied" {
		t.Errorf("outcome = %v, want PermissionDenied", rec["outcome"])
	}
	if af, _ := rec["auth_failure"].(bool); !af {
		t.Errorf("auth_failure = %v, want true", rec["auth_failure"])
	}
}

// TestIntegration_Audit_ReadRPC_NoRecord: successful read-only RPCs by an
// authenticated principal are not written to the audit log, keeping the stream
// focused on writes, admin actions, and access denials.
func TestIntegration_Audit_ReadRPC_NoRecord(t *testing.T) {
	t.Parallel()
	keys := []auth.Key{{Key: "sekret", Name: "reader", Scope: auth.ScopeReadWrite}}
	c, db, path := startAuditServer(t, keys)

	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatalf("seed collection: %v", err)
	}
	id, _, err := col.InsertWithExpiry(map[string]any{"a": float64(1)}, time.Time{})
	if err != nil {
		t.Fatalf("seed record: %v", err)
	}

	// A unary read and a streaming read, both authenticated and successful.
	if _, err := c.FindById(keyCtx("sekret"), &pb.FindByIdRequest{Collection: "c", Id: id}); err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if _, err := c.ListCollections(keyCtx("sekret"), &pb.ListCollectionsRequest{}); err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	stream, err := c.Find(keyCtx("sekret"), &pb.FindRequest{Collection: "c"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}

	if lines := readAuditLines(t, path); len(lines) != 0 {
		t.Fatalf("read-only RPCs produced %d audit records, want 0: %v", len(lines), lines)
	}
}
