package server_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// startNamesServer runs a real in-process server whose data directory is
// <parent>/root, with decoy files beside it that a traversal would hit.
func startNamesServer(t *testing.T) (pb.ScrivaClient, string, string) {
	t.Helper()
	parent := t.TempDir()
	for _, d := range []string{"outer", "victim"} {
		if err := os.MkdirAll(filepath.Join(parent, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(parent, d, "keep"), []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	root := filepath.Join(parent, "root")
	db, err := engine.Open(root, engine.CollectionConfig{CompactInterval: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	srv := grpc.NewServer()
	pb.RegisterScrivaServer(srv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewScrivaClient(conn), parent, lis.Addr().String()
}

func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best-effort walk
		}
		rel, _ := filepath.Rel(root, p)
		out[rel] = fi.Mode().String()
		if fi.Mode().IsRegular() {
			b, _ := os.ReadFile(p)
			out[rel] += string(b)
		}
		return nil
	})
	return out
}

func requireTreeEqual(t *testing.T, what string, before, after map[string]string) {
	t.Helper()
	for k, v := range before {
		if after[k] != v {
			t.Errorf("%s: %q changed or vanished", what, k)
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			t.Errorf("%s: new entry %q", what, k)
		}
	}
}

func wantInvalid(t *testing.T, op, name string, err error) {
	t.Helper()
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("%s(%q): code %v (%v), want InvalidArgument", op, name, status.Code(err), err)
	}
}

func TestGRPCHostileCollectionNames(t *testing.T) {
	c, parent, _ := startNamesServer(t)
	ctx := context.Background()
	before := tree(t, parent)
	names := []string{"..", "../outer", "../victim", "a/b", `a\b`, "/etc", `C:\x`, "a b", "é",
		strings.Repeat("a", 65), "xtx", "xtx.foo", "CON", "collections", "a.b"}
	for _, n := range names {
		_, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: n})
		wantInvalid(t, "CreateCollection", n, err)
		_, err = c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: n, DefaultTtlSeconds: 5})
		wantInvalid(t, "CreateCollection+ttl", n, err)
		_, err = c.DropCollection(ctx, &pb.DropCollectionRequest{Name: n})
		wantInvalid(t, "DropCollection", n, err)
		_, err = c.Insert(ctx, &pb.InsertRequest{Collection: n})
		wantInvalid(t, "Insert", n, err)
		fs, err := c.Find(ctx, &pb.FindRequest{Collection: n})
		if err == nil {
			_, err = fs.Recv()
		}
		wantInvalid(t, "Find", n, err)
		ws, err := c.Watch(ctx, &pb.WatchRequest{Collection: n})
		if err == nil {
			_, err = ws.Recv()
		}
		wantInvalid(t, "Watch", n, err)
		_, err = c.FindById(ctx, &pb.FindByIdRequest{Collection: n, Id: 1})
		wantInvalid(t, "FindById", n, err)
		_, err = c.Delete(ctx, &pb.DeleteRequest{Collection: n, Id: 1})
		wantInvalid(t, "Delete", n, err)
		_, err = c.EnsureIndex(ctx, &pb.EnsureIndexRequest{Collection: n, Field: "f"})
		wantInvalid(t, "EnsureIndex", n, err)
		_, err = c.Compact(ctx, &pb.CompactRequest{Collection: n})
		wantInvalid(t, "Compact", n, err)
		_, err = c.BeginTx(ctx, &pb.BeginTxRequest{Collection: n})
		wantInvalid(t, "BeginTx", n, err)
		_, err = c.BeginXTx(ctx, &pb.BeginXTxRequest{Collections: []string{n}})
		wantInvalid(t, "BeginXTx", n, err)
	}
	requireTreeEqual(t, "gRPC hostile names", before, tree(t, parent))

	// A valid name still works, and case-fold duplicates are AlreadyExists.
	if _, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "users"}); err != nil {
		t.Fatal(err)
	}
	_, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "Users"})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("Users: %v", err)
	}
	if _, err := c.DropCollection(ctx, &pb.DropCollectionRequest{Name: "users"}); err != nil {
		t.Fatal(err)
	}
	requireTreeEqual(t, "after valid create/drop", before, tree(t, parent))
}

func TestRESTHostileCollectionNames(t *testing.T) {
	_, parent, addr := startNamesServer(t)
	h, err := server.NewRESTGateway(context.Background(), addr, insecure.NewCredentials(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	before := tree(t, parent)

	do := func(method, path, body string) int {
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	for _, p := range []string{"%2e%2e", "..%2Fouter", "a%5Cb", "a%20b", "xtx.foo", "%2Fetc"} {
		if code := do(http.MethodDelete, "/v1/collections/"+p, ""); code == http.StatusOK {
			t.Errorf("DELETE /v1/collections/%s -> 200", p)
		}
		if code := do(http.MethodPost, "/v1/"+p+"/records", `{"data":{"a":1}}`); code == http.StatusOK {
			t.Errorf("POST /v1/%s/records -> 200", p)
		}
	}
	for _, n := range []string{"../outer", "a/b", "xtx"} {
		body := `{"name":"` + n + `"}`
		if code := do(http.MethodPost, "/v1/collections", body); code != http.StatusBadRequest {
			t.Errorf("create %q -> %d, want 400", n, code)
		}
	}
	requireTreeEqual(t, "REST hostile names", before, tree(t, parent))
}
