//nolint:errcheck
package server_test

// Authorization matrix (ratified design D3/D4): every principal shape × every
// RPC, run through gRPC and through the REST gateway against one real
// in-process server, asserting identical outcomes on both transports and that a
// scoped principal learns nothing about collections outside its allow-list.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/auth"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
)

const (
	collA      = "a_orders"  // inside every scoped key's allow-list
	collB      = "b_secrets" // exists, outside every scoped key's allow-list
	collGhost  = "z_nowhere" // does not exist, outside every allow-list
	secretMark = "TOPSECRET-b_secrets"
)

type authzHarness struct {
	t       *testing.T
	dir     string
	authn   *auth.Authenticator
	client  pb.ScrivaClient
	restURL string
	http    *http.Client
}

var authzKeys = []auth.Key{
	{Key: "k-admin", Name: "admin", Scope: auth.ScopeReadWrite},
	{Key: "k-reader", Name: "reader", Scope: auth.ScopeRead},
	// rw_a may create exactly these new names (plus operate on a_orders).
	{Key: "k-rwa", Name: "rw_a", Scope: auth.ScopeReadWrite,
		Collections: []string{collA, "a_new_g", "a_new_r", "a_ghost"}},
	{Key: "k-roa", Name: "ro_a", Scope: auth.ScopeRead, Collections: []string{collA}},
	// peer has the same allow-list as rw_a: ownership, not the list, must keep
	// it away from rw_a's transaction handles.
	{Key: "k-peer", Name: "peer", Scope: auth.ScopeReadWrite, Collections: []string{collA}},
}

func newAuthzHarness(t *testing.T) *authzHarness {
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

	authn, err := auth.New(authzKeys)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	au, as := authn.Interceptors()
	grpcSrv := grpc.NewServer(grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
	pb.RegisterScrivaServer(grpcSrv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	gwCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	gw, err := server.NewRESTGateway(gwCtx, lis.Addr().String(), insecure.NewCredentials(), nil)
	if err != nil {
		t.Fatalf("NewRESTGateway: %v", err)
	}
	hs := httptest.NewServer(gw)
	t.Cleanup(hs.Close)

	h := &authzHarness{t: t, dir: dir, authn: authn, client: pb.NewScrivaClient(conn),
		restURL: hs.URL, http: &http.Client{Timeout: 10 * time.Second}}
	h.seed()
	return h
}

func (h *authzHarness) seed() {
	t := h.t
	cx := keyCtx("k-admin")
	for _, name := range []string{collA, collB, "scratch0", "scratch1"} {
		if _, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: name}); err != nil {
			t.Fatalf("seed CreateCollection(%s): %v", name, err)
		}
	}
	for _, name := range []string{collA, collB} {
		data := mustStruct(t, map[string]any{"v": "TOPSECRET-" + name})
		if _, err := h.client.Insert(cx, &pb.InsertRequest{Collection: name, Data: data}); err != nil {
			t.Fatalf("seed Insert(%s): %v", name, err)
		}
		if _, err := h.client.Upsert(cx, &pb.UpsertRequest{Collection: name, Key: "k1", Data: data}); err != nil {
			t.Fatalf("seed Upsert(%s): %v", name, err)
		}
	}
}

// rest performs one REST call and returns the HTTP status and body.
func (h *authzHarness) rest(key, method, path, body string) (int, string) {
	h.t.Helper()
	timeout := 5 * time.Second
	if strings.HasSuffix(path, "/watch") {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.restURL+path, rd)
	if err != nil {
		h.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("x-api-key", key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.http.Do(req)
	if err != nil {
		// A watch that stays open past the deadline is an authorized, idle one.
		if ctx.Err() != nil {
			return http.StatusOK, ""
		}
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(b)
}

// grpcCtx returns the outgoing context for a key ("" = no key).
func authzGrpcCtx(key string) context.Context {
	if key == "" {
		return context.Background()
	}
	return keyCtx(key)
}

// authzCase is one RPC invocation, expressed once for gRPC and once for REST.
// n is the transport index (0 gRPC, 1 REST) so mutating cases can use a
// distinct target per transport.
type authzCase struct {
	name string
	// want maps principal → expected gRPC code. Only listed principals run.
	want map[string]codes.Code
	grpc func(h *authzHarness, cx context.Context, n int) error
	// rest is nil for RPCs that have no REST route.
	rest func(n int) (method, path, body string)
}

const (
	pAnon   = "anon"
	pBad    = "bad"
	pReader = "k-reader"
	pROA    = "k-roa"
	pRWA    = "k-rwa"
	pAdmin  = "k-admin"
)

// order puts the most privileged principal last so destructive admin cases run
// only after the denied principals have been refused.
var principalOrder = []string{pAnon, pBad, pReader, pROA, pRWA, pAdmin}

func authzKeyFor(p string) string {
	switch p {
	case pAnon:
		return ""
	case pBad:
		return "k-wrong"
	}
	return p
}

var (
	ok   = codes.OK
	deny = codes.PermissionDenied
	noau = codes.Unauthenticated
)

// matrix builds the expectation map for the common shapes.
func authzShape(admin, reader, rwa, roa codes.Code) map[string]codes.Code {
	return map[string]codes.Code{
		pAnon: noau, pBad: noau,
		pAdmin: admin, pReader: reader, pRWA: rwa, pROA: roa,
	}
}

func jsonData(v string) string { return fmt.Sprintf(`{"data":{"v":%q}}`, v) }

func authzCases() []authzCase {
	sd := func(h *authzHarness) *structpb.Struct { return mustStruct(h.t, map[string]any{"v": "x"}) }
	var cs []authzCase

	// ---- Single-collection reads: a_orders (in scope) and b_secrets (out). ----
	for _, c := range []struct {
		coll string
		exp  map[string]codes.Code
	}{
		{collA, authzShape(ok, ok, ok, ok)},
		{collB, authzShape(ok, ok, deny, deny)},
		{collGhost, authzShape(codes.NotFound, codes.NotFound, deny, deny)},
	} {
		c := c
		cs = append(cs,
			authzCase{
				name: "FindById/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.FindById(cx, &pb.FindByIdRequest{Collection: c.coll, Id: 1})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "GET", "/v1/" + c.coll + "/records/1", ""
				},
			},
			authzCase{
				name: "FindByKey/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.FindByKey(cx, &pb.FindByKeyRequest{Collection: c.coll, Key: "k1"})
					return err
				},
				rest: func(n int) (string, string, string) { return "GET", "/v1/" + c.coll + "/keys/k1", "" },
			},
			authzCase{
				name: "Find/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error { return authzDrainFind(h, cx, c.coll) },
				rest: func(n int) (string, string, string) { return "POST", "/v1/" + c.coll + "/records/find", "{}" },
			},
			authzCase{
				name: "Aggregate/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					st, err := h.client.Aggregate(cx, &pb.AggregateRequest{Collection: c.coll})
					if err != nil {
						return err
					}
					return authzDrain(func() error { _, e := st.Recv(); return e })
				},
				rest: func(n int) (string, string, string) { return "POST", "/v1/" + c.coll + "/aggregate", "{}" },
			},
			authzCase{
				name: "CollectionStats/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.CollectionStats(cx, &pb.CollectionStatsRequest{Collection: c.coll})
					return err
				},
				rest: func(n int) (string, string, string) { return "GET", "/v1/" + c.coll + "/stats", "" },
			},
			authzCase{
				name: "ListIndexes/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.ListIndexes(cx, &pb.ListIndexesRequest{Collection: c.coll})
					return err
				},
				rest: func(n int) (string, string, string) { return "GET", "/v1/" + c.coll + "/indexes", "" },
			},
			authzCase{
				name: "Watch/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					wc, cancel := context.WithTimeout(cx, 700*time.Millisecond)
					defer cancel()
					st, err := h.client.Watch(wc, &pb.WatchRequest{Collection: c.coll})
					if err != nil {
						return err
					}
					err = authzDrain(func() error { _, e := st.Recv(); return e })
					if status.Code(err) == codes.DeadlineExceeded {
						return nil // authorized, idle watch
					}
					return err
				},
				rest: func(n int) (string, string, string) { return "POST", "/v1/" + c.coll + "/watch", "{}" },
			},
		)
	}

	// ---- Single-collection writes. ----
	for _, c := range []struct {
		coll string
		exp  map[string]codes.Code
	}{
		{collA, authzShape(ok, deny, ok, deny)},
		{collB, authzShape(ok, deny, deny, deny)},
		{collGhost, authzShape(codes.NotFound, deny, deny, deny)},
	} {
		c := c
		cs = append(cs,
			authzCase{
				name: "Insert/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.Insert(cx, &pb.InsertRequest{Collection: c.coll, Data: sd(h)})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "POST", "/v1/" + c.coll + "/records", jsonData("x")
				},
			},
			authzCase{
				name: "InsertMany/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.InsertMany(cx, &pb.InsertManyRequest{Collection: c.coll, Records: []*structpb.Struct{sd(h)}})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "POST", "/v1/" + c.coll + "/records/batch", `{"records":[{"v":"x"}]}`
				},
			},
			authzCase{
				name: "Update/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.Update(cx, &pb.UpdateRequest{Collection: c.coll, Id: 1, Data: sd(h)})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "PUT", "/v1/" + c.coll + "/records/1", jsonData("x")
				},
			},
			authzCase{
				name: "Upsert/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.Upsert(cx, &pb.UpsertRequest{Collection: c.coll, Key: "k1", Data: sd(h)})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "POST", "/v1/" + c.coll + "/records:upsert", `{"key":"k1","data":{"v":"x"}}`
				},
			},
			authzCase{
				name: "UpdateByKey/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.UpdateByKey(cx, &pb.UpdateByKeyRequest{Collection: c.coll, Key: "k1", Data: sd(h)})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "PUT", "/v1/" + c.coll + "/keys/k1", jsonData("x")
				},
			},
			authzCase{
				name: "UpdateIfRev/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.UpdateIfRev(cx, &pb.UpdateIfRevRequest{Collection: c.coll, Key: "k1", ExpectedRev: 9999, Data: sd(h)})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "POST", "/v1/" + c.coll + "/keys/k1:cas", `{"expectedRev":9999,"data":{"v":"x"}}`
				},
			},
			authzCase{
				name: "DeleteByKey/" + c.coll + "/missing", want: authzWithAllowed(c.exp, codes.NotFound),
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.DeleteByKey(cx, &pb.DeleteByKeyRequest{Collection: c.coll, Key: "no-such-key"})
					return err
				},
				rest: func(n int) (string, string, string) { return "DELETE", "/v1/" + c.coll + "/keys/no-such-key", "" },
			},
			authzCase{
				name: "Delete/" + c.coll + "/missing", want: authzWithAllowed(c.exp, codes.NotFound),
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.Delete(cx, &pb.DeleteRequest{Collection: c.coll, Id: 999999})
					return err
				},
				rest: func(n int) (string, string, string) { return "DELETE", "/v1/" + c.coll + "/records/999999", "" },
			},
			authzCase{
				name: "EnsureIndex/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.EnsureIndex(cx, &pb.EnsureIndexRequest{Collection: c.coll, Field: "v"})
					return err
				},
				rest: func(n int) (string, string, string) {
					return "POST", "/v1/" + c.coll + "/indexes", `{"field":"v"}`
				},
			},
			authzCase{
				name: "Compact/" + c.coll, want: c.exp,
				grpc: func(h *authzHarness, cx context.Context, n int) error {
					_, err := h.client.Compact(cx, &pb.CompactRequest{Collection: c.coll})
					return err
				},
				rest: func(n int) (string, string, string) { return "POST", "/v1/" + c.coll + "/compact", "{}" },
			},
		)
	}

	// ---- Collection lifecycle. ----
	cs = append(cs,
		authzCase{
			name: "CreateCollection/in-list",
			want: map[string]codes.Code{pAnon: noau, pBad: noau, pReader: deny, pROA: deny, pRWA: ok},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: []string{"a_new_g", "a_new_r"}[n]})
				return err
			},
			rest: func(n int) (string, string, string) {
				return "POST", "/v1/collections", fmt.Sprintf(`{"name":%q}`, []string{"a_new_g", "a_new_r"}[n])
			},
		},
		authzCase{
			name: "CreateCollection/out-of-list/new",
			want: map[string]codes.Code{pAnon: noau, pBad: noau, pReader: deny, pROA: deny, pRWA: deny},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: "b_new"})
				return err
			},
			rest: func(n int) (string, string, string) { return "POST", "/v1/collections", `{"name":"b_new"}` },
		},
		authzCase{
			name: "CreateCollection/out-of-list/existing",
			want: map[string]codes.Code{pROA: deny, pRWA: deny},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: collB})
				return err
			},
			rest: func(n int) (string, string, string) { return "POST", "/v1/collections", `{"name":"` + collB + `"}` },
		},
		authzCase{
			name: "CreateCollection/admin",
			want: map[string]codes.Code{pAdmin: ok},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: fmt.Sprintf("adm_new_%d", n)})
				return err
			},
			rest: func(n int) (string, string, string) {
				return "POST", "/v1/collections", fmt.Sprintf(`{"name":"adm_new_%d"}`, n)
			},
		},
		authzCase{
			name: "DropCollection/own-listed-collection",
			want: map[string]codes.Code{pReader: deny, pROA: deny, pRWA: deny},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.DropCollection(cx, &pb.DropCollectionRequest{Name: collA})
				return err
			},
			rest: func(n int) (string, string, string) { return "DELETE", "/v1/collections/" + collA, "" },
		},
		authzCase{
			name: "DropCollection/foreign-existing",
			want: map[string]codes.Code{pReader: deny, pROA: deny, pRWA: deny},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.DropCollection(cx, &pb.DropCollectionRequest{Name: collB})
				return err
			},
			rest: func(n int) (string, string, string) { return "DELETE", "/v1/collections/" + collB, "" },
		},
		authzCase{
			name: "DropCollection/anon-and-admin",
			want: map[string]codes.Code{pAnon: noau, pBad: noau, pAdmin: ok},
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.DropCollection(cx, &pb.DropCollectionRequest{Name: fmt.Sprintf("scratch%d", n)})
				return err
			},
			rest: func(n int) (string, string, string) {
				return "DELETE", fmt.Sprintf("/v1/collections/scratch%d", n), ""
			},
		},
		authzCase{
			name: "ListCollections",
			want: authzShape(ok, ok, ok, ok),
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.ListCollections(cx, &pb.ListCollectionsRequest{})
				return err
			},
			rest: func(n int) (string, string, string) { return "GET", "/v1/collections", "" },
		},
	)

	// ---- Whole-database surfaces. ----
	cs = append(cs,
		authzCase{
			name: "ReplicationStatus",
			want: authzShape(ok, ok, deny, deny),
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.ReplicationStatus(cx, &pb.ReplicationStatusRequest{})
				return err
			},
			rest: func(n int) (string, string, string) { return "GET", "/v1/replication/status", "" },
		},
		authzCase{
			name: "Promote",
			// An unrestricted read-write key passes authorization; the node is a
			// leader so the handler then refuses with FailedPrecondition.
			want: authzShape(codes.FailedPrecondition, deny, deny, deny),
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.Promote(cx, &pb.PromoteRequest{})
				return err
			},
			rest: func(n int) (string, string, string) { return "POST", "/v1/replication/promote", "{}" },
		},
		authzCase{
			name: "Snapshot", // gRPC only
			want: authzShape(ok, ok, deny, deny),
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				st, err := h.client.Snapshot(cx, &pb.SnapshotRequest{})
				if err != nil {
					return err
				}
				// Only the outcome of the first receive matters: a denied stream
				// must fail before any archive byte is sent.
				if _, err := st.Recv(); err != nil && err != io.EOF {
					return err
				}
				return nil
			},
		},
		authzCase{
			name: "Replicate", // gRPC only
			// Authorized callers reach the handler, which refuses because this
			// node has no replication feed configured.
			want: authzShape(codes.FailedPrecondition, codes.FailedPrecondition, deny, deny),
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				rc, cancel := context.WithTimeout(cx, 700*time.Millisecond)
				defer cancel()
				st, err := h.client.Replicate(rc, &pb.ReplicateRequest{FromLsn: 0, FollowerId: "t"})
				if err != nil {
					return err
				}
				for {
					if _, err := st.Recv(); err != nil {
						if err == io.EOF || status.Code(err) == codes.DeadlineExceeded {
							return nil
						}
						return err
					}
				}
			},
		},
		authzCase{
			name: "XTxStatus", // any authenticated principal, no collection data
			want: authzShape(ok, ok, ok, ok),
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.XTxStatus(cx, &pb.XTxStatusRequest{Ref: "no-such-ref"})
				return err
			},
			rest: func(n int) (string, string, string) { return "GET", "/v1/xtx/status?ref=no-such-ref", "" },
		},
	)

	// ---- Transactions: Begin names collections (all-or-nothing). ----
	for _, c := range []struct {
		name  string
		colls []string
		exp   map[string]codes.Code
	}{
		{"in-scope", []string{collA}, authzShape(ok, deny, ok, deny)},
		{"foreign", []string{collB}, authzShape(ok, deny, deny, deny)},
		{"mixed-in-first", []string{collA, collB}, authzShape(ok, deny, deny, deny)},
		{"mixed-foreign-first", []string{collB, collA}, authzShape(ok, deny, deny, deny)},
		{"ghost", []string{collA, collGhost}, authzShape(codes.NotFound, deny, deny, deny)},
	} {
		c := c
		cs = append(cs, authzCase{
			name: "BeginXTx/" + c.name, want: c.exp,
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.BeginXTx(cx, &pb.BeginXTxRequest{Collections: c.colls})
				return err
			},
			rest: func(n int) (string, string, string) {
				b, _ := json.Marshal(map[string]any{"collections": c.colls})
				return "POST", "/v1/xtx", string(b)
			},
		})
	}
	for _, c := range []struct {
		coll string
		exp  map[string]codes.Code
	}{
		{collA, authzShape(ok, deny, ok, deny)},
		{collB, authzShape(ok, deny, deny, deny)},
	} {
		c := c
		cs = append(cs, authzCase{
			name: "BeginTx/" + c.coll, want: c.exp,
			grpc: func(h *authzHarness, cx context.Context, n int) error {
				_, err := h.client.BeginTx(cx, &pb.BeginTxRequest{Collection: c.coll})
				return err
			},
		})
	}
	return cs
}

// withAllowed rewrites the expected "authorized" outcome of a shape.
func authzWithAllowed(m map[string]codes.Code, c codes.Code) map[string]codes.Code {
	out := make(map[string]codes.Code, len(m))
	for k, v := range m {
		if v == codes.OK {
			v = c
		}
		out[k] = v
	}
	return out
}

func authzDrain(recv func() error) error {
	for {
		if err := recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func authzDrainFind(h *authzHarness, cx context.Context, coll string) error {
	st, err := h.client.Find(cx, &pb.FindRequest{Collection: coll})
	if err != nil {
		return err
	}
	return authzDrain(func() error { _, e := st.Recv(); return e })
}

func authzCodeOf(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return status.Code(err)
}

// TestAuthz_Matrix runs every case as every listed principal through gRPC and
// REST and asserts both transports agree with the expected code.
func TestAuthz_Matrix(t *testing.T) {
	h := newAuthzHarness(t)
	for _, tc := range authzCases() {
		for _, who := range principalOrder {
			want, listed := tc.want[who]
			if !listed {
				continue
			}
			key := authzKeyFor(who)
			gotG := authzCodeOf(tc.grpc(h, authzGrpcCtx(key), 0))
			if gotG != want {
				t.Errorf("%s as %s over gRPC: got %v, want %v", tc.name, who, gotG, want)
			}
			if tc.rest == nil {
				continue
			}
			method, path, body := tc.rest(1)
			status, resp := h.rest(key, method, path, body)
			wantHTTP := runtime.HTTPStatusFromCode(want)
			if want == codes.OK {
				wantHTTP = http.StatusOK
			}
			if status != wantHTTP {
				t.Errorf("%s as %s over REST: HTTP %d, want %d (gRPC code %v) body=%s",
					tc.name, who, status, wantHTTP, want, resp)
			}
			// Parity of the *denial* itself: a gRPC denial maps to a REST denial.
			if (gotG == deny || gotG == noau) && status == http.StatusOK {
				t.Errorf("%s as %s: gRPC denied but REST allowed", tc.name, who)
			}
		}
	}
}

// TestAuthz_NoDiskEffectOnDenied checks that refused creates leave nothing
// behind and refused drops do not delete.
func TestAuthz_NoDiskEffectOnDenied(t *testing.T) {
	h := newAuthzHarness(t)
	cx := keyCtx("k-rwa")
	for _, name := range []string{"b_new", "b_other", "../escape"} {
		_, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: name})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("CreateCollection(%q) as scoped key: %v, want PermissionDenied", name, err)
		}
		if _, statErr := os.Stat(h.dir + "/" + name); statErr == nil {
			t.Errorf("denied CreateCollection(%q) left a directory behind", name)
		}
	}
	if _, err := h.client.DropCollection(cx, &pb.DropCollectionRequest{Name: collA}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped DropCollection: %v, want PermissionDenied", err)
	}
	if _, err := h.client.DropCollection(cx, &pb.DropCollectionRequest{Name: collB}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped DropCollection(foreign): %v, want PermissionDenied", err)
	}
	names := h.listAs("k-admin")
	for _, want := range []string{collA, collB} {
		if !authzContains(names, want) {
			t.Errorf("collection %s vanished after denied drops: %v", want, names)
		}
	}
}

func (h *authzHarness) listAs(key string) []string {
	h.t.Helper()
	r, err := h.client.ListCollections(authzGrpcCtx(key), &pb.ListCollectionsRequest{})
	if err != nil {
		h.t.Fatalf("ListCollections(%s): %v", key, err)
	}
	return r.Names
}

func authzContains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// TestAuthz_ListCollectionsFiltered covers D4: allow-list ∩ existing, sorted,
// never an xtx.* name, and live on reload (L-1, L-2).
func TestAuthz_ListCollectionsFiltered(t *testing.T) {
	h := newAuthzHarness(t)

	// An XTx journal exists after one commit; its internals never appear.
	cx := keyCtx("k-admin")
	b, err := h.client.BeginXTx(cx, &pb.BeginXTxRequest{Collections: []string{collA, collB}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.StageXTx(cx, &pb.StageXTxRequest{XtxId: b.XtxId, Collection: collA,
		Op: pb.XTxOpKind_XTX_OP_INSERT, Data: mustStruct(t, map[string]any{"v": 1})}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.CommitXTx(cx, &pb.CommitXTxRequest{XtxId: b.XtxId}); err != nil {
		t.Fatal(err)
	}

	// rw_a's list is {a_orders, a_new_g, a_new_r, a_ghost}; only a_orders exists.
	got := h.listAs("k-rwa")
	if len(got) != 1 || got[0] != collA {
		t.Errorf("scoped list = %v, want [%s]", got, collA)
	}
	st, body := h.rest("k-rwa", "GET", "/v1/collections", "")
	if st != 200 || strings.Contains(body, collB) || strings.Contains(body, "a_ghost") {
		t.Errorf("REST scoped list: %d %s", st, body)
	}

	all := h.listAs("k-admin")
	if !authzContains(all, collA) || !authzContains(all, collB) {
		t.Errorf("unrestricted list = %v, want both a_orders and b_secrets", all)
	}
	for _, n := range all {
		if strings.HasPrefix(n, "xtx.") {
			t.Errorf("unrestricted list leaked internal name %q", n)
		}
	}

	// Newly created in-list collection shows up for the scoped key.
	if _, err := h.client.CreateCollection(keyCtx("k-rwa"), &pb.CreateCollectionRequest{Name: "a_new_g"}); err != nil {
		t.Fatalf("scoped create in list: %v", err)
	}
	if got := h.listAs("k-rwa"); len(got) != 2 || got[0] != "a_new_g" || got[1] != collA {
		t.Errorf("scoped list after create = %v, want sorted [a_new_g a_orders]", got)
	}

	// L-2: shrink the allow-list; the next call reflects it with no restart.
	keys := append([]auth.Key(nil), authzKeys...)
	for i := range keys {
		if keys[i].Key == "k-rwa" {
			keys[i].Collections = []string{"a_new_g"}
		}
	}
	if err := h.authn.Reload(keys); err != nil {
		t.Fatal(err)
	}
	if got := h.listAs("k-rwa"); len(got) != 1 || got[0] != "a_new_g" {
		t.Errorf("list after reload = %v, want [a_new_g]", got)
	}
	if _, err := h.client.FindById(keyCtx("k-rwa"), &pb.FindByIdRequest{Collection: collA, Id: 1}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("read of a collection removed from the list: %v, want PermissionDenied", err)
	}
}

// TestAuthz_NoDisclosure asserts a scoped principal cannot tell a foreign
// collection that exists from one that does not, and never receives data or
// names from outside its list (L-3, L-4, A-6).
func TestAuthz_NoDisclosure(t *testing.T) {
	h := newAuthzHarness(t)
	cx := keyCtx("k-rwa")

	type probe struct {
		name string
		do   func(coll string) error
	}
	probes := []probe{
		{"FindById", func(c string) error {
			_, err := h.client.FindById(cx, &pb.FindByIdRequest{Collection: c, Id: 1})
			return err
		}},
		{"FindByKey", func(c string) error {
			_, err := h.client.FindByKey(cx, &pb.FindByKeyRequest{Collection: c, Key: "k1"})
			return err
		}},
		{"Find", func(c string) error { return authzDrainFind(h, cx, c) }},
		{"Insert", func(c string) error {
			_, err := h.client.Insert(cx, &pb.InsertRequest{Collection: c, Data: mustStruct(t, map[string]any{"v": 1})})
			return err
		}},
		{"Update", func(c string) error {
			_, err := h.client.Update(cx, &pb.UpdateRequest{Collection: c, Id: 1, Data: mustStruct(t, map[string]any{"v": 1})})
			return err
		}},
		{"Delete", func(c string) error {
			_, err := h.client.Delete(cx, &pb.DeleteRequest{Collection: c, Id: 1})
			return err
		}},
		{"CollectionStats", func(c string) error {
			_, err := h.client.CollectionStats(cx, &pb.CollectionStatsRequest{Collection: c})
			return err
		}},
		{"BeginTx", func(c string) error {
			_, err := h.client.BeginTx(cx, &pb.BeginTxRequest{Collection: c})
			return err
		}},
		{"BeginXTx", func(c string) error {
			_, err := h.client.BeginXTx(cx, &pb.BeginXTxRequest{Collections: []string{collA, c}})
			return err
		}},
		{"Drop", func(c string) error {
			_, err := h.client.DropCollection(cx, &pb.DropCollectionRequest{Name: c})
			return err
		}},
		{"Create", func(c string) error {
			_, err := h.client.CreateCollection(cx, &pb.CreateCollectionRequest{Name: c})
			return err
		}},
	}
	for _, p := range probes {
		exists := p.do(collB)
		missing := p.do(collGhost)
		if status.Code(exists) != codes.PermissionDenied || status.Code(missing) != codes.PermissionDenied {
			t.Errorf("%s: existing=%v missing=%v, want both PermissionDenied", p.name, exists, missing)
			continue
		}
		// Same message once the caller-supplied name is factored out.
		a := strings.ReplaceAll(status.Convert(exists).Message(), collB, "<c>")
		b := strings.ReplaceAll(status.Convert(missing).Message(), collGhost, "<c>")
		if a != b {
			t.Errorf("%s: error text distinguishes existing from missing: %q vs %q", p.name, a, b)
		}
		if strings.Contains(status.Convert(exists).Message(), secretMark) {
			t.Errorf("%s: error leaked record contents", p.name)
		}
	}

	// Snapshot/Replicate: refused with an empty stream (L-3/L-4).
	for _, k := range []string{"k-rwa", "k-roa"} {
		st, err := h.client.Snapshot(keyCtx(k), &pb.SnapshotRequest{})
		if err == nil {
			var chunk *pb.SnapshotChunk
			chunk, err = st.Recv()
			if err == nil {
				t.Errorf("%s: Snapshot sent %d bytes", k, len(chunk.Data))
			}
		}
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: Snapshot: %v, want PermissionDenied", k, err)
		}
		rs, err := h.client.Replicate(keyCtx(k), &pb.ReplicateRequest{FollowerId: "x"})
		if err == nil {
			_, err = rs.Recv()
		}
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: Replicate: %v, want PermissionDenied", k, err)
		}
	}

	// Nothing any scoped call returned over REST mentions the foreign
	// collection or its contents.
	for _, k := range []string{"k-rwa", "k-roa"} {
		for _, req := range [][3]string{
			{"GET", "/v1/collections", ""},
			{"GET", "/v1/replication/status", ""},
			{"POST", "/v1/" + collA + "/records/find", "{}"},
			{"GET", "/v1/" + collB + "/records/1", ""},
		} {
			_, body := h.rest(k, req[0], req[1], req[2])
			if strings.Contains(body, secretMark) {
				t.Errorf("%s %s %s leaked contents: %s", k, req[0], req[1], body)
			}
			if req[1] != "/v1/"+collB+"/records/1" && strings.Contains(body, collB) {
				t.Errorf("%s %s %s leaked the foreign collection name: %s", k, req[0], req[1], body)
			}
		}
	}
	// And the in-scope data a scoped key reads is its own.
	r, err := h.client.FindByKey(keyCtx("k-roa"), &pb.FindByKeyRequest{Collection: collA, Key: "k1"})
	if err != nil {
		t.Fatalf("ro_a FindByKey on own collection: %v", err)
	}
	if v := r.GetRecord().GetData().GetFields()["v"].GetStringValue(); v != "TOPSECRET-"+collA {
		t.Errorf("ro_a read %q", v)
	}
}

// TestAuthz_HandleOwnership covers A-4/A-5: transaction handles are bound to
// the principal that began them.
func TestAuthz_HandleOwnership(t *testing.T) {
	h := newAuthzHarness(t)
	owner, peer, admin := keyCtx("k-rwa"), keyCtx("k-peer"), keyCtx("k-admin")
	data := mustStruct(t, map[string]any{"v": "mine"})

	b, err := h.client.BeginXTx(owner, &pb.BeginXTxRequest{Collections: []string{collA}})
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	id := b.XtxId
	stage := func(cx context.Context) error {
		_, err := h.client.StageXTx(cx, &pb.StageXTxRequest{XtxId: id, Collection: collA, Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data})
		return err
	}
	for name, err := range map[string]error{
		"peer StageXTx": stage(peer),
		"peer GetXTx": func() error {
			_, err := h.client.GetXTx(peer, &pb.GetXTxRequest{XtxId: id, Collection: collA, Id: 1})
			return err
		}(),
		"peer CommitXTx": func() error {
			_, err := h.client.CommitXTx(peer, &pb.CommitXTxRequest{XtxId: id})
			return err
		}(),
		"peer RollbackXTx": func() error {
			_, err := h.client.RollbackXTx(peer, &pb.RollbackXTxRequest{XtxId: id})
			return err
		}(),
		"admin CommitXTx": func() error {
			_, err := h.client.CommitXTx(admin, &pb.CommitXTxRequest{XtxId: id})
			return err
		}(),
		"admin StageXTx": stage(admin),
	} {
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s on another principal's handle: %v, want PermissionDenied", name, err)
		}
	}
	// The handle is still intact for its owner.
	if err := stage(owner); err != nil {
		t.Fatalf("owner StageXTx after refused attempts: %v", err)
	}
	if _, err := h.client.CommitXTx(owner, &pb.CommitXTxRequest{XtxId: id}); err != nil {
		t.Fatalf("owner CommitXTx: %v", err)
	}

	// A refused BeginXTx creates no handle and writes nothing.
	if _, err := h.client.BeginXTx(owner, &pb.BeginXTxRequest{Collections: []string{collA, collB}, IdempotencyKey: "nope-key"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("BeginXTx across the list: %v", err)
	}
	if r, err := h.client.XTxStatus(admin, &pb.XTxStatusRequest{Ref: "nope-key"}); err != nil || r.Status != pb.XTxState_XTX_STATUS_UNKNOWN {
		t.Errorf("refused BeginXTx left state behind: %v %v", r, err)
	}

	// Single-collection transactions are bound the same way, including the
	// x-tx-id header on ordinary data RPCs.
	tx, err := h.client.BeginTx(owner, &pb.BeginTxRequest{Collection: collA})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	withTx := func(key string) context.Context {
		return metadata.AppendToOutgoingContext(keyCtx(key), "x-tx-id", tx.TxId)
	}
	if _, err := h.client.Insert(withTx("k-peer"), &pb.InsertRequest{Collection: collA, Data: data}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("peer Insert into owner's tx: %v, want PermissionDenied", err)
	}
	if _, err := h.client.CommitTx(peer, &pb.CommitTxRequest{TxId: tx.TxId}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("peer CommitTx: %v, want PermissionDenied", err)
	}
	if _, err := h.client.RollbackTx(peer, &pb.RollbackTxRequest{TxId: tx.TxId}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("peer RollbackTx: %v, want PermissionDenied", err)
	}
	if _, err := h.client.Insert(withTx("k-rwa"), &pb.InsertRequest{Collection: collA, Data: data}); err != nil {
		t.Fatalf("owner Insert into own tx: %v", err)
	}
	if _, err := h.client.CommitTx(owner, &pb.CommitTxRequest{TxId: tx.TxId}); err != nil {
		t.Fatalf("owner CommitTx: %v", err)
	}
}

// TestAuthz_ReadKeyGainsKeyedReads pins the D3 change that FindByKey and
// Aggregate are explicit reads: an unrestricted read key may call them.
func TestAuthz_ReadKeyGainsKeyedReads(t *testing.T) {
	h := newAuthzHarness(t)
	cx := keyCtx("k-reader")
	if _, err := h.client.FindByKey(cx, &pb.FindByKeyRequest{Collection: collB, Key: "k1"}); err != nil {
		t.Errorf("read key FindByKey: %v", err)
	}
	st, err := h.client.Aggregate(cx, &pb.AggregateRequest{Collection: collB})
	if err == nil {
		err = authzDrain(func() error { _, e := st.Recv(); return e })
	}
	if err != nil {
		t.Errorf("read key Aggregate: %v", err)
	}
}
