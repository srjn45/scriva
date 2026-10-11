package auth

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pb "github.com/srjn45/scriva/internal/pb/proto"
)

// serviceMethods lists every full method name of the services the server
// registers, from the generated descriptors.
func serviceMethods() []string {
	var out []string
	for _, d := range []*grpc.ServiceDesc{&pb.Scriva_ServiceDesc, &healthpb.Health_ServiceDesc} {
		for _, m := range d.Methods {
			out = append(out, "/"+d.ServiceName+"/"+m.MethodName)
		}
		for _, s := range d.Streams {
			out = append(out, "/"+d.ServiceName+"/"+s.StreamName)
		}
	}
	return out
}

// TestPolicy_CoversEveryRPC (D3 A-1) fails when the proto gains an RPC that
// has no classification, and when the table keeps an entry for a method that
// no longer exists.
func TestPolicy_CoversEveryRPC(t *testing.T) {
	declared := map[string]bool{}
	for _, m := range serviceMethods() {
		declared[m] = true
		a, ok := PolicyFor(m)
		if !ok {
			t.Errorf("RPC %s has no authorization policy: add it to methodPolicies in internal/auth/policy.go", m)
		}
		if a == AccessDenied {
			t.Errorf("RPC %s is classified AccessDenied; give it a real capability class", m)
		}
	}
	for _, m := range PolicyMethods() {
		if !declared[m] {
			t.Errorf("policy entry %s matches no registered RPC (stale entry)", m)
		}
	}
	if len(declared) < 34 {
		t.Errorf("only %d RPCs found in the service descriptors; descriptor walk is broken", len(declared))
	}
}

// TestPolicy_UnknownMethodFailsClosed: an unclassified method is denied to
// read keys and to restricted read-write keys, and reachable only by an
// unrestricted read-write principal (db:admin) or with auth disabled.
func TestPolicy_UnknownMethodFailsClosed(t *testing.T) {
	a := mustNew(t,
		Key{Key: "ro", Name: "ro", Scope: ScopeRead},
		Key{Key: "rw", Name: "rw", Scope: ScopeReadWrite},
		Key{Key: "ro-l", Name: "ro-l", Scope: ScopeRead, Collections: []string{"a"}},
		Key{Key: "rw-l", Name: "rw-l", Scope: ScopeReadWrite, Collections: []string{"a"}},
	)
	for _, m := range []string{"/scriva.v1.Scriva/BrandNewRPC", "/other.v1.Svc/Insert", "", "Insert"} {
		for key, want := range map[string]codes.Code{
			"ro": codes.PermissionDenied, "ro-l": codes.PermissionDenied, "rw-l": codes.PermissionDenied,
			"rw": codes.OK, "": codes.Unauthenticated,
		} {
			ctx := context.Background()
			if key != "" {
				ctx = ctxWithKey(key)
			}
			if got := call(t, a, ctx, m); got != want {
				t.Errorf("unknown method %q as %q: got %v, want %v", m, key, got, want)
			}
		}
	}
}

// TestPolicy_Capabilities walks each access class against each principal shape.
func TestPolicy_Capabilities(t *testing.T) {
	a := mustNew(t,
		Key{Key: "admin", Name: "admin", Scope: ScopeReadWrite},
		Key{Key: "reader", Name: "reader", Scope: ScopeRead},
		Key{Key: "rw-l", Name: "rw-l", Scope: ScopeReadWrite, Collections: []string{"a"}},
		Key{Key: "ro-l", Name: "ro-l", Scope: ScopeRead, Collections: []string{"a"}},
	)
	cases := []struct {
		method string
		req    any
		want   map[string]codes.Code // admin, reader, rw-l, ro-l
	}{
		{"CreateCollection", &fakeCollReq{"a"}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 0, "ro-l": 7}},
		{"CreateCollection", &fakeCollReq{"b"}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 7, "ro-l": 7}},
		{"DropCollection", &fakeCollReq{"a"}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 7, "ro-l": 7}},
		{"ListCollections", nil, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 0, "ro-l": 0}},
		{"Find", &fakeCollReq{"a"}, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 0, "ro-l": 0}},
		{"Find", &fakeCollReq{"b"}, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 7, "ro-l": 7}},
		{"FindByKey", &fakeCollReq{"a"}, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 0, "ro-l": 0}},
		{"Aggregate", &fakeCollReq{"b"}, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 7, "ro-l": 7}},
		{"Upsert", &fakeCollReq{"a"}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 0, "ro-l": 7}},
		{"UpdateIfRev", &fakeCollReq{"b"}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 7, "ro-l": 7}},
		{"BeginXTx", &fakeMultiCollReq{[]string{"a", "b"}}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 7, "ro-l": 7}},
		{"BeginXTx", &fakeMultiCollReq{[]string{"b", "a"}}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 7, "ro-l": 7}},
		{"BeginXTx", &fakeMultiCollReq{[]string{"a"}}, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 0, "ro-l": 7}},
		{"CommitXTx", nil, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 0, "ro-l": 7}},
		{"XTxStatus", nil, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 0, "ro-l": 0}},
		{"Snapshot", nil, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 7, "ro-l": 7}},
		{"Replicate", nil, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 7, "ro-l": 7}},
		{"ReplicationStatus", nil, map[string]codes.Code{"admin": 0, "reader": 0, "rw-l": 7, "ro-l": 7}},
		{"Promote", nil, map[string]codes.Code{"admin": 0, "reader": 7, "rw-l": 7, "ro-l": 7}},
	}
	unary, _ := a.Interceptors()
	for _, tc := range cases {
		for who, want := range tc.want {
			ran := false
			_, err := unary(ctxWithKey(who), tc.req, &grpc.UnaryServerInfo{FullMethod: "/scriva.v1.Scriva/" + tc.method},
				func(context.Context, any) (any, error) { ran = true; return nil, nil })
			got := codes.OK
			if err != nil {
				got = codeFrom(err)
			}
			if got != want {
				t.Errorf("%s(%v) as %s: got %v, want %v", tc.method, tc.req, who, got, want)
			}
			if (got == codes.OK) != ran {
				t.Errorf("%s as %s: handler ran=%v with code %v", tc.method, who, ran, got)
			}
		}
	}
}

// TestPolicy_RequestWithoutCollectionFailsClosed: a restricted principal
// calling a collection RPC that names nothing (empty name, unreadable request)
// is refused rather than passed through.
func TestPolicy_RequestWithoutCollectionFailsClosed(t *testing.T) {
	a := mustNew(t, Key{Key: "rw-l", Name: "rw-l", Scope: ScopeReadWrite, Collections: []string{"a"}})
	for name, req := range map[string]any{"empty": &fakeCollReq{""}, "nil": nil, "opaque": struct{}{}} {
		if got := callReq(t, a, ctxWithKey("rw-l"), "/scriva.v1.Scriva/Insert", req); got != codes.PermissionDenied {
			t.Errorf("%s request: got %v, want PermissionDenied", name, got)
		}
	}
}

// TestPolicy_PrincipalIDsAreDistinct: handle ownership relies on a per-
// credential id, so two keys sharing a display name must not share an id.
func TestPolicy_PrincipalIDsAreDistinct(t *testing.T) {
	a := mustNew(t,
		Key{Key: "k1", Scope: ScopeReadWrite},
		Key{Key: "k2", Scope: ScopeReadWrite},
	)
	ks := a.keys.Load()
	if len(ks.entries) != 2 || ks.entries[0].principal.id == "" ||
		ks.entries[0].principal.id == ks.entries[1].principal.id {
		t.Fatalf("principal ids not distinct: %q %q", ks.entries[0].principal.id, ks.entries[1].principal.id)
	}
}

func codeFrom(err error) codes.Code { return status.Code(err) }

// callReq runs the unary interceptor with an arbitrary request value.
func callReq(t *testing.T, a *Authenticator, ctx context.Context, method string, req any) codes.Code {
	t.Helper()
	unary, _ := a.Interceptors()
	ran := false
	_, err := unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: method},
		func(context.Context, any) (any, error) { ran = true; return nil, nil })
	if err == nil {
		if !ran {
			t.Fatal("no error but handler did not run")
		}
		return codes.OK
	}
	if ran {
		t.Fatal("handler ran despite an error")
	}
	return status.Code(err)
}
