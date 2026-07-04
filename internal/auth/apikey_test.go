package auth

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	writeMethod = "/filedb.v1.FileDB/Insert"
	readMethod  = "/filedb.v1.FileDB/Find"
)

func ctxWithKey(key string) context.Context {
	md := metadata.New(map[string]string{metadataKey: key})
	return metadata.NewIncomingContext(context.Background(), md)
}

// call runs the unary interceptor for a given method + context and reports the
// gRPC status code (codes.OK when the handler ran).
func call(t *testing.T, a *Authenticator, ctx context.Context, method string) codes.Code {
	t.Helper()
	unary, _ := a.Interceptors()
	handlerRan := false
	handler := func(context.Context, any) (any, error) {
		handlerRan = true
		return nil, nil
	}
	_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	if err == nil {
		if !handlerRan {
			t.Fatal("no error but handler did not run")
		}
		return codes.OK
	}
	if handlerRan {
		t.Fatal("handler ran despite an auth error")
	}
	return status.Code(err)
}

func mustNew(t *testing.T, keys ...Key) *Authenticator {
	t.Helper()
	a, err := New(keys)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func TestAuthenticator_NoKeysDisablesAuth(t *testing.T) {
	a := mustNew(t) // no keys
	// Even with no metadata at all, requests pass through.
	if code := call(t, a, context.Background(), writeMethod); code != codes.OK {
		t.Errorf("disabled auth: write got %v, want OK", code)
	}
	if code := call(t, a, context.Background(), readMethod); code != codes.OK {
		t.Errorf("disabled auth: read got %v, want OK", code)
	}
}

func TestAuthenticator_UnknownKeyRejected(t *testing.T) {
	a := mustNew(t, Key{Key: "good", Name: "app", Scope: ScopeReadWrite})
	if code := call(t, a, ctxWithKey("wrong"), readMethod); code != codes.Unauthenticated {
		t.Errorf("unknown key: got %v, want Unauthenticated", code)
	}
	if code := call(t, a, context.Background(), readMethod); code != codes.Unauthenticated {
		t.Errorf("missing key: got %v, want Unauthenticated", code)
	}
}

func TestAuthenticator_ReadWriteAllowsEverything(t *testing.T) {
	a := mustNew(t, Key{Key: "rw", Name: "app", Scope: ScopeReadWrite})
	if code := call(t, a, ctxWithKey("rw"), writeMethod); code != codes.OK {
		t.Errorf("read-write on write: got %v, want OK", code)
	}
	if code := call(t, a, ctxWithKey("rw"), readMethod); code != codes.OK {
		t.Errorf("read-write on read: got %v, want OK", code)
	}
}

func TestAuthenticator_ReadScopeRejectedOnWrite(t *testing.T) {
	a := mustNew(t, Key{Key: "ro", Name: "analytics", Scope: ScopeRead})
	if code := call(t, a, ctxWithKey("ro"), readMethod); code != codes.OK {
		t.Errorf("read-only on read: got %v, want OK", code)
	}
	if code := call(t, a, ctxWithKey("ro"), writeMethod); code != codes.PermissionDenied {
		t.Errorf("read-only on write: got %v, want PermissionDenied", code)
	}
	// An unclassified/unknown method is treated as a write (fail-safe).
	if code := call(t, a, ctxWithKey("ro"), "/filedb.v1.FileDB/SomethingNew"); code != codes.PermissionDenied {
		t.Errorf("read-only on unknown method: got %v, want PermissionDenied", code)
	}
}

func TestAuthenticator_ReloadPicksUpNewKeys(t *testing.T) {
	a := mustNew(t, Key{Key: "old", Name: "app", Scope: ScopeReadWrite})
	if code := call(t, a, ctxWithKey("old"), writeMethod); code != codes.OK {
		t.Fatalf("pre-reload old key: got %v, want OK", code)
	}

	if err := a.Reload([]Key{{Key: "new", Name: "app", Scope: ScopeReadWrite}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if code := call(t, a, ctxWithKey("new"), writeMethod); code != codes.OK {
		t.Errorf("post-reload new key: got %v, want OK", code)
	}
	if code := call(t, a, ctxWithKey("old"), writeMethod); code != codes.Unauthenticated {
		t.Errorf("post-reload old key: got %v, want Unauthenticated", code)
	}
}

func TestAuthenticator_MultipleKeysDistinctScopes(t *testing.T) {
	a := mustNew(t,
		Key{Key: "reader", Name: "analytics", Scope: ScopeRead},
		Key{Key: "writer", Name: "app", Scope: ScopeReadWrite},
	)
	if code := call(t, a, ctxWithKey("reader"), writeMethod); code != codes.PermissionDenied {
		t.Errorf("reader on write: got %v, want PermissionDenied", code)
	}
	if code := call(t, a, ctxWithKey("writer"), writeMethod); code != codes.OK {
		t.Errorf("writer on write: got %v, want OK", code)
	}
}

func TestNew_RejectsDuplicateAndEmptyKeys(t *testing.T) {
	if _, err := New([]Key{{Key: "", Name: "empty", Scope: ScopeRead}}); err == nil {
		t.Error("expected error for empty key value")
	}
	if _, err := New([]Key{
		{Key: "same", Name: "a", Scope: ScopeRead},
		{Key: "same", Name: "b", Scope: ScopeReadWrite},
	}); err == nil {
		t.Error("expected error for duplicate key value")
	}
}

func TestStreamInterceptor_EnforcesScope(t *testing.T) {
	a := mustNew(t, Key{Key: "ro", Name: "analytics", Scope: ScopeRead})
	_, stream := a.Interceptors()
	handler := func(any, grpc.ServerStream) error { return nil }
	err := stream(nil, fakeStream{ctx: ctxWithKey("ro")},
		&grpc.StreamServerInfo{FullMethod: "/filedb.v1.FileDB/Watch"}, handler)
	if err != nil {
		t.Errorf("read-only on Watch stream: got %v, want nil", err)
	}
	// A read-only key cannot open the (write-classified) Snapshot? Snapshot is a
	// read; use a write stream method sensibly — there are none streaming, so
	// assert unknown streaming method is denied.
	err = stream(nil, fakeStream{ctx: ctxWithKey("ro")},
		&grpc.StreamServerInfo{FullMethod: "/filedb.v1.FileDB/MutateStream"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("read-only on unknown stream: got %v, want PermissionDenied", err)
	}
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeStream) Context() context.Context { return f.ctx }

// fakeCollReq is a request message that names a target collection, mirroring the
// generated GetCollection accessor the ACL check keys off.
type fakeCollReq struct{ coll string }

func (f *fakeCollReq) GetCollection() string { return f.coll }

// callColl runs the unary interceptor with a collection-scoped request targeting
// coll and reports the resulting gRPC status code.
func callColl(t *testing.T, a *Authenticator, ctx context.Context, method, coll string) codes.Code {
	t.Helper()
	unary, _ := a.Interceptors()
	handlerRan := false
	handler := func(context.Context, any) (any, error) {
		handlerRan = true
		return nil, nil
	}
	_, err := unary(ctx, &fakeCollReq{coll: coll}, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	if err == nil {
		if !handlerRan {
			t.Fatal("no error but handler did not run")
		}
		return codes.OK
	}
	if handlerRan {
		t.Fatal("handler ran despite an auth error")
	}
	return status.Code(err)
}

func TestAuthenticator_CollectionACL_ConfinesKey(t *testing.T) {
	a := mustNew(t, Key{Key: "scoped", Name: "app", Scope: ScopeReadWrite, Collections: []string{"a", "c"}})

	// Allowed on its collections, for both a read and a mutating RPC.
	if code := callColl(t, a, ctxWithKey("scoped"), readMethod, "a"); code != codes.OK {
		t.Errorf("scoped key read on allowed collection: got %v, want OK", code)
	}
	if code := callColl(t, a, ctxWithKey("scoped"), writeMethod, "c"); code != codes.OK {
		t.Errorf("scoped key write on allowed collection: got %v, want OK", code)
	}
	// Denied elsewhere with PermissionDenied.
	if code := callColl(t, a, ctxWithKey("scoped"), readMethod, "b"); code != codes.PermissionDenied {
		t.Errorf("scoped key on foreign collection: got %v, want PermissionDenied", code)
	}
	if code := callColl(t, a, ctxWithKey("scoped"), writeMethod, "b"); code != codes.PermissionDenied {
		t.Errorf("scoped key write on foreign collection: got %v, want PermissionDenied", code)
	}
	// A request with no collection field (nil req here) is not collection-scoped,
	// so a restricted key may still call it (e.g. ListCollections).
	if code := call(t, a, ctxWithKey("scoped"), readMethod); code != codes.OK {
		t.Errorf("scoped key on non-collection RPC: got %v, want OK", code)
	}
}

func TestAuthenticator_NoCollectionListAllowsEverywhere(t *testing.T) {
	a := mustNew(t, Key{Key: "any", Name: "app", Scope: ScopeReadWrite})
	for _, coll := range []string{"a", "b", "anything"} {
		if code := callColl(t, a, ctxWithKey("any"), writeMethod, coll); code != codes.OK {
			t.Errorf("unrestricted key on %q: got %v, want OK", coll, code)
		}
	}
}

func TestAuthenticator_ReloadPicksUpACLChanges(t *testing.T) {
	a := mustNew(t, Key{Key: "k", Name: "app", Scope: ScopeReadWrite, Collections: []string{"a"}})
	if code := callColl(t, a, ctxWithKey("k"), readMethod, "b"); code != codes.PermissionDenied {
		t.Fatalf("pre-reload on b: got %v, want PermissionDenied", code)
	}

	// Widen the allow-list to include b; the change must take effect after reload.
	if err := a.Reload([]Key{{Key: "k", Name: "app", Scope: ScopeReadWrite, Collections: []string{"a", "b"}}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if code := callColl(t, a, ctxWithKey("k"), readMethod, "b"); code != codes.OK {
		t.Errorf("post-reload on b: got %v, want OK", code)
	}

	// Dropping the list entirely makes the key unrestricted.
	if err := a.Reload([]Key{{Key: "k", Name: "app", Scope: ScopeReadWrite}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if code := callColl(t, a, ctxWithKey("k"), readMethod, "z"); code != codes.OK {
		t.Errorf("post-reload unrestricted on z: got %v, want OK", code)
	}
}

// fakeCollStream is a server stream whose first RecvMsg yields a request naming
// coll, so the stream ACL check has a collection to enforce against.
type fakeCollStream struct {
	grpc.ServerStream
	ctx  context.Context
	coll string
}

func (f *fakeCollStream) Context() context.Context { return f.ctx }

func (f *fakeCollStream) RecvMsg(m any) error {
	if r, ok := m.(*fakeCollReq); ok {
		r.coll = f.coll
	}
	return nil
}

func TestStreamInterceptor_CollectionACL(t *testing.T) {
	a := mustNew(t, Key{Key: "scoped", Name: "app", Scope: ScopeReadWrite, Collections: []string{"a"}})
	_, stream := a.Interceptors()
	// The handler drives the stream the way a real one does: it receives the
	// client's first (collection-bearing) message before doing any work.
	handler := func(_ any, ss grpc.ServerStream) error {
		var req fakeCollReq
		return ss.RecvMsg(&req)
	}
	watch := &grpc.StreamServerInfo{FullMethod: "/filedb.v1.FileDB/Watch"}

	if err := stream(nil, &fakeCollStream{ctx: ctxWithKey("scoped"), coll: "a"}, watch, handler); err != nil {
		t.Errorf("scoped stream on allowed collection: got %v, want nil", err)
	}
	err := stream(nil, &fakeCollStream{ctx: ctxWithKey("scoped"), coll: "b"}, watch, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("scoped stream on foreign collection: got %v, want PermissionDenied", err)
	}
}

func TestCertAuth_UnaffectedByCollectionACL(t *testing.T) {
	// A cert-authenticated principal carries no allow-list, so it reaches every
	// collection regardless of other keys' ACLs (per-cert ACLs are out of scope).
	a := mustNewCertAuth(t, Key{Key: "scoped", Name: "app", Scope: ScopeReadWrite, Collections: []string{"a"}})
	ctx := ctxWithVerifiedCert(&x509.Certificate{Subject: pkix.Name{CommonName: "svc-a"}})
	for _, coll := range []string{"a", "b", "anything"} {
		if code := callColl(t, a, ctx, writeMethod, coll); code != codes.OK {
			t.Errorf("cert principal on %q: got %v, want OK", coll, code)
		}
	}
}

func TestParseScope(t *testing.T) {
	cases := map[string]Scope{
		"":           ScopeRead,
		"read":       ScopeRead,
		"ro":         ScopeRead,
		"read-write": ScopeReadWrite,
		"rw":         ScopeReadWrite,
		"write":      ScopeReadWrite,
	}
	for in, want := range cases {
		got, err := ParseScope(in)
		if err != nil {
			t.Errorf("ParseScope(%q): unexpected error %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseScope(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := ParseScope("admin"); err == nil {
		t.Error("expected error for unknown scope")
	}
}
