// Package auth provides gRPC interceptors for API key authentication with
// per-key scoping (read vs read-write) and hot-reloadable key sets for rotation.
package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metadataKey = "x-api-key"

// Key is a single configured API key and the principal it authenticates.
type Key struct {
	Key   string // the secret presented in the x-api-key header
	Name  string // human-readable principal name (for logging/audit)
	Scope Scope  // read or read-write
}

// principal is the resolved identity behind a validated key.
type principal struct {
	name  string
	scope Scope
}

// Principal is the authenticated identity resolved from a valid API key. It is
// stored on the request context after a successful authorization so downstream
// interceptors (logging, audit) can attribute the RPC to a principal.
type Principal struct {
	Name  string
	Scope Scope
}

// principalContextKey is the context key under which the resolved Principal is
// stored.
type principalContextKey struct{}

// PrincipalFromContext returns the authenticated principal the auth interceptor
// attached to ctx. ok is false when auth is disabled or the RPC was not
// authenticated (no principal was resolved).
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalContextKey{}).(Principal)
	return p, ok
}

// withPrincipal returns a copy of ctx carrying p.
func withPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, p)
}

// wrappedServerStream overrides Context so a stream interceptor can thread a
// derived context (carrying the resolved principal) down to the handler.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

type keyEntry struct {
	key       []byte
	principal principal
}

// keySet is an immutable snapshot of the configured keys. A new snapshot is
// built on every reload and swapped in atomically, so in-flight requests always
// see a consistent set.
type keySet struct {
	entries []keyEntry
}

// enabled reports whether authentication is active. An empty key set disables
// auth entirely (backward-compatible with the historical "empty api key = no
// auth" behaviour).
func (ks *keySet) enabled() bool { return len(ks.entries) > 0 }

// lookup resolves a presented key to its principal. It compares against every
// entry in constant time and never short-circuits, so the response time does
// not reveal which (or whether a) key matched.
func (ks *keySet) lookup(provided []byte) (principal, bool) {
	var found principal
	matched := 0
	for i := range ks.entries {
		if subtle.ConstantTimeCompare(ks.entries[i].key, provided) == 1 {
			found = ks.entries[i].principal
			matched = 1
		}
	}
	return found, matched == 1
}

func buildKeySet(keys []Key) (*keySet, error) {
	seen := make(map[string]string, len(keys))
	entries := make([]keyEntry, 0, len(keys))
	for _, k := range keys {
		if k.Key == "" {
			return nil, fmt.Errorf("api key %q: empty key value", k.Name)
		}
		if prev, dup := seen[k.Key]; dup {
			return nil, fmt.Errorf("duplicate api key value shared by %q and %q", prev, k.Name)
		}
		seen[k.Key] = k.Name
		name := k.Name
		if name == "" {
			name = "unnamed"
		}
		entries = append(entries, keyEntry{
			key:       []byte(k.Key),
			principal: principal{name: name, scope: k.Scope},
		})
	}
	return &keySet{entries: entries}, nil
}

// Authenticator validates API keys and enforces per-RPC scope. It is safe for
// concurrent use, and its key set can be swapped at runtime via Reload to
// support key rotation without a restart. When mutual-TLS is enabled (see
// WithCertAuth) it also accepts a verified client certificate as an alternative
// credential.
type Authenticator struct {
	keys     atomic.Pointer[keySet]
	certAuth bool // when true, a verified client cert authenticates the request
}

// Option configures an Authenticator at construction time.
type Option func(*Authenticator)

// WithCertAuth enables mutual-TLS authentication: a request that carries no
// valid API key but presents a client certificate verified against the server's
// configured client-CA pool is authenticated as the certificate's principal.
// It is off by default, so plain API-key auth is unchanged.
func WithCertAuth(enabled bool) Option {
	return func(a *Authenticator) { a.certAuth = enabled }
}

// New builds an Authenticator from the given keys. Passing no keys — and no
// cert-auth option — yields an authenticator that allows all requests (auth
// disabled).
func New(keys []Key, opts ...Option) (*Authenticator, error) {
	ks, err := buildKeySet(keys)
	if err != nil {
		return nil, err
	}
	a := &Authenticator{}
	a.keys.Store(ks)
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

// Reload atomically replaces the active key set. In-flight requests keep using
// the previous set; requests that arrive after the swap see the new one. It
// returns an error (and leaves the current set untouched) if the new keys are
// invalid.
func (a *Authenticator) Reload(keys []Key) error {
	ks, err := buildKeySet(keys)
	if err != nil {
		return err
	}
	a.keys.Store(ks)
	return nil
}

// Interceptors returns the unary and stream server interceptors that enforce
// authentication and scoping using this Authenticator's current key set.
func (a *Authenticator) Interceptors() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := a.authorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := a.authorize(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedServerStream{ServerStream: ss, ctx: ctx})
	}
	return unary, stream
}

// authorize resolves the request's principal and checks that its scope permits
// fullMethod. A principal is resolved from a valid API key when key auth is
// configured, or — when mutual-TLS is enabled — from a verified client
// certificate as an alternative. On success it returns a context carrying the
// resolved Principal (unchanged when auth is disabled entirely).
//
// Composition: an explicit, valid API key always wins. A presented-but-invalid
// key is rejected outright rather than silently falling back to a certificate.
// Only when no API key is presented does a verified client certificate satisfy
// authentication, so server-only TLS + API keys behaves exactly as before.
func (a *Authenticator) authorize(ctx context.Context, fullMethod string) (context.Context, error) {
	ks := a.keys.Load()
	keyAuth := ks.enabled()
	if !keyAuth && !a.certAuth {
		return ctx, nil // auth disabled
	}

	var resolved principal
	var ok bool

	// Prefer an explicit API key. A presented key must be valid; a missing key
	// is not an error here so a client may instead authenticate by certificate.
	if keyAuth {
		if md, hasMD := metadata.FromIncomingContext(ctx); hasMD {
			if vals := md.Get(metadataKey); len(vals) > 0 {
				resolved, ok = ks.lookup([]byte(vals[0]))
				if !ok {
					return ctx, status.Error(codes.Unauthenticated, "invalid api key")
				}
			}
		}
	}

	// Fall back to a verified client certificate (mutual-TLS).
	if !ok && a.certAuth {
		if cp, certOK := principalFromPeerCert(ctx); certOK {
			resolved, ok = principal{name: cp.Name, scope: cp.Scope}, true
		}
	}

	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "missing api key or client certificate")
	}

	if methodRequiresWrite(fullMethod) && resolved.scope != ScopeReadWrite {
		return ctx, status.Errorf(codes.PermissionDenied,
			"principal %q has read-only scope; %s requires read-write", resolved.name, fullMethod)
	}
	return withPrincipal(ctx, Principal{Name: resolved.name, Scope: resolved.scope}), nil
}
