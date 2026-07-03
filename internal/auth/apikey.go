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
// support key rotation without a restart.
type Authenticator struct {
	keys atomic.Pointer[keySet]
}

// New builds an Authenticator from the given keys. Passing no keys yields an
// authenticator that allows all requests (auth disabled).
func New(keys []Key) (*Authenticator, error) {
	ks, err := buildKeySet(keys)
	if err != nil {
		return nil, err
	}
	a := &Authenticator{}
	a.keys.Store(ks)
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
		if err := a.authorize(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := a.authorize(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}

// authorize validates the request's API key and checks that the resolved
// principal's scope permits fullMethod.
func (a *Authenticator) authorize(ctx context.Context, fullMethod string) error {
	ks := a.keys.Load()
	if !ks.enabled() {
		return nil // auth disabled
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get(metadataKey)
	if len(vals) == 0 {
		return status.Errorf(codes.Unauthenticated, "missing %s", metadataKey)
	}

	p, ok := ks.lookup([]byte(vals[0]))
	if !ok {
		return status.Error(codes.Unauthenticated, "invalid api key")
	}

	if methodRequiresWrite(fullMethod) && p.scope != ScopeReadWrite {
		return status.Errorf(codes.PermissionDenied,
			"api key %q has read-only scope; %s requires read-write", p.name, fullMethod)
	}
	return nil
}
