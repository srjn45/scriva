package server

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/internal/auth"
)

// callerID is the identity a transaction handle is bound to: the credential
// that created it. It is "" when authentication is disabled, so with auth off
// every caller owns every handle (there is nobody to confine it from).
func callerID(ctx context.Context) string {
	if p, ok := auth.PrincipalFromContext(ctx); ok {
		return p.ID
	}
	return ""
}

// handleOwners records which credential created each transaction handle (both
// single-collection and cross-collection ids). Handle ids are 128-bit random,
// but unguessability is not the access control: a second principal that learns
// an id must still be refused. The table is advisory memory beside the
// engine's own handle registries, so entries whose handle has gone are swept
// lazily when the table has doubled since the last sweep.
type handleOwners struct {
	mu      sync.Mutex
	owner   map[string]string
	sweepAt int
}

const handleOwnersMinSweep = 1024

func newHandleOwners() *handleOwners {
	return &handleOwners{owner: make(map[string]string), sweepAt: handleOwnersMinSweep}
}

// bind records id as owned by owner. exists reports whether a previously
// recorded handle is still live; it is called (without the lock being held by
// exists' own callers) only during a sweep.
func (h *handleOwners) bind(id, owner string, exists func(string) bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.owner[id] = owner
	if len(h.owner) < h.sweepAt {
		return
	}
	for k := range h.owner {
		if !exists(k) {
			delete(h.owner, k)
		}
	}
	h.sweepAt = 2 * len(h.owner)
	if h.sweepAt < handleOwnersMinSweep {
		h.sweepAt = handleOwnersMinSweep
	}
}

func (h *handleOwners) forget(id string) {
	h.mu.Lock()
	delete(h.owner, id)
	h.mu.Unlock()
}

// check returns PermissionDenied unless the caller owns the handle. A handle
// with no recorded owner belongs to the unauthenticated identity "".
func (h *handleOwners) check(ctx context.Context, id string) error {
	h.mu.Lock()
	owner := h.owner[id]
	h.mu.Unlock()
	if owner != callerID(ctx) {
		return status.Error(codes.PermissionDenied, "transaction handle belongs to another principal")
	}
	return nil
}
