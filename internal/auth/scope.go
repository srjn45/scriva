package auth

import (
	"fmt"
	"strings"
)

// Scope describes what a principal (API key) is permitted to do.
type Scope int

const (
	// ScopeRead permits only non-mutating RPCs (reads, lists, stats, watch).
	// ScopeRead is the zero value so an unspecified scope is the
	// least-privileged one. The per-RPC matrix lives in policy.go.
	ScopeRead Scope = iota
	// ScopeReadWrite permits every RPC, including mutations.
	ScopeReadWrite
)

// String returns the canonical config spelling of the scope.
func (s Scope) String() string {
	switch s {
	case ScopeRead:
		return "read"
	case ScopeReadWrite:
		return "read-write"
	default:
		return fmt.Sprintf("Scope(%d)", int(s))
	}
}

// ParseScope converts a config string into a Scope. It accepts "read" (or "ro",
// "readonly") and "read-write" (or "rw", "readwrite", "write"). An empty string
// defaults to the least-privileged ScopeRead.
func ParseScope(s string) (Scope, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "read", "ro", "readonly", "read-only":
		return ScopeRead, nil
	case "read-write", "readwrite", "rw", "write":
		return ScopeReadWrite, nil
	default:
		return ScopeRead, fmt.Errorf("unknown scope %q (want \"read\" or \"read-write\")", s)
	}
}
