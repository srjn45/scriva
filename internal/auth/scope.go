package auth

import (
	"fmt"
	"strings"
)

// Scope describes what a principal (API key) is permitted to do.
type Scope int

const (
	// ScopeRead permits only non-mutating RPCs (reads, lists, stats, watch,
	// snapshot). ScopeRead is the zero value so an unspecified scope is the
	// least-privileged one.
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

// writeMethods is the set of RPCs (short method names) that mutate state and
// therefore require ScopeReadWrite. Everything not listed here is treated as a
// read. Unknown methods are treated as writes (fail-safe: a read-only key can
// never reach an unclassified RPC).
var writeMethods = map[string]bool{
	"CreateCollection": true,
	"DropCollection":   true,
	"Insert":           true,
	"InsertMany":       true,
	"Update":           true,
	"Delete":           true,
	"EnsureIndex":      true,
	"DropIndex":        true,
	"BeginTx":          true,
	"CommitTx":         true,
	"RollbackTx":       true,
	"Compact":          true,
}

// readMethods is the set of RPCs known to be non-mutating. It exists only so
// that methodRequiresWrite can distinguish a known read from an unknown method
// (which is denied to read-only keys as a precaution).
var readMethods = map[string]bool{
	"ListCollections":   true,
	"FindById":          true,
	"Find":              true,
	"ListIndexes":       true,
	"Watch":             true,
	"CollectionStats":   true,
	"Snapshot":          true,
	"Replicate":         true,
	"ReplicationStatus": true,
}

// methodRequiresWrite reports whether the given gRPC full method name
// ("/filedb.v1.FileDB/Insert") requires ScopeReadWrite. Unknown methods require
// write access so a read-only key is never silently allowed through a new RPC.
func methodRequiresWrite(fullMethod string) bool {
	name := fullMethod
	if i := strings.LastIndexByte(fullMethod, '/'); i >= 0 {
		name = fullMethod[i+1:]
	}
	if writeMethods[name] {
		return true
	}
	if readMethods[name] {
		return false
	}
	return true
}
