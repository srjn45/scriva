package auth

import (
	"fmt"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// This file is the single authorization matrix for the service (ratified
// design D3/D4). Every RPC must have exactly one entry in methodPolicies;
// anything else — an unlisted method, a new RPC nobody classified, another
// service entirely — is denied to every principal that is not a database
// administrator (fail closed). A test walks the generated service descriptor
// and fails when the proto gains a method with no entry here.
//
// The model: a principal has a scope (read | read-write) and an optional
// collection allow-list. From those it holds, per collection C, read(C) and
// write(C), and — only when it has NO allow-list — the database-level
// capabilities db:read and db:admin. An allow-list therefore removes every
// database-level capability.

// Access is the capability class an RPC requires.
type Access int

const (
	// AccessDenied is the zero value and the answer for an unknown method.
	// Only a database administrator (or auth-disabled mode) may call it.
	AccessDenied Access = iota
	// AccessCollectionRead requires read(C) for every collection the request
	// names.
	AccessCollectionRead
	// AccessCollectionWrite requires write(C) for every collection the
	// request names.
	AccessCollectionWrite
	// AccessCreate is CreateCollection: db:admin, or write(name) when name is
	// literally in the principal's allow-list.
	AccessCreate
	// AccessDBAdmin requires db:admin: scope read-write and no allow-list.
	AccessDBAdmin
	// AccessDBRead requires db:read: no allow-list (any scope).
	AccessDBRead
	// AccessFiltered is any authenticated principal; the handler confines the
	// result to the principal's allow-list (ListCollections).
	AccessFiltered
	// AccessHandleWrite requires scope read-write. The request names a
	// transaction handle rather than a collection; the handler enforces that
	// the caller owns the handle.
	AccessHandleWrite
	// AccessAuthenticated is any authenticated principal and needs no
	// further capability (XTxStatus, gRPC health probes).
	AccessAuthenticated
)

var accessNames = map[Access]string{
	AccessDenied:          "denied",
	AccessCollectionRead:  "collection-read",
	AccessCollectionWrite: "collection-write",
	AccessCreate:          "create",
	AccessDBAdmin:         "db-admin",
	AccessDBRead:          "db-read",
	AccessFiltered:        "filtered",
	AccessHandleWrite:     "handle-write",
	AccessAuthenticated:   "authenticated",
}

func (a Access) String() string {
	if s, ok := accessNames[a]; ok {
		return s
	}
	return fmt.Sprintf("Access(%d)", int(a))
}

const svc = "/scriva.v1.Scriva/"

// methodPolicies maps full gRPC method names to the capability they require.
var methodPolicies = map[string]Access{
	// Collection management.
	svc + "CreateCollection": AccessCreate,
	svc + "DropCollection":   AccessDBAdmin,
	svc + "ListCollections":  AccessFiltered,

	// Record writes.
	svc + "Insert":      AccessCollectionWrite,
	svc + "InsertMany":  AccessCollectionWrite,
	svc + "Update":      AccessCollectionWrite,
	svc + "Delete":      AccessCollectionWrite,
	svc + "Upsert":      AccessCollectionWrite,
	svc + "UpdateByKey": AccessCollectionWrite,
	svc + "DeleteByKey": AccessCollectionWrite,
	svc + "UpdateIfRev": AccessCollectionWrite,

	// Record reads.
	svc + "FindById":        AccessCollectionRead,
	svc + "Find":            AccessCollectionRead,
	svc + "FindByKey":       AccessCollectionRead,
	svc + "Aggregate":       AccessCollectionRead,
	svc + "Watch":           AccessCollectionRead,
	svc + "CollectionStats": AccessCollectionRead,
	svc + "ListIndexes":     AccessCollectionRead,

	// Index and maintenance writes.
	svc + "EnsureIndex": AccessCollectionWrite,
	svc + "DropIndex":   AccessCollectionWrite,
	svc + "Compact":     AccessCollectionWrite,

	// Single-collection transactions: Begin names a collection; Commit and
	// Rollback name only the handle.
	svc + "BeginTx":    AccessCollectionWrite,
	svc + "CommitTx":   AccessHandleWrite,
	svc + "RollbackTx": AccessHandleWrite,

	// Cross-collection transactions. Begin names every participant; Stage and
	// Get name one collection plus the handle (ownership is a handler check).
	// GetXTx is a write on purpose: a transactional read joins the read set.
	svc + "BeginXTx":    AccessCollectionWrite,
	svc + "StageXTx":    AccessCollectionWrite,
	svc + "GetXTx":      AccessCollectionWrite,
	svc + "CommitXTx":   AccessHandleWrite,
	svc + "RollbackXTx": AccessHandleWrite,
	// XTxStatus returns only a state and a tx id — no collection or data.
	svc + "XTxStatus": AccessAuthenticated,

	// Whole-database surfaces.
	svc + "Snapshot":          AccessDBRead,
	svc + "Replicate":         AccessDBRead,
	svc + "ReplicationStatus": AccessDBRead,
	svc + "Promote":           AccessDBAdmin,

	// gRPC health probes: any authenticated principal (no principal when
	// authentication is disabled).
	"/grpc.health.v1.Health/Check": AccessAuthenticated,
	"/grpc.health.v1.Health/Watch": AccessAuthenticated,
	"/grpc.health.v1.Health/List":  AccessAuthenticated,
}

// PolicyFor returns the capability class for a full method name. ok is false
// for a method with no entry, which is treated as AccessDenied.
func PolicyFor(fullMethod string) (Access, bool) {
	a, ok := methodPolicies[fullMethod]
	return a, ok
}

// PolicyMethods returns every classified full method name, sorted.
func PolicyMethods() []string {
	out := make([]string, 0, len(methodPolicies))
	for m := range methodPolicies {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// restricted reports whether p carries a collection allow-list.
func (p principal) restricted() bool { return p.collections != nil }

// dbCapable reports whether p holds database-level capabilities, i.e. has no
// allow-list. Auth-disabled mode is an unrestricted read-write principal.
func (p principal) dbCapable() bool { return p.open || !p.restricted() }

func (p principal) canWrite() bool { return p.open || p.scope == ScopeReadWrite }

func denied(format string, args ...any) error {
	return status.Errorf(codes.PermissionDenied, format, args...)
}

// authorizeMethod performs every check that does not need the request
// message: scope and database-level capabilities.
func (p principal) authorizeMethod(fullMethod string) error {
	access, known := methodPolicies[fullMethod]
	if !known {
		access = AccessDenied
	}
	switch access {
	case AccessCollectionRead, AccessFiltered, AccessAuthenticated:
		return nil
	case AccessCollectionWrite, AccessCreate, AccessHandleWrite:
		if !p.canWrite() {
			return denied("principal %q has read-only scope; %s requires read-write", p.name, fullMethod)
		}
		return nil
	case AccessDBRead:
		if !p.dbCapable() {
			return denied("principal %q is restricted to a collection allow-list; %s requires an unrestricted principal", p.name, fullMethod)
		}
		return nil
	case AccessDBAdmin:
		if !p.canWrite() {
			return denied("principal %q has read-only scope; %s requires read-write", p.name, fullMethod)
		}
		if !p.dbCapable() {
			return denied("principal %q is restricted to a collection allow-list; %s requires an unrestricted principal", p.name, fullMethod)
		}
		return nil
	default: // AccessDenied: unclassified method, db:admin only.
		if !p.open && !(p.scope == ScopeReadWrite && p.dbCapable()) {
			return denied("principal %q is not permitted to call %s", p.name, fullMethod)
		}
		return nil
	}
}

// needsRequestCheck reports whether authorizeRequest has anything to enforce
// for fullMethod (so a stream can skip wrapping its first message).
func needsRequestCheck(fullMethod string) bool {
	switch methodPolicies[fullMethod] {
	case AccessCollectionRead, AccessCollectionWrite, AccessCreate:
		return true
	}
	return false
}

// authorizeRequest enforces the collection allow-list against the request
// message: every collection the request names must be in the list. A
// principal without an allow-list passes unchanged. The error never says
// whether a collection exists.
func (p principal) authorizeRequest(fullMethod string, req any) error {
	if !p.restricted() || p.open {
		return nil
	}
	access, known := methodPolicies[fullMethod]
	if !known {
		return nil // already refused by authorizeMethod
	}
	if access != AccessCollectionRead && access != AccessCollectionWrite && access != AccessCreate {
		return nil
	}
	names, ok := requestCollections(req)
	if !ok {
		// A collection-class RPC whose request we cannot read fails closed.
		return denied("principal %q is not permitted to access this collection", p.name)
	}
	if len(names) == 0 {
		// The request names no collection at all (e.g. an empty name): there is
		// nothing inside the allow-list to authorize it against.
		return denied("principal %q is not permitted to access this collection", p.name)
	}
	for _, name := range names {
		if !p.allowsCollection(name) {
			return denied("principal %q is not permitted to access collection %q", p.name, name)
		}
	}
	return nil
}

// collectionFieldNames are the request fields that carry a collection name.
var collectionFieldNames = []protoreflect.Name{"collection", "collections", "name"}

// requestCollections extracts every collection a request names. For generated
// proto messages it reads the collection-bearing fields by reflection, so a new
// field spelled collection/collections/name is covered without a code change.
// Other request types fall back to the GetCollection/GetCollections accessors.
// ok is false when the request is of no readable type.
func requestCollections(req any) (names []string, ok bool) {
	if m, isMsg := req.(proto.Message); isMsg && m != nil {
		rm := m.ProtoReflect()
		if !rm.IsValid() {
			return nil, false
		}
		fields := rm.Descriptor().Fields()
		for _, fname := range collectionFieldNames {
			fd := fields.ByName(fname)
			if fd == nil || fd.Kind() != protoreflect.StringKind {
				continue
			}
			v := rm.Get(fd)
			if fd.IsList() {
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					names = appendName(names, l.Get(i).String())
				}
			} else {
				names = appendName(names, v.String())
			}
		}
		return names, true
	}
	found := false
	if ms, isMulti := req.(multiCollectionScoped); isMulti {
		for _, n := range ms.GetCollections() {
			names = appendName(names, n)
		}
		found = true
	}
	if cs, isOne := req.(collectionScoped); isOne {
		names = appendName(names, cs.GetCollection())
		found = true
	}
	return names, found
}

func appendName(names []string, n string) []string {
	if n == "" {
		return names
	}
	return append(names, n)
}
