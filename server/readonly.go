package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/srjn45/filedbv2/internal/pb/proto"
)

// ReadOnlyReplicaMessage is the status message a follower returns when a client
// attempts a write against it. Clients (and the follower's own R1 apply loop,
// which never routes through the public API) can match on it to redirect writes
// to the leader.
const ReadOnlyReplicaMessage = "read-only replica; write to the leader"

// writeMethods is the set of gRPC full-method names a read-only follower refuses
// (R2). It covers every RPC that mutates durable state — data writes, keyed and
// compare-and-swap writes, collection/index schema changes, transaction control,
// and operator-triggered compaction — so a follower can only ever be read from.
// Read RPCs (Find/FindById/FindByKey/Aggregate/CollectionStats/ListCollections/
// ListIndexes/Watch), the replication feed, and Snapshot are intentionally
// absent: a follower serves them from its applied state.
var writeMethods = map[string]struct{}{
	pb.FileDB_CreateCollection_FullMethodName: {},
	pb.FileDB_DropCollection_FullMethodName:   {},
	pb.FileDB_Insert_FullMethodName:           {},
	pb.FileDB_InsertMany_FullMethodName:       {},
	pb.FileDB_Update_FullMethodName:           {},
	pb.FileDB_Delete_FullMethodName:           {},
	pb.FileDB_Upsert_FullMethodName:           {},
	pb.FileDB_UpdateByKey_FullMethodName:      {},
	pb.FileDB_DeleteByKey_FullMethodName:      {},
	pb.FileDB_UpdateIfRev_FullMethodName:      {},
	pb.FileDB_EnsureIndex_FullMethodName:      {},
	pb.FileDB_DropIndex_FullMethodName:        {},
	pb.FileDB_BeginTx_FullMethodName:          {},
	pb.FileDB_CommitTx_FullMethodName:         {},
	pb.FileDB_RollbackTx_FullMethodName:       {},
	pb.FileDB_Compact_FullMethodName:          {},
}

// isWriteMethod reports whether fullMethod mutates durable state and must be
// refused on a read-only follower.
func isWriteMethod(fullMethod string) bool {
	_, ok := writeMethods[fullMethod]
	return ok
}

// ReadOnlyInterceptors returns a unary and a stream interceptor that refuse every
// write RPC with FailedPrecondition and ReadOnlyReplicaMessage. They are wired
// only when the node runs as a follower (server started with --replicate-from),
// so installing them is itself the read-only signal — there is no per-call role
// check. Reads fall straight through to the handler. Centralising the guard here
// (rather than sprinkling a check into each write handler) keeps the follower's
// read-only contract in one auditable place, keyed on the generated method-name
// constants so a newly added write RPC is a one-line addition to writeMethods.
func ReadOnlyInterceptors() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isWriteMethod(info.FullMethod) {
			return nil, status.Error(codes.FailedPrecondition, ReadOnlyReplicaMessage)
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isWriteMethod(info.FullMethod) {
			return status.Error(codes.FailedPrecondition, ReadOnlyReplicaMessage)
		}
		return handler(srv, ss)
	}
	return unary, stream
}
