package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
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
// absent: a follower serves them from its applied state. Promote (R3) is also
// intentionally absent — it is the one admin RPC that must reach a follower, so
// an operator can flip it to leader.
var writeMethods = map[string]struct{}{
	pb.Scriva_CreateCollection_FullMethodName: {},
	pb.Scriva_DropCollection_FullMethodName:   {},
	pb.Scriva_Insert_FullMethodName:           {},
	pb.Scriva_InsertMany_FullMethodName:       {},
	pb.Scriva_Update_FullMethodName:           {},
	pb.Scriva_Delete_FullMethodName:           {},
	pb.Scriva_Upsert_FullMethodName:           {},
	pb.Scriva_UpdateByKey_FullMethodName:      {},
	pb.Scriva_DeleteByKey_FullMethodName:      {},
	pb.Scriva_UpdateIfRev_FullMethodName:      {},
	pb.Scriva_EnsureIndex_FullMethodName:      {},
	pb.Scriva_DropIndex_FullMethodName:        {},
	pb.Scriva_BeginTx_FullMethodName:          {},
	pb.Scriva_CommitTx_FullMethodName:         {},
	pb.Scriva_RollbackTx_FullMethodName:       {},
	pb.Scriva_Compact_FullMethodName:          {},
}

// isWriteMethod reports whether fullMethod mutates durable state and must be
// refused on a read-only follower.
func isWriteMethod(fullMethod string) bool {
	_, ok := writeMethods[fullMethod]
	return ok
}

// ReadOnlyInterceptors returns a unary and a stream interceptor that refuse every
// write RPC with FailedPrecondition and ReadOnlyReplicaMessage while the node is
// a follower. They are wired when the server starts in --replicate-from mode, but
// the guard is *dynamic*: it consults db.IsFollower() on every call, so a Promote
// (R3) that flips the node to leader lifts the guard at runtime without a restart.
// Reads fall straight through to the handler; once promoted, writes do too.
// Centralising the guard here (rather than sprinkling a check into each write
// handler) keeps the follower's read-only contract in one auditable place, keyed
// on the generated method-name constants so a newly added write RPC is a one-line
// addition to writeMethods.
func ReadOnlyInterceptors(db *engine.DB) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if db.IsFollower() && isWriteMethod(info.FullMethod) {
			return nil, status.Error(codes.FailedPrecondition, ReadOnlyReplicaMessage)
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if db.IsFollower() && isWriteMethod(info.FullMethod) {
			return status.Error(codes.FailedPrecondition, ReadOnlyReplicaMessage)
		}
		return handler(srv, ss)
	}
	return unary, stream
}
