package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/srjn45/filedbv2/engine"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
	"github.com/srjn45/filedbv2/query"
	"github.com/srjn45/filedbv2/store"
)

// GRPCServer implements pb.FileDBServer.
type GRPCServer struct {
	pb.UnimplementedFileDBServer
	db    *engine.DB
	txMgr *engine.TxManager
}

// NewGRPCServer creates a GRPCServer backed by the given DB. txTimeout bounds
// how long an idle open transaction is retained before it is reaped (0 disables
// expiry); see engine.NewTxManager.
func NewGRPCServer(db *engine.DB, txTimeout time.Duration) *GRPCServer {
	return &GRPCServer{db: db, txMgr: engine.NewTxManager(txTimeout)}
}

// Close releases server-owned background resources (the transaction sweeper).
func (s *GRPCServer) Close() {
	s.txMgr.Close()
}

// txIDFromContext extracts the x-tx-id value from incoming gRPC metadata.
// Returns "" if not present.
func txIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("x-tx-id")
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// ---- Collection management ------------------------------------------------

func (s *GRPCServer) CreateCollection(_ context.Context, req *pb.CreateCollectionRequest) (*pb.CreateCollectionResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name required")
	}
	if _, err := s.db.CreateCollection(req.Name); err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "%v", err)
	}
	return &pb.CreateCollectionResponse{
		Name:      req.Name,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func (s *GRPCServer) DropCollection(_ context.Context, req *pb.DropCollectionRequest) (*pb.DropCollectionResponse, error) {
	if err := s.db.DropCollection(req.Name); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &pb.DropCollectionResponse{Ok: true}, nil
}

func (s *GRPCServer) ListCollections(_ context.Context, _ *pb.ListCollectionsRequest) (*pb.ListCollectionsResponse, error) {
	return &pb.ListCollectionsResponse{Names: s.db.ListCollections()}, nil
}

// ---- CRUD -----------------------------------------------------------------

func (s *GRPCServer) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if txID := txIDFromContext(ctx); txID != "" {
		tx, ok := s.txMgr.Get(txID)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "tx %q not found", txID)
		}
		if tx.Collection != req.Collection {
			return nil, status.Errorf(codes.InvalidArgument, "tx %q is bound to collection %q, not %q", txID, tx.Collection, req.Collection)
		}
		id := col.ReserveID()
		tx.StageInsert(id, req.Data.AsMap())
		return &pb.InsertResponse{Id: id, DateAdded: time.Now().UTC().Format(time.RFC3339)}, nil
	}

	data := req.Data.AsMap()
	id, ts, err := col.Insert(data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert: %v", err)
	}
	return &pb.InsertResponse{Id: id, DateAdded: ts.Format(time.RFC3339)}, nil
}

func (s *GRPCServer) InsertMany(_ context.Context, req *pb.InsertManyRequest) (*pb.InsertManyResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	ids := make([]uint64, 0, len(req.Records))
	for _, r := range req.Records {
		id, _, err := col.Insert(r.AsMap())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "insertMany: %v", err)
		}
		ids = append(ids, id)
	}
	return &pb.InsertManyResponse{Ids: ids}, nil
}

func (s *GRPCServer) FindById(_ context.Context, req *pb.FindByIdRequest) (*pb.FindResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection: %v", err)
	}
	data, ts, err := col.FindByID(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	rec, err := toProtoRecord(req.Id, data, ts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.FindResponse{Record: rec}, nil
}

func (s *GRPCServer) Find(req *pb.FindRequest, stream pb.FileDB_FindServer) error {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	f, err := protoFilterToQuery(req.Filter)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}

	// The engine honours order/offset/limit and streams matches as it reads,
	// so a limited query never materialises the whole collection. stream.Context()
	// is threaded through so a client cancelling the RPC stops server-side work.
	opts := engine.ScanOptions{
		Filter:     f,
		Limit:      int(req.Limit),
		Offset:     int(req.Offset),
		OrderBy:    req.OrderBy,
		Descending: req.Descending,
	}
	err = col.ScanStream(stream.Context(), opts, func(r engine.ScanResult) error {
		rec, err := toProtoRecord(r.ID, r.Data, r.Ts)
		if err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		return stream.Send(&pb.FindResponse{Record: rec})
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		if _, ok := status.FromError(err); ok {
			return err // already a gRPC status (stream.Send / marshal error) — preserve its code
		}
		return status.Errorf(codes.Internal, "find: %v", err)
	}
	return nil
}

func (s *GRPCServer) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if txID := txIDFromContext(ctx); txID != "" {
		tx, ok := s.txMgr.Get(txID)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "tx %q not found", txID)
		}
		if tx.Collection != req.Collection {
			return nil, status.Errorf(codes.InvalidArgument, "tx %q is bound to collection %q, not %q", txID, tx.Collection, req.Collection)
		}
		tx.StageUpdate(req.Id, req.Data.AsMap())
		return &pb.UpdateResponse{Id: req.Id, DateModified: time.Now().UTC().Format(time.RFC3339)}, nil
	}

	ts, err := col.Update(req.Id, req.Data.AsMap())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &pb.UpdateResponse{Id: req.Id, DateModified: ts.Format(time.RFC3339)}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if txID := txIDFromContext(ctx); txID != "" {
		tx, ok := s.txMgr.Get(txID)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "tx %q not found", txID)
		}
		if tx.Collection != req.Collection {
			return nil, status.Errorf(codes.InvalidArgument, "tx %q is bound to collection %q, not %q", txID, tx.Collection, req.Collection)
		}
		tx.StageDelete(req.Id)
		return &pb.DeleteResponse{Ok: true}, nil
	}

	if err := col.Delete(req.Id); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// ---- Secondary indexes ----------------------------------------------------

func (s *GRPCServer) EnsureIndex(_ context.Context, req *pb.EnsureIndexRequest) (*pb.EnsureIndexResponse, error) {
	if req.Field == "" {
		return nil, status.Error(codes.InvalidArgument, "field required")
	}
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if err := col.EnsureIndex(req.Field); err != nil {
		return nil, status.Errorf(codes.Internal, "ensure index: %v", err)
	}
	return &pb.EnsureIndexResponse{Collection: req.Collection, Field: req.Field}, nil
}

func (s *GRPCServer) DropIndex(_ context.Context, req *pb.DropIndexRequest) (*pb.DropIndexResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if err := col.DropIndex(req.Field); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &pb.DropIndexResponse{Ok: true}, nil
}

func (s *GRPCServer) ListIndexes(_ context.Context, req *pb.ListIndexesRequest) (*pb.ListIndexesResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &pb.ListIndexesResponse{Fields: col.ListIndexes()}, nil
}

// ---- Watch ----------------------------------------------------------------

func (s *GRPCServer) Watch(req *pb.WatchRequest, stream pb.FileDB_WatchServer) error {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	f, err := protoFilterToQuery(req.Filter)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}

	_, ch, cancel := col.Subscribe()
	defer cancel()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			// An overflow sentinel carries no record and bypasses the filter —
			// the client must always learn it may have missed matching events.
			if ev.Op == engine.OpOverflow {
				if err := stream.Send(&pb.WatchEvent{
					Op:         pb.WatchOp_OVERFLOW,
					Collection: req.Collection,
					Ts:         timestamppb.New(ev.Ts),
				}); err != nil {
					return err
				}
				continue
			}
			if !f.Match(ev.Data) {
				continue
			}
			var op pb.WatchOp
			switch ev.Op {
			case store.OpInsert:
				op = pb.WatchOp_INSERTED
			case store.OpUpdate:
				op = pb.WatchOp_UPDATED
			case store.OpDelete:
				op = pb.WatchOp_DELETED
			}
			rec, _ := toProtoRecord(ev.ID, ev.Data, ev.Ts)
			if err := stream.Send(&pb.WatchEvent{
				Op:         op,
				Collection: req.Collection,
				Record:     rec,
				Ts:         timestamppb.New(ev.Ts),
			}); err != nil {
				return err
			}
		}
	}
}

// ---- Stats ----------------------------------------------------------------

func (s *GRPCServer) CollectionStats(_ context.Context, req *pb.CollectionStatsRequest) (*pb.CollectionStatsResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	st := col.Stats()
	return &pb.CollectionStatsResponse{
		Collection:   st.Name,
		RecordCount:  st.RecordCount,
		SegmentCount: st.SegmentCount,
		DirtyEntries: st.DirtyEntries,
		SizeBytes:    st.SizeBytes,
	}, nil
}

// ---- Admin ----------------------------------------------------------------

func (s *GRPCServer) Compact(_ context.Context, req *pb.CompactRequest) (*pb.CompactResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection required")
	}
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if err := col.CompactNow(); err != nil {
		return nil, status.Errorf(codes.Internal, "compact failed: %v", err)
	}
	return &pb.CompactResponse{Ok: true}, nil
}

// Snapshot streams a consistent gzip-compressed tar archive of the whole
// database to the client. The tar body is buffered so each streamed message
// carries a sizeable chunk rather than one message per tiny gzip/tar write.
func (s *GRPCServer) Snapshot(_ *pb.SnapshotRequest, stream pb.FileDB_SnapshotServer) error {
	bw := bufio.NewWriterSize(&snapshotChunkWriter{stream: stream}, 64*1024)
	if err := s.db.SnapshotTo(bw); err != nil {
		return status.Errorf(codes.Internal, "snapshot: %v", err)
	}
	if err := bw.Flush(); err != nil {
		return status.Errorf(codes.Internal, "snapshot flush: %v", err)
	}
	return nil
}

// snapshotChunkWriter adapts the server stream to io.Writer, sending each
// buffered block as a SnapshotChunk. stream.Send copies the bytes during
// marshaling, so reusing the buffer after Write returns is safe.
type snapshotChunkWriter struct {
	stream pb.FileDB_SnapshotServer
}

func (w *snapshotChunkWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&pb.SnapshotChunk{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// ---- Transactions ---------------------------------------------------------

func (s *GRPCServer) BeginTx(_ context.Context, req *pb.BeginTxRequest) (*pb.BeginTxResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection required")
	}
	if _, err := s.db.Collection(req.Collection); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	txID := s.txMgr.Begin(req.Collection)
	return &pb.BeginTxResponse{TxId: txID}, nil
}

func (s *GRPCServer) CommitTx(_ context.Context, req *pb.CommitTxRequest) (*pb.CommitTxResponse, error) {
	tx, ok := s.txMgr.Get(req.TxId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tx %q not found", req.TxId)
	}
	col, err := s.db.Collection(tx.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	ops := tx.Snapshot()
	s.txMgr.Remove(req.TxId)
	if err := col.CommitTx(ops); err != nil {
		return nil, status.Errorf(codes.Aborted, "commit failed: %v", err)
	}
	return &pb.CommitTxResponse{Ok: true}, nil
}

func (s *GRPCServer) RollbackTx(_ context.Context, req *pb.RollbackTxRequest) (*pb.RollbackTxResponse, error) {
	if _, ok := s.txMgr.Get(req.TxId); !ok {
		return nil, status.Errorf(codes.NotFound, "tx %q not found", req.TxId)
	}
	s.txMgr.Remove(req.TxId)
	return &pb.RollbackTxResponse{Ok: true}, nil
}

// ---- Helpers --------------------------------------------------------------

func toProtoRecord(id uint64, data map[string]any, ts time.Time) (*pb.Record, error) {
	s, err := structpb.NewStruct(data)
	if err != nil {
		return nil, fmt.Errorf("toProtoRecord: %w", err)
	}
	return &pb.Record{
		Id:           id,
		Data:         s,
		DateAdded:    timestamppb.New(ts),
		DateModified: timestamppb.New(ts),
	}, nil
}

func protoFilterToQuery(f *pb.Filter) (query.Filter, error) {
	if f == nil {
		return query.MatchAll, nil
	}
	switch k := f.Kind.(type) {
	case *pb.Filter_Field:
		return &query.FieldFilter{
			Field: k.Field.Field,
			Op:    protoOpToQuery(k.Field.Op),
			Value: k.Field.Value,
		}, nil
	case *pb.Filter_And:
		sub := make([]query.Filter, 0, len(k.And.Filters))
		for _, ff := range k.And.Filters {
			qf, err := protoFilterToQuery(ff)
			if err != nil {
				return nil, err
			}
			sub = append(sub, qf)
		}
		return &query.AndFilter{Filters: sub}, nil
	case *pb.Filter_Or:
		sub := make([]query.Filter, 0, len(k.Or.Filters))
		for _, ff := range k.Or.Filters {
			qf, err := protoFilterToQuery(ff)
			if err != nil {
				return nil, err
			}
			sub = append(sub, qf)
		}
		return &query.OrFilter{Filters: sub}, nil
	}
	return query.MatchAll, nil
}

func protoOpToQuery(op pb.FilterOp) query.Op {
	switch op {
	case pb.FilterOp_EQ:
		return query.OpEq
	case pb.FilterOp_NEQ:
		return query.OpNeq
	case pb.FilterOp_GT:
		return query.OpGt
	case pb.FilterOp_GTE:
		return query.OpGte
	case pb.FilterOp_LT:
		return query.OpLt
	case pb.FilterOp_LTE:
		return query.OpLte
	case pb.FilterOp_CONTAINS:
		return query.OpContains
	case pb.FilterOp_REGEX:
		return query.OpRegex
	default:
		return query.OpEq
	}
}
