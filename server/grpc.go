package server

import (
	"context"
	"fmt"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/srjn45/filedbv2/internal/engine"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
	"github.com/srjn45/filedbv2/internal/query"
	"github.com/srjn45/filedbv2/internal/store"
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

	results, err := col.Scan(f)
	if err != nil {
		return status.Errorf(codes.Internal, "scan: %v", err)
	}

	// Sort if requested.
	if req.OrderBy != "" {
		sort.Slice(results, func(i, j int) bool {
			vi := fmt.Sprintf("%v", results[i].Data[req.OrderBy])
			vj := fmt.Sprintf("%v", results[j].Data[req.OrderBy])
			// Numeric comparison when both values parse as float64.
			fi, ei := results[i].Data[req.OrderBy].(float64)
			fj, ej := results[j].Data[req.OrderBy].(float64)
			if ei && ej {
				if req.Descending {
					return fi > fj
				}
				return fi < fj
			}
			if req.Descending {
				return vi > vj
			}
			return vi < vj
		})
	}

	// Apply offset and limit.
	start := int(req.Offset)
	if start > len(results) {
		start = len(results)
	}
	results = results[start:]
	if req.Limit > 0 && int(req.Limit) < len(results) {
		results = results[:req.Limit]
	}

	for _, r := range results {
		rec, err := toProtoRecord(r.ID, r.Data, r.Ts)
		if err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		if err := stream.Send(&pb.FindResponse{Record: rec}); err != nil {
			return err
		}
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
