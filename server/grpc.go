package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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

	// Slow-query observability (O5). All optional: the zero value disables both
	// the metric and the log so the embedded/default path is unchanged.
	logger    *slog.Logger                             // nil = no slow-query log
	slowQuery time.Duration                            // 0 = slow-query log disabled
	onScan    func(collection string, rowsScanned int) // nil = no scan metric
}

// GRPCOption configures optional GRPCServer behaviour.
type GRPCOption func(*GRPCServer)

// WithSlowQueryLog enables the slow-query log: any Find whose wall-clock
// duration reaches threshold is logged at WARN via logger. A zero threshold
// leaves the log disabled.
func WithSlowQueryLog(logger *slog.Logger, threshold time.Duration) GRPCOption {
	return func(s *GRPCServer) {
		s.logger = logger
		s.slowQuery = threshold
	}
}

// WithScanObserver registers a hook invoked once per completed Find with the
// number of rows the scan examined, so the server layer can feed the metrics
// histogram without the engine importing a metrics package.
func WithScanObserver(fn func(collection string, rowsScanned int)) GRPCOption {
	return func(s *GRPCServer) { s.onScan = fn }
}

// NewGRPCServer creates a GRPCServer backed by the given DB. txTimeout bounds
// how long an idle open transaction is retained before it is reaped (0 disables
// expiry); see engine.NewTxManager.
func NewGRPCServer(db *engine.DB, txTimeout time.Duration, opts ...GRPCOption) *GRPCServer {
	s := &GRPCServer{db: db, txMgr: engine.NewTxManager(txTimeout)}
	for _, opt := range opts {
		opt(s)
	}
	return s
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
	if req.DefaultTtlSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "default_ttl_seconds must not be negative")
	}
	if req.DefaultTtlSeconds > 0 {
		ttl := time.Duration(req.DefaultTtlSeconds) * time.Second
		if _, err := s.db.CreateCollectionWithDefaultTTL(req.Name, ttl); err != nil {
			return nil, status.Errorf(codes.AlreadyExists, "%v", err)
		}
	} else if _, err := s.db.CreateCollection(req.Name); err != nil {
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
	if req.TtlSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "ttl_seconds must not be negative")
	}
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	// Keyed Create (N1): a caller-supplied key routes to the engine's
	// InsertWithKey, which rejects a duplicate key with ALREADY_EXISTS. Keyed
	// insert does not participate in transactions or per-record TTL.
	if req.Key != "" {
		if txIDFromContext(ctx) != "" {
			return nil, status.Error(codes.InvalidArgument, "keyed insert is not supported inside a transaction")
		}
		if req.TtlSeconds > 0 {
			return nil, status.Error(codes.InvalidArgument, "per-record ttl_seconds is not supported with a keyed insert")
		}
		id, ts, err := col.InsertWithKey(req.Key, req.Data.AsMap())
		if err != nil {
			return nil, keyedErr(err)
		}
		return &pb.InsertResponse{Id: id, DateAdded: ts.Format(time.RFC3339), Key: req.Key, Rev: 1}, nil
	}

	if txID := txIDFromContext(ctx); txID != "" {
		if req.TtlSeconds > 0 {
			return nil, status.Error(codes.InvalidArgument, "per-record ttl_seconds is not supported inside a transaction")
		}
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
	id, ts, err := col.InsertWithExpiry(data, ttlDeadline(req.TtlSeconds))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert: %v", err)
	}
	// A fresh, keyless insert always starts at revision 1 and carries no key.
	return &pb.InsertResponse{Id: id, DateAdded: ts.Format(time.RFC3339), Rev: 1}, nil
}

func (s *GRPCServer) InsertMany(_ context.Context, req *pb.InsertManyRequest) (*pb.InsertManyResponse, error) {
	if req.TtlSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "ttl_seconds must not be negative")
	}
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	deadline := ttlDeadline(req.TtlSeconds)
	ids := make([]uint64, 0, len(req.Records))
	for _, r := range req.Records {
		id, _, err := col.InsertWithExpiry(r.AsMap(), deadline)
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
	r, err := col.Get(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	// Field projection (N2): id/key/rev are passed separately and always kept.
	rec, err := toProtoRecord(r.ID, r.Key, r.Rev, engine.ProjectData(r.Data, req.Fields), r.Ts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.FindResponse{Record: rec}, nil
}

func (s *GRPCServer) Find(req *pb.FindRequest, stream pb.FileDB_FindServer) error {
	start := time.Now()
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
	// order_by_fields (N3) supersedes the deprecated scalar order_by/descending;
	// the engine falls back to the scalar when Sort is empty, so passing both is
	// safe. page_token drives keyset pagination.
	opts := engine.ScanOptions{
		Filter:     f,
		Limit:      int(req.Limit),
		Offset:     int(req.Offset),
		OrderBy:    req.OrderBy,    //nolint:staticcheck // deprecated scalar honoured for back-compat (N3)
		Descending: req.Descending, //nolint:staticcheck // deprecated scalar honoured for back-compat (N3)
		Sort:       protoOrderByToSort(req.OrderByFields),
		PageToken:  req.PageToken,
		Fields:     req.Fields,
	}

	// The next-page token is known only once the scan finishes, but a
	// server-streaming Find has no trailer to carry it. Send each record one step
	// behind so the final record message can carry the resume cursor in its
	// page_token; a keyless empty token on the last page just leaves the field
	// unset. Buffering one record keeps this backward compatible — no extra
	// record-less message that an older client would choke on.
	var pending *pb.Record
	flush := func(token string) error {
		if pending == nil {
			return nil
		}
		err := stream.Send(&pb.FindResponse{Record: pending, PageToken: token})
		pending = nil
		return err
	}
	stats, err := col.ScanStream(stream.Context(), opts, func(r engine.ScanResult) error {
		if err := flush(""); err != nil {
			return err
		}
		rec, err := toProtoRecord(r.ID, keyOf(r.Data), r.Rev, r.Data, r.Ts)
		if err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		pending = rec
		return nil
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		if errors.Is(err, engine.ErrInvalidPageToken) {
			return status.Errorf(codes.InvalidArgument, "%v", err)
		}
		if _, ok := status.FromError(err); ok {
			return err // already a gRPC status (stream.Send / marshal error) — preserve its code
		}
		return status.Errorf(codes.Internal, "find: %v", err)
	}
	if err := flush(stats.NextPageToken); err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "find: %v", err)
	}
	s.recordScan(req.Collection, req.Filter, stats, time.Since(start))
	return nil
}

// protoOrderByToSort maps the repeated proto OrderBy sort keys to the engine's
// SortField slice, preserving order and per-field direction. An empty input
// yields nil, letting the engine fall back to the deprecated scalar order_by.
func protoOrderByToSort(obs []*pb.OrderBy) []engine.SortField {
	if len(obs) == 0 {
		return nil
	}
	out := make([]engine.SortField, 0, len(obs))
	for _, ob := range obs {
		out = append(out, engine.SortField{Field: ob.Field, Desc: ob.Desc})
	}
	return out
}

// recordScan feeds a completed scan's cost to the metrics histogram and, when a
// slow-query threshold is configured and the query reached it, emits one WARN
// log line describing the filter shape and the scan's rows-scanned/returned,
// index-used, and duration. Both sinks are optional (see NewGRPCServer opts).
func (s *GRPCServer) recordScan(collection string, filter *pb.Filter, stats engine.ScanStats, dur time.Duration) {
	if s.onScan != nil {
		s.onScan(collection, stats.RowsScanned)
	}
	if s.logger != nil && s.slowQuery > 0 && dur >= s.slowQuery {
		s.logger.Warn("slow query",
			slog.String("collection", collection),
			slog.String("filter", filterShape(filter)),
			slog.Int("rows_scanned", stats.RowsScanned),
			slog.Int("rows_returned", stats.RowsReturned),
			slog.Bool("index_used", stats.IndexUsed),
			slog.Duration("duration", dur),
		)
	}
}

// filterShape renders a filter's structure — fields and operators, but not the
// compared values — so the slow-query log can identify a query pattern without
// leaking record data. A nil filter (match-all) renders as "*".
func filterShape(f *pb.Filter) string {
	if f == nil {
		return "*"
	}
	switch k := f.Kind.(type) {
	case *pb.Filter_Field:
		return k.Field.Field + " " + k.Field.Op.String()
	case *pb.Filter_And:
		return "and(" + joinShapes(k.And.Filters) + ")"
	case *pb.Filter_Or:
		return "or(" + joinShapes(k.Or.Filters) + ")"
	}
	return "*"
}

// joinShapes renders a slice of child filters as a comma-separated shape list.
func joinShapes(filters []*pb.Filter) string {
	parts := make([]string, len(filters))
	for i, sub := range filters {
		parts[i] = filterShape(sub)
	}
	return strings.Join(parts, ", ")
}

func (s *GRPCServer) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	if req.TtlSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "ttl_seconds must not be negative")
	}
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if txID := txIDFromContext(ctx); txID != "" {
		if req.TtlSeconds > 0 {
			return nil, status.Error(codes.InvalidArgument, "per-record ttl_seconds is not supported inside a transaction")
		}
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

	// A data-only update keeps the record's existing deadline; an explicit
	// ttl_seconds resets the deadline to that far from now.
	var ts time.Time
	if req.TtlSeconds > 0 {
		ts, err = col.UpdateWithExpiry(req.Id, req.Data.AsMap(), ttlDeadline(req.TtlSeconds))
	} else {
		ts, err = col.Update(req.Id, req.Data.AsMap())
	}
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	resp := &pb.UpdateResponse{Id: req.Id, DateModified: ts.Format(time.RFC3339)}
	// Surface the record's key and post-write revision best-effort.
	if r, gerr := col.Get(req.Id); gerr == nil {
		resp.Key, resp.Rev = r.Key, r.Rev
	}
	return resp, nil
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

// ---- Keyed CRUD, Upsert & compare-and-swap (N1) ---------------------------

// Upsert inserts data under req.Key if no live record carries it, or replaces
// the existing record's data if one does — atomically in the engine. It returns
// the resulting record with its (incremented on replace) revision.
func (s *GRPCServer) Upsert(_ context.Context, req *pb.UpsertRequest) (*pb.UpsertResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key required")
	}
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	rec, err := col.Upsert(req.Key, req.Data.AsMap())
	if err != nil {
		return nil, keyedErr(err)
	}
	prec, err := toProtoRecord(rec.ID, rec.Key, rec.Rev, rec.Data, rec.Ts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.UpsertResponse{Record: prec}, nil
}

// FindByKey returns the record carrying req.Key. A missing key is NOT_FOUND.
func (s *GRPCServer) FindByKey(_ context.Context, req *pb.FindByKeyRequest) (*pb.FindResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection: %v", err)
	}
	rec, err := col.GetByKey(req.Key)
	if err != nil {
		return nil, keyedErr(err)
	}
	// Field projection (N2): id/key/rev are passed separately and always kept.
	prec, err := toProtoRecord(rec.ID, rec.Key, rec.Rev, engine.ProjectData(rec.Data, req.Fields), rec.Ts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.FindResponse{Record: prec}, nil
}

// UpdateByKey overwrites the record carrying req.Key, preserving the key. A
// missing key is NOT_FOUND.
func (s *GRPCServer) UpdateByKey(_ context.Context, req *pb.UpdateByKeyRequest) (*pb.UpdateResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	ts, err := col.UpdateByKey(req.Key, req.Data.AsMap())
	if err != nil {
		return nil, keyedErr(err)
	}
	resp := &pb.UpdateResponse{DateModified: ts.Format(time.RFC3339), Key: req.Key}
	// Surface the record's id and post-write revision best-effort.
	if r, gerr := col.GetByKey(req.Key); gerr == nil {
		resp.Id, resp.Rev = r.ID, r.Rev
	}
	return resp, nil
}

// DeleteByKey removes the record carrying req.Key. A missing key is NOT_FOUND.
func (s *GRPCServer) DeleteByKey(_ context.Context, req *pb.DeleteByKeyRequest) (*pb.DeleteResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if err := col.DeleteByKey(req.Key); err != nil {
		return nil, keyedErr(err)
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// UpdateIfRev conditionally updates the record carrying req.Key only if its
// current revision equals req.ExpectedRev. A stale revision (or a missing key)
// is a clean no-op reported as swapped=false — never an error. When the swap
// applies, the resulting record (with its bumped revision) is returned.
func (s *GRPCServer) UpdateIfRev(_ context.Context, req *pb.UpdateIfRevRequest) (*pb.UpdateIfRevResponse, error) {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	swapped, err := col.UpdateIfRev(req.Key, req.ExpectedRev, req.Data.AsMap())
	if err != nil {
		return nil, keyedErr(err)
	}
	resp := &pb.UpdateIfRevResponse{Swapped: swapped}
	if swapped {
		if r, gerr := col.GetByKey(req.Key); gerr == nil {
			if prec, perr := toProtoRecord(r.ID, r.Key, r.Rev, r.Data, r.Ts); perr == nil {
				resp.Record = prec
			}
		}
	}
	return resp, nil
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
			// Watch events do not carry a revision; key is recovered from the
			// record's reserved _key field when present.
			rec, _ := toProtoRecord(ev.ID, keyOf(ev.Data), 0, ev.Data, ev.Ts)
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

// ---- Aggregations (N4) ----------------------------------------------------

// Aggregate computes count and numeric aggregations over the records matching the
// request's Filter, optionally grouped by a field, and server-streams one message
// per group. It maps straight onto the engine's Aggregate, which folds each record
// into its group's accumulator without materialising the collection.
func (s *GRPCServer) Aggregate(req *pb.AggregateRequest, stream pb.FileDB_AggregateServer) error {
	col, err := s.db.Collection(req.Collection)
	if err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}
	f, err := protoFilterToQuery(req.Filter)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}
	// SUM/AVG/MIN/MAX are numeric aggregations: they need a field to reduce over.
	if wantsNumericAgg(req.Aggregations) && req.Field == "" {
		return status.Error(codes.InvalidArgument, "sum/avg/min/max require a numeric field")
	}

	spec := engine.AggregateSpec{Filter: f, GroupBy: req.GroupBy, Field: req.Field}
	err = col.Aggregate(stream.Context(), spec, func(g engine.GroupResult) error {
		gv, verr := groupValue(g.Key)
		if verr != nil {
			return status.Errorf(codes.Internal, "aggregate: %v", verr)
		}
		return stream.Send(&pb.AggregateResponse{
			GroupValue: gv,
			Count:      g.Count,
			Sum:        g.Sum,
			Avg:        g.Avg,
			Min:        g.Min,
			Max:        g.Max,
			Numeric:    g.Numeric,
		})
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		if _, ok := status.FromError(err); ok {
			return err // already a gRPC status (stream.Send / marshal error)
		}
		return status.Errorf(codes.Internal, "aggregate: %v", err)
	}
	return nil
}

// wantsNumericAgg reports whether the requested aggregations include one that
// reduces over the numeric field (sum/avg/min/max) and therefore requires a field.
func wantsNumericAgg(ops []pb.AggregateOp) bool {
	for _, op := range ops {
		switch op {
		case pb.AggregateOp_AGG_SUM, pb.AggregateOp_AGG_AVG,
			pb.AggregateOp_AGG_MIN, pb.AggregateOp_AGG_MAX:
			return true
		}
	}
	return false
}

// groupValue converts an aggregation group key (nil for the whole-set/absent-field
// group, else a number/string/bool from the decoded record) into a proto Value,
// preserving its type. A nil key becomes a null Value.
func groupValue(k any) (*structpb.Value, error) {
	if k == nil {
		return structpb.NewNullValue(), nil
	}
	return structpb.NewValue(k)
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

// ttlDeadline converts a relative ttl in seconds into an absolute expiry
// instant. A non-positive ttl yields the zero time, which the engine treats as
// "apply the collection's default TTL (if any)".
func ttlDeadline(ttlSeconds int64) time.Time {
	if ttlSeconds <= 0 {
		return time.Time{}
	}
	return time.Now().UTC().Add(time.Duration(ttlSeconds) * time.Second)
}

func toProtoRecord(id uint64, key string, rev uint64, data map[string]any, ts time.Time) (*pb.Record, error) {
	s, err := structpb.NewStruct(data)
	if err != nil {
		return nil, fmt.Errorf("toProtoRecord: %w", err)
	}
	return &pb.Record{
		Id:           id,
		Data:         s,
		DateAdded:    timestamppb.New(ts),
		DateModified: timestamppb.New(ts),
		Key:          key,
		Rev:          rev,
	}, nil
}

// keyOf extracts a record's caller-supplied string key from its data map, where
// the engine stores it under the reserved _key field. It returns "" for records
// inserted without a key.
func keyOf(data map[string]any) string {
	k, _ := data[engine.KeyField].(string)
	return k
}

// keyedErr maps the typed engine errors surfaced by the keyed operations onto
// gRPC status codes: a missing key is NOT_FOUND, a duplicate key is
// ALREADY_EXISTS, and an attempt to set the reserved _key field via data is
// INVALID_ARGUMENT. Anything else is Internal.
func keyedErr(err error) error {
	switch {
	case errors.Is(err, engine.ErrKeyNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrDuplicateKey):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, engine.ErrReservedField):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
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
