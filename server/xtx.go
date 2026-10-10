package server

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/internal/xtxapi"
	"github.com/srjn45/scriva/store"
)

// This file is the gRPC surface of cross-collection transactions: thin
// handlers over the engine's transaction handle (engine.XTx) plus the one
// place that maps its typed errors onto gRPC codes and machine-readable
// reasons (xtxStatusErr).
//
// Isolation is whatever the handle implements, no more: optimistic validation
// of point reads and writes at commit, no predicate reads and therefore no
// phantom protection. Scans are rejected here as well (rejectXTxHeader).

// maxXTxTimeoutMs keeps a millisecond timeout from overflowing time.Duration.
const maxXTxTimeoutMs = math.MaxInt64 / int64(time.Millisecond)

// xtxMillis converts a request timeout in milliseconds, rejecting negative and
// overflowing values with INVALID_ARGUMENT.
func xtxMillis(field string, ms int64) (time.Duration, error) {
	if ms < 0 || ms > maxXTxTimeoutMs {
		return 0, xtxInvalid(field + " is out of range")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// ---- Handlers -------------------------------------------------------------

func (s *GRPCServer) BeginXTx(ctx context.Context, req *pb.BeginXTxRequest) (*pb.BeginXTxResponse, error) {
	idle, err := xtxMillis("idle_timeout_ms", req.IdleTimeoutMs)
	if err != nil {
		return nil, err
	}
	life, err := xtxMillis("max_lifetime_ms", req.MaxLifetimeMs)
	if err != nil {
		return nil, err
	}
	x, err := s.db.BeginXTx(ctx, req.Collections, engine.XTxOptions{
		Key:         req.IdempotencyKey,
		IdleTimeout: idle,
		MaxLifetime: life,
	})
	if err != nil {
		return nil, xtxStatusErr(err, req.IdempotencyKey)
	}
	return &pb.BeginXTxResponse{
		XtxId:          x.ID(),
		Collections:    x.Participants(),
		IdempotencyKey: x.Key(),
	}, nil
}

func (s *GRPCServer) StageXTx(_ context.Context, req *pb.StageXTxRequest) (*pb.StageXTxResponse, error) {
	x, err := s.xtxHandle(req.XtxId)
	if err != nil {
		return nil, err
	}
	op := engine.XTxOp{Collection: req.Collection, ID: req.Id, ExpectedRev: req.ExpectedRev}
	switch req.Op {
	case pb.XTxOpKind_XTX_OP_INSERT:
		op.Op = store.OpInsert
	case pb.XTxOpKind_XTX_OP_UPDATE:
		op.Op = store.OpUpdate
	case pb.XTxOpKind_XTX_OP_DELETE:
		op.Op = store.OpDelete
	default:
		return nil, xtxInvalid("op must be XTX_OP_INSERT, XTX_OP_UPDATE or XTX_OP_DELETE")
	}
	if op.Op != store.OpDelete {
		if req.Data == nil {
			return nil, xtxInvalid("data is required for an insert or update")
		}
		op.Data = req.Data.AsMap()
	}
	id, err := x.Stage(op)
	if err != nil {
		return nil, xtxStatusErr(err, xtxRef(x))
	}
	return &pb.StageXTxResponse{Id: id}, nil
}

func (s *GRPCServer) GetXTx(_ context.Context, req *pb.GetXTxRequest) (*pb.GetXTxResponse, error) {
	x, err := s.xtxHandle(req.XtxId)
	if err != nil {
		return nil, err
	}
	r, err := x.Get(req.Collection, req.Id)
	if err != nil {
		return nil, xtxStatusErr(err, xtxRef(x))
	}
	rec, err := toProtoRecord(r.ID, r.Key, r.Rev, r.Data, r.Ts)
	if err != nil {
		return nil, xtxStatusErr(err, xtxRef(x))
	}
	return &pb.GetXTxResponse{Record: rec}, nil
}

func (s *GRPCServer) CommitXTx(ctx context.Context, req *pb.CommitXTxRequest) (*pb.CommitXTxResponse, error) {
	wait, err := xtxMillis("lock_timeout_ms", req.LockTimeoutMs)
	if err != nil {
		return nil, err
	}
	x, err := s.xtxHandle(req.XtxId)
	if err != nil {
		return nil, err
	}
	if wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	res, err := x.Commit(ctx)
	if err != nil {
		return nil, xtxStatusErr(err, xtxRef(x))
	}
	out := &pb.CommitXTxResponse{TxId: res.TxID, Replayed: res.Replayed}
	for _, op := range res.Ops {
		out.Ops = append(out.Ops, &pb.XTxOpResult{
			Collection: op.Collection,
			Id:         op.ID,
			Rev:        op.Rev,
			Op:         xtxOpToProto(op.Op),
		})
	}
	return out, nil
}

func (s *GRPCServer) RollbackXTx(_ context.Context, req *pb.RollbackXTxRequest) (*pb.RollbackXTxResponse, error) {
	x, err := s.xtxHandle(req.XtxId)
	if err != nil {
		return nil, err
	}
	if err := x.Rollback(); err != nil {
		return nil, xtxStatusErr(err, xtxRef(x))
	}
	return &pb.RollbackXTxResponse{Ok: true}, nil
}

func (s *GRPCServer) XTxStatus(_ context.Context, req *pb.XTxStatusRequest) (*pb.XTxStatusResponse, error) {
	if req.Ref == "" {
		return nil, xtxInvalid("ref is required: a tx_id, an idempotency key or an xtx_id")
	}
	st, txid := s.db.XTxStatus(req.Ref)
	return &pb.XTxStatusResponse{Status: xtxStateToProto(st), TxId: txid}, nil
}

// ---- Helpers --------------------------------------------------------------

// xtxHandle resolves a handle id to its transaction, mapping a miss onto
// NOT_FOUND (XTX_NOT_FOUND).
func (s *GRPCServer) xtxHandle(id string) (*engine.XTx, error) {
	if id == "" {
		return nil, xtxInvalid("xtx_id is required")
	}
	x, err := s.db.XTxHandle(id)
	if err != nil {
		return nil, xtxStatusErr(err, id)
	}
	return x, nil
}

// xtxRef is the reference a client should hand to XTxStatus for x when the
// error itself carries no tx_id: the idempotency key when there is one (it
// survives a restart), otherwise the handle id.
func xtxRef(x *engine.XTx) string {
	if k := x.Key(); k != "" {
		return k
	}
	return x.ID()
}

func xtxOpToProto(op store.Op) pb.XTxOpKind {
	switch op {
	case store.OpInsert:
		return pb.XTxOpKind_XTX_OP_INSERT
	case store.OpUpdate:
		return pb.XTxOpKind_XTX_OP_UPDATE
	case store.OpDelete:
		return pb.XTxOpKind_XTX_OP_DELETE
	default:
		return pb.XTxOpKind_XTX_OP_UNSPECIFIED
	}
}

func xtxStateToProto(st engine.XTxStatus) pb.XTxState {
	switch st {
	case engine.XTxCommitted:
		return pb.XTxState_XTX_STATUS_COMMITTED
	case engine.XTxAborted:
		return pb.XTxState_XTX_STATUS_ABORTED
	case engine.XTxPending:
		return pb.XTxState_XTX_STATUS_PENDING
	case engine.XTxExpired:
		return pb.XTxState_XTX_STATUS_EXPIRED
	default:
		return pb.XTxState_XTX_STATUS_UNKNOWN
	}
}

// xtxIDFromContext extracts the x-xtx-id value from incoming gRPC metadata.
func xtxIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get(xtxapi.HeaderXTxID); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// rejectXTxHeader refuses an ordinary data RPC that a client tagged with a
// cross-collection transaction (the x-xtx-id header). Such an RPC would run
// outside the transaction — unvalidated and immediately visible — so it is
// never executed: a scan-shaped RPC is answered the way the handle answers a
// scan (UNIMPLEMENTED / XTX_SCAN_UNSUPPORTED, or XTX_NOT_PARTICIPANT), and any
// other RPC is pointed at StageXTx / GetXTx. It returns nil when the request
// carries no transaction.
func (s *GRPCServer) rejectXTxHeader(ctx context.Context, collection string, scan bool) error {
	id := xtxIDFromContext(ctx)
	if id == "" {
		return nil
	}
	if !scan {
		return xtxInvalid("this RPC cannot run inside a cross-collection transaction; use StageXTx for writes and GetXTx for point reads")
	}
	x, err := s.xtxHandle(id)
	if err != nil {
		return err
	}
	_, err = x.Scan(collection, nil)
	return xtxStatusErr(err, xtxRef(x))
}

// xtxInvalid is an INVALID_ARGUMENT (XTX_INVALID) status for a request the
// transport rejects before it reaches the engine.
func xtxInvalid(msg string) error {
	return xtxStatus(codes.InvalidArgument, xtxapi.ReasonInvalid, msg, map[string]string{xtxapi.MetaRetrySafe: "false"})
}

// xtxStatus builds a status carrying the ErrorInfo detail every transaction
// error has.
func xtxStatus(code codes.Code, reason, msg string, meta map[string]string) error {
	st := status.New(code, msg)
	withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: xtxapi.Domain, Metadata: meta})
	if err != nil {
		return st.Err()
	}
	return withInfo.Err()
}

// xtxStatusErr maps an engine transaction error onto a gRPC status with a
// machine-readable reason. ref is what the client should pass to XTxStatus
// when the error carries no tx_id (an idempotency key or a handle id).
//
// The mapping is one reason per failure class, and one code per reason:
//
//	conflict                 ABORTED              XTX_CONFLICT                 retry-safe
//	canceled before prepare  CANCELLED /          XTX_CANCELED_BEFORE_PREPARE  retry-safe, handle open
//	                         DEADLINE_EXCEEDED
//	outcome unknown          UNKNOWN              XTX_OUTCOME_UNKNOWN          NOT retry-safe: XTxStatus
//	commit in progress       UNAVAILABLE          XTX_IN_PROGRESS              NOT retry-safe: XTxStatus
//	durability failure       UNAVAILABLE          XTX_DURABILITY               retry-safe
//	not a participant        INVALID_ARGUMENT     XTX_NOT_PARTICIPANT
//	invalid / reserved       INVALID_ARGUMENT     XTX_INVALID
//	over a limit             INVALID_ARGUMENT     XTX_TOO_LARGE
//	scan / index lookup      UNIMPLEMENTED        XTX_SCAN_UNSUPPORTED
//	unknown handle           NOT_FOUND            XTX_NOT_FOUND
//	document absent          NOT_FOUND            XTX_DOC_NOT_FOUND
//	collection absent        NOT_FOUND            XTX_COLLECTION_NOT_FOUND
//	handle expired           FAILED_PRECONDITION  XTX_EXPIRED
//	handle finished          FAILED_PRECONDITION  XTX_FINISHED
//	read-only follower       FAILED_PRECONDITION  XTX_READ_ONLY
//	unsupported XTx state    FAILED_PRECONDITION  XTX_UNSUPPORTED
//	quota                    RESOURCE_EXHAUSTED   XTX_RESOURCE_EXHAUSTED       retry-safe
//	unique index             ALREADY_EXISTS       XTX_DUPLICATE_KEY
//	integrity                DATA_LOSS            XTX_DATA_LOSS
//	anything else            INTERNAL             XTX_INTERNAL
//
// Outcome-unknown is checked first and maps to UNKNOWN on purpose: it is the
// code a client also sees when a commit response is simply lost, and both
// demand the same reaction — look the outcome up, never blind-retry.
func xtxStatusErr(err error, ref string) error {
	if err == nil {
		return nil
	}
	meta := map[string]string{xtxapi.MetaRetrySafe: strconv.FormatBool(engine.XTxRetrySafe(err))}
	var xe *engine.XTxError
	if errors.As(err, &xe) && xe.Tx != "" {
		meta[xtxapi.MetaTxID] = xe.Tx
		ref = xe.Tx
	}
	msg := err.Error()

	switch {
	case errors.Is(err, engine.ErrXTxOutcomeUnknown):
		meta[xtxapi.MetaRetrySafe] = "false"
		meta[xtxapi.MetaStatusRef] = ref
		return xtxStatus(codes.Unknown, xtxapi.ReasonOutcomeUnknown,
			msg+": the transaction may or may not have been applied; do not retry, call XTxStatus with ref "+strconv.Quote(ref),
			meta)

	// The state of the handle comes before the cause it may wrap: a handle
	// finished by a failed commit reports ErrXTxFinished caused by, say, the
	// original conflict, and must read as "finished".
	case errors.Is(err, engine.ErrXTxFinished):
		return xtxStatus(codes.FailedPrecondition, xtxapi.ReasonFinished, msg, meta)
	case errors.Is(err, engine.ErrXTxExpired):
		meta[xtxapi.MetaRetrySafe] = "true" // nothing it staged was ever written
		return xtxStatus(codes.FailedPrecondition, xtxapi.ReasonExpired, msg, meta)

	case errors.Is(err, engine.ErrXTxConflict):
		var ce *engine.XTxConflictError
		if errors.As(err, &ce) {
			rev := func(present bool, rev uint64) string {
				if !present {
					return xtxapi.RevAbsent
				}
				return strconv.FormatUint(rev, 10)
			}
			kind := "read"
			if ce.Write {
				kind = "write"
			}
			meta[xtxapi.MetaConflictCollection] = ce.Collection
			meta[xtxapi.MetaConflictID] = strconv.FormatUint(ce.ID, 10)
			meta[xtxapi.MetaConflictKind] = kind
			meta[xtxapi.MetaConflictExpectedRev] = rev(ce.ExpectedPresent, ce.ExpectedRev)
			meta[xtxapi.MetaConflictActualRev] = rev(ce.ActualPresent, ce.ActualRev)
		}
		return xtxStatus(codes.Aborted, xtxapi.ReasonConflict, msg, meta)
	case errors.Is(err, engine.ErrXTxCanceled):
		code := codes.Canceled
		if errors.Is(err, context.DeadlineExceeded) {
			code = codes.DeadlineExceeded
		}
		return xtxStatus(code, xtxapi.ReasonCanceled, msg, meta)
	case errors.Is(err, engine.ErrXTxInProgress):
		meta[xtxapi.MetaStatusRef] = ref
		return xtxStatus(codes.Unavailable, xtxapi.ReasonInProgress, msg, meta)
	case errors.Is(err, engine.ErrXTxDurability):
		return xtxStatus(codes.Unavailable, xtxapi.ReasonDurability, msg, meta)

	case errors.Is(err, engine.ErrXTxNotParticipant):
		return xtxStatus(codes.InvalidArgument, xtxapi.ReasonNotParticipant, msg, meta)
	case errors.Is(err, engine.ErrXTxScanUnsupported):
		return xtxStatus(codes.Unimplemented, xtxapi.ReasonScanUnsupported, msg, meta)
	case errors.Is(err, engine.ErrXTxTooLarge), errors.Is(err, engine.ErrRecordTooLarge):
		return xtxStatus(codes.InvalidArgument, xtxapi.ReasonTooLarge, msg, meta)
	case errors.Is(err, engine.ErrXTxInvalid), errors.Is(err, engine.ErrReservedName),
		errors.Is(err, engine.ErrReservedField), errors.Is(err, engine.ErrXTxDuplicateOp):
		return xtxStatus(codes.InvalidArgument, xtxapi.ReasonInvalid, msg, meta)

	case errors.Is(err, engine.ErrXTxHandleNotFound):
		return xtxStatus(codes.NotFound, xtxapi.ReasonNotFound, msg, meta)
	case errors.Is(err, engine.ErrXTxDocNotFound):
		return xtxStatus(codes.NotFound, xtxapi.ReasonDocNotFound, msg, meta)
	case errors.Is(err, engine.ErrCollectionNotFound):
		return xtxStatus(codes.NotFound, xtxapi.ReasonCollectionNotFound, msg, meta)

	case errors.Is(err, engine.ErrReadOnly):
		return xtxStatus(codes.FailedPrecondition, xtxapi.ReasonReadOnly, ReadOnlyReplicaMessage, meta)
	case errors.Is(err, engine.ErrXTxUnsupported), errors.Is(err, engine.ErrFormatTooNew):
		return xtxStatus(codes.FailedPrecondition, xtxapi.ReasonUnsupported, msg, meta)

	case errors.Is(err, engine.ErrResourceExhausted):
		return xtxStatus(codes.ResourceExhausted, xtxapi.ReasonResourceExhausted, msg, meta)
	case errors.Is(err, engine.ErrDuplicateKey):
		return xtxStatus(codes.AlreadyExists, xtxapi.ReasonDuplicateKey, msg, meta)
	case errors.Is(err, engine.ErrIndexCorrupt), errors.Is(err, engine.ErrIntegrity):
		return xtxStatus(codes.DataLoss, xtxapi.ReasonDataLoss, msg, meta)
	default:
		return xtxStatus(codes.Internal, xtxapi.ReasonInternal, msg, meta)
	}
}
