package server

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/store"
)

// Typed replication-protocol errors. See docs/design-xtx-replication.md.
const (
	// ReplicationErrorDomain is the google.rpc.ErrorInfo domain of replication
	// protocol errors.
	ReplicationErrorDomain = "scriva.replication"
	// ReplicationReasonProtocolUnsupported is the ErrorInfo reason the leader
	// uses when a follower that did not advertise protocol v2 would need to
	// receive a transaction envelope.
	ReplicationReasonProtocolUnsupported = "REPLICATION_PROTOCOL_UNSUPPORTED"
)

var (
	// ErrReplicationProtocolUnsupported is returned (client side) when the
	// leader refused the stream because it must ship a transaction envelope to a
	// follower that does not support protocol v2.
	ErrReplicationProtocolUnsupported = errors.New("replication: transaction envelope requires protocol v2")
	// ErrUnknownRecordVersion: the leader sent a record_version this follower
	// does not understand. The stream stops; nothing is applied.
	ErrUnknownRecordVersion = errors.New("replication: unknown record version")
	// ErrEnvelopeApplyFailed wraps any failure to apply a transaction envelope
	// atomically. Nothing of the transaction is visible and the applied LSN did
	// not advance. Follower.Run stops and returns it (errors.Is also matches the
	// engine cause, e.g. engine.ErrReplicationDivergence).
	ErrEnvelopeApplyFailed = errors.New("replication: failed to apply transaction envelope")
)

// EnvelopeApplyObserver receives one event per replicated transaction envelope
// a follower processes: outcome is applied, duplicate, diverged, promoted or
// failed. It is how the server layer feeds metrics without the engine calling
// them.
type EnvelopeApplyObserver func(outcome string, ops int, dur time.Duration)

// SetEnvelopeApplyObserver installs the observer (nil clears it). Call before Run.
func (f *Follower) SetEnvelopeApplyObserver(o EnvelopeApplyObserver) { f.applyObserver = o }

func (f *Follower) observeApply(outcome string, ops int, dur time.Duration) {
	if f.applyObserver != nil {
		f.applyObserver(outcome, ops, dur)
	}
}

func envelopeOutcome(err error) string {
	switch {
	case errors.Is(err, engine.ErrReplicationDivergence):
		return "diverged"
	case errors.Is(err, engine.ErrReplicationPromoted):
		return "promoted"
	default:
		return "failed"
	}
}

// followerProtocolVersions is what a Follower advertises in ReplicateRequest.
var followerProtocolVersions = []uint32{engine.ReplicationProtocolV1, engine.ReplicationProtocolV2}

// supportsProtocolV2 reports whether an advertised version list includes v2.
func supportsProtocolV2(versions []uint32) bool {
	for _, v := range versions {
		if v == engine.ReplicationProtocolV2 {
			return true
		}
	}
	return false
}

// protocolUnsupportedStatus builds the leader's typed refusal for a follower
// that cannot receive the envelope at lsn.
func protocolUnsupportedStatus(lsn uint64, txid string) error {
	st := status.New(codes.FailedPrecondition, fmt.Sprintf(
		"replication: transaction %s (lsn %d) requires replication protocol v2, which this follower did not advertise; upgrade the follower (transactions are never downgraded to per-entry records)", txid, lsn))
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: ReplicationReasonProtocolUnsupported,
		Domain: ReplicationErrorDomain,
		Metadata: map[string]string{
			"required_version": fmt.Sprint(engine.ReplicationProtocolV2),
			"lsn":              fmt.Sprint(lsn),
			"tx_id":            txid,
		},
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// IsReplicationProtocolError reports whether err is the leader's typed
// "protocol v2 required" refusal.
func IsReplicationProtocolError(err error) bool {
	if errors.Is(err, ErrReplicationProtocolUnsupported) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok &&
			info.Domain == ReplicationErrorDomain && info.Reason == ReplicationReasonProtocolUnsupported {
			return true
		}
	}
	return false
}

// txEnvelopeToProto maps an engine envelope to its v2 wire record.
func txEnvelopeToProto(lsn uint64, env *engine.TxEnvelope) (*pb.ReplicationRecord, error) {
	tx := &pb.ReplicationTx{TxId: env.TxID, Key: env.Key, Digest: append([]byte(nil), env.Digest[:]...)}
	for i, op := range env.Ops {
		e := op.Entry
		o := &pb.ReplicationTxOp{
			Collection: op.Collection,
			Op:         replOpToProto(e.Op),
			Id:         e.ID,
			Rev:        e.Rev,
			ExpiresAt:  e.ExpiresAt,
			Ts:         timestamppb.New(e.Ts),
			Epoch:      e.Epoch,
		}
		if e.Data != nil {
			st, err := structpb.NewStruct(e.Data)
			if err != nil {
				return nil, fmt.Errorf("txEnvelopeToProto: op %d: %w", i, err)
			}
			o.Data = st
		}
		tx.Ops = append(tx.Ops, o)
	}
	return &pb.ReplicationRecord{
		Lsn:           lsn,
		RecordVersion: engine.ReplicationProtocolV2,
		Tx:            tx,
	}, nil
}

// txEnvelopeFromProto rebuilds an engine envelope from a v2 wire record and
// verifies its digest. A mismatch is engine.ErrEnvelopeDigest; a structurally
// invalid envelope is engine.ErrEnvelopeInvalid.
func txEnvelopeFromProto(rec *pb.ReplicationRecord) (*engine.TxEnvelope, error) {
	tx := rec.GetTx()
	if tx == nil {
		return nil, fmt.Errorf("%w: record version 2 without tx payload", engine.ErrEnvelopeInvalid)
	}
	if len(tx.Digest) != len(engine.TxEnvelope{}.Digest) {
		return nil, fmt.Errorf("%w: digest length %d", engine.ErrEnvelopeInvalid, len(tx.Digest))
	}
	env := &engine.TxEnvelope{TxID: tx.TxId, Key: tx.Key}
	copy(env.Digest[:], tx.Digest)
	for _, o := range tx.Ops {
		e := store.Entry{
			ID:        o.Id,
			Op:        replOpFromProto(o.Op),
			Rev:       o.Rev,
			ExpiresAt: o.ExpiresAt,
			Epoch:     o.Epoch,
		}
		if o.Ts != nil {
			e.Ts = o.Ts.AsTime()
		}
		if o.Data != nil {
			e.Data = o.Data.AsMap()
		}
		env.Ops = append(env.Ops, engine.TxEnvelopeOp{Collection: o.Collection, Entry: e})
	}
	if err := env.Verify(); err != nil {
		return nil, err
	}
	return env, nil
}

// applyEnvelope applies a decoded, digest-verified envelope with its single LSN
// atomically through the follower's durable cross-collection coordinator
// (engine.DB.ApplyReplicatedTx). It returns nil only after the whole
// transaction is locally committed and visible — or was already applied
// (idempotent redelivery). The applied LSN is advanced by the engine in the
// same step; the caller's SetAppliedLSN is then a no-op.
func (f *Follower) applyEnvelope(lsn uint64, env *engine.TxEnvelope) error {
	start := time.Now()
	applied, err := f.db.ApplyReplicatedTx(lsn, env)
	if err != nil {
		f.observeApply(envelopeOutcome(err), len(env.Ops), time.Since(start))
		return fmt.Errorf("%w: %w", ErrEnvelopeApplyFailed, err)
	}
	if applied {
		f.observeApply("applied", len(env.Ops), time.Since(start))
	} else {
		f.observeApply("duplicate", len(env.Ops), time.Since(start))
	}
	if !applied {
		f.logger.Debug("replicated transaction already applied", "tx_id", env.TxID, "lsn", lsn)
	}
	return nil
}
