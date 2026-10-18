package engine

// Single-collection batch routing (hardening design §3.1, SR-D2 option A).
//
// A batch of two or more entries (InsertMany, single-collection CommitTx) is
// meant to run as a one-participant cross-collection transaction through
// DB.commitXTx, so a crash can never expose a prefix of it. That writer is
// one-way on disk (xtx.format, xtx.journal, stamped lines), so it is gated:
// it stays off until every reader/recovery capability it depends on exists.
// While off, batches take the unchanged legacy path and write no XTx bytes.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/srjn45/scriva/store"
)

// ErrBatchNotAtomic: a batch of two or more entries was submitted to a
// collection that has no transaction coordinator (opened with OpenCollection
// rather than through a DB), so the engine cannot make it all-or-nothing.
// Nothing was written. Open the collection through engine.Open, or set
// CollectionConfig.AllowNonAtomicBatch to accept prefix-visible batches.
var ErrBatchNotAtomic = errors.New("engine: collection has no transaction coordinator; batch cannot be atomic")

// atomicBatchCapabilities names the prerequisites of the atomic batch writer.
// Every one must be true before atomicBatchWriterReady may be enabled; each is
// owned by a later task of the single-collection batch plan. The writer is
// deliberately not derived from them at runtime: flipping the gate is a code
// change reviewed together with the capability that completes the set.
type atomicBatchCapabilities struct {
	// RecoveryTruncatesDeadRuns: a one-participant uncommitted run is truncated
	// and a committed one replayed on reopen (index, compaction, verify/repair).
	RecoveryTruncatesDeadRuns bool
	// CompactionPreservesStamps: compaction keeps stamps and decision digests.
	CompactionPreservesStamps bool
	// BackupPreservesDecisions: snapshot/restore carry xtx.journal evidence.
	BackupPreservesDecisions bool
	// ReplicationEnvelopes: leader publishes, follower applies one envelope.
	ReplicationEnvelopes bool
	// LegacyRootOptIn: an operator-controlled conversion of legacy roots.
	LegacyRootOptIn bool
}

// missing lists the capabilities that are not yet in place.
func (k atomicBatchCapabilities) missing() []string {
	var m []string
	for _, c := range []struct {
		ok   bool
		name string
	}{
		{k.RecoveryTruncatesDeadRuns, "recovery/index/verify for one-participant runs"},
		{k.CompactionPreservesStamps, "compaction preserving stamps and digests"},
		{k.BackupPreservesDecisions, "snapshot/restore preserving decision evidence"},
		{k.ReplicationEnvelopes, "replication of single-participant envelopes"},
		{k.LegacyRootOptIn, "operator opt-in conversion of legacy roots"},
	} {
		if !c.ok {
			m = append(m, c.name)
		}
	}
	return m
}

// atomicBatchPrereqs records what exists today. Later tasks flip entries.
var atomicBatchPrereqs = atomicBatchCapabilities{}

// atomicBatchWriterReady is the single switch that lets a batch write XTx
// bytes. It is true only when no prerequisite is missing.
var atomicBatchWriterReady = len(atomicBatchPrereqs.missing()) == 0

type batchRoute int

const (
	// batchRouteLegacy: the pre-existing one-lock, N-lines path. Writes no XTx
	// bytes. Used for single-entry writes (design §2.3) and while the atomic
	// writer is gated off or the root has not been converted.
	batchRouteLegacy batchRoute = iota
	// batchRouteAtomic: one-participant XTx through DB.commitXTx.
	batchRouteAtomic
)

// atomicBatchMode reports whether this root runs batches atomically. Today
// that is exactly the roots that already hold xtx.format/xtx.journal; the
// explicit legacy-root opt-in and new-root default are later tasks and fail
// closed (legacy path) until they exist.
func (db *DB) atomicBatchMode() bool {
	return db.journalOrNil() != nil
}

// routeBatch decides how a batch of n entries is executed. It never touches
// the disk. It must be called before taking c.mu (lock order: DB.mu → c.mu →
// journal.mu).
func (c *Collection) routeBatch(n int) (batchRoute, error) {
	if n < 2 || !atomicBatchWriterReady {
		return batchRouteLegacy, nil
	}
	if c.db == nil {
		if c.cfg.AllowNonAtomicBatch {
			return batchRouteLegacy, nil
		}
		return batchRouteLegacy, fmt.Errorf("%w: collection %q (use engine.Open, or set AllowNonAtomicBatch)", ErrBatchNotAtomic, c.name)
	}
	if !c.db.atomicBatchMode() {
		return batchRouteLegacy, nil
	}
	return batchRouteAtomic, nil
}

// BatchResult identifies a batch's durable decision. TxID is the engine txid;
// DB.XTxStatus(TxID) resolves an unknown outcome.
type BatchResult struct {
	TxID string
}

// BatchTxID extracts the txid carried by a batch error, if any.
// DB.XTxStatus(txid) resolves an unknown outcome.
func BatchTxID(err error) string {
	var xe *XTxError
	if errors.As(err, &xe) {
		return xe.Tx
	}
	return ""
}

// commitBatchAtomic runs ops (all on c) as a one-participant XTx. ts is the
// shared record timestamp. The error is returned as *XTxError so callers can
// errors.Is the XTx sentinel (ErrXTxOutcomeUnknown, ErrXTxDurability,
// ErrXTxTooLarge, ...) and the original cause (ErrDuplicateKey, quota, ...),
// and errors.As it for the txid.
func (c *Collection) commitBatchAtomic(ops []XTxOp, ts time.Time) (*BatchResult, error) {
	if c.db == nil {
		return nil, ErrBatchNotAtomic
	}
	for i := range ops {
		ops[i].Collection = c.name
		ops[i].ts = ts
	}
	res, err := c.db.commitXTx(context.Background(), "", ops, nil)
	if err != nil {
		return nil, err
	}
	return &BatchResult{TxID: res.TxID}, nil
}

// txOpsToXTx converts staged single-collection transaction ops.
func txOpsToXTx(ops []txOp) []XTxOp {
	out := make([]XTxOp, len(ops))
	for i, op := range ops {
		x := XTxOp{ID: op.id, Data: op.data}
		switch op.kind {
		case txOpInsert:
			x.Op = store.OpInsert
		case txOpUpdate:
			x.Op = store.OpUpdate
		default:
			x.Op = store.OpDelete
		}
		out[i] = x
	}
	return out
}
