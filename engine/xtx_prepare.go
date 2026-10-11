package engine

import (
	"errors"
)

// xtxRun is one participant's ordered run of entries for a transaction: the
// entries carry everything but the tx stamp, which xtxPrepareAndDecide adds.
type xtxRun struct {
	col     *Collection
	entries []stampedEntry
}

// xtxReplBind ties a follower-applied transaction's journal COMMIT record to the
// replication stream position it came from (docs/design-xtx-replication.md §9).
type xtxReplBind struct {
	LSN    uint64 // the transaction's single replication LSN
	LeadTx string // the leader's txid
	Digest string // hex SHA-256 envelope digest
}

// xtxPrepareAndDecide is S1..S4 of the commit protocol, shared by the leader's
// commit path and the follower's replicated apply: it appends every run as
// stamped entries to the participants' active segments (S1), fsyncs every
// participant unconditionally (S2), and appends + fsyncs the journal COMMIT
// record (S3+S4), optionally bound to a replication position. The caller holds
// every participant lock. It returns the segment offset of every entry by
// collection name.
//
// On any failure before the decision is durable the runs are rolled back and
// the txid is aborted; ErrXTxOutcomeUnknown is returned with the runs left in
// place for recovery to resolve.
func (db *DB) xtxPrepareAndDecide(j *xtxJournal, key, txid string, runs []xtxRun, bind *xtxReplBind) (map[string][]int64, error) {
	startSizes := make(map[string]int64, len(runs))
	offsets := make(map[string][]int64, len(runs))
	parts := make([]xtxPart, 0, len(runs))

	rollbackRuns := func() {
		for _, r := range runs {
			if sz, ok := startSizes[r.col.name]; ok {
				_ = r.col.active.rollback(sz)
			}
		}
	}

	// S1: Append stamped runs to active segments
	for _, r := range runs {
		c := r.col
		startSizes[c.name] = c.active.Size()
		n := uint32(len(r.entries))
		opRefs := make([]xtxOpRef, n)
		colOffsets := make([]int64, n)

		for i, se := range r.entries {
			se.Tx = TxStamp{T: txid, I: uint32(i), N: n}
			offset, err := c.active.AppendStamped(se)
			if err != nil {
				rollbackRuns()
				_ = j.abort(txid, key, "io")
				return nil, xtxErr(txid, ErrXTxDurability, err)
			}
			colOffsets[i] = offset
			opRefs[i] = xtxOpRef{ID: se.ID, Op: string(se.Op), Rev: se.Rev}
		}
		offsets[c.name] = colOffsets
		parts = append(parts, xtxPart{C: c.name, N: n, D: partDigest(opRefs)})
	}

	// S2: Fsync all participant active segments unconditionally
	for _, r := range runs {
		if err := r.col.active.Sync(); err != nil {
			r.col.active.poisoned = err
			rollbackRuns()
			_ = j.abort(txid, key, "io")
			return nil, xtxErr(txid, ErrXTxDurability, err)
		}
	}

	db.xtxHookAt(xtxHookPrepared, key, txid)

	// S3 + S4: Commit record in coordinator journal and fsync
	if err := j.commitPreparedBound(txid, key, parts, bind); err != nil {
		if errors.Is(err, ErrXTxOutcomeUnknown) {
			// S4 fsync failed: outcome is unknown to this process. Do NOT roll back runs.
			// The transaction may yet be COMMIT after a restart without ever having been
			// published, so force every follower through a snapshot (design-xtx-replication §5).
			// A follower applying a replicated transaction publishes nothing.
			if db.broker != nil && bind == nil {
				db.broker.resyncBarrier()
			}
			return nil, xtxErr(txid, ErrXTxOutcomeUnknown, err)
		}
		rollbackRuns()
		_ = j.abort(txid, key, "io")
		return nil, xtxErr(txid, ErrXTxDurability, err)
	}

	db.xtxHookAt(xtxHookDecided, key, txid)
	return offsets, nil
}
