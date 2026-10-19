package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/srjn45/scriva/store"
)

var (
	// ErrReplicationDivergence: a replicated transaction was redelivered whose
	// identity (LSN or leader txid) is already applied here but whose content
	// differs. The follower's history and the leader's no longer agree, so the
	// stream must stop and the follower be re-bootstrapped; nothing is applied
	// and the applied LSN does not advance.
	ErrReplicationDivergence = errors.New("engine: replicated transaction diverges from the locally applied transaction")
	// ErrReplicationPromoted: the node was promoted to leader; it no longer
	// accepts replicated writes.
	ErrReplicationPromoted = errors.New("engine: node was promoted; replicated transactions are no longer applied")
)

// ensureReplCollection returns the named collection, creating it first when
// this follower has not seen it yet (so collections created on the leader
// replicate automatically), tolerating a concurrent creation.
func (db *DB) ensureReplCollection(name string) (*Collection, error) {
	col, err := db.Collection(name)
	if err == nil {
		return col, nil
	}
	col, err = db.CreateCollection(name)
	if err != nil {
		if col2, err2 := db.Collection(name); err2 == nil {
			return col2, nil
		}
		return nil, err
	}
	return col, nil
}

// ApplyReplicatedTx applies one replicated cross-collection transaction (a
// protocol v2 envelope delivered at lsn) to this follower atomically, through
// the same durable coordinator protocol as a leader commit: stamped prepare
// runs on every participant, fsync, then one journal COMMIT that is also the
// binding of the transaction to lsn (the COMMIT record carries lsn, the
// leader's txid and the envelope digest). It is NOT a loop of ApplyReplication.
//
// Semantics:
//
//   - Ids, revisions, timestamps, expiries and encryption epochs are the
//     leader's, verbatim. Unique constraints are not re-checked.
//   - Participant collections missing locally are created first (as legacy
//     apply does). Creating an empty collection is harmless if the apply then
//     fails.
//   - All participant locks are held, in canonical order, from before PREPARE
//     until every index is updated, so readers see none or all of the
//     transaction. It works on a read-only follower.
//   - The applied LSN advances to lsn only after the transaction is durable
//     (journal COMMIT fsynced) and visible. If the process dies after the
//     COMMIT but before replication.json is written, recovery re-derives the
//     applied LSN from the journal binding (see recoverCoordinator).
//   - The follower's own broker is not touched: a replicated transaction is not
//     a new leader LSN. A later Promote continues from the applied LSN.
//
// The returned bool is true when the transaction was applied now, false when it
// was already present (redelivery, or contained in the bootstrap snapshot): in
// both cases nil means the caller may advance to lsn. A redelivery whose
// content differs from what was applied is ErrReplicationDivergence.
func (db *DB) ApplyReplicatedTx(lsn uint64, env *TxEnvelope) (bool, error) {
	if env == nil || lsn == 0 {
		return false, fmt.Errorf("%w: missing envelope or lsn", ErrEnvelopeInvalid)
	}
	if err := env.Verify(); err != nil {
		return false, err
	}

	// replApplyMu serializes replicated applies and is what Promote waits on,
	// so a promotion either excludes this transaction or sees it complete.
	db.replApplyMu.Lock()
	defer db.replApplyMu.Unlock()
	if db.promoted.Load() {
		return false, ErrReplicationPromoted
	}

	applied, err := db.applyReplicatedTxLocked(lsn, env)
	if err != nil {
		return false, err
	}
	db.xtxHookAt(xtxHookReplApplied, env.Key, env.TxID)
	// Best-effort persistence: the journal binding already makes the position
	// recoverable, so a lost write only costs a harmless redelivery.
	_ = db.SetAppliedLSN(lsn)
	return applied, nil
}

func (db *DB) applyReplicatedTxLocked(lsn uint64, env *TxEnvelope) (bool, error) {
	parts := env.Participants()
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = p.Collection
	}
	for _, n := range names {
		if err := db.checkParticipantName(n); err != nil {
			return false, xtxErr(env.TxID, err, nil)
		}
	}

	// Create missing participants and any mandatory _key index before taking
	// locks (both take collection locks themselves).
	keyed := map[string]bool{}
	for _, op := range env.Ops {
		if op.Entry.Data != nil {
			if _, ok := op.Entry.Data[KeyField]; ok {
				keyed[op.Collection] = true
			}
		}
	}
	for _, name := range names {
		col, err := db.ensureReplCollection(name)
		if err != nil {
			return false, fmt.Errorf("apply replicated tx %s: collection %q: %w", env.TxID, name, err)
		}
		if keyed[name] {
			if err := col.ensureKeyIndex(); err != nil {
				return false, err
			}
		}
	}

	// An online snapshot must not straddle the transaction.
	db.xtxSnapshotMu.RLock()
	defer db.xtxSnapshotMu.RUnlock()

	j, err := db.ensureXTxJournal()
	if err != nil {
		return false, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	write := make(map[string]bool, len(names))
	for _, n := range names {
		write[n] = true
	}
	cols, unlock, err := db.lockXTxCollections(context.Background(), env.Key, append([]string(nil), names...), write)
	if err != nil {
		return false, err
	}
	unlocked := false
	release := func() {
		if !unlocked {
			unlocked = true
			unlock()
		}
	}
	defer release()
	db.xtxHookAt(xtxHookLocked, env.Key, env.TxID)

	// Idempotency / divergence, under the participant locks.
	digestHex := hex.EncodeToString(env.Digest[:])
	if d, ok := j.boundDecision(lsn, env.TxID); ok && d.Outcome == xtxKindCommit {
		if d.RL == lsn && d.RT == env.TxID && d.RD == digestHex {
			return false, nil
		}
		return false, xtxErr(d.Tx, ErrReplicationDivergence, fmt.Errorf(
			"tx %s lsn %d digest %s redelivered as tx %s lsn %d digest %s",
			d.RT, d.RL, d.RD, env.TxID, lsn, digestHex))
	}
	// A transaction already contained in the bootstrap snapshot carries the
	// leader's own txid (the snapshot ships the leader's journal).
	if d, ok := j.decision(env.TxID); ok && d.Outcome == xtxKindCommit && d.RL == 0 {
		if sameParts(d.Parts, envelopeParts(env)) {
			return false, nil
		}
		return false, xtxErr(d.Tx, ErrReplicationDivergence, fmt.Errorf(
			"tx %s from the bootstrap snapshot differs from the replicated envelope at lsn %d", env.TxID, lsn))
	}

	// Revision-level idempotency: drop operations whose effect is already
	// present (snapshot overlap or a newer superseding write), exactly as the
	// single-entry apply path does. What remains is applied atomically.
	runs := make([]xtxRun, 0, len(parts))
	type opSpec struct {
		col    *Collection
		entry  store.Entry
		exists bool
	}
	specsByCol := make(map[string][]opSpec, len(parts))
	for _, op := range env.Ops {
		c := cols[op.Collection]
		e := op.Entry
		cur, exists := c.index.Get(e.ID)
		switch e.Op {
		case store.OpDelete:
			if !exists {
				continue
			}
		case store.OpInsert, store.OpUpdate:
			if exists && cur.Rev >= e.Rev {
				continue
			}
		}
		specsByCol[op.Collection] = append(specsByCol[op.Collection], opSpec{col: c, entry: e, exists: exists})
	}
	if len(specsByCol) == 0 {
		return false, nil // everything already present
	}
	var participants []*Collection
	for _, p := range parts {
		specs := specsByCol[p.Collection]
		if len(specs) == 0 {
			continue
		}
		c := cols[p.Collection]
		if c.active.poisoned != nil {
			return false, xtxErr(env.TxID, ErrXTxDurability, fmt.Errorf("%w: %s: %w", ErrSegmentPoisoned, c.name, c.active.poisoned))
		}
		entries := make([]stampedEntry, len(specs))
		for i, s := range specs {
			ts := s.entry.Ts
			if ts.IsZero() {
				ts = time.Now()
			}
			entries[i] = stampedEntry{
				ID:        s.entry.ID,
				Op:        s.entry.Op,
				Ts:        ts.UTC().Format(time.RFC3339Nano),
				Rev:       s.entry.Rev,
				Data:      s.entry.Data,
				Epoch:     s.entry.Epoch,
				ExpiresAt: s.entry.ExpiresAt,
			}
		}
		runs = append(runs, xtxRun{col: c, entries: entries})
		participants = append(participants, c)
	}

	txid, err := j.allocateTxID(env.Key)
	if err != nil {
		return false, err
	}
	bind := &xtxReplBind{LSN: lsn, LeadTx: env.TxID, Digest: digestHex}
	offsets, err := db.xtxPrepareAndDecide(j, env.Key, txid, runs, bind)
	if err != nil {
		return false, err
	}

	// S5: apply to the in-memory indexes while every participant lock is held.
	var events []struct {
		col   *Collection
		event WatchEvent
	}
	needsRotate := map[string]bool{}
	bumped := map[string]bool{}
	for _, c := range participants {
		for i, s := range specsByCol[c.name] {
			e := s.entry
			off := offsets[c.name][i]
			switch e.Op {
			case store.OpInsert, store.OpUpdate:
				c.index.Set(e.ID, IndexEntry{SegmentPath: c.active.Path(), Offset: off, Rev: e.Rev, ExpiresAt: e.ExpiresAt, Epoch: e.Epoch})
				if s.exists {
					c.sidxUpdateEntry(e.ID, e.Data)
				} else {
					c.sidxIndexEntry(e.ID, e.Data)
				}
			case store.OpDelete:
				c.index.Delete(e.ID)
				c.sidxRemoveEntry(e.ID)
			}
			// Keep the id counter ahead of every applied id so a promoted
			// follower never reuses an id the leader assigned.
			for {
				old := c.idSeq.Load()
				if e.ID <= old || c.idSeq.CompareAndSwap(old, e.ID) {
					break
				}
			}
			bumped[c.name] = true
			events = append(events, struct {
				col   *Collection
				event WatchEvent
			}{c, WatchEvent{Op: e.Op, ID: e.ID, Data: e.Data, Ts: e.Ts}})
		}
		if c.active.Size() >= c.cfg.SegmentMaxSize {
			needsRotate[c.name] = true
		}
	}

	// Visible: release the participant locks, then run S6 maintenance.
	release()
	for _, ev := range events {
		ev.col.emit(ev.event)
	}
	for _, c := range participants {
		if needsRotate[c.name] {
			_ = c.rotateSegment()
		}
		if bumped[c.name] {
			// The batch is applied and durable, so apply must not report
			// failure (the leader would re-ship it). The counter is
			// re-derived from the segment tail on reopen; log and go on.
			if err := c.writeMeta(nil); err != nil {
				if l := c.cfg.Logger; l != nil {
					l.Error("replication apply: persist meta failed", "collection", c.name, "err", err)
				}
			}
		}
	}
	return true, nil
}

// envelopeParts computes the journal participant list (collection, run length,
// run digest) of a whole envelope, the shape a leader commit recorded.
func envelopeParts(env *TxEnvelope) []xtxPart {
	var out []xtxPart
	var refs []xtxOpRef
	cur := ""
	flush := func() {
		if cur != "" {
			out = append(out, xtxPart{C: cur, N: uint32(len(refs)), D: partDigest(refs)})
		}
		refs = nil
	}
	for _, op := range env.Ops {
		if op.Collection != cur {
			flush()
			cur = op.Collection
		}
		refs = append(refs, xtxOpRef{ID: op.Entry.ID, Op: string(op.Entry.Op), Rev: op.Entry.Rev})
	}
	flush()
	return out
}

func sameParts(a, b []xtxPart) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
