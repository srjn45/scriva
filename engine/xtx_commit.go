package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/srjn45/scriva/store"
)

// XTxOp represents one operation within a cross-collection transaction.
type XTxOp struct {
	Collection  string         `json:"collection"`
	Op          store.Op       `json:"op"`
	ID          uint64         `json:"id"`
	Data        map[string]any `json:"data,omitempty"`
	ExpectedRev uint64         `json:"expected_rev,omitempty"`
	ExpiresAt   int64          `json:"expires_at,omitempty"`
}

// XTxOpResult is the committed outcome of one operation.
type XTxOpResult struct {
	Collection string   `json:"collection"`
	ID         uint64   `json:"id"`
	Rev        uint64   `json:"rev"`
	Op         store.Op `json:"op"`
}

// XTxResult is the result returned by a committed cross-collection transaction (§8.1).
type XTxResult struct {
	TxID string        `json:"tx_id"`
	Ops  []XTxOpResult `json:"ops"`
}

// CommitXTx executes a cross-collection transaction across 2..N collections of this DB.
// It acquires participants in canonical bytewise-ascending order, pre-validates all ops under
// the write locks, writes invisible stamped prepare entries, forces unconditional fsync on all
// participants (PREPARED), records a durable COMMIT in the coordinator journal (COMMITTED),
// applies all runs to in-memory indexes, and returns idempotently (§6, §8).
func (db *DB) CommitXTx(key string, ops []XTxOp) (*XTxResult, error) {
	if len(ops) == 0 {
		return nil, errors.New("engine: cannot commit empty transaction")
	}
	if len(key) > xtxMaxKeyLen {
		return nil, xtxErr("", ErrXTxTooLarge, fmt.Errorf("idempotency key longer than %d bytes", xtxMaxKeyLen))
	}
	if db.IsFollower() {
		return nil, xtxErr("", ErrReadOnly, errors.New("cannot commit transaction on read-only follower"))
	}

	// Group and validate unique collections and operations
	distinctCols := make(map[string]bool)
	seenID := make(map[string]map[uint64]bool)
	var approxBytes int64

	for _, op := range ops {
		if op.Collection == "" {
			return nil, errors.New("engine: operation has empty collection name")
		}
		if isReservedXTxName(op.Collection) {
			return nil, xtxErr("", ErrReservedName, fmt.Errorf("collection %q uses reserved prefix", op.Collection))
		}
		distinctCols[op.Collection] = true
		colSeen := seenID[op.Collection]
		if colSeen == nil {
			colSeen = make(map[uint64]bool)
			seenID[op.Collection] = colSeen
		}
		if colSeen[op.ID] {
			return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("%w: collection %q id %d", ErrXTxDuplicateOp, op.Collection, op.ID))
		}
		colSeen[op.ID] = true
		approxBytes += 128 + int64(len(op.Collection))
		if op.Data != nil {
			approxBytes += 256
		}
	}

	// Limit validation (§5.3)
	limits := defaultXTxLimits()
	if err := limits.check(len(ops), len(distinctCols), approxBytes); err != nil {
		return nil, err
	}

	// Idempotency check (§8.2)
	if key != "" {
		if err := db.beginXTxInFlight(key); err != nil {
			return nil, err
		}
		defer db.endXTxInFlight(key)

		db.xtxMu.Lock()
		j := db.xtxJournal
		db.xtxMu.Unlock()

		if j != nil {
			st, prevTx := j.statusByKey(key)
			if st == XTxCommitted {
				// Return committed result idempotently
				res := &XTxResult{TxID: prevTx}
				for _, op := range ops {
					res.Ops = append(res.Ops, XTxOpResult{Collection: op.Collection, ID: op.ID, Rev: 1, Op: op.Op})
				}
				return res, nil
			}
		}
	}

	// Ensure coordinator journal exists
	j, err := db.ensureXTxJournal()
	if err != nil {
		return nil, err
	}

	// S0: Acquire DB.mu.RLock and validate under participant locks
	db.mu.RLock()
	defer db.mu.RUnlock()

	partNames := make([]string, 0, len(distinctCols))
	for name := range distinctCols {
		partNames = append(partNames, name)
	}
	sort.Strings(partNames) // canonical bytewise-ascending order (§6.1)

	participants := make([]*Collection, len(partNames))
	for i, name := range partNames {
		col, ok := db.collections[name]
		if !ok {
			return nil, xtxErr("", ErrCollectionNotFound, fmt.Errorf("collection %q not found", name))
		}
		participants[i] = col
	}

	// Acquire participant locks in canonical order
	for _, c := range participants {
		c.mu.Lock()
	}
	defer func() {
		for i := len(participants) - 1; i >= 0; i-- {
			participants[i].mu.Unlock()
		}
	}()

	// Check if any participant segment is poisoned
	for _, c := range participants {
		if c.active.poisoned != nil {
			return nil, xtxErr("", ErrXTxDurability, fmt.Errorf("%w: %s: %w", ErrSegmentPoisoned, c.name, c.active.poisoned))
		}
	}

	opsByCol := make(map[string][]XTxOp)
	for _, op := range ops {
		opsByCol[op.Collection] = append(opsByCol[op.Collection], op)
	}

	computedRevs := make(map[string][]uint64)
	computedExps := make(map[string][]int64)
	sealedDatas := make(map[string][]map[string]any)
	epochs := make(map[string]uint64)

	for _, c := range participants {
		colOps := opsByCol[c.name]
		enc := c.enc.Load()
		var txEpoch uint64
		if enc != nil {
			txEpoch = enc.epoch
		}
		epochs[c.name] = txEpoch

		colRevs := make([]uint64, len(colOps))
		colExps := make([]int64, len(colOps))
		colSealed := make([]map[string]any, len(colOps))

		var txOps []txOp
		var newRecords uint64
		var newBytes int64

		for i, op := range colOps {
			switch op.Op {
			case store.OpInsert:
				colRevs[i] = 1
				exp := op.ExpiresAt
				if exp == 0 {
					exp = c.resolveInsertExpiry(time.Time{})
				}
				colExps[i] = exp
				colSealed[i] = op.Data
				if enc != nil && op.Data != nil {
					sd, err := enc.encrypt(op.Data)
					if err != nil {
						return nil, xtxErr("", ErrXTxConflict, err)
					}
					colSealed[i] = sd
				}
				txOps = append(txOps, txOp{kind: txOpInsert, id: op.ID, data: op.Data, ts: time.Now().UTC()})
				newRecords++
				if c.quotaEnabled() && c.cfg.MaxBytes > 0 {
					e := store.NewInsert(op.ID, op.Data)
					e.Rev = 1
					e.ExpiresAt = exp
					newBytes += c.entryQuotaBytes(e)
				}

			case store.OpUpdate:
				loc, ok := c.index.Get(op.ID)
				if !ok {
					return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q: update target id %d not found", c.name, op.ID))
				}
				if c.isExpired(loc) {
					return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q: update target id %d expired", c.name, op.ID))
				}
				if op.ExpectedRev != 0 && loc.Rev != op.ExpectedRev {
					return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q: id %d expected rev %d, actual %d", c.name, op.ID, op.ExpectedRev, loc.Rev))
				}
				colRevs[i] = loc.Rev + 1
				exp := op.ExpiresAt
				if exp == 0 {
					exp = loc.ExpiresAt
				}
				colExps[i] = exp
				colSealed[i] = op.Data
				if enc != nil && op.Data != nil {
					sd, err := enc.encrypt(op.Data)
					if err != nil {
						return nil, xtxErr("", ErrXTxConflict, err)
					}
					colSealed[i] = sd
				}
				txOps = append(txOps, txOp{kind: txOpUpdate, id: op.ID, data: op.Data, ts: time.Now().UTC()})

			case store.OpDelete:
				loc, ok := c.index.Get(op.ID)
				if !ok {
					return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q: delete target id %d not found", c.name, op.ID))
				}
				if c.isExpired(loc) {
					return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q: delete target id %d expired", c.name, op.ID))
				}
				if op.ExpectedRev != 0 && loc.Rev != op.ExpectedRev {
					return nil, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q: id %d expected rev %d, actual %d", c.name, op.ID, op.ExpectedRev, loc.Rev))
				}
				colRevs[i] = loc.Rev + 1
				colExps[i] = 0
				colSealed[i] = nil
				txOps = append(txOps, txOp{kind: txOpDelete, id: op.ID, ts: time.Now().UTC()})

			default:
				return nil, fmt.Errorf("unknown op %q", op.Op)
			}
		}

		// Check uniqueness across unique secondary indexes
		if err := c.txCheckUnique(txOps); err != nil {
			return nil, xtxErr("", ErrXTxConflict, err)
		}

		// Check quota
		if c.quotaEnabled() && newRecords > 0 {
			if err := c.checkQuotaLocked(newRecords, newBytes); err != nil {
				return nil, xtxErr("", ErrResourceExhausted, err)
			}
		}

		computedRevs[c.name] = colRevs
		computedExps[c.name] = colExps
		sealedDatas[c.name] = colSealed
	}

	// Allocate txid under journal.mu
	txid, err := j.allocateTxID(key)
	if err != nil {
		return nil, err
	}

	// S1: Append stamped runs to active segments
	startSizes := make(map[string]int64)
	offsets := make(map[string][]int64)
	parts := make([]xtxPart, 0, len(participants))
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)

	rollbackRuns := func() {
		for _, p := range participants {
			if sz, ok := startSizes[p.name]; ok {
				_ = p.active.rollback(sz)
			}
		}
	}

	for _, c := range participants {
		startSizes[c.name] = c.active.Size()
		colOps := opsByCol[c.name]
		colRevs := computedRevs[c.name]
		colExps := computedExps[c.name]
		colSealed := sealedDatas[c.name]
		txEpoch := epochs[c.name]
		n := uint32(len(colOps))
		opRefs := make([]xtxOpRef, n)
		colOffsets := make([]int64, n)

		for i, op := range colOps {
			se := stampedEntry{
				ID:        op.ID,
				Op:        op.Op,
				Ts:        nowStr,
				Rev:       colRevs[i],
				Data:      colSealed[i],
				Epoch:     txEpoch,
				ExpiresAt: colExps[i],
				Tx:        TxStamp{T: txid, I: uint32(i), N: n},
			}
			offset, err := c.active.AppendStamped(se)
			if err != nil {
				rollbackRuns()
				_ = j.abort(txid, key, "io")
				return nil, xtxErr(txid, ErrXTxDurability, err)
			}
			colOffsets[i] = offset
			opRefs[i] = xtxOpRef{ID: op.ID, Op: string(op.Op), Rev: colRevs[i]}
		}
		offsets[c.name] = colOffsets
		parts = append(parts, xtxPart{C: c.name, N: n, D: partDigest(opRefs)})
	}

	// S2: Fsync all participant active segments unconditionally
	for _, c := range participants {
		if err := c.active.Sync(); err != nil {
			c.active.poisoned = err
			rollbackRuns()
			_ = j.abort(txid, key, "io")
			return nil, xtxErr(txid, ErrXTxDurability, err)
		}
	}

	// S3 + S4: Commit record in coordinator journal and fsync
	if err := j.commitPrepared(txid, key, parts); err != nil {
		if errors.Is(err, ErrXTxOutcomeUnknown) {
			// S4 fsync failed: outcome is unknown to this process. Do NOT roll back runs.
			return nil, xtxErr(txid, ErrXTxOutcomeUnknown, err)
		}
		rollbackRuns()
		_ = j.abort(txid, key, "io")
		return nil, xtxErr(txid, ErrXTxDurability, err)
	}

	// S5: Apply runs to in-memory primary and secondary indexes
	var opResults []XTxOpResult
	var watchEvents []struct {
		col   *Collection
		event WatchEvent
	}
	maxInsertIDs := make(map[string]uint64)
	needsRotate := make(map[string]bool)

	for _, c := range participants {
		colOps := opsByCol[c.name]
		colRevs := computedRevs[c.name]
		colExps := computedExps[c.name]
		txEpoch := epochs[c.name]
		colOffsets := offsets[c.name]

		for i, op := range colOps {
			off := colOffsets[i]
			rev := colRevs[i]
			exp := colExps[i]

			switch op.Op {
			case store.OpInsert:
				c.index.Set(op.ID, IndexEntry{SegmentPath: c.active.Path(), Offset: off, Rev: rev, ExpiresAt: exp, Epoch: txEpoch})
				c.sidxIndexEntry(op.ID, op.Data)
				if op.ID > maxInsertIDs[c.name] {
					maxInsertIDs[c.name] = op.ID
				}
			case store.OpUpdate:
				c.index.Set(op.ID, IndexEntry{SegmentPath: c.active.Path(), Offset: off, Rev: rev, ExpiresAt: exp, Epoch: txEpoch})
				c.sidxUpdateEntry(op.ID, op.Data)
			case store.OpDelete:
				c.index.Delete(op.ID)
				c.sidxRemoveEntry(op.ID)
			}

			ts, _ := time.Parse(time.RFC3339Nano, nowStr)
			e := store.Entry{
				ID:        op.ID,
				Op:        op.Op,
				Ts:        ts,
				Rev:       rev,
				Data:      sealedDatas[c.name][i],
				Epoch:     txEpoch,
				ExpiresAt: exp,
			}
			c.publishCommit(e)
			watchEvents = append(watchEvents, struct {
				col   *Collection
				event WatchEvent
			}{col: c, event: WatchEvent{Op: op.Op, ID: op.ID, Data: op.Data, Ts: ts}})

			opResults = append(opResults, XTxOpResult{Collection: c.name, ID: op.ID, Rev: rev, Op: op.Op})
		}

		if maxInsertIDs[c.name] > 0 {
			for {
				cur := c.idSeq.Load()
				if maxInsertIDs[c.name] <= cur || c.idSeq.CompareAndSwap(cur, maxInsertIDs[c.name]) {
					break
				}
			}
		}
		if c.active.Size() >= c.cfg.SegmentMaxSize {
			needsRotate[c.name] = true
		}
	}

	// S6: Post-lock maintenance and notifications
	defer func() {
		for _, c := range participants {
			if needsRotate[c.name] {
				_ = c.rotateSegment()
			}
			if maxInsertIDs[c.name] > 0 {
				_ = persistMeta(filepath.Join(c.dir, metaFilename), c.metaSnapshot())
			}
		}
		for _, w := range watchEvents {
			w.col.emit(w.event)
		}
	}()

	return &XTxResult{TxID: txid, Ops: opResults}, nil
}
