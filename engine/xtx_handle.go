package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/srjn45/scriva/query"
	"github.com/srjn45/scriva/store"
)

// This file is the transaction handle: the staging and validation layer in
// front of the cross-collection coordinator (xtx_commit.go). A handle buffers
// writes and remembers what it observed; nothing reaches a segment, an index
// or the journal before Commit, and Commit goes through DB.commitXTx.
//
// Isolation, precisely (docs/design-cross-collection-transactions.md §17):
//
//   - Every point read records the document's state (absent, or present at a
//     revision). Staging an update/delete records the target's revision the
//     same way, as the write's base.
//   - Commit re-checks every recorded observation under the participant locks,
//     taken in canonical name order, and applies the writes atomically or not
//     at all. A changed observation is ErrXTxConflict.
//   - There are no predicate reads, so there is no phantom protection: scans
//     and index lookups are rejected with ErrXTxScanUnsupported.
//
// That is optimistic concurrency control over point reads and writes. It is
// serializable for transactions that touch documents only by id; it makes no
// claim about anything else.

// DefaultXTxIdleTimeout is the idle timeout of a transaction handle when
// CollectionConfig.XTxIdleTimeout is zero.
const DefaultXTxIdleTimeout = time.Minute

// Events reported through CollectionConfig.OnXTx.
const (
	XTxEventBegin    = "begin"
	XTxEventCommit   = "commit"   // committed (or replayed by idempotency key)
	XTxEventConflict = "conflict" // commit rejected by validation; nothing applied
	XTxEventAbort    = "abort"    // commit failed for another reason; nothing applied
	XTxEventUnknown  = "unknown"  // commit outcome unknown (ErrXTxOutcomeUnknown)
	XTxEventCanceled = "canceled" // commit canceled before prepare; handle still open
	XTxEventRollback = "rollback"
	XTxEventExpired  = "expired"
)

// XTxOptions configures one transaction handle.
type XTxOptions struct {
	// Key is the caller-chosen idempotency key (≤ 128 bytes, optional). A
	// commit whose key already committed returns the original outcome and
	// applies nothing; DB.XTxStatus(Key) resolves the outcome after a lost
	// response or a restart.
	Key string
	// IdleTimeout overrides CollectionConfig.XTxIdleTimeout for this handle.
	// Zero inherits; negative disables idle expiry.
	IdleTimeout time.Duration
	// MaxLifetime, when positive, expires the handle that long after Begin
	// regardless of activity.
	MaxLifetime time.Duration
}

type xtxState int8

const (
	xtxActive xtxState = iota
	xtxCommitting
	xtxCommitted
	xtxFailed     // commit rejected or aborted; nothing applied
	xtxRolledBack // explicit Rollback
	xtxExpired    // reaped
	xtxOutcomeUnknown
	xtxClosed // database closed underneath the handle
)

type xtxDocKey struct {
	col string
	id  uint64
}

// xtxObservation is what the handle saw of one stored document.
type xtxObservation struct {
	present bool
	rev     uint64
	write   bool // base revision of a staged update/delete
}

// XTx is an open cross-collection transaction on one DB. It is safe for
// concurrent use; operations on one handle are serialized.
type XTx struct {
	db           *DB
	id           string
	key          string
	participants []string // canonical (sorted) order
	partSet      map[string]struct{}
	idle         time.Duration // ≤ 0: no idle expiry
	maxLife      time.Duration // ≤ 0: no lifetime bound
	createdAt    time.Time

	mu         sync.Mutex
	state      xtxState
	lastUsed   time.Time
	finishedAt time.Time
	writes     map[xtxDocKey]*XTxOp
	order      []xtxDocKey // staging order; keys absent from writes are skipped
	observed   map[xtxDocKey]xtxObservation
	result     *XTxResult // committed
	err        error      // failed / outcome unknown
	txid       string     // coordinator txid, once one was allocated
}

// BeginXTx opens a transaction over the declared participant collections.
// Every later operation on the handle must name one of them; anything else is
// ErrXTxNotParticipant. Participants must exist (ErrCollectionNotFound) and
// are limited as in §5.3 (ErrXTxTooLarge). Beginning acquires no lock and
// writes nothing.
func (db *DB) BeginXTx(ctx context.Context, participants []string, opts XTxOptions) (*XTx, error) {
	if err := ctx.Err(); err != nil {
		return nil, xtxCanceled(err)
	}
	if len(participants) == 0 {
		return nil, xtxErr("", ErrXTxInvalid, errors.New("no participant collections declared"))
	}
	if len(opts.Key) > xtxMaxKeyLen {
		return nil, xtxErr("", ErrXTxTooLarge, fmt.Errorf("idempotency key longer than %d bytes", xtxMaxKeyLen))
	}
	set := make(map[string]struct{}, len(participants))
	for _, name := range participants {
		if name == "" {
			return nil, xtxErr("", ErrXTxInvalid, errors.New("empty participant collection name"))
		}
		if isReservedXTxName(name) {
			return nil, xtxErr("", ErrReservedName, fmt.Errorf("collection %q uses reserved prefix", name))
		}
		set[name] = struct{}{}
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	if limit := defaultXTxLimits().MaxParticipants; len(names) > limit {
		return nil, xtxErr("", ErrXTxTooLarge, fmt.Errorf("%d participants > limit %d", len(names), limit))
	}
	for _, name := range names {
		if _, err := db.xtxCollection(name); err != nil {
			return nil, err
		}
	}

	idle := opts.IdleTimeout
	if idle == 0 {
		idle = db.defaultCfg.XTxIdleTimeout
	}
	if idle == 0 {
		idle = DefaultXTxIdleTimeout
	}
	now := time.Now()
	x := &XTx{
		db:           db,
		id:           newTxID(),
		key:          opts.Key,
		participants: names,
		partSet:      set,
		idle:         idle,
		maxLife:      opts.MaxLifetime,
		createdAt:    now,
		lastUsed:     now,
		writes:       make(map[xtxDocKey]*XTxOp),
		observed:     make(map[xtxDocKey]xtxObservation),
	}
	if err := db.xtxHandles.add(db, x); err != nil {
		return nil, err
	}
	db.xtxEvent(XTxEventBegin, 0)
	return x, nil
}

// XTxHandle returns the open or recently finished transaction handle with the
// given id (XTx.ID), or ErrXTxHandleNotFound. A finished handle stays
// resolvable for one idle timeout so a retried Commit or Rollback addressed
// to it still gets the original outcome.
func (db *DB) XTxHandle(id string) (*XTx, error) {
	if x := db.xtxHandles.get(id); x != nil {
		return x, nil
	}
	return nil, xtxErr("", ErrXTxHandleNotFound, fmt.Errorf("handle %q", id))
}

func (db *DB) xtxCollection(name string) (*Collection, error) {
	db.mu.RLock()
	col, ok := db.collections[name]
	db.mu.RUnlock()
	if !ok {
		return nil, xtxErr("", ErrCollectionNotFound, fmt.Errorf("collection %q not found", name))
	}
	return col, nil
}

func (db *DB) xtxEvent(event string, since time.Duration) {
	if h := db.defaultCfg.OnXTx; h != nil {
		h(event, since)
	}
}

// ID is the handle id: a process-local name for this open transaction. It is
// not the coordinator txid (assigned at commit, see XTxResult.TxID) and does
// not survive a restart; use the idempotency key for that.
func (x *XTx) ID() string { return x.id }

// Key is the idempotency key the transaction was begun with, if any.
func (x *XTx) Key() string { return x.key }

// Participants returns the declared collections in canonical order.
func (x *XTx) Participants() []string {
	return append([]string(nil), x.participants...)
}

// usable checks that the handle can take another operation and expires it if
// its time has passed. Caller holds x.mu.
func (x *XTx) usable(now time.Time) error {
	if x.state == xtxActive && x.expiredAt(now) {
		x.finish(xtxExpired, now)
	}
	return x.stateErr()
}

// stateErr is the error for operating on a handle in its current state, nil
// when active. Caller holds x.mu.
func (x *XTx) stateErr() error {
	switch x.state {
	case xtxActive:
		return nil
	case xtxCommitting:
		return xtxErr(x.txid, ErrXTxInProgress, errors.New("commit in progress"))
	case xtxExpired:
		return xtxErr("", ErrXTxExpired, nil)
	case xtxOutcomeUnknown:
		return x.err
	case xtxCommitted:
		return xtxErr(x.txid, ErrXTxFinished, errors.New("committed"))
	case xtxRolledBack:
		return xtxErr("", ErrXTxFinished, errors.New("rolled back"))
	case xtxClosed:
		return xtxErr("", ErrXTxFinished, errors.New("database closed"))
	default:
		return xtxErr(x.txid, ErrXTxFinished, fmt.Errorf("commit failed: %w", x.err))
	}
}

func (x *XTx) expiredAt(now time.Time) bool {
	if x.idle > 0 && now.Sub(x.lastUsed) > x.idle {
		return true
	}
	return x.maxLife > 0 && now.Sub(x.createdAt) > x.maxLife
}

// finish moves the handle to a final state and releases everything it holds:
// the staged writes and the read set. A handle holds no locks between calls,
// so there is nothing else to release. Caller holds x.mu.
func (x *XTx) finish(state xtxState, now time.Time) {
	x.state = state
	x.finishedAt = now
	x.writes = nil
	x.order = nil
	x.observed = nil
}

func (x *XTx) participant(collection string) (*Collection, error) {
	if _, ok := x.partSet[collection]; !ok {
		return nil, xtxErr("", ErrXTxNotParticipant, fmt.Errorf("collection %q", collection))
	}
	return x.db.xtxCollection(collection)
}

// observe returns the handle's observation of a stored document, taking one
// from the collection's primary index the first time. Caller holds x.mu.
func (x *XTx) observe(col *Collection, k xtxDocKey) xtxObservation {
	if obs, ok := x.observed[k]; ok {
		return obs
	}
	rev, present := col.liveRev(k.id)
	obs := xtxObservation{present: present, rev: rev}
	x.observed[k] = obs
	return obs
}

func xtxDocNotFound(k xtxDocKey) error {
	return xtxErr("", ErrXTxDocNotFound, fmt.Errorf("collection %q id %d", k.col, k.id))
}

// Insert stages an insert into collection and returns the id reserved for the
// new document. The id is provisional until Commit succeeds (§5.2).
func (x *XTx) Insert(collection string, data map[string]any) (uint64, error) {
	return x.Stage(XTxOp{Collection: collection, Op: store.OpInsert, Data: data})
}

// Update stages a full replacement of document id in collection.
func (x *XTx) Update(collection string, id uint64, data map[string]any) error {
	_, err := x.Stage(XTxOp{Collection: collection, Op: store.OpUpdate, ID: id, Data: data})
	return err
}

// Delete stages the deletion of document id in collection.
func (x *XTx) Delete(collection string, id uint64) error {
	_, err := x.Stage(XTxOp{Collection: collection, Op: store.OpDelete, ID: id})
	return err
}

// Stage buffers one operation in the handle and returns the document id it
// applies to. Nothing is written and no other reader can see it before Commit.
//
//   - Insert: op.ID must be zero; an id is reserved and returned.
//   - Update/Delete of a stored document: the document's current revision is
//     recorded as the write's base (unless the handle already read it) and
//     re-checked at Commit, so a concurrent change is a write-write conflict.
//     A target absent in the handle's view is ErrXTxDocNotFound. A non-zero
//     op.ExpectedRev that differs from the observed revision is ErrXTxConflict
//     immediately.
//   - Operations on a document this handle already staged fold into one net
//     operation (insert+update = insert, insert+delete = nothing,
//     update+delete = delete).
//
// A rejected Stage leaves the handle open and unchanged.
func (x *XTx) Stage(op XTxOp) (uint64, error) {
	col, err := x.participant(op.Collection)
	if err != nil {
		return 0, err
	}
	switch op.Op {
	case store.OpInsert:
		if op.ID != 0 {
			return 0, xtxErr("", ErrXTxInvalid, errors.New("insert must not carry an id; one is reserved by the transaction"))
		}
		if op.ExpectedRev != 0 {
			return 0, xtxErr("", ErrXTxInvalid, errors.New("insert must not carry an expected revision"))
		}
	case store.OpUpdate, store.OpDelete:
		if op.ID == 0 {
			return 0, xtxErr("", ErrXTxInvalid, fmt.Errorf("%s requires a document id", op.Op))
		}
	default:
		return 0, xtxErr("", ErrXTxInvalid, fmt.Errorf("unknown op %q", op.Op))
	}
	if op.Op == store.OpDelete {
		op.Data = nil
	} else {
		op.Data = cloneXTxData(op.Data)
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	now := time.Now()
	if err := x.usable(now); err != nil {
		return 0, err
	}
	x.lastUsed = now

	if op.Op == store.OpInsert {
		if len(x.writes) >= defaultXTxLimits().MaxOps {
			return 0, xtxErr("", ErrXTxTooLarge, fmt.Errorf("more than %d staged ops", defaultXTxLimits().MaxOps))
		}
		op.ID = col.ReserveID()
		k := xtxDocKey{op.Collection, op.ID}
		x.writes[k] = &op
		x.order = append(x.order, k)
		return op.ID, nil
	}

	k := xtxDocKey{op.Collection, op.ID}
	if w := x.writes[k]; w != nil {
		switch w.Op {
		case store.OpDelete:
			return 0, xtxDocNotFound(k)
		case store.OpInsert:
			if op.ExpectedRev != 0 {
				return 0, xtxErr("", ErrXTxConflict, fmt.Errorf("collection %q id %d is an uncommitted insert of this transaction and has no revision", k.col, k.id))
			}
			if op.Op == store.OpDelete {
				delete(x.writes, k) // net effect: nothing
				return op.ID, nil
			}
			w.Data = op.Data
			if op.ExpiresAt != 0 {
				w.ExpiresAt = op.ExpiresAt
			}
		default: // staged update
			if op.ExpectedRev != 0 && op.ExpectedRev != w.ExpectedRev {
				return 0, x.stageRevConflict(k, op.ExpectedRev)
			}
			w.Op, w.Data, w.ExpiresAt = op.Op, op.Data, op.ExpiresAt
		}
		return op.ID, nil
	}

	obs := x.observe(col, k)
	if !obs.present {
		return 0, xtxDocNotFound(k)
	}
	if op.ExpectedRev != 0 && op.ExpectedRev != obs.rev {
		return 0, x.stageRevConflict(k, op.ExpectedRev)
	}
	if len(x.writes) >= defaultXTxLimits().MaxOps {
		return 0, xtxErr("", ErrXTxTooLarge, fmt.Errorf("more than %d staged ops", defaultXTxLimits().MaxOps))
	}
	obs.write = true
	x.observed[k] = obs
	op.ExpectedRev = obs.rev
	x.writes[k] = &op
	x.order = append(x.order, k)
	return op.ID, nil
}

// stageRevConflict reports a caller-supplied expected revision that does not
// match the handle's observation. Caller holds x.mu.
func (x *XTx) stageRevConflict(k xtxDocKey, expected uint64) error {
	obs := x.observed[k]
	return xtxErr("", ErrXTxConflict, &XTxConflictError{
		Collection: k.col, ID: k.id, Write: true,
		ExpectedPresent: true, ExpectedRev: expected,
		ActualPresent: obs.present, ActualRev: obs.rev,
	})
}

// Get is a point read through the transaction.
//
// It sees the handle's own staged writes first (read-your-writes): a staged
// insert or update returns the staged data, a staged delete is
// ErrXTxDocNotFound. Record.Rev is then the committed revision the write is
// based on (zero for a staged insert), because the new revision is only
// assigned at Commit.
//
// Otherwise it reads the committed document and records what it saw — present
// at a revision, or absent (ErrXTxDocNotFound) — in the read set that Commit
// validates. Reading a document twice is repeatable: if its committed state
// changed since the first read, Get returns ErrXTxConflict instead of the new
// value, and Commit would fail the same way.
func (x *XTx) Get(collection string, id uint64) (Record, error) {
	col, err := x.participant(collection)
	if err != nil {
		return Record{}, err
	}
	k := xtxDocKey{collection, id}

	x.mu.Lock()
	defer x.mu.Unlock()
	now := time.Now()
	if err := x.usable(now); err != nil {
		return Record{}, err
	}
	x.lastUsed = now

	if w := x.writes[k]; w != nil {
		if w.Op == store.OpDelete {
			return Record{}, xtxDocNotFound(k)
		}
		data := cloneXTxData(w.Data)
		key, _ := data[KeyField].(string)
		return Record{ID: id, Key: key, Rev: w.ExpectedRev, Data: data}, nil
	}

	rec, present, err := col.getIfPresent(id)
	if err != nil {
		return Record{}, err
	}
	if prev, seen := x.observed[k]; seen {
		if prev.present != present || prev.rev != rec.Rev {
			return Record{}, xtxErr("", ErrXTxConflict, &XTxConflictError{
				Collection: collection, ID: id, Write: prev.write,
				ExpectedPresent: prev.present, ExpectedRev: prev.rev,
				ActualPresent: present, ActualRev: rec.Rev,
			})
		}
	} else {
		x.observed[k] = xtxObservation{present: present, rev: rec.Rev}
	}
	if !present {
		return Record{}, xtxDocNotFound(k)
	}
	return rec, nil
}

// Scan is rejected: a transaction supports point reads only. A scan is a
// predicate read, and nothing validates predicates (phantoms) at commit.
func (x *XTx) Scan(collection string, _ query.Filter) ([]ScanResult, error) {
	return nil, x.rejectScan(collection, "scan")
}

// IndexLookup is rejected for the same reason as Scan.
func (x *XTx) IndexLookup(collection, field, _ string) ([]uint64, error) {
	return nil, x.rejectScan(collection, "index lookup on "+field)
}

func (x *XTx) rejectScan(collection, what string) error {
	if _, ok := x.partSet[collection]; !ok {
		return xtxErr("", ErrXTxNotParticipant, fmt.Errorf("collection %q", collection))
	}
	return xtxErr("", ErrXTxScanUnsupported, fmt.Errorf("%s of collection %q", what, collection))
}

// Commit validates the transaction and applies it atomically.
//
// Under the locks of every touched collection, taken in canonical name order
// (so transactions touching the same collections in opposite orders cannot
// deadlock), it re-checks every recorded point read and write base. If any
// changed, Commit returns ErrXTxConflict (with an *XTxConflictError cause),
// nothing is applied anywhere, and the handle is finished. Otherwise the
// staged writes go through the cross-collection coordinator (§6) and become
// visible together. A transaction with no staged writes only validates its
// read set and returns an empty result.
//
// ctx is honoured while waiting for locks, before prepare: a cancelled wait
// returns ErrXTxCanceled (matching the context error too), nothing is
// written, and the handle stays open — Commit may be retried or the handle
// rolled back. Once prepare starts ctx is ignored and Commit returns the real
// outcome. The one ambiguous result is ErrXTxOutcomeUnknown (the decision
// record could not be confirmed durable); resolve it with DB.XTxStatus.
//
// Commit is idempotent: on a committed handle it returns the original result,
// and a handle whose idempotency key already committed (by an earlier handle,
// or before a restart) returns that outcome with Replayed set and applies
// nothing.
func (x *XTx) Commit(ctx context.Context) (*XTxResult, error) {
	x.mu.Lock()
	now := time.Now()
	if x.state == xtxCommitted {
		res := x.result.clone()
		x.mu.Unlock()
		return res, nil
	}
	if err := x.usable(now); err != nil {
		expired := x.state == xtxExpired && x.finishedAt.Equal(now)
		x.mu.Unlock()
		if expired {
			x.db.xtxEvent(XTxEventExpired, now.Sub(x.createdAt))
		}
		return nil, err
	}
	x.state = xtxCommitting
	ops := make([]XTxOp, 0, len(x.writes))
	for _, k := range x.order {
		if w := x.writes[k]; w != nil {
			ops = append(ops, *w)
		}
	}
	reads := make([]xtxReadCheck, 0, len(x.observed))
	for k, obs := range x.observed {
		reads = append(reads, xtxReadCheck{Collection: k.col, ID: k.id, Present: obs.present, Rev: obs.rev, Write: obs.write})
	}
	x.mu.Unlock()
	// Deterministic validation order, so the reported conflict is stable.
	sort.Slice(reads, func(a, b int) bool {
		if reads[a].Collection != reads[b].Collection {
			return reads[a].Collection < reads[b].Collection
		}
		return reads[a].ID < reads[b].ID
	})

	var res *XTxResult
	var err error
	if len(ops) == 0 {
		res, err = &XTxResult{}, x.db.validateXTxReadSet(ctx, reads)
	} else {
		res, err = x.db.commitXTx(ctx, x.key, ops, reads)
	}

	x.mu.Lock()
	now = time.Now()
	event := XTxEventCommit
	switch {
	case err == nil:
		x.result = res
		x.txid = res.TxID
		x.finish(xtxCommitted, now)
		res = res.clone()
	case errors.Is(err, ErrXTxCanceled), errors.Is(err, ErrXTxInProgress):
		// Nothing was written: the handle is still good.
		x.state = xtxActive
		x.lastUsed = now
		event = XTxEventCanceled
	default:
		var xe *XTxError
		if errors.As(err, &xe) {
			x.txid = xe.Tx
		}
		x.err = err
		switch {
		case errors.Is(err, ErrXTxOutcomeUnknown):
			x.finish(xtxOutcomeUnknown, now)
			event = XTxEventUnknown
		case errors.Is(err, ErrXTxConflict):
			x.finish(xtxFailed, now)
			event = XTxEventConflict
		default:
			x.finish(xtxFailed, now)
			event = XTxEventAbort
		}
	}
	x.mu.Unlock()
	x.db.xtxEvent(event, now.Sub(x.createdAt))
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Rollback discards everything the handle staged. It is idempotent and safe
// after a failed Commit, an expiry, or a previous Rollback (all return nil:
// nothing of the transaction is or will become visible).
//
// It cannot undo a decision: on a committed handle it returns ErrXTxFinished,
// while a Commit is executing it returns ErrXTxInProgress, and after an
// ErrXTxOutcomeUnknown commit it returns that error again, because the
// transaction may have committed.
func (x *XTx) Rollback() error {
	x.mu.Lock()
	now := time.Now()
	switch x.state {
	case xtxActive:
		x.finish(xtxRolledBack, now)
		x.mu.Unlock()
		x.db.xtxEvent(XTxEventRollback, now.Sub(x.createdAt))
		return nil
	case xtxFailed, xtxRolledBack, xtxExpired, xtxClosed:
		x.mu.Unlock()
		return nil
	}
	err := x.stateErr()
	x.mu.Unlock()
	return err
}

// validateXTxReadSet is the commit of a transaction that staged no writes: it
// checks the read set under read locks taken in canonical order. Nothing is
// written and no txid is allocated.
func (db *DB) validateXTxReadSet(ctx context.Context, reads []xtxReadCheck) error {
	if len(reads) == 0 {
		return nil
	}
	if err := lockCtx(ctx, db.mu.TryRLock, db.mu.RLock, db.mu.RUnlock); err != nil {
		return xtxCanceled(err)
	}
	defer db.mu.RUnlock()

	seen := make(map[string]bool)
	var names []string
	for _, rc := range reads {
		if !seen[rc.Collection] {
			seen[rc.Collection] = true
			names = append(names, rc.Collection)
		}
	}
	cols, unlock, err := db.lockXTxCollections(ctx, names, nil)
	if err != nil {
		return err
	}
	defer unlock()
	return validateXTxReads(cols, reads)
}

// liveRev returns the committed revision of id, or false when the document is
// absent or expired.
func (c *Collection) liveRev(id uint64) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	loc, ok := c.index.Get(id)
	if !ok || c.isExpired(loc) {
		return 0, false
	}
	return loc.Rev, true
}

// getIfPresent is Get with absence reported as a value rather than an error,
// so a transaction can record "absent" in its read set and still tell it
// apart from an I/O or integrity failure.
func (c *Collection) getIfPresent(id uint64) (Record, bool, error) {
	for attempt := 0; ; attempt++ {
		if _, ok := c.liveRev(id); !ok {
			return Record{}, false, nil
		}
		rec, err := c.Get(id)
		if err == nil {
			return rec, true, nil
		}
		// Deleted or expired between the presence check and the read?
		if _, ok := c.liveRev(id); ok && attempt >= 2 {
			return Record{}, false, err
		}
	}
}

func (r *XTxResult) clone() *XTxResult {
	if r == nil {
		return nil
	}
	c := *r
	c.Ops = append([]XTxOpResult(nil), r.Ops...)
	return &c
}

// cloneXTxData deep-copies a JSON-shaped document so later mutation by the
// caller cannot reach a staged write (or the other way round).
func cloneXTxData(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneXTxValue(v)
	}
	return out
}

func cloneXTxValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneXTxData(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneXTxValue(e)
		}
		return out
	default:
		return v
	}
}

// --- handle registry and reaper ----------------------------------------------

// xtxResultCacheSize bounds the in-memory per-key results kept so an
// idempotent replay can return the original per-op revisions.
const xtxResultCacheSize = 4096

// xtxHandleRegistry owns the DB's transaction handles. Its mutex is a leaf: it
// is never held while taking a handle's mutex in the other direction, DB.mu or
// any collection lock.
type xtxHandleRegistry struct {
	mu      sync.Mutex
	handles map[string]*XTx
	closed  bool
	stop    chan struct{}
	done    chan struct{}

	results     map[string]*XTxResult // by idempotency key
	resultOrder []string
}

func (r *xtxHandleRegistry) add(db *DB, x *XTx) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return xtxErr("", ErrXTxFinished, errors.New("database closed"))
	}
	if r.handles == nil {
		r.handles = make(map[string]*XTx)
	}
	r.handles[x.id] = x
	if r.stop == nil {
		if interval := xtxSweepInterval(db.defaultCfg.XTxIdleTimeout); interval > 0 {
			r.stop = make(chan struct{})
			r.done = make(chan struct{})
			go r.sweepLoop(db, interval, r.stop, r.done)
		}
	}
	return nil
}

// xtxSweepInterval derives the reaper cadence from the DB-wide idle timeout.
// Expiry itself is exact (checked on every handle operation); the reaper only
// bounds how long an abandoned handle keeps its buffers.
func xtxSweepInterval(idle time.Duration) time.Duration {
	if idle < 0 {
		return 0
	}
	if idle == 0 {
		idle = DefaultXTxIdleTimeout
	}
	return max(min(idle/2, maxSweepInterval), time.Millisecond)
}

func (r *xtxHandleRegistry) sweepLoop(db *DB, interval time.Duration, stop, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			db.reapXTx(now)
		}
	}
}

func (r *xtxHandleRegistry) get(id string) *XTx {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handles[id]
}

func (r *xtxHandleRegistry) snapshot() []*XTx {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*XTx, 0, len(r.handles))
	for _, x := range r.handles {
		out = append(out, x)
	}
	return out
}

func (r *xtxHandleRegistry) remove(ids []string) {
	if len(ids) == 0 {
		return
	}
	r.mu.Lock()
	for _, id := range ids {
		delete(r.handles, id)
	}
	r.mu.Unlock()
}

// reapXTx expires every handle that is idle or past its lifetime at now,
// releasing what it staged, and forgets handles that finished more than one
// retention period ago. It returns the ids of the handles it expired.
func (db *DB) reapXTx(now time.Time) []string {
	var expired, forget []string
	for _, x := range db.xtxHandles.snapshot() {
		x.mu.Lock()
		switch x.state {
		case xtxCommitting:
		case xtxActive:
			if x.expiredAt(now) {
				x.finish(xtxExpired, now)
				expired = append(expired, x.id)
				db.xtxEvent(XTxEventExpired, now.Sub(x.createdAt))
			}
		default:
			retain := x.idle
			if retain <= 0 {
				retain = DefaultXTxIdleTimeout
			}
			if now.Sub(x.finishedAt) > retain {
				forget = append(forget, x.id)
			}
		}
		x.mu.Unlock()
	}
	db.xtxHandles.remove(forget)
	return expired
}

// status reports what the registry knows about a handle id or idempotency
// key. ok is false when no handle matches.
func (r *xtxHandleRegistry) status(ref string) (st XTxStatus, txid string, ok bool) {
	if ref == "" {
		return XTxUnknown, "", false
	}
	r.mu.Lock()
	var matches []*XTx
	if x := r.handles[ref]; x != nil {
		matches = append(matches, x)
	} else {
		for _, x := range r.handles {
			if x.key == ref {
				matches = append(matches, x)
			}
		}
	}
	r.mu.Unlock()

	rank := func(s XTxStatus) int {
		switch s {
		case XTxCommitted:
			return 3
		case XTxPending:
			return 2
		default:
			return 1
		}
	}
	for _, x := range matches {
		x.mu.Lock()
		var s XTxStatus
		switch x.state {
		case xtxActive, xtxCommitting, xtxOutcomeUnknown:
			s = XTxPending
		case xtxCommitted:
			s = XTxCommitted
		default:
			s = XTxAborted
		}
		tx := x.txid
		x.mu.Unlock()
		if !ok || rank(s) > rank(st) {
			st, txid, ok = s, tx, true
		}
	}
	return st, txid, ok
}

func (r *xtxHandleRegistry) rememberResult(key string, res *XTxResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.results == nil {
		r.results = make(map[string]*XTxResult)
	}
	if _, ok := r.results[key]; !ok {
		r.resultOrder = append(r.resultOrder, key)
		if len(r.resultOrder) > xtxResultCacheSize {
			delete(r.results, r.resultOrder[0])
			r.resultOrder = r.resultOrder[1:]
		}
	}
	r.results[key] = res.clone()
}

// cachedResult returns the original result of the committed transaction
// (key, txid) with Replayed set, or nil when this process no longer has it.
func (r *xtxHandleRegistry) cachedResult(key, txid string) *XTxResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := r.results[key]
	if res == nil || res.TxID != txid {
		return nil
	}
	c := res.clone()
	c.Replayed = true
	return c
}

// close fails every handle that is still open and stops the reaper.
func (r *xtxHandleRegistry) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	stop, done := r.stop, r.done
	handles := make([]*XTx, 0, len(r.handles))
	for _, x := range r.handles {
		handles = append(handles, x)
	}
	r.mu.Unlock()

	if stop != nil {
		close(stop)
		<-done
	}
	now := time.Now()
	for _, x := range handles {
		x.mu.Lock()
		if x.state == xtxActive {
			x.finish(xtxClosed, now)
		}
		x.mu.Unlock()
	}
}
