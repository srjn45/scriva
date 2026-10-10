package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
	"github.com/srjn45/scriva/store"
)

// xtxFixture is a DB with two collections, each seeded with one document.
type xtxFixture struct {
	db         *DB
	dir        string
	a, b       *Collection
	aID, bID   uint64
	aRev, bRev uint64
}

func newXTxFixture(t *testing.T, cfg CollectionConfig) *xtxFixture {
	t.Helper()
	f := &xtxFixture{dir: t.TempDir()}
	db, err := Open(f.dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	f.db = db
	t.Cleanup(func() { _ = f.db.Close() })
	if f.a, err = db.CreateCollection("accounts"); err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	if f.b, err = db.CreateCollection("ledger"); err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if f.aID, _, err = f.a.Insert(map[string]any{"balance": 100.0}); err != nil {
		t.Fatalf("seed accounts: %v", err)
	}
	if f.bID, _, err = f.b.Insert(map[string]any{"entries": 0.0}); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	f.aRev, f.bRev = mustRev(t, f.a, f.aID), mustRev(t, f.b, f.bID)
	return f
}

func (f *xtxFixture) begin(t *testing.T, opts XTxOptions) *XTx {
	t.Helper()
	x, err := f.db.BeginXTx(context.Background(), []string{"ledger", "accounts"}, opts)
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	return x
}

func mustRev(t *testing.T, c *Collection, id uint64) uint64 {
	t.Helper()
	rec, err := c.Get(id)
	if err != nil {
		t.Fatalf("Get %s/%d: %v", c.Name(), id, err)
	}
	return rec.Rev
}

func mustAbsent(t *testing.T, c *Collection, id uint64) {
	t.Helper()
	if _, ok := c.liveRev(id); ok {
		t.Fatalf("%s/%d is visible, want absent", c.Name(), id)
	}
}

func wantErrIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func TestXTxHandle_StagedAtomicCommitAcrossCollections(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{})

	if got := x.Participants(); len(got) != 2 || got[0] != "accounts" || got[1] != "ledger" {
		t.Fatalf("Participants = %v, want canonical [accounts ledger]", got)
	}
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 70.0}); err != nil {
		t.Fatalf("stage update: %v", err)
	}
	newID, err := x.Insert("ledger", map[string]any{"amount": 30.0})
	if err != nil {
		t.Fatalf("stage insert: %v", err)
	}
	if err := x.Delete("ledger", f.bID); err != nil {
		t.Fatalf("stage delete: %v", err)
	}

	// Nothing staged is visible to other readers before commit.
	if rec, err := f.a.Get(f.aID); err != nil || rec.Data["balance"] != 100.0 || rec.Rev != f.aRev {
		t.Fatalf("pre-commit accounts = %+v, %v; want untouched", rec, err)
	}
	mustAbsent(t, f.b, newID)
	if _, err := f.b.Get(f.bID); err != nil {
		t.Fatalf("pre-commit ledger doc gone: %v", err)
	}
	if st, _ := f.db.XTxStatus(x.ID()); st != XTxPending {
		t.Fatalf("status of open handle = %s, want PENDING", st)
	}

	res, err := x.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if res.TxID == "" || len(res.Ops) != 3 || res.Replayed {
		t.Fatalf("result = %+v", res)
	}
	if rec, err := f.a.Get(f.aID); err != nil || rec.Data["balance"] != 70.0 || rec.Rev != f.aRev+1 {
		t.Fatalf("accounts after commit = %+v, %v", rec, err)
	}
	if rec, err := f.b.Get(newID); err != nil || rec.Data["amount"] != 30.0 {
		t.Fatalf("ledger insert after commit = %+v, %v", rec, err)
	}
	mustAbsent(t, f.b, f.bID)
	if st, tx := f.db.XTxStatus(res.TxID); st != XTxCommitted || tx != res.TxID {
		t.Fatalf("status by txid = %s %s", st, tx)
	}
	if st, tx := f.db.XTxStatus(x.ID()); st != XTxCommitted || tx != res.TxID {
		t.Fatalf("status by handle id = %s %s", st, tx)
	}

	// Commit on a committed handle returns the same outcome; staging is refused.
	again, err := x.Commit(context.Background())
	if err != nil || again.TxID != res.TxID {
		t.Fatalf("second Commit = %+v, %v", again, err)
	}
	wantErrIs(t, x.Update("accounts", f.aID, map[string]any{"balance": 1.0}), ErrXTxFinished)
	wantErrIs(t, x.Rollback(), ErrXTxFinished)
	if rec, _ := f.a.Get(f.aID); rec.Rev != f.aRev+1 {
		t.Fatalf("re-commit applied again: rev %d", rec.Rev)
	}
}

func TestXTxHandle_UndeclaredParticipantRejected(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	other, err := f.db.CreateCollection("other")
	if err != nil {
		t.Fatal(err)
	}
	otherID, _, err := other.Insert(map[string]any{"v": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	x := f.begin(t, XTxOptions{})

	_, err = x.Insert("other", map[string]any{"v": 2.0})
	wantErrIs(t, err, ErrXTxNotParticipant)
	wantErrIs(t, x.Update("other", otherID, map[string]any{"v": 2.0}), ErrXTxNotParticipant)
	wantErrIs(t, x.Delete("other", otherID), ErrXTxNotParticipant)
	_, err = x.Get("other", otherID)
	wantErrIs(t, err, ErrXTxNotParticipant)
	_, err = x.Scan("other", query.Filter(nil))
	wantErrIs(t, err, ErrXTxNotParticipant)

	// The handle is still usable for its declared participants.
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 1.0}); err != nil {
		t.Fatalf("declared participant rejected: %v", err)
	}

	_, err = f.db.BeginXTx(context.Background(), []string{"accounts", "missing"}, XTxOptions{})
	wantErrIs(t, err, ErrCollectionNotFound)
	_, err = f.db.BeginXTx(context.Background(), nil, XTxOptions{})
	wantErrIs(t, err, ErrXTxInvalid)
	_, err = f.db.BeginXTx(context.Background(), []string{"xtx.journal"}, XTxOptions{})
	wantErrIs(t, err, ErrReservedName)
}

func TestXTxHandle_ReadYourWrites(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{})

	// Staged insert is readable through the handle only.
	in := map[string]any{"amount": 5.0, "tags": []any{"x"}}
	id, err := x.Insert("ledger", in)
	if err != nil {
		t.Fatal(err)
	}
	in["amount"] = 999.0 // caller mutation must not reach the staged write
	rec, err := x.Get("ledger", id)
	if err != nil || rec.Data["amount"] != 5.0 || rec.Rev != 0 {
		t.Fatalf("Get staged insert = %+v, %v", rec, err)
	}
	rec.Data["amount"] = -1.0 // nor may a returned record alias it
	if rec, _ = x.Get("ledger", id); rec.Data["amount"] != 5.0 {
		t.Fatalf("staged data aliased: %+v", rec.Data)
	}

	// Staged update shadows the committed value; Rev is the base revision.
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 60.0}); err != nil {
		t.Fatal(err)
	}
	rec, err = x.Get("accounts", f.aID)
	if err != nil || rec.Data["balance"] != 60.0 || rec.Rev != f.aRev {
		t.Fatalf("Get staged update = %+v, %v", rec, err)
	}

	// Update of a staged insert stays an insert; staged delete hides the doc.
	if err := x.Update("ledger", id, map[string]any{"amount": 6.0}); err != nil {
		t.Fatal(err)
	}
	if err := x.Delete("ledger", f.bID); err != nil {
		t.Fatal(err)
	}
	_, err = x.Get("ledger", f.bID)
	wantErrIs(t, err, ErrXTxDocNotFound)
	wantErrIs(t, x.Update("ledger", f.bID, map[string]any{"entries": 1.0}), ErrXTxDocNotFound)

	// Insert then delete nets out to nothing.
	gone, err := x.Insert("ledger", map[string]any{"amount": 7.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Delete("ledger", gone); err != nil {
		t.Fatal(err)
	}
	_, err = x.Get("ledger", gone)
	wantErrIs(t, err, ErrXTxDocNotFound)

	res, err := x.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(res.Ops) != 3 {
		t.Fatalf("ops = %+v, want insert+update+delete", res.Ops)
	}
	if rec, err := f.b.Get(id); err != nil || rec.Data["amount"] != 6.0 || rec.Rev != 1 {
		t.Fatalf("committed insert = %+v, %v", rec, err)
	}
	mustAbsent(t, f.b, gone)
	mustAbsent(t, f.b, f.bID)
}

func TestXTxHandle_ReadSetConflict(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{Key: "read-conflict"})

	if rec, err := x.Get("accounts", f.aID); err != nil || rec.Rev != f.aRev {
		t.Fatalf("Get = %+v, %v", rec, err)
	}
	newID, err := x.Insert("ledger", map[string]any{"amount": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Update("ledger", f.bID, map[string]any{"entries": 1.0}); err != nil {
		t.Fatal(err)
	}

	// A concurrent single-collection writer changes the document that was read.
	if _, err := f.a.Update(f.aID, map[string]any{"balance": 1.0}); err != nil {
		t.Fatal(err)
	}

	_, err = x.Commit(context.Background())
	wantErrIs(t, err, ErrXTxConflict)
	var ce *XTxConflictError
	if !errors.As(err, &ce) || ce.Collection != "accounts" || ce.ID != f.aID || ce.Write ||
		ce.ExpectedRev != f.aRev || ce.ActualRev != f.aRev+1 {
		t.Fatalf("conflict detail = %+v", ce)
	}
	if !XTxRetrySafe(err) {
		t.Fatal("conflict must be retry-safe")
	}

	// No partial state: the write-only participant is untouched too.
	mustAbsent(t, f.b, newID)
	if rec, _ := f.b.Get(f.bID); rec.Rev != f.bRev || rec.Data["entries"] != 0.0 {
		t.Fatalf("ledger changed by failed commit: %+v", rec)
	}
	if st, _ := f.db.XTxStatus("read-conflict"); st != XTxAborted {
		t.Fatalf("status by key after conflict = %s, want ABORTED", st)
	}
}

func TestXTxHandle_ReadOfAbsentIsValidated(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{})

	absentID := f.a.ReserveID()
	_, err := x.Get("accounts", absentID)
	wantErrIs(t, err, ErrXTxDocNotFound)
	if err := x.Update("ledger", f.bID, map[string]any{"entries": 1.0}); err != nil {
		t.Fatal(err)
	}

	// Someone creates the document the transaction saw as absent.
	if err := f.a.CommitTx([]txOp{{kind: txOpInsert, id: absentID, data: map[string]any{"balance": 5.0}, ts: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}

	_, err = x.Commit(context.Background())
	wantErrIs(t, err, ErrXTxConflict)
	var ce *XTxConflictError
	if !errors.As(err, &ce) || ce.ExpectedPresent || !ce.ActualPresent || ce.ID != absentID {
		t.Fatalf("conflict detail = %+v", ce)
	}
	if rec, _ := f.b.Get(f.bID); rec.Rev != f.bRev {
		t.Fatalf("ledger changed by failed commit: %+v", rec)
	}
}

func TestXTxHandle_RepeatableReadAndReadOnlyCommit(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())

	// A read-only transaction validates its read set and writes nothing.
	ro := f.begin(t, XTxOptions{})
	if _, err := ro.Get("accounts", f.aID); err != nil {
		t.Fatal(err)
	}
	res, err := ro.Commit(context.Background())
	if err != nil || res.TxID != "" || len(res.Ops) != 0 {
		t.Fatalf("read-only commit = %+v, %v", res, err)
	}

	x := f.begin(t, XTxOptions{})
	if _, err := x.Get("accounts", f.aID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.a.Update(f.aID, map[string]any{"balance": 2.0}); err != nil {
		t.Fatal(err)
	}
	// The second read does not return the newer value.
	_, err = x.Get("accounts", f.aID)
	wantErrIs(t, err, ErrXTxConflict)
	_, err = x.Commit(context.Background())
	wantErrIs(t, err, ErrXTxConflict)
}

func TestXTxHandle_WriteWriteConflict(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x1 := f.begin(t, XTxOptions{})
	x2 := f.begin(t, XTxOptions{})

	// Both stage blind updates of the same document, plus a second collection.
	for i, x := range []*XTx{x1, x2} {
		if err := x.Update("accounts", f.aID, map[string]any{"balance": float64(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := x.Insert("ledger", map[string]any{"by": float64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := x1.Commit(context.Background()); err != nil {
		t.Fatalf("first committer: %v", err)
	}
	ledgerCount := f.b.Stats().RecordCount

	_, err := x2.Commit(context.Background())
	wantErrIs(t, err, ErrXTxConflict)
	var ce *XTxConflictError
	if !errors.As(err, &ce) || !ce.Write || ce.Collection != "accounts" || ce.ID != f.aID {
		t.Fatalf("conflict detail = %+v", ce)
	}
	if rec, _ := f.a.Get(f.aID); rec.Data["balance"] != 0.0 || rec.Rev != f.aRev+1 {
		t.Fatalf("loser overwrote the winner: %+v", rec)
	}
	if got := f.b.Stats().RecordCount; got != ledgerCount {
		t.Fatalf("loser's insert leaked: %d records, want %d", got, ledgerCount)
	}

	// An explicit expected revision that is already stale is refused at stage
	// time and leaves the handle open.
	x3 := f.begin(t, XTxOptions{})
	_, err = x3.Stage(XTxOp{Collection: "accounts", Op: store.OpUpdate, ID: f.aID, ExpectedRev: f.aRev, Data: map[string]any{"balance": 9.0}})
	wantErrIs(t, err, ErrXTxConflict)
	if _, err := x3.Stage(XTxOp{Collection: "accounts", Op: store.OpUpdate, ID: f.aID, ExpectedRev: f.aRev + 1, Data: map[string]any{"balance": 9.0}}); err != nil {
		t.Fatalf("current expected rev rejected: %v", err)
	}
	if _, err := x3.Commit(context.Background()); err != nil {
		t.Fatalf("x3 commit: %v", err)
	}
}

func TestXTxHandle_Rollback(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{Key: "rb"})
	newID, err := x.Insert("ledger", map[string]any{"amount": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 0.0}); err != nil {
		t.Fatal(err)
	}

	if err := x.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := x.Rollback(); err != nil {
		t.Fatalf("second Rollback: %v", err)
	}
	mustAbsent(t, f.b, newID)
	if rec, _ := f.a.Get(f.aID); rec.Rev != f.aRev {
		t.Fatalf("rolled back update applied: %+v", rec)
	}
	_, err = x.Commit(context.Background())
	wantErrIs(t, err, ErrXTxFinished)
	_, err = x.Get("accounts", f.aID)
	wantErrIs(t, err, ErrXTxFinished)
	if st, _ := f.db.XTxStatus("rb"); st != XTxAborted {
		t.Fatalf("status after rollback = %s, want ABORTED", st)
	}
	if h, err := f.db.XTxHandle(x.ID()); err != nil || h != x {
		t.Fatalf("XTxHandle = %v, %v", h, err)
	}
	_, err = f.db.XTxHandle("nope")
	wantErrIs(t, err, ErrXTxHandleNotFound)
}

func TestXTxHandle_RollbackAfterFailedCommit(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{})
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 0.0}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.a.Update(f.aID, map[string]any{"balance": 3.0}); err != nil {
		t.Fatal(err)
	}
	_, err := x.Commit(context.Background())
	wantErrIs(t, err, ErrXTxConflict)

	if err := x.Rollback(); err != nil {
		t.Fatalf("Rollback after failed commit: %v", err)
	}
	if err := x.Rollback(); err != nil {
		t.Fatalf("repeated Rollback: %v", err)
	}
	// The failed handle stays failed: it never applies later.
	_, err = x.Commit(context.Background())
	wantErrIs(t, err, ErrXTxFinished)
	if rec, _ := f.a.Get(f.aID); rec.Data["balance"] != 3.0 || rec.Rev != f.aRev+1 {
		t.Fatalf("state after failed commit + rollback = %+v", rec)
	}
}

func TestXTxHandle_TimeoutReaping(t *testing.T) {
	cfg := defaultConfig()
	cfg.XTxIdleTimeout = time.Hour // reaping is driven by hand below
	var events sync.Map
	cfg.OnXTx = func(event string, _ time.Duration) {
		n, _ := events.LoadOrStore(event, new(atomic.Int64))
		n.(*atomic.Int64).Add(1)
	}
	f := newXTxFixture(t, cfg)

	idle := f.begin(t, XTxOptions{Key: "idle"})
	newID, err := idle.Insert("ledger", map[string]any{"amount": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idle.Get("accounts", f.aID); err != nil {
		t.Fatal(err)
	}
	fresh := f.begin(t, XTxOptions{IdleTimeout: 3 * time.Hour})

	if got := f.db.reapXTx(time.Now()); len(got) != 0 {
		t.Fatalf("reaped live handles: %v", got)
	}
	got := f.db.reapXTx(time.Now().Add(2 * time.Hour))
	if len(got) != 1 || got[0] != idle.ID() {
		t.Fatalf("reaped = %v, want [%s]", got, idle.ID())
	}

	// The reaped handle released its buffers and refuses everything.
	idle.mu.Lock()
	released := idle.writes == nil && idle.observed == nil && idle.order == nil
	idle.mu.Unlock()
	if !released {
		t.Fatal("reaped handle still holds staged state")
	}
	_, err = idle.Commit(context.Background())
	wantErrIs(t, err, ErrXTxExpired)
	_, err = idle.Insert("ledger", map[string]any{"amount": 2.0})
	wantErrIs(t, err, ErrXTxExpired)
	if err := idle.Rollback(); err != nil {
		t.Fatalf("Rollback of reaped handle: %v", err)
	}
	mustAbsent(t, f.b, newID)
	if st, _ := f.db.XTxStatus("idle"); st != XTxAborted {
		t.Fatalf("status of reaped handle = %s, want ABORTED", st)
	}

	// The handle with a longer timeout survived and still commits.
	if err := fresh.Update("accounts", f.aID, map[string]any{"balance": 4.0}); err != nil {
		t.Fatalf("surviving handle: %v", err)
	}
	if _, err := fresh.Commit(context.Background()); err != nil {
		t.Fatalf("surviving handle commit: %v", err)
	}

	// Finished handles are forgotten after their retention period.
	f.db.reapXTx(time.Now().Add(10 * time.Hour))
	_, err = f.db.XTxHandle(idle.ID())
	wantErrIs(t, err, ErrXTxHandleNotFound)
	_, err = f.db.XTxHandle(fresh.ID())
	wantErrIs(t, err, ErrXTxHandleNotFound)

	if n, ok := events.Load(XTxEventExpired); !ok || n.(*atomic.Int64).Load() != 1 {
		t.Fatalf("expired events = %v", n)
	}
	if n, ok := events.Load(XTxEventCommit); !ok || n.(*atomic.Int64).Load() != 1 {
		t.Fatalf("commit events = %v", n)
	}
}

func TestXTxHandle_ExpiryIsExactAndSweeperRuns(t *testing.T) {
	cfg := defaultConfig()
	cfg.XTxIdleTimeout = 20 * time.Millisecond
	f := newXTxFixture(t, cfg)

	// Lifetime bound: enforced on use even if the sweeper has not run.
	short := f.begin(t, XTxOptions{IdleTimeout: -1, MaxLifetime: time.Nanosecond})
	time.Sleep(time.Millisecond)
	wantErrIs(t, short.Update("accounts", f.aID, map[string]any{"balance": 0.0}), ErrXTxExpired)

	// Background sweeper expires an abandoned handle.
	x := f.begin(t, XTxOptions{})
	if _, err := x.Insert("ledger", map[string]any{"amount": 1.0}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		x.mu.Lock()
		state := x.state
		x.mu.Unlock()
		if state == xtxExpired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sweeper did not reap the idle handle")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestXTxHandle_ContextCancelBeforePrepare(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{Key: "cancel"})
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 10.0}); err != nil {
		t.Fatal(err)
	}
	newID, err := x.Insert("ledger", map[string]any{"amount": 90.0})
	if err != nil {
		t.Fatal(err)
	}

	// Already-cancelled context: rejected before any lock is taken.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = x.Commit(cancelled)
	wantErrIs(t, err, ErrXTxCanceled)
	wantErrIs(t, err, context.Canceled)
	_, err = f.db.BeginXTx(cancelled, []string{"accounts"}, XTxOptions{})
	wantErrIs(t, err, ErrXTxCanceled)

	// Deadline expires while waiting for the second participant's lock: the
	// first lock must be released and nothing may have been written.
	f.b.mu.Lock()
	ctx, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err = x.Commit(ctx)
	cancel2()
	wantErrIs(t, err, ErrXTxCanceled)
	wantErrIs(t, err, context.DeadlineExceeded)
	if !XTxRetrySafe(err) {
		t.Fatal("cancel before prepare must be retry-safe")
	}
	if !f.a.mu.TryLock() {
		t.Fatal("cancelled commit still holds the first participant's lock")
	}
	f.a.mu.Unlock()
	f.b.mu.Unlock()

	if rec, _ := f.a.Get(f.aID); rec.Rev != f.aRev {
		t.Fatalf("cancelled commit applied: %+v", rec)
	}
	mustAbsent(t, f.b, newID)
	if st, _ := f.db.XTxStatus("cancel"); st != XTxPending {
		t.Fatalf("status after cancelled commit = %s, want PENDING (handle still open)", st)
	}

	// The abandoned waiter must not leak the lock it was queued on.
	deadline := time.Now().Add(5 * time.Second)
	for !f.b.mu.TryLock() {
		if time.Now().After(deadline) {
			t.Fatal("cancelled lock wait leaked the participant lock")
		}
		time.Sleep(time.Millisecond)
	}
	f.b.mu.Unlock()

	// The handle survived the cancellation and commits on retry.
	res, err := x.Commit(context.Background())
	if err != nil {
		t.Fatalf("retry after cancel: %v", err)
	}
	if rec, _ := f.a.Get(f.aID); rec.Data["balance"] != 10.0 {
		t.Fatalf("retry did not apply: %+v", rec)
	}
	if st, tx := f.db.XTxStatus("cancel"); st != XTxCommitted || tx != res.TxID {
		t.Fatalf("status = %s %s", st, tx)
	}
}

func TestXTxHandle_IdempotentRetryByKey(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	const key = "transfer-42"

	stage := func() (*XTx, uint64) {
		x := f.begin(t, XTxOptions{Key: key})
		rec, err := x.Get("accounts", f.aID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		bal := rec.Data["balance"].(float64)
		if err := x.Update("accounts", f.aID, map[string]any{"balance": bal - 30}); err != nil {
			t.Fatalf("Update: %v", err)
		}
		id, err := x.Insert("ledger", map[string]any{"amount": 30.0})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		return x, id
	}

	if st, _ := f.db.XTxStatus(key); st != XTxUnknown {
		t.Fatalf("status before begin = %s", st)
	}
	x1, id1 := stage()
	if st, _ := f.db.XTxStatus(key); st != XTxPending {
		t.Fatalf("status while staged = %s, want PENDING", st)
	}
	res1, err := x1.Commit(context.Background())
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	ledgerCount := f.b.Stats().RecordCount

	// The client lost the response and replays the whole transaction with the
	// same key. Its reads now see its own first commit, but nothing re-applies.
	x2, id2 := stage()
	res2, err := x2.Commit(context.Background())
	if err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if res2.TxID != res1.TxID || !res2.Replayed {
		t.Fatalf("retry result = %+v, want replay of %s", res2, res1.TxID)
	}
	if len(res2.Ops) != len(res1.Ops) || res2.Ops[0] != res1.Ops[0] || res2.Ops[1] != res1.Ops[1] {
		t.Fatalf("retry ops = %+v, want original %+v", res2.Ops, res1.Ops)
	}
	if rec, _ := f.a.Get(f.aID); rec.Data["balance"] != 70.0 || rec.Rev != f.aRev+1 {
		t.Fatalf("double apply: %+v", rec)
	}
	if got := f.b.Stats().RecordCount; got != ledgerCount {
		t.Fatalf("ledger records = %d, want %d", got, ledgerCount)
	}
	mustAbsent(t, f.b, id2)
	if _, err := f.b.Get(id1); err != nil {
		t.Fatalf("original insert missing: %v", err)
	}
	if st, tx := f.db.XTxStatus(key); st != XTxCommitted || tx != res1.TxID {
		t.Fatalf("status by key = %s %s", st, tx)
	}

	// Same after a restart: the key is durable in the journal.
	if err := f.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := Open(f.dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	f.db = db
	f.a, _ = db.Collection("accounts")
	f.b, _ = db.Collection("ledger")
	if st, tx := db.XTxStatus(key); st != XTxCommitted || tx != res1.TxID {
		t.Fatalf("status by key after reopen = %s %s", st, tx)
	}
	x3, id3 := stage()
	res3, err := x3.Commit(context.Background())
	if err != nil || res3.TxID != res1.TxID || !res3.Replayed {
		t.Fatalf("retry after reopen = %+v, %v", res3, err)
	}
	if rec, _ := f.a.Get(f.aID); rec.Data["balance"] != 70.0 || rec.Rev != f.aRev+1 {
		t.Fatalf("double apply after reopen: %+v", rec)
	}
	mustAbsent(t, f.b, id3)
}

func TestXTxHandle_ScanRejected(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	if err := f.a.EnsureIndex("balance"); err != nil {
		t.Fatal(err)
	}
	x := f.begin(t, XTxOptions{})

	_, err := x.Scan("accounts", query.Filter(nil))
	wantErrIs(t, err, ErrXTxScanUnsupported)
	_, err = x.IndexLookup("accounts", "balance", "100")
	wantErrIs(t, err, ErrXTxScanUnsupported)

	// Rejection does not poison the handle.
	if _, err := x.Get("accounts", f.aID); err != nil {
		t.Fatalf("Get after rejected scan: %v", err)
	}
	if _, err := x.Commit(context.Background()); err != nil {
		t.Fatalf("Commit after rejected scan: %v", err)
	}
}

func TestXTxHandle_ReopenShowsCommittedState(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	x := f.begin(t, XTxOptions{})
	if err := x.Update("accounts", f.aID, map[string]any{"balance": 55.0}); err != nil {
		t.Fatal(err)
	}
	newID, err := x.Insert("ledger", map[string]any{"amount": 45.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Delete("ledger", f.bID); err != nil {
		t.Fatal(err)
	}
	res, err := x.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// A handle left open across Close is failed, not applied.
	open := f.begin(t, XTxOptions{})
	lostID, err := open.Insert("ledger", map[string]any{"amount": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = open.Commit(context.Background())
	wantErrIs(t, err, ErrXTxFinished)

	db, err := Open(f.dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	f.db = db
	a, _ := db.Collection("accounts")
	b, _ := db.Collection("ledger")
	if rec, err := a.Get(f.aID); err != nil || rec.Data["balance"] != 55.0 || rec.Rev != f.aRev+1 {
		t.Fatalf("accounts after reopen = %+v, %v", rec, err)
	}
	if rec, err := b.Get(newID); err != nil || rec.Data["amount"] != 45.0 {
		t.Fatalf("ledger insert after reopen = %+v, %v", rec, err)
	}
	mustAbsent(t, b, f.bID)
	mustAbsent(t, b, lostID)
	if st, _ := db.XTxStatus(res.TxID); st != XTxCommitted {
		t.Fatalf("status after reopen = %s", st)
	}
	// Ids reserved by the transaction are not handed out again.
	if id, _, err := b.Insert(map[string]any{"amount": 2.0}); err != nil || id <= newID {
		t.Fatalf("post-reopen insert id = %d (staged insert was %d), err %v", id, newID, err)
	}
}

// Transactions that touch the same two collections in opposite orders must
// not deadlock: locks are taken in canonical order whatever the staging order.
func TestXTxHandle_InverseOrderNoDeadlock(t *testing.T) {
	f := newXTxFixture(t, defaultConfig())
	const workers, rounds = 4, 25

	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			order := []string{"accounts", "ledger"}
			if w%2 == 1 {
				order = []string{"ledger", "accounts"}
			}
			for i := 0; i < rounds; i++ {
				x, err := f.db.BeginXTx(context.Background(), order, XTxOptions{})
				if err != nil {
					done <- err
					return
				}
				for _, name := range order {
					if _, err := x.Insert(name, map[string]any{"w": float64(w), "i": float64(i)}); err != nil {
						done <- err
						return
					}
				}
				if _, err := x.Commit(context.Background()); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(w)
	}
	timeout := time.After(60 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("worker: %v", err)
			}
		case <-timeout:
			t.Fatal("inverse-order transactions deadlocked")
		}
	}
	want := uint64(workers*rounds + 1)
	if a, b := f.a.Stats().RecordCount, f.b.Stats().RecordCount; a != want || b != want {
		t.Fatalf("record counts = %d, %d; want %d each", a, b, want)
	}
}

// A commit that fills the active segment rotates it after the participant
// locks are released (§6.1 rule 7).
func TestXTxHandle_CommitRotatesSegmentAfterUnlock(t *testing.T) {
	cfg := defaultConfig()
	cfg.SegmentMaxSize = 256
	f := newXTxFixture(t, cfg)

	finished := make(chan error, 1)
	go func() {
		for i := 0; i < 5; i++ {
			x, err := f.db.BeginXTx(context.Background(), []string{"accounts", "ledger"}, XTxOptions{})
			if err != nil {
				finished <- err
				return
			}
			for _, name := range []string{"accounts", "ledger"} {
				if _, err := x.Insert(name, map[string]any{"pad": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}); err != nil {
					finished <- err
					return
				}
			}
			if _, err := x.Commit(context.Background()); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("commit that needs rotation hung")
	}
	if got := f.a.Stats().RecordCount; got != 6 {
		t.Fatalf("accounts records = %d, want 6", got)
	}
	f.a.mu.RLock()
	sealed := len(f.a.sealed)
	f.a.mu.RUnlock()
	if sealed == 0 {
		t.Fatal("no rotation happened; the test did not exercise the post-unlock path")
	}
}
