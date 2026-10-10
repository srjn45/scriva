package scriva_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva"
	"github.com/srjn45/scriva/engine"
)

// wardenStore stands up the warden-shaped collection set through the façade in a
// handful of lines — the EMB-2 acceptance shape. It returns the DB and its
// collections so tests can exercise them.
func wardenStore(t *testing.T, dir string) (*scriva.DB, map[string]*engine.Collection) {
	t.Helper()
	db, err := scriva.Open(dir)
	if err != nil {
		t.Fatalf("scriva.Open: %v", err)
	}
	cols := map[string]*engine.Collection{
		"sessions": db.MustCollection("sessions", scriva.WithUniqueIndex("name")),
		"events":   db.MustCollection("events"),
		"messages": db.MustCollection("messages"),
		"context":  db.MustCollection("context"),
		// spend needs an fsync on every write — opt out of the interval default.
		"spend": db.MustCollection("spend", scriva.WithCollectionSyncMode(engine.SyncModeAlways)),
	}
	return db, cols
}

func TestOpenCollectionsCRUDReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	db, cols := wardenStore(t, dir)

	// Keyed CRUD on sessions.
	if _, _, err := cols["sessions"].InsertWithKey("sess-1", map[string]any{"name": "alpha", "status": "open"}); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := cols["sessions"].UpdateByKey("sess-1", map[string]any{"name": "alpha", "status": "closed"}); err != nil {
		t.Fatalf("update session: %v", err)
	}
	// Plain inserts on the append-only collections.
	if _, _, err := cols["events"].Insert(map[string]any{"session": "sess-1", "kind": "tick"}); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if _, _, err := cols["messages"].Insert(map[string]any{"to": "agent-a", "body": "hi"}); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if _, err := cols["context"].Upsert("cfg-key", map[string]any{"v": 1}); err != nil {
		t.Fatalf("upsert context: %v", err)
	}
	if _, _, err := cols["spend"].Insert(map[string]any{"amount": 42}); err != nil {
		t.Fatalf("insert spend: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen recovers all data across every collection.
	db2, cols2 := wardenStore(t, dir)
	defer db2.Close()

	data, _, err := cols2["sessions"].FindByKey("sess-1")
	if err != nil {
		t.Fatalf("reopen find session: %v", err)
	}
	if data["status"] != "closed" {
		t.Fatalf("session status after reopen = %v, want closed", data["status"])
	}
	for _, name := range []string{"events", "messages", "context", "spend"} {
		got, err := cols2[name].Scan(nil)
		if err != nil {
			t.Fatalf("scan %q: %v", name, err)
		}
		if len(got) != 1 {
			t.Fatalf("collection %q after reopen has %d records, want 1", name, len(got))
		}
	}

	// The spend override must survive reopen — not be clobbered by the global
	// interval default.
	if got := cols2["spend"].Config().SyncMode; got != engine.SyncModeAlways {
		t.Fatalf("spend SyncMode after reopen = %q, want %q", got, engine.SyncModeAlways)
	}
}

func TestDefaultSyncModeInterval(t *testing.T) {
	t.Parallel()
	db, err := scriva.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	col := db.MustCollection("plain")
	cfg := col.Config()
	if cfg.SyncMode != engine.SyncModeInterval {
		t.Fatalf("default SyncMode = %q, want %q", cfg.SyncMode, engine.SyncModeInterval)
	}
	if cfg.SyncInterval != time.Second {
		t.Fatalf("default SyncInterval = %v, want 1s", cfg.SyncInterval)
	}
}

func TestPerCollectionSyncAlwaysOverride(t *testing.T) {
	t.Parallel()
	db, err := scriva.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ledger := db.MustCollection("spend", scriva.WithCollectionSyncMode(engine.SyncModeAlways))
	if got := ledger.Config().SyncMode; got != engine.SyncModeAlways {
		t.Fatalf("overridden SyncMode = %q, want %q", got, engine.SyncModeAlways)
	}

	// A sibling collection with no override keeps the interval default — proving
	// the override is scoped per collection, not global.
	plain := db.MustCollection("events")
	if got := plain.Config().SyncMode; got != engine.SyncModeInterval {
		t.Fatalf("sibling SyncMode = %q, want %q (interval default)", got, engine.SyncModeInterval)
	}
}

func TestPerCollectionQuotaOptions(t *testing.T) {
	t.Parallel()
	db, err := scriva.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	capped := db.MustCollection("capped", scriva.WithMaxRecords(1), scriva.WithMaxBytes(4096))
	if cfg := capped.Config(); cfg.MaxRecords != 1 || cfg.MaxBytes != 4096 {
		t.Fatalf("capped quota cfg = {%d, %d}, want {1, 4096}", cfg.MaxRecords, cfg.MaxBytes)
	}

	// The cap is enforced through the embedded write path.
	if _, _, err := capped.Insert(map[string]any{"n": float64(1)}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, _, err = capped.Insert(map[string]any{"n": float64(2)})
	if !errors.Is(err, engine.ErrResourceExhausted) {
		t.Fatalf("over-quota insert: got %v, want ErrResourceExhausted", err)
	}

	// A sibling with no quota option stays unlimited.
	if cfg := db.MustCollection("free").Config(); cfg.MaxRecords != 0 || cfg.MaxBytes != 0 {
		t.Fatalf("sibling quota cfg = {%d, %d}, want {0, 0}", cfg.MaxRecords, cfg.MaxBytes)
	}
}

func TestOpenOptionOverridesDefault(t *testing.T) {
	t.Parallel()
	db, err := scriva.Open(t.TempDir(), scriva.WithSyncMode(engine.SyncModeNone))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if got := db.MustCollection("c").Config().SyncMode; got != engine.SyncModeNone {
		t.Fatalf("SyncMode with WithSyncMode(none) = %q, want %q", got, engine.SyncModeNone)
	}
}

func TestUniqueIndexAtOpen(t *testing.T) {
	t.Parallel()
	db, err := scriva.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sessions := db.MustCollection("sessions", scriva.WithUniqueIndex("name"))
	if _, _, err := sessions.Insert(map[string]any{"name": "alpha"}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, _, err = sessions.Insert(map[string]any{"name": "alpha"})
	if !errors.Is(err, engine.ErrDuplicateKey) {
		t.Fatalf("duplicate insert err = %v, want ErrDuplicateKey", err)
	}
}

func TestCollectionFirstCallWinsAndCaches(t *testing.T) {
	t.Parallel()
	db, err := scriva.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	first := db.MustCollection("c", scriva.WithCollectionSyncMode(engine.SyncModeAlways))
	// A second call for the same name returns the cached handle and ignores its
	// options (first call wins).
	second := db.MustCollection("c", scriva.WithCollectionSyncMode(engine.SyncModeNone))
	if first != second {
		t.Fatal("second Collection call returned a different handle; expected cached")
	}
	if got := second.Config().SyncMode; got != engine.SyncModeAlways {
		t.Fatalf("cached SyncMode = %q, want %q (first call wins)", got, engine.SyncModeAlways)
	}
}

func TestOpenFailsClosedOnCorruptionAndReportOptsIn(t *testing.T) {
	dir := t.TempDir()
	db, err := scriva.Open(dir, scriva.WithSegmentMaxSize(300))
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if _, _, err := col.Insert(map[string]any{"xxxxxxxx": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "c", "seg_000001.ndjson")
	b, _ := os.ReadFile(p)
	b[20] ^= 1
	_ = os.WriteFile(p, b, 0o644)
	_ = os.Remove(filepath.Join(dir, "c", "index.json"))

	var outcome string
	hook := scriva.WithOnIntegrity(func(_ string, _ engine.IntegrityPolicy, o string, _ *engine.CollectionReport) { outcome = o })
	if _, err := scriva.Open(dir, hook); !errors.Is(err, engine.ErrIntegrity) || outcome != engine.IntegrityOutcomeFailed {
		t.Fatalf("default Open: err=%v outcome=%q", err, outcome)
	}
	db, err = scriva.Open(dir, hook, scriva.WithIntegrityPolicy(engine.PolicyReport))
	if err != nil {
		t.Fatalf("report policy: %v", err)
	}
	defer db.Close()
	if outcome != engine.IntegrityOutcomeReported {
		t.Fatalf("outcome = %q", outcome)
	}
}

// xtxStore opens a façade DB with two participant collections.
func xtxStore(t *testing.T, dir string, opts ...scriva.Option) (*scriva.DB, *engine.Collection, *engine.Collection) {
	t.Helper()
	db, err := scriva.Open(dir, opts...)
	if err != nil {
		t.Fatalf("scriva.Open: %v", err)
	}
	return db, db.MustCollection("accounts"), db.MustCollection("ledger")
}

func TestXTxCommitIsAtomicAcrossCollections(t *testing.T) {
	t.Parallel()
	db, accounts, ledger := xtxStore(t, t.TempDir())
	defer db.Close()
	ctx := context.Background()

	acct, _, err := accounts.Insert(map[string]any{"balance": 100.0})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginXTx(ctx, []string{"ledger", "accounts"}, engine.XTxOptions{Key: "transfer-1"})
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	if got := tx.Participants(); len(got) != 2 || got[0] != "accounts" || got[1] != "ledger" {
		t.Fatalf("participants = %v, want canonical order", got)
	}
	if same, err := db.XTx(tx.ID()); err != nil || same != tx {
		t.Fatalf("XTx(%q) = %v, %v", tx.ID(), same, err)
	}

	if err := tx.Update("accounts", acct, map[string]any{"balance": 60.0}); err != nil {
		t.Fatalf("stage update: %v", err)
	}
	entry, err := tx.Insert("ledger", map[string]any{"account": float64(acct), "delta": -40.0})
	if err != nil {
		t.Fatalf("stage insert: %v", err)
	}

	// Read-your-writes: the handle sees its staged data, nobody else does.
	if r, err := tx.Get("accounts", acct); err != nil || r.Data["balance"] != 60.0 {
		t.Fatalf("tx.Get accounts = %+v, %v", r, err)
	}
	if r, err := tx.Get("ledger", entry); err != nil || r.Data["delta"] != -40.0 {
		t.Fatalf("tx.Get ledger = %+v, %v", r, err)
	}
	if r, err := accounts.Get(acct); err != nil || r.Data["balance"] != 100.0 {
		t.Fatalf("uncommitted update visible outside the transaction: %+v, %v", r, err)
	}
	if _, err := ledger.Get(entry); err == nil {
		t.Fatal("uncommitted insert visible outside the transaction")
	}
	if st, _ := db.XTxStatus("transfer-1"); st != engine.XTxPending {
		t.Fatalf("status before commit = %s, want PENDING", st)
	}

	res, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if res.TxID == "" || len(res.Ops) != 2 || res.Replayed {
		t.Fatalf("result = %+v", res)
	}
	if r, err := accounts.Get(acct); err != nil || r.Data["balance"] != 60.0 {
		t.Fatalf("committed update: %+v, %v", r, err)
	}
	if r, err := ledger.Get(entry); err != nil || r.Data["delta"] != -40.0 {
		t.Fatalf("committed insert: %+v, %v", r, err)
	}

	// Outcome lookup by key, by txid and by handle id.
	for _, ref := range []string{"transfer-1", res.TxID, tx.ID()} {
		if st, txid := db.XTxStatus(ref); st != engine.XTxCommitted || txid != res.TxID {
			t.Fatalf("XTxStatus(%q) = %s, %q", ref, st, txid)
		}
	}

	// Commit is idempotent on the handle, and a new transaction under the same
	// key replays the original outcome without applying anything.
	if again, err := tx.Commit(ctx); err != nil || again.TxID != res.TxID {
		t.Fatalf("second Commit = %+v, %v", again, err)
	}
	retry, err := db.BeginXTx(ctx, []string{"accounts", "ledger"}, engine.XTxOptions{Key: "transfer-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := retry.Update("accounts", acct, map[string]any{"balance": 20.0}); err != nil {
		t.Fatal(err)
	}
	replay, err := retry.Commit(ctx)
	if err != nil || !replay.Replayed || replay.TxID != res.TxID {
		t.Fatalf("replayed Commit = %+v, %v", replay, err)
	}
	if r, _ := accounts.Get(acct); r.Data["balance"] != 60.0 {
		t.Fatalf("replay applied its writes: %+v", r)
	}
}

func TestXTxConflictRollbackAndTypedErrors(t *testing.T) {
	t.Parallel()
	db, accounts, ledger := xtxStore(t, t.TempDir())
	defer db.Close()
	ctx := context.Background()
	cols := []string{"accounts", "ledger"}

	acct, _, err := accounts.Insert(map[string]any{"balance": 100.0})
	if err != nil {
		t.Fatal(err)
	}

	// Conflict: a document the transaction read changes before commit.
	tx, err := db.BeginXTx(ctx, cols, engine.XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Get("accounts", acct); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Insert("ledger", map[string]any{"delta": -1.0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.Update(acct, map[string]any{"balance": 5.0}); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit(ctx)
	var conflict *engine.XTxConflictError
	if !errors.Is(err, engine.ErrXTxConflict) || !errors.As(err, &conflict) {
		t.Fatalf("Commit after concurrent update = %v, want ErrXTxConflict", err)
	}
	if conflict.Collection != "accounts" || conflict.ID != acct || conflict.Write {
		t.Fatalf("conflict = %+v", conflict)
	}
	if !engine.XTxRetrySafe(err) {
		t.Fatal("a conflict must be retry-safe")
	}
	if _, err := ledger.Get(entry); err == nil {
		t.Fatal("conflicting transaction left a partial write")
	}
	if st, _ := db.XTxStatus(tx.ID()); st != engine.XTxAborted {
		t.Fatalf("status after conflict = %s, want ABORTED", st)
	}

	// Rollback leaves nothing behind and finishes the handle.
	tx, err = db.BeginXTx(ctx, cols, engine.XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entry, err = tx.Insert("ledger", map[string]any{"delta": -2.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if _, err := ledger.Get(entry); err == nil {
		t.Fatal("rolled back insert is visible")
	}
	if _, err := tx.Insert("ledger", map[string]any{}); !errors.Is(err, engine.ErrXTxFinished) {
		t.Fatalf("stage after rollback = %v, want ErrXTxFinished", err)
	}

	// Typed rejections.
	tx, err = db.BeginXTx(ctx, []string{"accounts"}, engine.XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Insert("ledger", map[string]any{}); !errors.Is(err, engine.ErrXTxNotParticipant) {
		t.Fatalf("stage on undeclared collection = %v, want ErrXTxNotParticipant", err)
	}
	if _, err := tx.Scan("accounts", nil); !errors.Is(err, engine.ErrXTxScanUnsupported) {
		t.Fatalf("Scan = %v, want ErrXTxScanUnsupported", err)
	}
	if _, err := tx.IndexLookup("accounts", "balance", "1"); !errors.Is(err, engine.ErrXTxScanUnsupported) {
		t.Fatalf("IndexLookup = %v, want ErrXTxScanUnsupported", err)
	}
	if _, err := tx.Get("accounts", 9999); !errors.Is(err, engine.ErrXTxDocNotFound) {
		t.Fatalf("Get of a missing document = %v, want ErrXTxDocNotFound", err)
	}
	if _, err := db.BeginXTx(ctx, []string{"accounts", "nope"}, engine.XTxOptions{}); !errors.Is(err, engine.ErrCollectionNotFound) {
		t.Fatalf("BeginXTx with a missing collection = %v, want ErrCollectionNotFound", err)
	}
	if _, err := db.BeginXTx(ctx, nil, engine.XTxOptions{}); !errors.Is(err, engine.ErrXTxInvalid) {
		t.Fatalf("BeginXTx with no collections = %v, want ErrXTxInvalid", err)
	}
	if _, err := db.XTx("no-such-handle"); !errors.Is(err, engine.ErrXTxHandleNotFound) {
		t.Fatalf("XTx(unknown) = %v, want ErrXTxHandleNotFound", err)
	}
	if st, _ := db.XTxStatus("no-such-ref"); st != engine.XTxUnknown {
		t.Fatalf("XTxStatus(unknown) = %s, want UNKNOWN", st)
	}
}

func TestTransact(t *testing.T) {
	t.Parallel()
	db, accounts, ledger := xtxStore(t, t.TempDir())
	defer db.Close()
	ctx := context.Background()
	cols := []string{"accounts", "ledger"}

	var a, l uint64
	res, err := db.Transact(ctx, cols, engine.XTxOptions{}, func(tx *engine.XTx) error {
		var err error
		if a, err = tx.Insert("accounts", map[string]any{"balance": 1.0}); err != nil {
			return err
		}
		l, err = tx.Insert("ledger", map[string]any{"delta": 1.0})
		return err
	})
	if err != nil || len(res.Ops) != 2 {
		t.Fatalf("Transact = %+v, %v", res, err)
	}
	if _, err := accounts.Get(a); err != nil {
		t.Fatalf("accounts insert missing: %v", err)
	}
	if _, err := ledger.Get(l); err != nil {
		t.Fatalf("ledger insert missing: %v", err)
	}

	// An error from fn rolls the transaction back and is returned as is.
	boom := errors.New("boom")
	var staged uint64
	_, err = db.Transact(ctx, cols, engine.XTxOptions{}, func(tx *engine.XTx) error {
		staged, _ = tx.Insert("ledger", map[string]any{"delta": 2.0})
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Transact error = %v, want boom", err)
	}
	if _, err := ledger.Get(staged); err == nil {
		t.Fatal("Transact wrote the insert of a failed fn")
	}
}

func TestXTxIdleTimeoutOptionAndReplayAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, accounts, _ := xtxStore(t, dir, scriva.WithXTxIdleTimeout(20*time.Millisecond))
	ctx := context.Background()

	// The DB-wide idle timeout discards an untouched handle.
	idle, err := db.BeginXTx(ctx, []string{"accounts"}, engine.XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := idle.Insert("accounts", map[string]any{}); !errors.Is(err, engine.ErrXTxExpired) {
		t.Fatalf("stage on an idle handle = %v, want ErrXTxExpired", err)
	}

	res, err := db.Transact(ctx, []string{"accounts", "ledger"}, engine.XTxOptions{Key: "k-reopen", IdleTimeout: time.Minute}, func(tx *engine.XTx) error {
		if _, err := tx.Insert("accounts", map[string]any{"balance": 7.0}); err != nil {
			return err
		}
		_, err := tx.Insert("ledger", map[string]any{"delta": 7.0})
		return err
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}
	before, err := accounts.Scan(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// After a restart the outcome is still resolvable by key and by txid, and a
	// retry under the same key replays it instead of applying again.
	db, accounts, _ = xtxStore(t, dir)
	defer db.Close()
	for _, ref := range []string{"k-reopen", res.TxID} {
		if st, txid := db.XTxStatus(ref); st != engine.XTxCommitted || txid != res.TxID {
			t.Fatalf("after reopen XTxStatus(%q) = %s, %q", ref, st, txid)
		}
	}
	replay, err := db.Transact(ctx, []string{"accounts", "ledger"}, engine.XTxOptions{Key: "k-reopen"}, func(tx *engine.XTx) error {
		if _, err := tx.Insert("accounts", map[string]any{"balance": 7.0}); err != nil {
			return err
		}
		_, err := tx.Insert("ledger", map[string]any{"delta": 7.0})
		return err
	})
	if err != nil || !replay.Replayed || replay.TxID != res.TxID {
		t.Fatalf("replay after reopen = %+v, %v", replay, err)
	}
	after, err := accounts.Scan(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("replay after reopen applied again: %d records, want %d", len(after), len(before))
	}
}
