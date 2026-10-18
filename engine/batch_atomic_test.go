package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func withAtomicBatchWriter(t *testing.T, on bool) {
	t.Helper()
	prev := atomicBatchWriterReady
	atomicBatchWriterReady = on
	t.Cleanup(func() { atomicBatchWriterReady = prev })
}

func xtxFilesExist(dir string) bool {
	for _, n := range []string{xtxFormatFile, xtxJournalFile} {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			return true
		}
	}
	return false
}

func batchRecs(n int) []map[string]any {
	r := make([]map[string]any, n)
	for i := range r {
		r[i] = map[string]any{"n": float64(i)}
	}
	return r
}

// The gate must not open while a prerequisite is missing.
func TestAtomicBatchGate_ClosedWhilePrerequisitesMissing(t *testing.T) {
	if len(atomicBatchPrereqs.missing()) > 0 && atomicBatchWriterReady {
		t.Fatalf("atomic batch writer enabled with missing prerequisites: %v", atomicBatchPrereqs.missing())
	}
	if atomicBatchWriterReady {
		t.Fatal("atomic batch writer must stay disabled in this change")
	}
	if n := len((atomicBatchCapabilities{}).missing()); n != 5 {
		t.Fatalf("zero capabilities: missing = %d, want 5", n)
	}
	if n := len((atomicBatchCapabilities{true, true, true, true, true}).missing()); n != 0 {
		t.Fatalf("full capabilities: missing = %d, want 0", n)
	}
}

// With the gate closed (production default) batches write no XTx bytes and
// behave exactly as before, on legacy roots and on roots that hold xtx.format.
func TestBatchRouting_GateClosedWritesNoXTxBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := db.CreateCollection("a")
	if err != nil {
		t.Fatal(err)
	}
	ids, _, err := c.InsertMany(batchRecs(3), time.Time{})
	if err != nil || len(ids) != 3 {
		t.Fatalf("InsertMany: %v %v", ids, err)
	}
	if err := c.CommitTx([]txOp{{kind: txOpInsert, id: c.ReserveID(), data: map[string]any{"x": 1.0}, ts: time.Now()}, {kind: txOpInsert, id: c.ReserveID(), data: map[string]any{"x": 2.0}, ts: time.Now()}}); err != nil {
		t.Fatalf("CommitTx: %v", err)
	}
	if xtxFilesExist(dir) {
		t.Fatal("legacy root gained xtx.* files with the gate closed")
	}
	// Even on a root that already holds the XTx format, nothing is stamped.
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(filepath.Join(dir, xtxJournalFile))
	if _, _, err := c.InsertMany(batchRecs(2), time.Time{}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(filepath.Join(dir, xtxJournalFile))
	if before.Size() != after.Size() {
		t.Fatalf("journal grew %d -> %d with the gate closed", before.Size(), after.Size())
	}
}

func TestBatchRouting_Decisions(t *testing.T) {
	withAtomicBatchWriter(t, true)

	// Single entry is never a batch.
	db := openTestDB(t, defaultConfig())
	c, _ := db.CreateCollection("a")
	if r, err := c.routeBatch(1); r != batchRouteLegacy || err != nil {
		t.Fatalf("n=1: %v %v", r, err)
	}
	// Legacy root: stay on the legacy path until opt-in exists.
	if r, err := c.routeBatch(2); r != batchRouteLegacy || err != nil {
		t.Fatalf("legacy root: %v %v", r, err)
	}
	// Root with xtx.format: atomic.
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}
	if r, err := c.routeBatch(2); r != batchRouteAtomic || err != nil {
		t.Fatalf("xtx root: %v %v", r, err)
	}

	// Unmanaged collection: actionable error unless explicitly allowed.
	u, err := OpenCollection("u", t.TempDir(), defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if _, _, err := u.InsertMany(batchRecs(2), time.Time{}); !errors.Is(err, ErrBatchNotAtomic) {
		t.Fatalf("unmanaged InsertMany = %v, want ErrBatchNotAtomic", err)
	}
	if n, _ := u.Count(nil); n != 0 {
		t.Fatalf("rejected batch wrote %d records", n)
	}
	if _, _, err := u.InsertMany(batchRecs(1), time.Time{}); err != nil {
		t.Fatalf("unmanaged single insert via InsertMany: %v", err)
	}
	cfg := defaultConfig()
	cfg.AllowNonAtomicBatch = true
	u2, err := OpenCollection("u2", t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer u2.Close()
	if _, _, err := u2.InsertMany(batchRecs(2), time.Time{}); err != nil {
		t.Fatalf("AllowNonAtomicBatch: %v", err)
	}
}

func TestAtomicBatch_InsertManyAndCommitTxThroughDecision(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	c, _ := db.CreateCollection("a")
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}

	ids, ts, err := c.InsertMany(batchRecs(3), time.Time{})
	if err != nil || len(ids) != 3 {
		t.Fatalf("InsertMany: %v %v", ids, err)
	}
	for i, id := range ids {
		r, err := c.Get(id)
		if err != nil || r.Data["n"] != float64(i) || r.Rev != 1 || !r.Ts.Equal(ts) {
			t.Fatalf("record %d: %+v err=%v (ts want %v)", id, r, err, ts)
		}
	}

	// Single-collection CommitTx: update + insert + delete in one decision.
	newID := c.ReserveID()
	now := time.Now().UTC()
	err = c.CommitTx([]txOp{
		{kind: txOpUpdate, id: ids[0], data: map[string]any{"n": 100.0}, ts: now},
		{kind: txOpInsert, id: newID, data: map[string]any{"n": 7.0}, ts: now},
		{kind: txOpDelete, id: ids[1], ts: now},
	})
	if err != nil {
		t.Fatalf("CommitTx: %v", err)
	}
	if r, _ := c.Get(ids[0]); r.Data["n"] != 100.0 || r.Rev != 2 {
		t.Fatalf("update not applied: %+v", r)
	}
	if _, err := c.Get(ids[1]); err == nil {
		t.Fatal("deleted record still visible")
	}

	// Durable identity: both batches are COMMITTED decisions in the journal.
	j := db.journalOrNil()
	if j == nil {
		t.Fatal("no journal")
	}
	if n := len(j.decisionsMap()); n != 2 {
		t.Fatalf("decisions = %d, want 2", n)
	}
	for tx := range j.decisionsMap() {
		if st, _ := db.XTxStatus(tx); st != XTxCommitted {
			t.Fatalf("tx %s status %s, want COMMITTED", tx, st)
		}
	}

	// Reopen: all-or-nothing state survives.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	c2, _ := db2.Collection("a")
	if n, _ := c2.Count(nil); n != 3 { // 3 inserted - 1 deleted + 1 inserted
		t.Fatalf("after reopen count = %d, want 3", n)
	}
}

func TestAtomicBatch_ErrorsAreActionableAndWriteNothing(t *testing.T) {
	withAtomicBatchWriter(t, true)
	db := openTestDB(t, defaultConfig())
	c, _ := db.CreateCollection("a")
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}
	// Over the XTx op limit: refused up front, nothing written.
	_, _, err := c.InsertMany(batchRecs(defaultXTxLimits().MaxOps+1), time.Time{})
	if !errors.Is(err, ErrXTxTooLarge) || XTxRetrySafe(err) {
		t.Fatalf("oversize = %v, want ErrXTxTooLarge (not retry-safe)", err)
	}
	if n, _ := c.Count(nil); n != 0 {
		t.Fatalf("oversize batch wrote %d records", n)
	}
	// A fenced collection aborts with ErrXTxDurability and no decision.
	// CommitTx on a vanished target is a conflict, retry-safe, and carries no commit.
	err = c.CommitTx([]txOp{
		{kind: txOpDelete, id: 999, ts: time.Now()},
		{kind: txOpDelete, id: 1000, ts: time.Now()},
	})
	if !errors.Is(err, ErrXTxConflict) || !XTxRetrySafe(err) {
		t.Fatalf("missing target = %v, want retry-safe ErrXTxConflict", err)
	}

	// S4 decision sync failure yields ErrXTxOutcomeUnknown, not retry-safe,
	// with a non-empty txid recoverable via BatchTxID and resolvable via XTxStatus.
	dir := t.TempDir()
	ffs := newFaultFS()
	cfg := defaultConfig()
	cfg.wrapFile = ffs.wrap
	dbFault, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer dbFault.Close()
	cFault, err := dbFault.CreateCollection("faulty")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dbFault.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}
	// 1 participant sync active segment (sync #1), then journal COMMIT sync (sync #2)
	ffs.failSyncAt(ffs.count("sync")+2, nil)
	_, _, err = cFault.InsertMany(batchRecs(2), time.Time{})
	if !errors.Is(err, ErrXTxOutcomeUnknown) {
		t.Fatalf("got %v, want ErrXTxOutcomeUnknown", err)
	}
	if XTxRetrySafe(err) {
		t.Fatal("ErrXTxOutcomeUnknown must not be retry-safe")
	}
	txid := BatchTxID(err)
	if txid == "" {
		t.Fatal("BatchTxID should return non-empty txid on ErrXTxOutcomeUnknown")
	}
	st, _ := dbFault.XTxStatus(txid)
	if st != XTxPending {
		t.Fatalf("XTxStatus(%s) = %v, want XTxPending", txid, st)
	}
}

func TestBatchTxID(t *testing.T) {
	if got := BatchTxID(nil); got != "" {
		t.Fatalf("BatchTxID(nil) = %q, want empty", got)
	}
	if got := BatchTxID(errors.New("plain error")); got != "" {
		t.Fatalf("BatchTxID(plain error) = %q, want empty", got)
	}
	xe := &XTxError{Tx: "tx-batch-test", Err: ErrXTxOutcomeUnknown}
	if got := BatchTxID(xe); got != "tx-batch-test" {
		t.Fatalf("BatchTxID(xe) = %q, want tx-batch-test", got)
	}
}

