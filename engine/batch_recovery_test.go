package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// TestBatchRecovery_Undecided_TruncatesDeadRunAndExposesNoPrefix verifies that an
// undecided single-collection batch (prepared on disk but lacking a journal COMMIT)
// is rolled back at open recovery: the dead run is truncated from the active segment tail,
// and all reads (Get, GetByKey, FindByID, ScanStream, Count) see the full old state.
// Never a visible prefix.
func TestBatchRecovery_Undecided_TruncatesDeadRunAndExposesNoPrefix(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c, err := db.CreateCollection("items")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := c.EnsureIndex("sku"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if err := c.ensureKeyIndex(); err != nil {
		t.Fatalf("ensureKeyIndex: %v", err)
	}

	// Seed pre-existing records: id 1 and id 2
	id1, _, err := c.InsertWithKey("item-1", map[string]any{"sku": "SKU-A", "qty": 10.0})
	if err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	id2, _, err := c.InsertWithKey("item-2", map[string]any{"sku": "SKU-B", "qty": 20.0})
	if err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	preSegSize := c.active.Size()
	preCount, _ := c.Count(nil)
	if preCount != 2 {
		t.Fatalf("preCount = %d, want 2", preCount)
	}

	// Now simulate an in-flight batch that prepares stamped lines but crashes before commit:
	// op 1: update id 1 (qty -> 99)
	// op 2: insert id 3
	// op 3: insert id 4
	// op 4: delete id 2
	j, err := db.ensureXTxJournal()
	if err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	txid, err := j.allocateTxID("")
	if err != nil {
		t.Fatalf("allocateTxID: %v", err)
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	stampedOps := []stampedEntry{
		{
			ID:   id1,
			Op:   store.OpUpdate,
			Ts:   nowStr,
			Rev:  2,
			Data: map[string]any{KeyField: "item-1", "sku": "SKU-A", "qty": 99.0},
			Tx:   TxStamp{T: txid, I: 0, N: 4},
		},
		{
			ID:   3,
			Op:   store.OpInsert,
			Ts:   nowStr,
			Rev:  1,
			Data: map[string]any{KeyField: "item-3", "sku": "SKU-C", "qty": 30.0},
			Tx:   TxStamp{T: txid, I: 1, N: 4},
		},
		{
			ID:   4,
			Op:   store.OpInsert,
			Ts:   nowStr,
			Rev:  1,
			Data: map[string]any{KeyField: "item-4", "sku": "SKU-D", "qty": 40.0},
			Tx:   TxStamp{T: txid, I: 2, N: 4},
		},
		{
			ID:   id2,
			Op:   store.OpDelete,
			Ts:   nowStr,
			Rev:  2,
			Tx:   TxStamp{T: txid, I: 3, N: 4},
		},
	}

	for _, se := range stampedOps {
		if _, err := c.active.AppendStamped(se); err != nil {
			t.Fatalf("AppendStamped: %v", err)
		}
	}
	if err := c.active.Sync(); err != nil {
		t.Fatalf("Sync active: %v", err)
	}

	// Stamped entries are on disk, but NO commit record was written to j.
	// Close and simulate crash/reopen.
	_ = db.Close()

	// Reopen the database. Recovery must truncate the dead run and record ABORT.
	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen after undecided batch: %v", err)
	}
	defer db2.Close()

	c2, err := db2.Collection("items")
	if err != nil {
		t.Fatalf("Collection(items): %v", err)
	}

	// Check: active segment was truncated back to pre-batch size!
	if c2.active.Size() != preSegSize {
		t.Fatalf("active segment size = %d, want preSegSize %d (dead run not truncated)", c2.active.Size(), preSegSize)
	}

	// Check: full old state!
	// Count
	n, err := c2.Count(nil)
	if err != nil || n != 2 {
		t.Fatalf("Count = %d, err = %v, want 2", n, err)
	}

	// Get(id1) must have old qty = 10, rev = 1
	rec1, err := c2.Get(id1)
	if err != nil {
		t.Fatalf("Get(id1): %v", err)
	}
	if q, _ := rec1.Data["qty"].(float64); q != 10.0 {
		t.Fatalf("rec1 qty = %v, want 10.0", q)
	}
	if rec1.Rev != 1 {
		t.Fatalf("rec1 rev = %d, want 1", rec1.Rev)
	}

	// Get(id2) must still exist with qty = 20
	rec2, err := c2.Get(id2)
	if err != nil {
		t.Fatalf("Get(id2): %v", err)
	}
	if q, _ := rec2.Data["qty"].(float64); q != 20.0 {
		t.Fatalf("rec2 qty = %v, want 20.0", q)
	}

	// Get(3) and Get(4) must NOT exist
	if _, err := c2.Get(3); err == nil {
		t.Fatal("Get(3) succeeded, want not found (prefix exposed!)")
	}
	if _, err := c2.Get(4); err == nil {
		t.Fatal("Get(4) succeeded, want not found (prefix exposed!)")
	}

	// GetByKey
	if _, err := c2.GetByKey("item-3"); err == nil {
		t.Fatal("GetByKey(item-3) succeeded, want not found")
	}
	if _, err := c2.GetByKey("item-4"); err == nil {
		t.Fatal("GetByKey(item-4) succeeded, want not found")
	}

	// Secondary index scan: sku == SKU-C / SKU-D must not match
	skuMatches, _ := c2.indexLookup("sku", "SKU-C")
	if len(skuMatches) != 0 {
		t.Fatalf("secondary index has SKU-C: %v", skuMatches)
	}

	// ScanStream must return exactly id 1 and id 2
	var scanned []uint64
	_, _ = c2.ScanStream(context.Background(), ScanOptions{}, func(r ScanResult) error {
		scanned = append(scanned, r.ID)
		return nil
	})
	if len(scanned) != 2 || scanned[0] != id1 || scanned[1] != id2 {
		t.Fatalf("ScanStream = %v, want [%d, %d]", scanned, id1, id2)
	}

	// Journal record is ABORT
	st, _ := db2.XTxStatus(txid)
	if st != XTxAborted {
		t.Fatalf("XTxStatus(%s) = %v, want XTxAborted", txid, st)
	}

	// New write succeeds and assigns next id monotonically
	newID, _, err := c2.Insert(map[string]any{"sku": "SKU-E"})
	if err != nil {
		t.Fatalf("new insert after recovery: %v", err)
	}
	if newID <= id2 {
		t.Fatalf("newID = %d, want > %d", newID, id2)
	}
}

// TestBatchRecovery_Committed_ReplaysFullState verifies that a committed batch
// whose index was not persisted is fully replayed on reopen across primary and
// secondary indexes, exposing the full new state and never a prefix.
func TestBatchRecovery_Committed_ReplaysFullState(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, err := db.CreateCollection("items")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := c.EnsureIndex("category"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}

	// Batch 1: insert 3 items atomically
	recs := []map[string]any{
		{"category": "books", "title": "B1"},
		{"category": "books", "title": "B2"},
		{"category": "electronics", "title": "E1"},
	}
	ids, _, err := c.InsertMany(recs, time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids len = %d, want 3", len(ids))
	}

	// Close cleanly. Then delete index.json to simulate recovery without persisted index.
	_ = db.Close()
	_ = os.Remove(filepath.Join(dir, "items", "index.json"))
	_ = os.WriteFile(filepath.Join(dir, "items", "sidx_category.json"), []byte("{}"), 0o644)

	// Reopen
	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	c2, err := db2.Collection("items")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// All 3 records must be live and correct
	count, err := c2.Count(nil)
	if err != nil || count != 3 {
		t.Fatalf("Count = %d, err = %v, want 3", count, err)
	}

	for i, id := range ids {
		rec, err := c2.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if rec.Data["title"] != recs[i]["title"] {
			t.Fatalf("rec %d title = %v, want %v", id, rec.Data["title"], recs[i]["title"])
		}
	}

	// Secondary index must match
	bookMatches, _ := c2.indexLookup("category", "books")
	if len(bookMatches) != 2 {
		t.Fatalf("books count = %d, want 2", len(bookMatches))
	}

	// Repeated recovery (second reopen) must also yield exact same state
	_ = db2.Close()
	db3, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen 2: %v", err)
	}
	defer db3.Close()
	c3, _ := db3.Collection("items")
	if n, _ := c3.Count(nil); n != 3 {
		t.Fatalf("reopen 2 count = %d, want 3", n)
	}
}

// TestBatchRecovery_IncompleteRun_FailsClosed verifies that if a batch is COMMITTED
// in the journal but only a prefix k < N of its entries exists on disk, recovery
// refuses to open the database with ErrXTxIncomplete (fail-closed, never prefix-visible).
func TestBatchRecovery_IncompleteRun_FailsClosed(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, _ := db.CreateCollection("items")
	_, _, err = c.InsertMany(batchRecs(5), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Read segment and find offset of the 3rd entry so we can simulate crash where
	// only entries 0 and 1 made it to disk.
	activePath := c.active.Path()
	_ = db.Close()

	sr, err := ScanSegmentTolerant(activePath)
	if err != nil || len(sr.Entries) != 5 {
		t.Fatalf("ScanSegmentTolerant = %d entries, want 5", len(sr.Entries))
	}

	// Truncate segment to end of entry 1 (so entries 2, 3, 4 are missing)
	truncateAt := sr.Entries[1].Offset + sr.Entries[1].Length
	if err := os.Truncate(activePath, truncateAt); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Open MUST fail closed with ErrXTxIncomplete
	db2, err := Open(dir, defaultConfig())
	if err == nil {
		_ = db2.Close()
		t.Fatal("Open succeeded on incomplete committed batch; want ErrXTxIncomplete")
	}
	if !errors.Is(err, ErrXTxIncomplete) {
		t.Fatalf("Open err = %v, want ErrXTxIncomplete", err)
	}

	// Second attempt must also fail closed
	_, err = Open(dir, defaultConfig())
	if !errors.Is(err, ErrXTxIncomplete) {
		t.Fatalf("repeated Open err = %v, want ErrXTxIncomplete", err)
	}
}

// TestBatchRecovery_CorruptRun_FailsClosed verifies that if a committed batch
// has a corrupted record on disk (bad CRC or altered byte), open recovery
// fails closed with ErrXTxIncomplete (digest mismatch or missing entry).
func TestBatchRecovery_CorruptRun_FailsClosed(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, _ := db.CreateCollection("items")
	_, _, err = c.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	activePath := c.active.Path()
	_ = db.Close()

	// Corrupt a byte in the middle of entry 1
	b, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	sr, _ := ScanSegmentTolerant(activePath)
	if len(sr.Entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(sr.Entries))
	}
	corruptPos := sr.Entries[1].Offset + sr.Entries[1].Length/2
	b[corruptPos] ^= 0xff
	if err := os.WriteFile(activePath, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// Open MUST fail closed
	_, err = Open(dir, defaultConfig())
	if err == nil {
		t.Fatal("Open succeeded on corrupt committed batch; want ErrXTxIncomplete or ErrIntegrity")
	}
	if !errors.Is(err, ErrXTxIncomplete) && !errors.Is(err, ErrIntegrity) {
		t.Fatalf("got err = %v, want ErrXTxIncomplete or ErrIntegrity", err)
	}
}

// TestBatchRecovery_CorruptJournal_FailsClosed verifies that if stamped batch entries
// exist on disk but xtx.journal is missing or empty/zeroed, open recovery fails closed
// with ErrXTxJournalMissing or ErrXTxRecoveryRequired, never adopting stamped lines as
// ordinary plain entries.
func TestBatchRecovery_CorruptJournal_FailsClosed(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, _ := db.CreateCollection("items")
	_, _, err = c.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	_ = db.Close()

	// Zero out xtx.journal
	jPath := filepath.Join(dir, xtxJournalFile)
	if err := os.WriteFile(jPath, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, defaultConfig())
	if !errors.Is(err, ErrXTxRecoveryRequired) {
		t.Fatalf("zeroed journal: got %v, want ErrXTxRecoveryRequired", err)
	}

	// Remove xtx.journal completely
	_ = os.Remove(jPath)
	_, err = Open(dir, defaultConfig())
	if !errors.Is(err, ErrXTxJournalMissing) && !errors.Is(err, ErrXTxRecoveryRequired) {
		t.Fatalf("missing journal: got %v, want ErrXTxJournalMissing", err)
	}
}

// TestBatchRecovery_Compaction_PreservesStampsAndDigests verifies that compaction
// preserves stamped entries and decision digests for committed batches, and
// that reopening after compaction successfully passes coordinator recovery.
func TestBatchRecovery_Compaction_PreservesStampsAndDigests(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	cfg := defaultConfig()
	cfg.SegmentMaxSize = 500 // small segments to force multiple segments
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, _ := db.CreateCollection("items")
	if err := c.EnsureIndex("n"); err != nil {
		t.Fatal(err)
	}

	// Write two batches
	_, _, err = c.InsertMany(batchRecs(4), time.Time{})
	if err != nil {
		t.Fatalf("batch 1: %v", err)
	}
	_, _, err = c.InsertMany(batchRecs(4), time.Time{})
	if err != nil {
		t.Fatalf("batch 2: %v", err)
	}

	// Rotate segment so older one is sealed
	if err := c.rotateSegment(); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Run compaction
	if err := c.compact(true); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Reopen database: coordinator recovery must verify digests and open cleanly
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	defer db2.Close()

	c2, err := db2.Collection("items")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	if n, _ := c2.Count(nil); n != 8 {
		t.Fatalf("count after compaction & reopen = %d, want 8", n)
	}

	// Secondary index works
	matches, _ := c2.indexLookup("n", "0")
	if len(matches) != 2 { // two batches each have n: 0
		t.Fatalf("n=0 matches = %d, want 2", len(matches))
	}
}

// TestBatchRecovery_Compaction_PurgesAbortedRuns verifies that compaction drops
// stamped entries whose transactions were aborted, keeping only committed data.
func TestBatchRecovery_Compaction_PurgesAbortedRuns(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c, _ := db.CreateCollection("items")

	// Seed 2 committed records
	id1, _, _ := c.Insert(map[string]any{"v": 1.0})
	id2, _, _ := c.Insert(map[string]any{"v": 2.0})
	_ = id2

	// Inject an aborted transaction run directly into active segment
	j, _ := db.ensureXTxJournal()
	txid, _ := j.allocateTxID("")
	if err := j.abort(txid, "", "recovery"); err != nil {
		t.Fatalf("abort: %v", err)
	}

	// Append stamped entries for txid (simulating a dead run that was aborted)
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	_, _ = c.active.AppendStamped(stampedEntry{
		ID:   id1,
		Op:   store.OpUpdate,
		Ts:   nowStr,
		Rev:  2,
		Data: map[string]any{"v": 999.0},
		Tx:   TxStamp{T: txid, I: 0, N: 1},
	})

	// Also write a committed record after it so the aborted entry is not at the active tail
	_, _, _ = c.Insert(map[string]any{"v": 3.0})
	_ = c.active.Sync()

	// Rotate segment so the aborted entry is in a sealed segment
	if err := c.rotateSegment(); err != nil {
		t.Fatal(err)
	}

	// Compact
	if err := c.compact(true); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// After compaction, id1 must still have v = 1.0 (the aborted update was purged)
	rec1, err := c.Get(id1)
	if err != nil {
		t.Fatalf("Get(id1): %v", err)
	}
	if v, _ := rec1.Data["v"].(float64); v != 1.0 {
		t.Fatalf("id1 v = %v, want 1.0", v)
	}
	if n, _ := c.Count(nil); n != 3 {
		t.Fatalf("Count = %d, want 3", n)
	}

	// Verify the compacted segment file contains NO entries for txid
	for _, s := range c.sealed {
		_ = s.ScanStampedFromOffset(0, func(_ int64, _ store.Entry, tx *TxStamp) error {
			if tx != nil && tx.T == txid {
				t.Fatalf("found aborted txid %s in compacted segment", txid)
			}
			return nil
		})
	}
	_ = db.Close()
}

// TestBatchRecovery_CoverageTracking_XTxApplied verifies that:
// 1. Persisted index coverage records XTxApplied and XTxEpoch when stamped entries exist.
// 2. A clean reopen trusts the coverage and does tail replay (FullRebuilds == 0).
// 3. Modifying XTxApplied to an uncommitted txid forces a full rebuild.
// 4. Missing XTxApplied over committed stamped bytes forces a full rebuild.
// 5. Epoch mismatch forces a full rebuild.
func TestBatchRecovery_CoverageTracking_XTxApplied(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, _ := db.CreateCollection("items")

	_, _, err = c.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Persist indexes explicitly
	if err := c.persistIndexes(); err != nil {
		t.Fatalf("persistIndexes: %v", err)
	}

	// Inspect index.json
	indexPath := filepath.Join(dir, "items", "index.json")
	b, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile index.json: %v", err)
	}
	var f struct {
		Coverage []SegmentCoverage `json:"coverage"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("Unmarshal index.json: %v", err)
	}
	if len(f.Coverage) == 0 {
		t.Fatal("empty coverage in index.json")
	}
	cv := f.Coverage[0]
	if len(cv.XTxApplied) == 0 {
		t.Fatal("XTxApplied is empty in index.json coverage, want recorded txids")
	}
	txid := cv.XTxApplied[0]
	if cv.XTxEpoch == "" {
		t.Fatal("XTxEpoch is empty in index.json coverage")
	}

	_ = db.Close()

	// 1. Clean reopen: coverage is trusted!
	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("clean reopen: %v", err)
	}
	c2, _ := db2.Collection("items")
	stats := c2.IndexRecoveryStats()
	if stats.FullRebuilds != 0 {
		t.Fatalf("FullRebuilds = %d, want 0 (clean coverage should be trusted)", stats.FullRebuilds)
	}
	_ = db2.Close()

	// 2. Tamper: corrupt XTxApplied to an unknown txid
	tamperedPayload := func(mod func(*indexPayload)) {
		var raw map[string]any
		_ = json.Unmarshal(b, &raw)
		// We re-save index using Snapshot and Persist or edit the payload directly
	}
	_ = tamperedPayload

	// Create sub-test for tampered unknown txid
	{
		dirUnknown := copyDirImage(t, dir)
		idxPath := filepath.Join(dirUnknown, "items", "index.json")
		tamperIndex(t, idxPath, func(cov *[]SegmentCoverage) {
			(*cov)[0].XTxApplied = []string{"tx-unknown-00000000"}
		})

		dbT, err := Open(dirUnknown, defaultConfig())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		cT, _ := dbT.Collection("items")
		if cT.IndexRecoveryStats().FullRebuilds == 0 {
			t.Fatal("FullRebuilds == 0, want rebuild when XTxApplied has unknown txid")
		}
		_ = dbT.Close()
	}

	// 3. Tamper: remove XTxApplied while covering stamped committed bytes
	{
		dirEmpty := copyDirImage(t, dir)
		idxPath := filepath.Join(dirEmpty, "items", "index.json")
		tamperIndex(t, idxPath, func(cov *[]SegmentCoverage) {
			(*cov)[0].XTxApplied = nil
		})

		dbT, err := Open(dirEmpty, defaultConfig())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		cT, _ := dbT.Collection("items")
		if cT.IndexRecoveryStats().FullRebuilds == 0 {
			t.Fatal("FullRebuilds == 0, want rebuild when XTxApplied is empty over stamped committed bytes")
		}
		_ = dbT.Close()
	}

	// 4. Tamper: wrong XTxEpoch
	{
		dirEpoch := copyDirImage(t, dir)
		idxPath := filepath.Join(dirEpoch, "items", "index.json")
		tamperIndex(t, idxPath, func(cov *[]SegmentCoverage) {
			(*cov)[0].XTxEpoch = "0000000000000000"
		})

		dbT, err := Open(dirEpoch, defaultConfig())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		cT, _ := dbT.Collection("items")
		if cT.IndexRecoveryStats().FullRebuilds == 0 {
			t.Fatal("FullRebuilds == 0, want rebuild on XTxEpoch mismatch")
		}
		_ = dbT.Close()
	}
	_ = txid
}

// TestBatchRecovery_StandaloneOpenRefusedWhenStampedEntriesExist verifies that
// OpenCollection directly on a collection that has stamped entries fails with
// ErrXTxRecoveryRequired, even if xtx.format was removed.
func TestBatchRecovery_StandaloneOpenRefusedWhenStampedEntriesExist(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatalf("ensureXTxJournal: %v", err)
	}
	c, _ := db.CreateCollection("items")
	_, _, err = c.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	_ = db.Close()

	// Direct OpenCollection must fail with ErrXTxRecoveryRequired
	_, err = OpenCollection("items", dir, CollectionConfig{})
	if !errors.Is(err, ErrXTxRecoveryRequired) {
		t.Fatalf("OpenCollection err = %v, want ErrXTxRecoveryRequired", err)
	}

	// Even if xtx.format is removed, xtx.journal remains -> still refuses
	_ = os.Remove(filepath.Join(dir, xtxFormatFile))
	_, err = OpenCollection("items", dir, CollectionConfig{})
	if !errors.Is(err, ErrXTxRecoveryRequired) {
		t.Fatalf("OpenCollection without format err = %v, want ErrXTxRecoveryRequired", err)
	}
}

// Helper to tamper index.json
func tamperIndex(t *testing.T, path string, modify func(cov *[]SegmentCoverage)) {
	t.Helper()
	idx := newIndex()
	if err := idx.Load(path); err != nil {
		t.Fatalf("Load index for tamper: %v", err)
	}
	idx.mu.Lock()
	modify(&idx.coverage)
	snap := &IndexSnapshot{entries: idx.entries, coverage: idx.coverage}
	idx.mu.Unlock()
	if err := snap.Persist(path); err != nil {
		t.Fatalf("Persist tampered index: %v", err)
	}
}
