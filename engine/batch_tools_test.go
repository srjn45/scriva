package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

func TestSnapshot_SingleParticipantBatchPreservesDecisions(t *testing.T) {
	withAtomicBatchWriter(t, true)
	src := t.TempDir()
	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	db, err := Open(src, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	items, err := db.CreateCollection("items")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	ids, ts, err := items.InsertMany(batchRecs(5), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(ids) != 5 {
		t.Fatalf("ids len = %d, want 5", len(ids))
	}
	if ts.IsZero() {
		t.Fatal("InsertMany returned zero timestamp")
	}

	// CommitTx batch
	err = items.CommitTx([]txOp{
		{kind: txOpUpdate, id: ids[0], data: map[string]any{"n": 100.0, "updated": true}, ts: time.Now()},
		{kind: txOpDelete, id: ids[1], ts: time.Now()},
	})
	if err != nil {
		t.Fatalf("CommitTx: %v", err)
	}

	j := db.journalOrNil()
	if j == nil {
		t.Fatal("expected non-nil xtx journal")
	}
	decisions := j.decisionsMap()
	if len(decisions) != 2 {
		t.Fatalf("decisions count = %d, want 2", len(decisions))
	}

	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	// Restore into a fresh directory
	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)

	for _, base := range []string{xtxFormatFile, xtxJournalFile} {
		if _, err := os.Stat(filepath.Join(dst, base)); err != nil {
			t.Fatalf("restored root missing coordinator file %s: %v", base, err)
		}
	}

	// Check on-disk segment entries in restored directory:
	// stamped entries must preserve their TxStamp and never be flattened into plain v1 lines.
	segs, _ := filepath.Glob(filepath.Join(dst, "items", "seg_*.ndjson"))
	if len(segs) == 0 {
		t.Fatal("no segments in restored items collection")
	}
	rep, err := ScanSegmentTolerant(segs[0])
	if err != nil {
		t.Fatalf("ScanSegmentTolerant: %v", err)
	}
	var stampedCount int
	for _, se := range rep.Entries {
		if se.Tx != nil && se.Tx.T != "" {
			stampedCount++
		}
	}
	if stampedCount == 0 {
		t.Fatal("restored segment has no stamped entries; batch decisions were flattened!")
	}

	// Reopen restored DB
	restored, err := Open(dst, defaultConfig())
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer restored.Close()

	for txid := range decisions {
		st, _ := restored.XTxStatus(txid)
		if st != XTxCommitted {
			t.Fatalf("tx %s status %v, want XTxCommitted", txid, st)
		}
	}

	rItems, err := restored.Collection("items")
	if err != nil {
		t.Fatalf("restored items collection: %v", err)
	}

	// Count: 5 inserted, 1 deleted = 4
	n, err := rItems.Count(nil)
	if err != nil || n != 4 {
		t.Fatalf("Count = %d, err = %v; want 4", n, err)
	}

	// Check updated record
	rec, _, err := rItems.FindByID(ids[0])
	if err != nil {
		t.Fatalf("FindByID(%d): %v", ids[0], err)
	}
	if rec["n"] != 100.0 || rec["updated"] != true {
		t.Fatalf("updated record mismatch: %v", rec)
	}

	// Check deleted record is gone
	if _, _, err := rItems.FindByID(ids[1]); err == nil {
		t.Fatalf("deleted record %d is still visible", ids[1])
	}
}

func TestSnapshot_SingleParticipantBatchUncommittedTruncated(t *testing.T) {
	withAtomicBatchWriter(t, true)
	src := t.TempDir()
	db, err := Open(src, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	c, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	j, err := db.ensureXTxJournal()
	if err != nil {
		t.Fatal(err)
	}

	// Staged 1-participant batch with txid, but NO commit decision appended to journal.
	txid, err := j.allocateTxID("uncommitted-batch")
	if err != nil {
		t.Fatal(err)
	}
	id1, id2 := c.ReserveID(), c.ReserveID()
	for i, id := range []uint64{id1, id2} {
		line := stampedEntry{
			ID:   id,
			Op:   store.OpInsert,
			Rev:  1,
			Data: map[string]any{"val": float64(i)},
			Tx:   TxStamp{T: txid, I: uint32(i), N: 2},
		}
		if _, err := c.active.AppendStamped(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.active.Sync(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	db.Close()

	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)

	// Open restored DB: uncommitted batch must not be fabricated into committed.
	restored, err := Open(dst, defaultConfig())
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer restored.Close()

	rc, err := restored.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := rc.Count(nil); n != 0 {
		t.Fatalf("uncommitted batch visible after restore! count = %d, want 0", n)
	}
	st, _ := restored.XTxStatus(txid)
	if st != XTxAborted {
		t.Fatalf("uncommitted batch status = %v, want XTxAborted", st)
	}
}

func TestSnapshot_PreservesOptedInRootWithoutTransactions(t *testing.T) {
	src := t.TempDir()
	// Open with opt-in on an empty directory: creates xtx.format
	if err := ensureXTxFormat(src, "opt-in"); err != nil {
		t.Fatal(err)
	}
	db, err := Open(src, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	db.Close()

	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)

	if _, err := os.Stat(filepath.Join(dst, xtxFormatFile)); err != nil {
		t.Fatalf("restored root missing xtx.format: %v", err)
	}
}

func TestVerify_SingleParticipantBatchClean(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := db.CreateCollection("col")
	_, _, err = c.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
	if err != nil {
		t.Fatalf("VerifyDir: %v", err)
	}
	for _, f := range rep.Findings {
		if f.Severity >= SeverityRepairableIndex {
			t.Fatalf("unexpected finding: %v", f)
		}
	}
}

func TestVerify_SingleParticipantContradictoryEvidence_FailsClosed(t *testing.T) {
	withAtomicBatchWriter(t, true)

	setupBatch := func(t *testing.T) (string, string) {
		t.Helper()
		dir := t.TempDir()
		cfg := defaultConfig()
		cfg.EnableAtomicBatches = true
		db, err := Open(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		c, err := db.CreateCollection("col")
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = c.InsertMany(batchRecs(3), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		txid := ""
		for tx := range db.journalOrNil().decisionsMap() {
			txid = tx
		}
		db.Close()
		return dir, txid
	}

	t.Run("missing_journal", func(t *testing.T) {
		dir, _ := setupBatch(t)
		if err := os.Remove(filepath.Join(dir, xtxJournalFile)); err != nil {
			t.Fatal(err)
		}
		rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, f := range rep.Findings {
			if f.Code == CodeXTxJournalMissing {
				found = true
			}
		}
		if !found {
			t.Fatal("expected CodeXTxJournalMissing finding")
		}
	})

	t.Run("undecided_run", func(t *testing.T) {
		dir := t.TempDir()
		db, err := Open(dir, defaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		c, _ := db.CreateCollection("col")
		j, _ := db.ensureXTxJournal()
		txid, _ := j.allocateTxID("undecided")
		// Write stamped entry without decision
		entry := stampedEntry{ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"a": 1}, Tx: TxStamp{T: txid, I: 0, N: 2}}
		_, _ = c.active.AppendStamped(entry)
		_ = c.active.Sync()
		db.Close()

		rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, f := range rep.Findings {
			if f.Code == CodeXTxUndecidedRun {
				found = true
			}
		}
		if !found {
			t.Fatal("expected CodeXTxUndecidedRun finding")
		}
	})

	t.Run("digest_mismatch", func(t *testing.T) {
		dir, _ := setupBatch(t)
		// Truncate last entry from segment to cause digest mismatch
		segs, _ := filepath.Glob(filepath.Join(dir, "col", "seg_*.ndjson"))
		if len(segs) == 0 {
			t.Fatal("no segments")
		}
		sr, err := ScanSegmentTolerant(segs[0])
		if err != nil || len(sr.Entries) < 2 {
			t.Fatalf("scan: %v, entries: %d", err, len(sr.Entries))
		}
		// Truncate to second entry offset (missing third entry of 3-entry batch)
		last := sr.Entries[len(sr.Entries)-1]
		if err := os.Truncate(segs[0], last.Offset); err != nil {
			t.Fatal(err)
		}

		rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, f := range rep.Findings {
			if f.Code == CodeXTxDigestMismatch {
				found = true
			}
		}
		if !found {
			t.Fatal("expected CodeXTxDigestMismatch finding")
		}
	})

	t.Run("missing_participant", func(t *testing.T) {
		dir, _ := setupBatch(t)
		// Remove collection directory completely, leaving journal with committed decision
		if err := os.RemoveAll(filepath.Join(dir, "col")); err != nil {
			t.Fatal(err)
		}
		rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, f := range rep.Findings {
			if f.Code == CodeXTxParticipantMissing {
				found = true
			}
		}
		if !found {
			t.Fatal("expected CodeXTxParticipantMissing finding")
		}
	})
}

func TestRepair_SingleParticipantContradictoryEvidence_Refuses(t *testing.T) {
	withAtomicBatchWriter(t, true)

	setupBatch := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		cfg := defaultConfig()
		cfg.EnableAtomicBatches = true
		db, err := Open(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		c, err := db.CreateCollection("col")
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = c.InsertMany(batchRecs(3), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
		return dir
	}

	t.Run("refuses_missing_journal", func(t *testing.T) {
		dir := setupBatch(t)
		if err := os.Remove(filepath.Join(dir, xtxJournalFile)); err != nil {
			t.Fatal(err)
		}
		_, err := Repair(context.Background(), dir, RepairOptions{Salvage: true})
		if !errors.Is(err, ErrXTxIncomplete) {
			t.Fatalf("got %v, want ErrXTxIncomplete", err)
		}
	})

	t.Run("refuses_digest_mismatch", func(t *testing.T) {
		dir := setupBatch(t)
		segs, _ := filepath.Glob(filepath.Join(dir, "col", "seg_*.ndjson"))
		sr, _ := ScanSegmentTolerant(segs[0])
		last := sr.Entries[len(sr.Entries)-1]
		if err := os.Truncate(segs[0], last.Offset); err != nil {
			t.Fatal(err)
		}
		_, err := Repair(context.Background(), dir, RepairOptions{Salvage: true})
		if !errors.Is(err, ErrXTxIncomplete) {
			t.Fatalf("got %v, want ErrXTxIncomplete", err)
		}
	})
}

func TestRepairSalvage_PreservesStampedBatchEntries(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.CreateCollection("salvage_col")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.InsertMany(batchRecs(3), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Append corrupt bytes after the valid batch in the segment file
	segs, _ := filepath.Glob(filepath.Join(dir, "salvage_col", "seg_*.ndjson"))
	if len(segs) == 0 {
		t.Fatal("no segment")
	}
	f, err := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("{corrupted bad line\n")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Run repair with Salvage: true
	rep, err := Repair(context.Background(), dir, RepairOptions{Salvage: true})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if len(rep.Collections) == 0 || rep.Collections[0].Salvage == nil {
		t.Fatal("expected salvage action in repair report")
	}

	// Verify the new salvaged segment preserves the stamped batch entries
	salvagedSeg := filepath.Join(dir, "salvage_col", rep.Collections[0].Salvage.NewSegment)
	sRep, err := ScanSegmentTolerant(salvagedSeg)
	if err != nil {
		t.Fatalf("ScanSegmentTolerant on salvaged: %v", err)
	}
	if len(sRep.Entries) != 3 {
		t.Fatalf("salvaged entries = %d, want 3", len(sRep.Entries))
	}
	for i, e := range sRep.Entries {
		if e.Tx == nil || e.Tx.T == "" {
			t.Fatalf("entry %d was flattened to unstamped line!", i)
		}
	}

	// Reopen DB: all 3 records visible
	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen after repair: %v", err)
	}
	defer db2.Close()
	c2, _ := db2.Collection("salvage_col")
	if n, _ := c2.Count(nil); n != 3 {
		t.Fatalf("after repair count = %d, want 3", n)
	}
}

func TestRepairBackup_CopiesAndVerifiesCoordinatorFiles(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := db.CreateCollection("b_col")
	_, _, _ = c.InsertMany(batchRecs(2), time.Time{})
	db.Close()

	// Trigger a benign repair (e.g. touch or remove index.json)
	_ = os.Remove(filepath.Join(dir, "b_col", "index.json"))

	backupParent := t.TempDir()
	rep, err := Repair(context.Background(), dir, RepairOptions{BackupDir: backupParent})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if rep.BackupDir == "" {
		t.Fatal("expected non-empty BackupDir")
	}

	// Check coordinator files exist in backup dir and match source
	for _, base := range []string{xtxFormatFile, xtxJournalFile} {
		srcBytes, err := os.ReadFile(filepath.Join(dir, base))
		if err != nil {
			t.Fatalf("read src %s: %v", base, err)
		}
		bkBytes, err := os.ReadFile(filepath.Join(rep.BackupDir, base))
		if err != nil {
			t.Fatalf("read backup %s: %v", base, err)
		}
		if !bytes.Equal(srcBytes, bkBytes) {
			t.Fatalf("backup %s does not match source byte-for-byte", base)
		}
	}
}

func TestLegacyRoot_UnconvertedKeepsLegacyBatch(t *testing.T) {
	dir := t.TempDir()
	// Create legacy collection directory with a segment file to mark it as legacy
	colDir := filepath.Join(dir, "legacy_col")
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(colDir, "seg_000001.ndjson")
	if err := os.WriteFile(segPath, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	// Open without EnableAtomicBatches
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(filepath.Join(dir, xtxFormatFile)); err == nil {
		t.Fatal("xtx.format should not be created for unconverted legacy root")
	}

	c, err := db.Collection("legacy_col")
	if err != nil {
		t.Fatal(err)
	}
	ids, _, err := c.InsertMany(batchRecs(2), time.Time{})
	if err != nil || len(ids) != 2 {
		t.Fatalf("InsertMany: %v, len=%d", err, len(ids))
	}

	// No xtx.* files created
	if xtxFilesExist(dir) {
		t.Fatal("unconverted legacy root created xtx files")
	}
}

func TestLegacyRoot_OptInWithEnableAtomicBatches(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()
	colDir := filepath.Join(dir, "legacy_col")
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(colDir, "seg_000001.ndjson")
	if err := os.WriteFile(segPath, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	// Open with EnableAtomicBatches: true
	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open with opt-in: %v", err)
	}
	defer db.Close()

	// xtx.format must be created
	f, err := readXTxFormat(dir)
	if err != nil || f == nil {
		t.Fatalf("readXTxFormat: %v, f: %v", err, f)
	}
	if f.CreatedBy != "opt-in" {
		t.Fatalf("CreatedBy = %q, want 'opt-in'", f.CreatedBy)
	}

	c, err := db.Collection("legacy_col")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.InsertMany(batchRecs(2), time.Time{})
	if err != nil {
		t.Fatalf("InsertMany after opt-in: %v", err)
	}

	// Journal must exist now
	if _, err := os.Stat(filepath.Join(dir, xtxJournalFile)); err != nil {
		t.Fatalf("xtx.journal not created after batch: %v", err)
	}
}

func TestNewRoot_OptInEnablesAtomicBatches(t *testing.T) {
	withAtomicBatchWriter(t, true)
	dir := t.TempDir()

	cfg := defaultConfig()
	cfg.EnableAtomicBatches = true
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open fresh root with opt-in: %v", err)
	}
	defer db.Close()

	f, err := readXTxFormat(dir)
	if err != nil || f == nil {
		t.Fatalf("opted-in root should have xtx.format: %v", err)
	}
	if f.CreatedBy != "opt-in" {
		t.Fatalf("CreatedBy = %q, want 'opt-in'", f.CreatedBy)
	}
}

func TestDowngrade_ConvertedRootRefusedWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	// Write format with an unsupported future reader version
	unsupportedFormat, err := encodeXTxFormat(xtxFormat{
		Format:    xtxFormatName,
		MinReader: 999,
		Features:  []string{"journal", "stamped-v2"},
		CreatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, xtxFormatFile), unsupportedFormat, 0o644); err != nil {
		t.Fatal(err)
	}
	// Place a collection segment with valid content
	colDir := filepath.Join(dir, "col")
	_ = os.MkdirAll(colDir, 0o755)
	segPath := filepath.Join(colDir, "seg_000001.ndjson")
	_ = os.WriteFile(segPath, []byte("{\"id\":1,\"op\":\"insert\",\"rev\":1,\"data\":{\"a\":1},\"crc\":0}\n"), 0o644)

	beforeStat, _ := os.Stat(segPath)

	// Open must fail closed with ErrFormatTooNew
	_, err = Open(dir, defaultConfig())
	if !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("Open = %v, want ErrFormatTooNew", err)
	}

	// VerifyDir must also report format error without touching files
	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
	if err != nil {
		t.Fatalf("VerifyDir error: %v", err)
	}
	var hasCorruptFinding bool
	for _, f := range rep.Findings {
		if f.Code == CodeXTxJournalCorrupt {
			hasCorruptFinding = true
		}
	}
	if !hasCorruptFinding {
		t.Fatal("expected CodeXTxJournalCorrupt finding for unsupported format")
	}

	// Repair must refuse
	_, err = Repair(context.Background(), dir, RepairOptions{Salvage: true})
	if !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("Repair = %v, want ErrFormatTooNew", err)
	}

	// Segments must be unmodified
	afterStat, _ := os.Stat(segPath)
	if beforeStat.Size() != afterStat.Size() || beforeStat.ModTime() != afterStat.ModTime() {
		t.Fatal("segment was mutated during downgrade refusal")
	}
}
