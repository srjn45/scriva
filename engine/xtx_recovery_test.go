package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/store"
)

func TestXTxRecovery_StandaloneOpenRefusedInXTxRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.CreateCollection("orders"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := db.CreateCollection("stock"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	// Enable XTx by ensuring format / journal
	if err := ensureXTxFormat(dir, "test"); err != nil {
		t.Fatalf("ensureXTxFormat: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Standalone OpenCollection in XTx-enabled root must fail with ErrXTxRecoveryRequired (§7.1)
	_, err = OpenCollection("orders", dir, CollectionConfig{})
	if !errors.Is(err, ErrXTxRecoveryRequired) {
		t.Fatalf("expected ErrXTxRecoveryRequired, got: %v", err)
	}

	// But opening via DB.Open succeeds
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("DB.Open: %v", err)
	}
	defer func() { _ = db2.Close() }()

	if _, err := db2.Collection("orders"); err != nil {
		t.Fatalf("db2.Collection(orders): %v", err)
	}
}

func TestXTxRecovery_AllCommittedOrAllAborted_PrepareCrash(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	orders, _ := db.CreateCollection("orders")
	stock, _ := db.CreateCollection("stock")

	// Seed records
	stockID, _, err := stock.Insert(map[string]any{"sku": "s1", "qty": 100.0})
	if err != nil {
		t.Fatalf("seed stock: %v", err)
	}
	orderID := orders.ReserveID()
	_ = db.Close()

	// Re-open journal and prepare tx without committing
	j, err := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
	if err != nil {
		t.Fatalf("openOrCreateXTxJournal: %v", err)
	}
	txid, err := j.allocateTxID("tx-crash-1")
	if err != nil {
		t.Fatalf("allocateTxID: %v", err)
	}
	_ = j.close()

	// Append stamped runs to orders and stock directly (simulating S1/S2 prepare)
	ordEntry, _ := encodeStamped(stampedEntry{
		ID:  orderID,
		Op:  store.OpInsert,
		Rev: 1,
		Data: map[string]any{
			"sku": "s1",
			"qty": 5.0,
		},
		Tx: TxStamp{T: txid, I: 0, N: 1},
	})
	if err := os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordEntry, 0o644); err != nil {
		t.Fatalf("write orders seg: %v", err)
	}

	stockEntry, _ := encodeStamped(stampedEntry{
		ID:  stockID,
		Op:  store.OpUpdate,
		Rev: 2,
		Data: map[string]any{
			"sku": "s1",
			"qty": 95.0,
		},
		Tx: TxStamp{T: txid, I: 0, N: 1},
	})
	f, err := os.OpenFile(filepath.Join(dir, "stock", "seg_000001.ndjson"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open stock seg: %v", err)
	}
	_, _ = f.Write(stockEntry)
	_ = f.Close()

	// Now open DB. Recovery must resolve the undecided transaction as ABORTED (presumed abort, row 2)
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer func() { _ = db2.Close() }()

	// Verify orders does not have the uncommitted order
	ordCol, _ := db2.Collection("orders")
	if _, err := ordCol.Get(orderID); err == nil {
		t.Fatalf("expected order %d to be invisible after abort", orderID)
	}

	// Verify stock has rev 1, qty 100
	stockCol, _ := db2.Collection("stock")
	stRec, err := stockCol.Get(stockID)
	if err != nil {
		t.Fatalf("stockCol.Get: %v", err)
	}
	if stRec.Rev != 1 || stRec.Data["qty"] != 100.0 {
		t.Fatalf("stock should have rev 1, qty 100; got %+v", stRec)
	}

	// Verify status
	if st, _ := db2.XTxStatus(txid); st != XTxAborted {
		t.Fatalf("expected XTxAborted, got %s", st)
	}
}

func TestXTxRecovery_CommittedTransactionReplayed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	orders, _ := db.CreateCollection("orders")
	stock, _ := db.CreateCollection("stock")
	_ = orders.EnsureIndex("sku")
	_ = stock.EnsureIndex("sku")

	stockID, _, _ := stock.Insert(map[string]any{"sku": "widget", "qty": 50.0})
	orderID := orders.ReserveID()
	_ = db.Close()

	// Create journal, prepare and commit
	j, err := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
	if err != nil {
		t.Fatalf("openOrCreateXTxJournal: %v", err)
	}
	txid, err := j.allocateTxID("tx-commit-replay")
	if err != nil {
		t.Fatalf("allocateTxID: %v", err)
	}

	// Stamped entries
	ordRef := xtxOpRef{ID: orderID, Op: "insert", Rev: 1}
	stockRef := xtxOpRef{ID: stockID, Op: "update", Rev: 2}

	parts := []xtxPart{
		{C: "orders", N: 1, D: partDigest([]xtxOpRef{ordRef})},
		{C: "stock", N: 1, D: partDigest([]xtxOpRef{stockRef})},
	}
	if err := j.commitPrepared(txid, "tx-commit-replay", parts); err != nil {
		t.Fatalf("commitPrepared: %v", err)
	}
	_ = j.close()

	// Append stamped entries to participant segments
	ordLine, _ := encodeStamped(stampedEntry{
		ID:   orderID,
		Op:   store.OpInsert,
		Rev:  1,
		Data: map[string]any{"sku": "widget", "qty": 1.0},
		Tx:   TxStamp{T: txid, I: 0, N: 1},
	})
	_ = os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordLine, 0o644)

	stockLine, _ := encodeStamped(stampedEntry{
		ID:   stockID,
		Op:   store.OpUpdate,
		Rev:  2,
		Data: map[string]any{"sku": "widget", "qty": 49.0},
		Tx:   TxStamp{T: txid, I: 0, N: 1},
	})
	f, _ := os.OpenFile(filepath.Join(dir, "stock", "seg_000001.ndjson"), os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.Write(stockLine)
	_ = f.Close()

	// Reopen DB — recovery must resolve COMMIT as COMMITTED and replay into indexes
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer func() { _ = db2.Close() }()

	ordCol, _ := db2.Collection("orders")
	stockCol, _ := db2.Collection("stock")

	ordRec, err := ordCol.Get(orderID)
	if err != nil {
		t.Fatalf("ordCol.Get(%d): %v", orderID, err)
	}
	if ordRec.Rev != 1 || ordRec.Data["sku"] != "widget" {
		t.Fatalf("unexpected order record: %+v", ordRec)
	}

	stRec, err := stockCol.Get(stockID)
	if err != nil {
		t.Fatalf("stockCol.Get(%d): %v", stockID, err)
	}
	if stRec.Rev != 2 || stRec.Data["qty"] != 49.0 {
		t.Fatalf("unexpected stock record: %+v", stRec)
	}

	// Check secondary index lookups
	ordIDs, _ := ordCol.IndexLookup("sku", "widget")
	if len(ordIDs) != 1 || ordIDs[0] != orderID {
		t.Fatalf("orders secondary index lookup failed: %v", ordIDs)
	}

	stockIDs, _ := stockCol.IndexLookup("sku", "widget")
	if len(stockIDs) != 1 || stockIDs[0] != stockID {
		t.Fatalf("stock secondary index lookup failed: %v", stockIDs)
	}

	// Status check
	if st, _ := db2.XTxStatus(txid); st != XTxCommitted {
		t.Fatalf("expected XTxCommitted, got %s", st)
	}
}

func TestXTxRecovery_TruthTableFailClosed(t *testing.T) {
	t.Run("MissingParticipantRun_Row5", func(t *testing.T) {
		dir := t.TempDir()
		db, _ := Open(dir, CollectionConfig{})
		_, _ = db.CreateCollection("orders")
		_, _ = db.CreateCollection("stock")
		_ = db.Close()

		j, _ := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
		txid, _ := j.allocateTxID("k1")
		parts := []xtxPart{
			{C: "orders", N: 1, D: partDigest([]xtxOpRef{{ID: 1, Op: "insert", Rev: 1}})},
			{C: "stock", N: 1, D: partDigest([]xtxOpRef{{ID: 2, Op: "insert", Rev: 1}})},
		}
		_ = j.commitPrepared(txid, "k1", parts)
		_ = j.close()

		// Write orders stamped entry, but stock has none
		ordLine, _ := encodeStamped(stampedEntry{
			ID: 1, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordLine, 0o644)

		_, err := Open(dir, CollectionConfig{})
		if !errors.Is(err, ErrXTxIncomplete) {
			t.Fatalf("expected ErrXTxIncomplete, got %v", err)
		}
	})

	t.Run("DigestMismatch_Row5", func(t *testing.T) {
		dir := t.TempDir()
		db, _ := Open(dir, CollectionConfig{})
		_, _ = db.CreateCollection("orders")
		_, _ = db.CreateCollection("stock")
		_ = db.Close()

		j, _ := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
		txid, _ := j.allocateTxID("k2")
		parts := []xtxPart{
			{C: "orders", N: 1, D: partDigest([]xtxOpRef{{ID: 1, Op: "insert", Rev: 1}})},
			{C: "stock", N: 1, D: partDigest([]xtxOpRef{{ID: 2, Op: "insert", Rev: 1}})},
		}
		_ = j.commitPrepared(txid, "k2", parts)
		_ = j.close()

		// Write orders stamped entry with ID 999 instead of 1 (digest mismatch)
		ordLine, _ := encodeStamped(stampedEntry{
			ID: 999, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordLine, 0o644)

		stockLine, _ := encodeStamped(stampedEntry{
			ID: 2, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "stock", "seg_000001.ndjson"), stockLine, 0o644)

		_, err := Open(dir, CollectionConfig{})
		if !errors.Is(err, ErrXTxIncomplete) {
			t.Fatalf("expected ErrXTxIncomplete, got %v", err)
		}
	})

	t.Run("MissingCollectionDir_Row6", func(t *testing.T) {
		dir := t.TempDir()
		db, _ := Open(dir, CollectionConfig{})
		_, _ = db.CreateCollection("orders")
		_ = db.Close()

		j, _ := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
		txid, _ := j.allocateTxID("k3")
		parts := []xtxPart{
			{C: "nonexistent_col", N: 1, D: partDigest([]xtxOpRef{{ID: 2, Op: "insert", Rev: 1}})},
			{C: "orders", N: 1, D: partDigest([]xtxOpRef{{ID: 1, Op: "insert", Rev: 1}})},
		}
		if err := j.commitPrepared(txid, "k3", parts); err != nil {
			t.Fatalf("commitPrepared: %v", err)
		}
		_ = j.close()

		ordLine, _ := encodeStamped(stampedEntry{
			ID: 1, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordLine, 0o644)

		_, err := Open(dir, CollectionConfig{})
		if !errors.Is(err, ErrXTxIncomplete) {
			t.Fatalf("expected ErrXTxIncomplete, got %v", err)
		}
	})

	t.Run("ForeignRun_Row7", func(t *testing.T) {
		dir := t.TempDir()
		db, _ := Open(dir, CollectionConfig{})
		_, _ = db.CreateCollection("orders")
		_, _ = db.CreateCollection("stock")
		_, _ = db.CreateCollection("extra")
		_ = db.Close()

		j, _ := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
		txid, _ := j.allocateTxID("k4")
		parts := []xtxPart{
			{C: "orders", N: 1, D: partDigest([]xtxOpRef{{ID: 1, Op: "insert", Rev: 1}})},
			{C: "stock", N: 1, D: partDigest([]xtxOpRef{{ID: 2, Op: "insert", Rev: 1}})},
		}
		_ = j.commitPrepared(txid, "k4", parts)
		_ = j.close()

		ordLine, _ := encodeStamped(stampedEntry{
			ID: 1, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordLine, 0o644)
		stockLine, _ := encodeStamped(stampedEntry{
			ID: 2, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "stock", "seg_000001.ndjson"), stockLine, 0o644)

		// Extra collection has stamped entry for this txid (not in parts)
		extraLine, _ := encodeStamped(stampedEntry{
			ID: 3, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: txid, I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "extra", "seg_000001.ndjson"), extraLine, 0o644)

		_, err := Open(dir, CollectionConfig{})
		if !errors.Is(err, ErrXTxIncomplete) {
			t.Fatalf("expected ErrXTxIncomplete, got %v", err)
		}
	})

	t.Run("JournalMissingWithStampedEntries_Row12", func(t *testing.T) {
		dir := t.TempDir()
		db, _ := Open(dir, CollectionConfig{})
		_, _ = db.CreateCollection("orders")
		_ = db.Close()

		_ = ensureXTxFormat(dir, "test")

		// Stamped entry exists in orders, but xtx.journal is missing
		ordLine, _ := encodeStamped(stampedEntry{
			ID: 1, Op: store.OpInsert, Rev: 1, Tx: TxStamp{T: "4f9c2a7e11b0d3a5-0000000000000001", I: 0, N: 1},
		})
		_ = os.WriteFile(filepath.Join(dir, "orders", "seg_000001.ndjson"), ordLine, 0o644)

		_, err := Open(dir, CollectionConfig{})
		if !errors.Is(err, ErrXTxJournalMissing) {
			t.Fatalf("expected ErrXTxJournalMissing, got %v", err)
		}
	})
}

func TestXTxRecovery_IdempotentRetryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _ = db.CreateCollection("c1")
	_, _ = db.CreateCollection("c2")

	ops := []XTxOp{
		{Collection: "c1", Op: store.OpInsert, ID: 10, Data: map[string]any{"v": "first"}},
		{Collection: "c2", Op: store.OpInsert, ID: 20, Data: map[string]any{"v": "second"}},
	}

	res1, err := db.CommitXTx("idemp-key-1", ops)
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}
	_ = db.Close()

	// Reopen DB
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer func() { _ = db2.Close() }()

	// Retry with same key
	res2, err := db2.CommitXTx("idemp-key-1", ops)
	if err != nil {
		t.Fatalf("CommitXTx retry: %v", err)
	}
	if res2.TxID != res1.TxID {
		t.Fatalf("expected same TxID %s, got %s", res1.TxID, res2.TxID)
	}
}
