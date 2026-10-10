package engine

import (
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/crypto"
	"github.com/srjn45/scriva/store"
)

func openTestDB(t *testing.T, cfg CollectionConfig) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCommitXTx_TwoCollectionCommit(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	orders, err := db.CreateCollection("orders")
	if err != nil {
		t.Fatalf("CreateCollection orders: %v", err)
	}
	stock, err := db.CreateCollection("stock")
	if err != nil {
		t.Fatalf("CreateCollection stock: %v", err)
	}

	// Seed stock with item
	stockID, ts, err := stock.Insert(map[string]any{"sku": "item-1", "qty": 50.0})
	if err != nil {
		t.Fatalf("seed stock: %v", err)
	}
	if ts.IsZero() {
		t.Fatal("seed timestamp zero")
	}

	orderID := orders.ReserveID()

	ops := []XTxOp{
		{
			Collection: "orders",
			Op:         store.OpInsert,
			ID:         orderID,
			Data:       map[string]any{"sku": "item-1", "qty": 2.0},
		},
		{
			Collection:  "stock",
			Op:          store.OpUpdate,
			ID:          stockID,
			Data:        map[string]any{"sku": "item-1", "qty": 48.0},
			ExpectedRev: 1,
		},
	}

	res, err := db.CommitXTx("tx-order-1", ops)
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}

	if res.TxID == "" {
		t.Fatal("empty TxID in commit result")
	}
	if len(res.Ops) != 2 {
		t.Fatalf("want 2 ops in result, got %d", len(res.Ops))
	}

	// Verify primary index and records
	ordRec, err := orders.Get(orderID)
	if err != nil {
		t.Fatalf("orders.Get(%d): %v", orderID, err)
	}
	if ordRec.Rev != 1 || ordRec.Data["sku"] != "item-1" {
		t.Fatalf("unexpected order record: %+v", ordRec)
	}

	stockRec, err := stock.Get(stockID)
	if err != nil {
		t.Fatalf("stock.Get(%d): %v", stockID, err)
	}
	if stockRec.Rev != 2 || stockRec.Data["qty"] != 48.0 {
		t.Fatalf("unexpected stock record: %+v", stockRec)
	}

	// Verify journal status
	if st, tx := db.XTxStatus("tx-order-1"); st != XTxCommitted || tx != res.TxID {
		t.Fatalf("statusByKey: got %s, %s; want %s, %s", st, tx, XTxCommitted, res.TxID)
	}
	if st, _ := db.XTxStatus(res.TxID); st != XTxCommitted {
		t.Fatalf("statusByTx: got %s, want %s", st, XTxCommitted)
	}

	// Verify segment file contains stamped entries with v2 CRC
	entries, err := orders.active.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll orders: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry in orders active segment, got %d", len(entries))
	}
	if entries[0].ID != orderID {
		t.Fatalf("orders entry ID=%d, want %d", entries[0].ID, orderID)
	}
}

func TestCommitXTx_ThreeCollectionsWithDeletes(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	c1, _ := db.CreateCollection("c1")
	c2, _ := db.CreateCollection("c2")
	c3, _ := db.CreateCollection("c3")

	id1, _, _ := c1.Insert(map[string]any{"v": "old1"})
	id2, _, _ := c2.Insert(map[string]any{"v": "old2"})
	id3 := c3.ReserveID()

	ops := []XTxOp{
		{Collection: "c1", Op: store.OpDelete, ID: id1, ExpectedRev: 1},
		{Collection: "c2", Op: store.OpUpdate, ID: id2, Data: map[string]any{"v": "new2"}, ExpectedRev: 1},
		{Collection: "c3", Op: store.OpInsert, ID: id3, Data: map[string]any{"v": "new3"}},
	}

	res, err := db.CommitXTx("tx-multi-1", ops)
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}
	if res.TxID == "" {
		t.Fatal("empty TxID")
	}

	// c1: id 1 deleted
	if _, err := c1.Get(id1); err == nil {
		t.Fatal("c1 id 1 should be deleted")
	}
	// c2: id 2 updated to rev 2
	rec2, err := c2.Get(id2)
	if err != nil || rec2.Rev != 2 || rec2.Data["v"] != "new2" {
		t.Fatalf("c2 id 2 mismatch: %+v, err=%v", rec2, err)
	}
	// c3: id 3 inserted with rev 1
	rec3, err := c3.Get(id3)
	if err != nil || rec3.Rev != 1 || rec3.Data["v"] != "new3" {
		t.Fatalf("c3 id 3 mismatch: %+v, err=%v", rec3, err)
	}
}

func TestCommitXTx_Idempotency(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"x": 10.0}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"y": 20.0}},
	}

	// First commit
	res1, err := db.CommitXTx("idemp-key-100", ops)
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Retry with same key
	res2, err := db.CommitXTx("idemp-key-100", ops)
	if err != nil {
		t.Fatalf("retry commit: %v", err)
	}

	if res2.TxID != res1.TxID {
		t.Fatalf("idempotent retry returned different TxID: got %s, want %s", res2.TxID, res1.TxID)
	}

	// Verify records were applied only once (rev is still 1)
	recA, err := colA.Get(idA)
	if err != nil || recA.Rev != 1 {
		t.Fatalf("colA record rev=%d (want 1), err=%v", recA.Rev, err)
	}
	recB, err := colB.Get(idB)
	if err != nil || recB.Rev != 1 {
		t.Fatalf("colB record rev=%d (want 1), err=%v", recB.Rev, err)
	}
}

func TestCommitXTx_CanonicalOrderingDeadlockFree(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")
	colC, _ := db.CreateCollection("colC")

	const workers = 8
	const iters = 25

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				key := fmt.Sprintf("tx-worker-%d-%d", workerID, i)
				idA := colA.ReserveID()
				idB := colB.ReserveID()
				idC := colC.ReserveID()

				var ops []XTxOp
				// Deliberately shuffle staging order across workers
				if workerID%3 == 0 {
					ops = []XTxOp{
						{Collection: "colC", Op: store.OpInsert, ID: idC, Data: map[string]any{"w": workerID}},
						{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"w": workerID}},
						{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"w": workerID}},
					}
				} else if workerID%3 == 1 {
					ops = []XTxOp{
						{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"w": workerID}},
						{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"w": workerID}},
						{Collection: "colC", Op: store.OpInsert, ID: idC, Data: map[string]any{"w": workerID}},
					}
				} else {
					ops = []XTxOp{
						{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"w": workerID}},
						{Collection: "colC", Op: store.OpInsert, ID: idC, Data: map[string]any{"w": workerID}},
					}
				}

				_, err := db.CommitXTx(key, ops)
				if err != nil {
					t.Errorf("worker %d iter %d: %v", workerID, i, err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestCommitXTx_ValidationErrorsNoWrites(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	id1, _, _ := colA.Insert(map[string]any{"k": "v1"})
	startSizeA := colA.active.Size()
	startSizeB := colB.active.Size()

	// 1. Target ID not found
	ops1 := []XTxOp{
		{Collection: "colA", Op: store.OpUpdate, ID: 999, Data: map[string]any{"k": "v2"}},
		{Collection: "colB", Op: store.OpInsert, ID: colB.ReserveID(), Data: map[string]any{"k": "v"}},
	}
	_, err := db.CommitXTx("v-err-1", ops1)
	if !errors.Is(err, ErrXTxConflict) {
		t.Fatalf("not found: got %v, want ErrXTxConflict", err)
	}
	if colA.active.Size() != startSizeA || colB.active.Size() != startSizeB {
		t.Fatal("bytes written on S0 validation failure")
	}

	// 2. ExpectedRev mismatch
	ops2 := []XTxOp{
		{Collection: "colA", Op: store.OpUpdate, ID: id1, Data: map[string]any{"k": "v2"}, ExpectedRev: 99},
		{Collection: "colB", Op: store.OpInsert, ID: colB.ReserveID(), Data: map[string]any{"k": "v"}},
	}
	_, err = db.CommitXTx("v-err-2", ops2)
	if !errors.Is(err, ErrXTxConflict) {
		t.Fatalf("expected_rev mismatch: got %v, want ErrXTxConflict", err)
	}

	// 3. Duplicate op in batch
	ops3 := []XTxOp{
		{Collection: "colA", Op: store.OpUpdate, ID: id1, Data: map[string]any{"k": "v2"}},
		{Collection: "colA", Op: store.OpUpdate, ID: id1, Data: map[string]any{"k": "v3"}},
	}
	_, err = db.CommitXTx("v-err-3", ops3)
	if !errors.Is(err, ErrXTxConflict) {
		t.Fatalf("duplicate op in batch: got %v, want ErrXTxConflict", err)
	}

	// 4. Missing collection
	ops4 := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"k": "v"}},
		{Collection: "nonExistent", Op: store.OpInsert, ID: 1, Data: map[string]any{"k": "v"}},
	}
	_, err = db.CommitXTx("v-err-4", ops4)
	if !errors.Is(err, ErrCollectionNotFound) {
		t.Fatalf("missing collection: got %v, want ErrCollectionNotFound", err)
	}

	// 5. Follower mode
	db.role.Store(int32(RoleFollower))
	ops5 := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"k": "v"}},
		{Collection: "colB", Op: store.OpInsert, ID: colB.ReserveID(), Data: map[string]any{"k": "v"}},
	}
	_, err = db.CommitXTx("v-err-5", ops5)
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("follower write: got %v, want ErrReadOnly", err)
	}
	db.role.Store(int32(RoleLeader))
}

func TestCommitXTx_UniqueConstraintConflict(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	if err := colA.EnsureUniqueIndex("email"); err != nil {
		t.Fatal(err)
	}

	// Existing committed email
	_, _, err := colA.Insert(map[string]any{"email": "alice@example.com"})
	if err != nil {
		t.Fatal(err)
	}

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	// Commit trying to reuse the unique email
	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"email": "alice@example.com"}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"x": 1.0}},
	}

	_, err = db.CommitXTx("tx-uniq-1", ops)
	if !errors.Is(err, ErrXTxConflict) || !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("unique conflict: got %v, want ErrXTxConflict + ErrDuplicateKey", err)
	}

	// Verify colB never got idB
	if _, err := colB.Get(idB); err == nil {
		t.Fatal("colB should not have idB visible")
	}
}

func TestCommitXTx_QuotaExceeded(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxRecords = 2
	db := openTestDB(t, cfg)

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	// Seed 2 records in colA so colA is at max capacity
	_, _, _ = colA.Insert(map[string]any{"a": 1})
	_, _, _ = colA.Insert(map[string]any{"a": 2})

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"a": 3}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"b": 1}},
	}

	_, err := db.CommitXTx("tx-quota-1", ops)
	if !errors.Is(err, ErrResourceExhausted) && !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("quota exceeded: got %v, want ErrResourceExhausted/ErrQuotaExceeded", err)
	}

	// Verify colB never inserted idB
	if _, err := colB.Get(idB); err == nil {
		t.Fatal("colB should not have idB")
	}
}

func TestCommitXTx_WatchEventsEmitted(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	_, subA, cancelA := colA.Subscribe()
	defer cancelA()
	_, subB, cancelB := colB.Subscribe()
	defer cancelB()

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"a": 42.0}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"b": 99.0}},
	}

	_, err := db.CommitXTx("tx-watch-1", ops)
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}

	select {
	case evA := <-subA:
		if evA.ID != idA || evA.Op != store.OpInsert {
			t.Fatalf("unexpected watch event on colA: %+v", evA)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event on colA")
	}

	select {
	case evB := <-subB:
		if evB.ID != idB || evB.Op != store.OpInsert {
			t.Fatalf("unexpected watch event on colB: %+v", evB)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event on colB")
	}
}

func TestCommitXTx_FaultInjectedFsyncPoisonsSegment(t *testing.T) {
	ffs := newFaultFS()
	cfg := defaultConfig()
	cfg.wrapFile = ffs.wrap

	dir := t.TempDir()
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"x": 1.0}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"y": 2.0}},
	}

	// Pre-ensure the journal exists before arming fault for participant fsync
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}

	// Inject EIO on the first fsync inside S2
	ffs.failSyncAt(ffs.count("sync")+1, syscall.EIO)

	_, err = db.CommitXTx("tx-fault-fsync", ops)
	if !errors.Is(err, ErrXTxDurability) {
		t.Fatalf("got %v, want ErrXTxDurability", err)
	}

	// Verify colA segment is poisoned
	if colA.active.poisoned == nil {
		t.Fatal("colA segment should be marked poisoned")
	}

	// Stamped entries must NOT be visible in index
	if _, err := colA.Get(idA); err == nil {
		t.Fatal("colA idA should not be visible")
	}
	if _, err := colB.Get(idB); err == nil {
		t.Fatal("colB idB should not be visible")
	}

	// Journal status should be ABORTED (best-effort abort was written)
	st, _ := db.XTxStatus("tx-fault-fsync")
	if st != XTxAborted {
		t.Fatalf("status: got %s, want %s", st, XTxAborted)
	}
}

func TestCommitXTx_S1AppendFailureRollback(t *testing.T) {
	ffs := newFaultFS()
	cfg := defaultConfig()
	cfg.wrapFile = ffs.wrap

	dir := t.TempDir()
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"x": 1.0}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"y": 2.0}},
	}

	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}

	startSizeA := colA.active.Size()
	startSizeB := colB.active.Size()

	// Inject error on 2nd write (which will be colB's append in S1)
	ffs.failWriteAt(ffs.count("write")+2, 0, syscall.ENOSPC)

	_, err = db.CommitXTx("tx-fault-write", ops)
	if !errors.Is(err, ErrXTxDurability) {
		t.Fatalf("got %v, want ErrXTxDurability", err)
	}

	// Verify colA was rolled back to its startSize
	if colA.active.Size() != startSizeA {
		t.Fatalf("colA active size = %d, want %d (rollback failed)", colA.active.Size(), startSizeA)
	}
	if colB.active.Size() != startSizeB {
		t.Fatalf("colB active size = %d, want %d", colB.active.Size(), startSizeB)
	}

	// Stamped entries must NOT be visible
	if _, err := colA.Get(idA); err == nil {
		t.Fatal("colA idA should not be visible")
	}
	if _, err := colB.Get(idB); err == nil {
		t.Fatal("colB idB should not be visible")
	}
}

func TestCommitXTx_ConcurrentInProgressKey(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	// Manually mark key as in flight
	if err := db.beginXTxInFlight("in-flight-key"); err != nil {
		t.Fatal(err)
	}

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"a": 1}},
		{Collection: "colB", Op: store.OpInsert, ID: colB.ReserveID(), Data: map[string]any{"b": 2}},
	}

	_, err := db.CommitXTx("in-flight-key", ops)
	if !errors.Is(err, ErrXTxInProgress) {
		t.Fatalf("got %v, want ErrXTxInProgress", err)
	}

	db.endXTxInFlight("in-flight-key")

	// Now it should succeed
	res, err := db.CommitXTx("in-flight-key", ops)
	if err != nil {
		t.Fatalf("commit after ending in-flight: %v", err)
	}
	if res.TxID == "" {
		t.Fatal("empty TxID")
	}
}

func TestCommitXTx_Limits(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	// 1. More than MaxOps (1000)
	ops := make([]XTxOp, 1001)
	for i := 0; i < 1001; i++ {
		col := "colA"
		if i%2 == 1 {
			col = "colB"
		}
		ops[i] = XTxOp{Collection: col, Op: store.OpInsert, ID: uint64(i + 1), Data: map[string]any{"i": i}}
	}

	_, err := db.CommitXTx("tx-limit-ops", ops)
	if !errors.Is(err, ErrXTxTooLarge) {
		t.Fatalf("ops limit: got %v, want ErrXTxTooLarge", err)
	}

	// 2. More than MaxParticipants (16)
	var manyColOps []XTxOp
	for i := 0; i < 17; i++ {
		name := fmt.Sprintf("pcol%02d", i)
		c, err := db.CreateCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		manyColOps = append(manyColOps, XTxOp{Collection: name, Op: store.OpInsert, ID: c.ReserveID(), Data: map[string]any{"x": 1}})
	}

	_, err = db.CommitXTx("tx-limit-participants", manyColOps)
	if !errors.Is(err, ErrXTxTooLarge) {
		t.Fatalf("participants limit: got %v, want ErrXTxTooLarge", err)
	}
}

func TestCommitXTx_Encryption(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	kr, err := crypto.NewKeyring("k1", key)
	if err != nil {
		t.Fatal(err)
	}

	cfg := defaultConfig()
	cfg.KeyProvider = kr
	cfg.EncryptionByCollection = map[string]*EncryptionPolicy{
		"users": {Mode: EncryptModeFields, Fields: []string{"ssn"}},
		"audit": {Mode: EncryptModeFields, Fields: []string{"details"}},
	}

	db := openTestDB(t, cfg)

	users, err := db.CollectionWithConfig("users", cfg)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := db.CollectionWithConfig("audit", cfg)
	if err != nil {
		t.Fatal(err)
	}

	uid := users.ReserveID()
	aid := audit.ReserveID()

	ops := []XTxOp{
		{Collection: "users", Op: store.OpInsert, ID: uid, Data: map[string]any{"name": "Alice", "ssn": "123-45-6789"}},
		{Collection: "audit", Op: store.OpInsert, ID: aid, Data: map[string]any{"action": "create_user", "details": "secret_details"}},
	}

	res, err := db.CommitXTx("tx-enc-1", ops)
	if err != nil {
		t.Fatalf("CommitXTx enc: %v", err)
	}
	if res.TxID == "" {
		t.Fatal("empty TxID")
	}

	// Plaintext returned by Get
	uRec, err := users.Get(uid)
	if err != nil || uRec.Data["ssn"] != "123-45-6789" {
		t.Fatalf("users.Get mismatch: %+v, err=%v", uRec, err)
	}
	aRec, err := audit.Get(aid)
	if err != nil || aRec.Data["details"] != "secret_details" {
		t.Fatalf("audit.Get mismatch: %+v, err=%v", aRec, err)
	}

	// On disk, ssn and details should NOT be present in plaintext
	uEntry, _, err := users.active.ReadStampedAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if uEntry.Data["ssn"] == "123-45-6789" {
		t.Fatal("ssn was stored as plaintext on disk!")
	}
}

func TestCommitXTx_SecondaryIndexes(t *testing.T) {
	db := openTestDB(t, defaultConfig())

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	_ = colA.EnsureIndex("city")
	_ = colB.EnsureIndex("category")

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"city": "Paris"}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"category": "Books"}},
	}

	_, err := db.CommitXTx("tx-sidx-1", ops)
	if err != nil {
		t.Fatal(err)
	}

	// Check secondary index lookups
	idsA := colA.sidxMap["city"].Lookup("Paris")
	if len(idsA) != 1 || idsA[0] != idA {
		t.Fatalf("sidx lookup colA: got %v, want [%d]", idsA, idA)
	}

	idsB := colB.sidxMap["category"].Lookup("Books")
	if len(idsB) != 1 || idsB[0] != idB {
		t.Fatalf("sidx lookup colB: got %v, want [%d]", idsB, idB)
	}
}

func TestCommitXTx_S4FsyncFailurePoisonsJournalOutcomeUnknown(t *testing.T) {
	ffs := newFaultFS()
	cfg := defaultConfig()
	cfg.wrapFile = ffs.wrap

	dir := t.TempDir()
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"x": 10.0}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"y": 20.0}},
	}

	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}

	// In CommitXTx:
	// 2 participants sync active segments (sync #1 and sync #2)
	// Then coordinator journal syncs at S4 (sync #3)
	ffs.failSyncAt(ffs.count("sync")+3, syscall.EIO)

	_, err = db.CommitXTx("tx-s4-fault", ops)
	if !errors.Is(err, ErrXTxOutcomeUnknown) {
		t.Fatalf("got %v, want ErrXTxOutcomeUnknown", err)
	}

	// Journal must be poisoned
	db.xtxMu.Lock()
	j := db.xtxJournal
	db.xtxMu.Unlock()
	if j.poisoned == nil {
		t.Fatal("journal should be poisoned")
	}

	// Outcomes unknown must not be retry-safe
	if XTxRetrySafe(err) {
		t.Fatal("ErrXTxOutcomeUnknown must not be retry-safe")
	}
}

func TestCommitXTx_ReplicationBrokerGroup(t *testing.T) {
	cfg := defaultConfig()
	cfg.ReplicationRingSize = 100
	db := openTestDB(t, cfg)

	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	idA := colA.ReserveID()
	idB := colB.ReserveID()

	ops := []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: idA, Data: map[string]any{"a": 1}},
		{Collection: "colB", Op: store.OpInsert, ID: idB, Data: map[string]any{"b": 2}},
	}

	res, err := db.CommitXTx("tx-repl-1", ops)
	if err != nil {
		t.Fatal(err)
	}
	if res.TxID == "" {
		t.Fatal("empty TxID")
	}

	// Verify broker received entries with contiguous LSNs
	if db.broker == nil {
		t.Fatal("broker is nil")
	}
	_, backlog, ok := db.broker.subscribe(0, "follower-test")
	if !ok {
		t.Fatal("broker subscribe failed")
	}
	if len(backlog) != 2 {
		t.Fatalf("broker backlog: got %d, want 2", len(backlog))
	}
	if db.broker.currentLSN() != 2 {
		t.Fatalf("highest LSN = %d, want 2", db.broker.currentLSN())
	}
	if backlog[0].LSN != 1 || backlog[1].LSN != 2 {
		t.Fatalf("LSNs not contiguous: [%d, %d]", backlog[0].LSN, backlog[1].LSN)
	}
}
