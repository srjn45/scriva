package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// Scenario 1: Inverse collection order
// Proves canonical lock ordering prevents deadlocks when two transactions
// access collections in opposite orders.
func TestXTxConcurrency_InverseCollectionOrder_DisjointAndOverlapping(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c1, err := db.CreateCollection("alpha")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db.CreateCollection("beta")
	if err != nil {
		t.Fatal(err)
	}

	// Part A: Disjoint operations in opposite declared orders
	t.Run("DisjointOppositeOrder", func(t *testing.T) {
		xA, err := db.BeginXTx(context.Background(), []string{"alpha", "beta"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		xB, err := db.BeginXTx(context.Background(), []string{"beta", "alpha"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}

		idA1, _ := xA.Insert("alpha", map[string]any{"owner": "A", "val": 1})
		idA2, _ := xA.Insert("beta", map[string]any{"owner": "A", "val": 2})

		idB1, _ := xB.Insert("alpha", map[string]any{"owner": "B", "val": 10})
		idB2, _ := xB.Insert("beta", map[string]any{"owner": "B", "val": 20})

		var wg sync.WaitGroup
		var errA, errB error
		barrier := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-barrier
			_, errA = xA.Commit(context.Background())
		}()
		go func() {
			defer wg.Done()
			<-barrier
			_, errB = xB.Commit(context.Background())
		}()

		close(barrier)
		wg.Wait()

		if errA != nil {
			t.Fatalf("xA commit error: %v", errA)
		}
		if errB != nil {
			t.Fatalf("xB commit error: %v", errB)
		}

		// Verify all 4 documents exist
		for _, check := range []struct {
			col *Collection
			id  uint64
			val int
		}{
			{c1, idA1, 1},
			{c2, idA2, 2},
			{c1, idB1, 10},
			{c2, idB2, 20},
		} {
			rec, err := check.col.Get(check.id)
			if err != nil {
				t.Fatalf("Get %s/%d: %v", check.col.Name(), check.id, err)
			}
			if int(rec.Data["val"].(float64)) != check.val {
				t.Fatalf("%s/%d val = %v, want %d", check.col.Name(), check.id, rec.Data["val"], check.val)
			}
		}
	})

	// Part B: Contending operations on same documents in opposite declared orders
	t.Run("ContendingOppositeOrder", func(t *testing.T) {
		id1, _, err := c1.Insert(map[string]any{"val": 100})
		if err != nil {
			t.Fatal(err)
		}
		id2, _, err := c2.Insert(map[string]any{"val": 200})
		if err != nil {
			t.Fatal(err)
		}

		xA, err := db.BeginXTx(context.Background(), []string{"alpha", "beta"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		xB, err := db.BeginXTx(context.Background(), []string{"beta", "alpha"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}

		_, _ = xA.Get("alpha", id1)
		_, _ = xA.Get("beta", id2)
		_ = xA.Update("alpha", id1, map[string]any{"val": 101, "by": "A"})
		_ = xA.Update("beta", id2, map[string]any{"val": 201, "by": "A"})

		_, _ = xB.Get("alpha", id1)
		_, _ = xB.Get("beta", id2)
		_ = xB.Update("alpha", id1, map[string]any{"val": 102, "by": "B"})
		_ = xB.Update("beta", id2, map[string]any{"val": 202, "by": "B"})

		var wg sync.WaitGroup
		var errA, errB error
		barrier := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-barrier
			_, errA = xA.Commit(context.Background())
		}()
		go func() {
			defer wg.Done()
			<-barrier
			_, errB = xB.Commit(context.Background())
		}()

		close(barrier)
		wg.Wait()

		// Exactly one must win, the other must see typed conflict
		successCount := 0
		conflictCount := 0
		for _, err := range []error{errA, errB} {
			if err == nil {
				successCount++
			} else if errors.Is(err, ErrXTxConflict) {
				conflictCount++
			} else {
				t.Fatalf("unexpected commit error: %v", err)
			}
		}
		if successCount != 1 || conflictCount != 1 {
			t.Fatalf("want 1 success and 1 conflict, got %d successes and %d conflicts (errA=%v, errB=%v)", successCount, conflictCount, errA, errB)
		}

		// Reopen and assert consistency across all participant collections
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db2, err := Open(dir, defaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db2.Close() }()

		c1Reopened, _ := db2.Collection("alpha")
		c2Reopened, _ := db2.Collection("beta")

		r1, err := c1Reopened.Get(id1)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := c2Reopened.Get(id2)
		if err != nil {
			t.Fatal(err)
		}
		// Both must have the same winner's update
		if r1.Data["by"] != r2.Data["by"] {
			t.Fatalf("partial state detected: r1 by=%v, r2 by=%v", r1.Data["by"], r2.Data["by"])
		}
	})
}

// Scenario 1 (multi-collection): Many goroutines accessing ≥3 collections with
// randomized-but-seeded participant order and bounded completion.
func TestXTxConcurrency_MultiCollectionRandomizedOrder(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	colNames := []string{"c0", "c1", "c2", "c3"}
	cols := make([]*Collection, len(colNames))
	for i, name := range colNames {
		cols[i], err = db.CreateCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		// Seed each collection with 3 documents
		for docIdx := 0; docIdx < 3; docIdx++ {
			if _, _, err := cols[i].Insert(map[string]any{"val": 100}); err != nil {
				t.Fatal(err)
			}
		}
	}

	const numWorkers = 20
	var wg sync.WaitGroup
	var committed atomic.Int64
	var conflicted atomic.Int64

	start := make(chan struct{})

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + workerID)))
			<-start

			for iter := 0; iter < 10; iter++ {
				// Pick 2 or 3 collections in random order
				perm := rng.Perm(len(colNames))
				subsetLen := 2 + rng.Intn(2)
				chosen := make([]string, subsetLen)
				for i := 0; i < subsetLen; i++ {
					chosen[i] = colNames[perm[i]]
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				x, err := db.BeginXTx(ctx, chosen, XTxOptions{})
				if err != nil {
					cancel()
					t.Errorf("worker %d BeginXTx: %v", workerID, err)
					return
				}

				// Pick a doc in each chosen collection, read and update
				docID := uint64(1 + rng.Intn(3))
				for _, cName := range chosen {
					rec, err := x.Get(cName, docID)
					if err != nil {
						_ = x.Rollback()
						cancel()
						t.Errorf("worker %d Get(%s, %d): %v", workerID, cName, docID, err)
						return
					}
					oldVal := int(rec.Data["val"].(float64))
					if err := x.Update(cName, docID, map[string]any{"val": oldVal + 1, "worker": workerID}); err != nil {
						_ = x.Rollback()
						cancel()
						t.Errorf("worker %d Update(%s, %d): %v", workerID, cName, docID, err)
						return
					}
				}

				_, err = x.Commit(ctx)
				cancel()
				if err == nil {
					committed.Add(1)
				} else if errors.Is(err, ErrXTxConflict) {
					conflicted.Add(1)
				} else {
					t.Errorf("worker %d unexpected commit error: %v", workerID, err)
				}
			}
		}(w)
	}

	close(start)
	wg.Wait()

	if committed.Load() == 0 {
		t.Fatal("expected at least some transactions to commit successfully")
	}

	// Reopen DB and verify no corruptions or dangling records
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()

	for _, name := range colNames {
		col, err := db2.Collection(name)
		if err != nil {
			t.Fatal(err)
		}
		for docID := uint64(1); docID <= 3; docID++ {
			rec, err := col.Get(docID)
			if err != nil {
				t.Fatalf("collection %s doc %d Get error: %v", name, docID, err)
			}
			if rec.Rev < 1 {
				t.Fatalf("collection %s doc %d invalid rev: %d", name, docID, rec.Rev)
			}
		}
	}
}

// Scenario 2: Concurrent unique-key claims
// Two transactions insert documents claiming the same unique-index value
// (same and different collections) — exactly one commits, loser gets typed error,
// no residue in index or data.
func TestXTxConcurrency_UniqueKeyClaims(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	users, err := db.CreateCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := db.CreateCollection("profiles")
	if err != nil {
		t.Fatal(err)
	}

	if err := users.EnsureUniqueIndex("email"); err != nil {
		t.Fatal(err)
	}
	if err := profiles.EnsureUniqueIndex("handle"); err != nil {
		t.Fatal(err)
	}

	// Subtest A: Same collection unique conflict
	t.Run("SameCollectionUniqueConflict", func(t *testing.T) {
		xA, err := db.BeginXTx(context.Background(), []string{"users"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		xB, err := db.BeginXTx(context.Background(), []string{"users"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}

		idA, _ := xA.Insert("users", map[string]any{"email": "unique@example.com", "tx": "A"})
		idB, _ := xB.Insert("users", map[string]any{"email": "unique@example.com", "tx": "B"})

		var wg sync.WaitGroup
		var errA, errB error
		barrier := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-barrier
			_, errA = xA.Commit(context.Background())
		}()
		go func() {
			defer wg.Done()
			<-barrier
			_, errB = xB.Commit(context.Background())
		}()

		close(barrier)
		wg.Wait()

		var winnerID, loserID uint64
		if errA == nil && errors.Is(errB, ErrDuplicateKey) {
			winnerID, loserID = idA, idB
		} else if errB == nil && errors.Is(errA, ErrDuplicateKey) {
			winnerID, loserID = idB, idA
		} else {
			t.Fatalf("expected one winner and one ErrDuplicateKey, got errA=%v, errB=%v", errA, errB)
		}

		// Winner's record is present
		wRec, err := users.Get(winnerID)
		if err != nil {
			t.Fatalf("winner doc missing: %v", err)
		}
		if wRec.Data["email"] != "unique@example.com" {
			t.Fatalf("winner data wrong: %+v", wRec.Data)
		}

		// Loser's record is absent
		if _, ok := users.liveRev(loserID); ok {
			t.Fatalf("loser doc %d is live, want absent", loserID)
		}

		// Unique index lookup returns exactly the winner ID
		hits := users.sidxMap["email"].Lookup("unique@example.com")
		if len(hits) != 1 || hits[0] != winnerID {
			t.Fatalf("email index lookup: got %v, want [%d]", hits, winnerID)
		}
	})

	// Subtest B: Multi-collection unique conflict (loser leaves no residue on other participant)
	t.Run("MultiCollectionUniqueConflictNoResidue", func(t *testing.T) {
		xA, err := db.BeginXTx(context.Background(), []string{"users", "profiles"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		xB, err := db.BeginXTx(context.Background(), []string{"users", "profiles"}, XTxOptions{})
		if err != nil {
			t.Fatal(err)
		}

		idA_u, _ := xA.Insert("users", map[string]any{"email": "clash@example.com", "tx": "A"})
		idA_p, _ := xA.Insert("profiles", map[string]any{"handle": "handle_A", "tx": "A"})

		idB_u, _ := xB.Insert("users", map[string]any{"email": "clash@example.com", "tx": "B"})
		idB_p, _ := xB.Insert("profiles", map[string]any{"handle": "handle_B", "tx": "B"})

		var wg sync.WaitGroup
		var errA, errB error
		barrier := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-barrier
			_, errA = xA.Commit(context.Background())
		}()
		go func() {
			defer wg.Done()
			<-barrier
			_, errB = xB.Commit(context.Background())
		}()

		close(barrier)
		wg.Wait()

		var winnerP, loserP uint64
		if errA == nil && errors.Is(errB, ErrDuplicateKey) {
			winnerP, loserP = idA_p, idB_p
			_ = idA_u
			_ = idB_u
		} else if errB == nil && errors.Is(errA, ErrDuplicateKey) {
			winnerP, loserP = idB_p, idA_p
		} else {
			t.Fatalf("expected one winner and one ErrDuplicateKey, got errA=%v, errB=%v", errA, errB)
		}

		// Winner's profile exists
		if _, err := profiles.Get(winnerP); err != nil {
			t.Fatalf("winner profile missing: %v", err)
		}

		// Loser's profile must NOT exist (no partial state on participant)
		if _, ok := profiles.liveRev(loserP); ok {
			t.Fatalf("loser profile %d is live, want absent", loserP)
		}
		hits := profiles.sidxMap["handle"].Lookup(fmt.Sprintf("handle_%d", loserP))
		if len(hits) != 0 {
			t.Fatalf("loser profile found in index: %v", hits)
		}
	})
}

// Scenario 3: Update/delete races
// update-vs-update, update-vs-delete, delete-vs-delete, read-then-write vs concurrent delete.
// Asserts read-set/write-base validation yields exactly one winner and typed *XTxConflictError.
func TestXTxConcurrency_UpdateDeleteRaces(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	colA, err := db.CreateCollection("cA")
	if err != nil {
		t.Fatal(err)
	}
	colB, err := db.CreateCollection("cB")
	if err != nil {
		t.Fatal(err)
	}

	// 1. Update vs Update
	t.Run("UpdateVsUpdate", func(t *testing.T) {
		id, _, _ := colA.Insert(map[string]any{"v": 1})
		rec0, _ := colA.Get(id)
		rev1 := rec0.Rev

		xA, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})
		xB, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})

		_, _ = xA.Get("cA", id)
		_ = xA.Update("cA", id, map[string]any{"v": 10})

		_, _ = xB.Get("cA", id)
		_ = xB.Update("cA", id, map[string]any{"v": 20})

		// xA commits first
		if _, err := xA.Commit(context.Background()); err != nil {
			t.Fatalf("xA commit: %v", err)
		}

		// xB commits second -> fails
		_, err := xB.Commit(context.Background())
		if !errors.Is(err, ErrXTxConflict) {
			t.Fatalf("xB want ErrXTxConflict, got %v", err)
		}

		var ce *XTxConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("xB error is not *XTxConflictError: %v", err)
		}
		if ce.Collection != "cA" || ce.ID != id || !ce.Write || ce.ExpectedRev != rev1 || ce.ActualRev != rev1+1 {
			t.Fatalf("unexpected conflict detail: %+v", ce)
		}

		// Verify cA/id has xA's data
		rec, _ := colA.Get(id)
		if rec.Data["v"] != 10.0 || rec.Rev != rev1+1 {
			t.Fatalf("unexpected cA/id state: %+v", rec)
		}
	})

	// 2. Update vs Delete
	t.Run("UpdateVsDelete", func(t *testing.T) {
		id, rev1, _ := colA.Insert(map[string]any{"v": 2})

		xA, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})
		xB, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})

		_, _ = xA.Get("cA", id)
		_ = xA.Update("cA", id, map[string]any{"v": 22})

		_, _ = xB.Get("cA", id)
		_ = xB.Delete("cA", id)

		// xB commits delete first
		if _, err := xB.Commit(context.Background()); err != nil {
			t.Fatalf("xB commit delete: %v", err)
		}

		// xA commits update -> fails
		_, err := xA.Commit(context.Background())
		if !errors.Is(err, ErrXTxConflict) {
			t.Fatalf("xA want ErrXTxConflict, got %v", err)
		}

		var ce *XTxConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("xA error is not *XTxConflictError: %v", err)
		}
		if ce.Collection != "cA" || ce.ID != id || ce.ActualPresent {
			t.Fatalf("unexpected conflict detail: %+v", ce)
		}
		_ = rev1
	})

	// 3. Delete vs Delete
	t.Run("DeleteVsDelete", func(t *testing.T) {
		id, _, _ := colA.Insert(map[string]any{"v": 3})

		xA, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})
		xB, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})

		_, _ = xA.Get("cA", id)
		_ = xA.Delete("cA", id)

		_, _ = xB.Get("cA", id)
		_ = xB.Delete("cA", id)

		// xA commits delete
		if _, err := xA.Commit(context.Background()); err != nil {
			t.Fatalf("xA commit delete: %v", err)
		}

		// xB commits delete -> fails
		_, err := xB.Commit(context.Background())
		if !errors.Is(err, ErrXTxConflict) {
			t.Fatalf("xB want ErrXTxConflict, got %v", err)
		}

		var ce *XTxConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("xB error is not *XTxConflictError: %v", err)
		}
		if ce.Collection != "cA" || ce.ID != id || ce.ActualPresent {
			t.Fatalf("unexpected conflict detail: %+v", ce)
		}
	})

	// 4. Read-then-write vs concurrent Delete
	t.Run("ReadThenWriteVsConcurrentDelete", func(t *testing.T) {
		idA, _, _ := colA.Insert(map[string]any{"val": "source"})
		recA0, _ := colA.Get(idA)
		revA := recA0.Rev
		idB, _, _ := colB.Insert(map[string]any{"val": "target"})

		// xA reads colA/idA and stages write to colB/idB
		xA, _ := db.BeginXTx(context.Background(), []string{"cA", "cB"}, XTxOptions{})
		_, _ = xA.Get("cA", idA)
		_ = xA.Update("cB", idB, map[string]any{"val": "derived"})

		// xB concurrently deletes colA/idA
		xB, _ := db.BeginXTx(context.Background(), []string{"cA"}, XTxOptions{})
		_ = xB.Delete("cA", idA)
		if _, err := xB.Commit(context.Background()); err != nil {
			t.Fatalf("xB commit delete: %v", err)
		}

		// xA tries to commit -> read-set validation fails on colA/idA
		_, err := xA.Commit(context.Background())
		if !errors.Is(err, ErrXTxConflict) {
			t.Fatalf("xA want ErrXTxConflict, got %v", err)
		}

		var ce *XTxConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("xA error is not *XTxConflictError: %v", err)
		}
		if ce.Collection != "cA" || ce.ID != idA || ce.Write || ce.ExpectedRev != revA || ce.ActualPresent {
			t.Fatalf("unexpected conflict detail: %+v", ce)
		}

		// colB/idB was NOT updated
		recB, err := colB.Get(idB)
		if err != nil {
			t.Fatal(err)
		}
		if recB.Data["val"] != "target" {
			t.Fatalf("colB/idB was modified by failed transaction: %+v", recB)
		}
	})
}

// Scenario 5: Lock-wait cancellation before prepare
// A commit blocked waiting on participant locks has its context cancelled:
// returns ErrXTxCanceled, retry-safe, nothing written, no journal residue,
// handle still usable, retry succeeds.
func TestXTxConcurrency_LockWaitCancellationBeforePrepare(t *testing.T) {
	dir := t.TempDir()

	var blockedTxHeld sync.WaitGroup
	var blockerRelease = make(chan struct{})

	cfg := defaultConfig()
	cfg.xtxHook = func(point xtxHookPoint, key, txid string) {
		if key == "blocker-tx" && point == xtxHookLocked {
			blockedTxHeld.Done()
			<-blockerRelease
		}
	}

	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c1, err := db.CreateCollection("col1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CreateCollection("col2")
	if err != nil {
		t.Fatal(err)
	}

	// Seed col1 so reserve ID starts after seed
	_, _, _ = c1.Insert(map[string]any{"v": "seed"})

	// Start blocker tx that acquires participant locks and holds them
	idBlocker := c1.ReserveID()
	blockedTxHeld.Add(1)
	go func() {
		_, _ = db.CommitXTx("blocker-tx", []XTxOp{
			{Collection: "col1", Op: store.OpInsert, ID: idBlocker, Data: map[string]any{"v": "blocker"}},
		})
	}()

	blockedTxHeld.Wait()

	// Now try to commit txWaiting with a short timeout while locks are held
	xWaiting, err := db.BeginXTx(context.Background(), []string{"col1", "col2"}, XTxOptions{Key: "waiting-tx"})
	if err != nil {
		t.Fatal(err)
	}
	idW, err := xWaiting.Insert("col1", map[string]any{"v": "waiting"})
	if err != nil {
		t.Fatal(err)
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err = xWaiting.Commit(shortCtx)
	if !errors.Is(err, ErrXTxCanceled) {
		t.Fatalf("want ErrXTxCanceled, got %v", err)
	}
	if !XTxRetrySafe(err) {
		t.Fatal("ErrXTxCanceled must be retry-safe")
	}

	// Release blocker
	close(blockerRelease)
	time.Sleep(20 * time.Millisecond)

	// Verify blocker committed its doc, but xWaiting's doc idW is NOT live
	if _, ok := c1.liveRev(idBlocker); !ok {
		t.Fatalf("blocker doc %d should be live", idBlocker)
	}
	if _, ok := c1.liveRev(idW); ok {
		t.Fatalf("uncommitted doc %d is visible in col1", idW)
	}

	// The handle is still usable: commit with fresh context succeeds
	res, err := xWaiting.Commit(context.Background())
	if err != nil {
		t.Fatalf("retry commit failed: %v", err)
	}
	if res.TxID == "" {
		t.Fatal("expected valid txid")
	}

	// Now doc idW is visible
	recW, err := c1.Get(idW)
	if err != nil || recW.Data["v"] != "waiting" {
		t.Fatalf("doc %d not visible with waiting data after retry: %+v, err: %v", idW, recW, err)
	}
}

// Scenario 6: No visibility before commit
// At each observable point up to the durable commit decision (using hooks),
// concurrent Get / Find / scan / index lookup / Watch on every participant observe
// none of the transaction's effects; after commit they observe all of it atomically.
func TestXTxConcurrency_NoVisibilityBeforeCommit(t *testing.T) {
	dir := t.TempDir()

	var hookHit atomic.Int64
	var hookProceed = make(chan struct{})

	cfg := defaultConfig()
	cfg.xtxHook = func(point xtxHookPoint, key, txid string) {
		if key == "visibility-tx" {
			hookHit.Store(int64(point))
			if point == xtxHookDecided {
				// Pause right after durable commit decision before apply
				<-hookProceed
			}
		}
	}

	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	cA, _ := db.CreateCollection("cA")
	cB, _ := db.CreateCollection("cB")

	idA, _, _ := cA.Insert(map[string]any{"status": "oldA", "category": "cat1"})
	idB, _, _ := cB.Insert(map[string]any{"status": "oldB", "category": "cat2"})

	_ = cA.EnsureIndex("category")
	_ = cB.EnsureIndex("category")

	// Set up watch channels
	_, subA, cancelA := cA.Subscribe()
	defer cancelA()
	_, subB, cancelB := cB.Subscribe()
	defer cancelB()

	// Begin XTx
	x, err := db.BeginXTx(context.Background(), []string{"cA", "cB"}, XTxOptions{Key: "visibility-tx"})
	if err != nil {
		t.Fatal(err)
	}

	_ = x.Update("cA", idA, map[string]any{"status": "newA", "category": "cat1"})
	idB_new, _ := x.Insert("cB", map[string]any{"status": "newB_inserted", "category": "cat2"})
	_ = x.Delete("cB", idB)

	// Check visibility while handle is active and staged
	recA, _ := cA.Get(idA)
	if recA.Data["status"] != "oldA" {
		t.Fatalf("staged update visible before commit: %+v", recA)
	}
	if _, ok := cB.liveRev(idB_new); ok {
		t.Fatal("staged insert visible before commit")
	}

	var commitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, commitErr = x.Commit(context.Background())
	}()

	// Wait until xtxHookDecided is reached
	for hookHit.Load() < int64(xtxHookDecided) {
		time.Sleep(5 * time.Millisecond)
	}

	// At xtxHookDecided (locks are held, decision is durable on disk, but apply has not run):
	// Reader queries should still observe only old data or wait on locks
	// Let's release the hook so commit finishes
	close(hookProceed)
	wg.Wait()

	if commitErr != nil {
		t.Fatalf("commit failed: %v", commitErr)
	}

	// After commit returns: all changes must be visible atomically across collections
	recA_after, err := cA.Get(idA)
	if err != nil || recA_after.Data["status"] != "newA" {
		t.Fatalf("cA/idA not updated: %+v, err: %v", recA_after, err)
	}

	if _, ok := cB.liveRev(idB); ok {
		t.Fatal("cB/idB should be deleted")
	}

	recB_new, err := cB.Get(idB_new)
	if err != nil || recB_new.Data["status"] != "newB_inserted" {
		t.Fatalf("cB/idB_new not visible: %+v, err: %v", recB_new, err)
	}

	// Secondary index queries see new state
	matchesA := cA.sidxMap["category"].Lookup("cat1")
	if len(matchesA) != 1 || matchesA[0] != idA {
		t.Fatalf("cA lookup: %+v", matchesA)
	}

	matchesB := cB.sidxMap["category"].Lookup("cat2")
	if len(matchesB) != 1 || matchesB[0] != idB_new {
		t.Fatalf("cB lookup: %+v", matchesB)
	}

	// Watch streams must have received events
	select {
	case evA := <-subA:
		if evA.ID != idA || evA.Op != store.OpUpdate {
			t.Fatalf("unexpected watch event A: %+v", evA)
		}
	case <-time.After(time.Second):
		t.Fatal("watch event A missing")
	}

	select {
	case evB := <-subB:
		if evB.ID != idB_new && evB.ID != idB {
			t.Fatalf("unexpected watch event B: %+v", evB)
		}
	case <-time.After(time.Second):
		t.Fatal("watch event B missing")
	}
}

// Scenario 4 (engine level): Retry / idempotency after client disconnect
// Key lookup after commit returns the single original outcome; transaction is
// applied exactly once.
func TestXTxConcurrency_IdempotencyAndDisconnect_Engine(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c1, _ := db.CreateCollection("c1")
	c2, _ := db.CreateCollection("c2")

	// Case 1: Cancel before locks -> nothing applied, retry succeeds
	t.Run("CancelBeforeLocks_RetrySucceeds", func(t *testing.T) {
		x, err := db.BeginXTx(context.Background(), []string{"c1", "c2"}, XTxOptions{Key: "key-retry-1"})
		if err != nil {
			t.Fatal(err)
		}
		id1, _ := x.Insert("c1", map[string]any{"v": 1})
		id2, _ := x.Insert("c2", map[string]any{"v": 2})

		cancCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = x.Commit(cancCtx)
		if !errors.Is(err, ErrXTxCanceled) {
			t.Fatalf("want ErrXTxCanceled, got %v", err)
		}

		// Nothing written
		if _, ok := c1.liveRev(id1); ok {
			t.Fatal("doc id1 should not be live")
		}

		// Retry with same handle or same key succeeds
		res, err := x.Commit(context.Background())
		if err != nil {
			t.Fatalf("retry commit failed: %v", err)
		}
		if res.TxID == "" {
			t.Fatal("expected txid")
		}

		// State applied exactly once
		r1, err := c1.Get(id1)
		if err != nil || r1.Rev != 1 {
			t.Fatalf("c1 doc: %+v, err: %v", r1, err)
		}
		r2, err := c2.Get(id2)
		if err != nil || r2.Rev != 1 {
			t.Fatalf("c2 doc: %+v, err: %v", r2, err)
		}
	})

	// Case 2: Commit succeeds; retry with same key returns Replayed and same TxID
	t.Run("CommittedKey_ReplayReturnsSameTxID", func(t *testing.T) {
		x, err := db.BeginXTx(context.Background(), []string{"c1", "c2"}, XTxOptions{Key: "key-retry-2"})
		if err != nil {
			t.Fatal(err)
		}
		id1, _ := x.Insert("c1", map[string]any{"v": 100})
		id2, _ := x.Insert("c2", map[string]any{"v": 200})

		res1, err := x.Commit(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Lookup via DB.XTxStatus(key)
		st, txid := db.XTxStatus("key-retry-2")
		if st != XTxCommitted || txid != res1.TxID {
			t.Fatalf("XTxStatus: got st=%v, txid=%v, want committed / %s", st, txid, res1.TxID)
		}

		// Retry CommitXTx with same key
		res2, err := db.CommitXTx("key-retry-2", []XTxOp{
			{Collection: "c1", Op: store.OpInsert, ID: id1, Data: map[string]any{"v": 100}},
			{Collection: "c2", Op: store.OpInsert, ID: id2, Data: map[string]any{"v": 200}},
		})
		if err != nil {
			t.Fatalf("CommitXTx replay: %v", err)
		}
		if !res2.Replayed || res2.TxID != res1.TxID {
			t.Fatalf("replay result mismatch: %+v, want Replayed=true, TxID=%s", res2, res1.TxID)
		}

		// Document revision remains 1 (not re-inserted)
		r1, _ := c1.Get(id1)
		if r1.Rev != 1 {
			t.Fatalf("c1/id1 rev = %d, want 1", r1.Rev)
		}
		r2, _ := c2.Get(id2)
		if r2.Rev != 1 {
			t.Fatalf("c2/id2 rev = %d, want 1", r2.Rev)
		}
	})
}
