//nolint:errcheck
package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/srjn45/scriva/store"
)

// Upsert on an absent key inserts a fresh record at rev 1, stamps the key, and
// emits an OpInsert watch event.
func TestUpsertAbsentInserts(t *testing.T) {
	col := openTestCollection(t)

	_, ch, cancel := col.Subscribe()
	defer cancel()

	rec, err := col.Upsert("sess-1", map[string]any{"status": "open"})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if rec.Rev != 1 {
		t.Fatalf("upsert-absent rev = %d, want 1", rec.Rev)
	}
	if rec.Key != "sess-1" {
		t.Fatalf("upsert-absent key = %q, want sess-1", rec.Key)
	}
	if rec.Data["status"] != "open" || rec.Data[KeyField] != "sess-1" {
		t.Fatalf("upsert-absent data = %+v, want status open and stamped _key", rec.Data)
	}

	// The record is retrievable by key and carries rev 1.
	got, err := col.GetByKey("sess-1")
	if err != nil {
		t.Fatalf("GetByKey: %v", err)
	}
	if got.Rev != 1 || got.Data["status"] != "open" {
		t.Fatalf("GetByKey = %+v, want rev 1 status open", got)
	}

	// The watch event reflects an insert.
	ev := <-ch
	if ev.Op != store.OpInsert {
		t.Fatalf("watch op = %q, want %q", ev.Op, store.OpInsert)
	}
	if ev.ID != rec.ID || ev.Data[KeyField] != "sess-1" {
		t.Fatalf("watch event = %+v, want id %d with stamped _key", ev, rec.ID)
	}
}

// Upsert on a present key replaces the record's data in place: the id is stable,
// the revision is bumped, an OpUpdate event fires, and after compaction exactly
// one live entry survives.
func TestUpsertPresentReplaces(t *testing.T) {
	col := openTestCollection(t)

	first, err := col.Upsert("sess-1", map[string]any{"status": "open"})
	if err != nil {
		t.Fatalf("Upsert insert: %v", err)
	}

	_, ch, cancel := col.Subscribe()
	defer cancel()

	second, err := col.Upsert("sess-1", map[string]any{"status": "closed"})
	if err != nil {
		t.Fatalf("Upsert replace: %v", err)
	}

	// Same record (id preserved), revision incremented.
	if second.ID != first.ID {
		t.Fatalf("upsert-present id = %d, want stable id %d", second.ID, first.ID)
	}
	if second.Rev != 2 {
		t.Fatalf("upsert-present rev = %d, want 2", second.Rev)
	}
	if second.Data["status"] != "closed" {
		t.Fatalf("upsert-present data = %+v, want status closed", second.Data)
	}

	// A replace emits an update, not an insert.
	ev := <-ch
	if ev.Op != store.OpUpdate {
		t.Fatalf("watch op = %q, want %q", ev.Op, store.OpUpdate)
	}

	// Several more replaces, then compact: only one live entry may remain.
	for i := 0; i < 5; i++ {
		if _, err := col.Upsert("sess-1", map[string]any{"status": "open", "n": i}); err != nil {
			t.Fatalf("Upsert loop: %v", err)
		}
	}
	col.rotateSegment()
	if err := col.compact(false); err != nil {
		t.Fatalf("compact: %v", err)
	}

	results, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("after compaction %d live entries, want exactly 1", len(results))
	}
	// Revision continued monotonically across the replaces: insert(1),
	// replace(2), then five more (3..7).
	if rec, _ := col.GetByKey("sess-1"); rec.Rev != 7 {
		t.Fatalf("final rev = %d, want 7 (contiguous across replaces)", rec.Rev)
	}
}

// Supplying the reserved _key field inside Upsert data is rejected.
func TestUpsertRejectsReservedField(t *testing.T) {
	col := openTestCollection(t)
	if _, err := col.Upsert("k", map[string]any{KeyField: "other"}); !errors.Is(err, ErrReservedField) {
		t.Fatalf("Upsert with _key in data err = %v, want ErrReservedField", err)
	}
}

// Concurrent upserts on a single key must serialise cleanly under -race:
// exactly one live record remains, the revision is the contiguous count of
// writes (no lost updates), and the surviving data is one writer's value.
func TestUpsertConcurrentSameKeySerialises(t *testing.T) {
	col := openTestCollection(t)

	const goroutines = 32
	var succeeded atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			if _, err := col.Upsert("k", map[string]any{"worker": worker}); err != nil {
				t.Errorf("Upsert: %v", err)
				return
			}
			succeeded.Add(1)
		}(i)
	}
	close(start)
	wg.Wait()

	if got := succeeded.Load(); got != goroutines {
		t.Fatalf("%d upserts succeeded, want all %d", got, goroutines)
	}

	// Exactly one live record for the key.
	results, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("%d live records after concurrent upserts, want exactly 1", len(results))
	}

	// First writer inserts (rev 1), the remaining goroutines-1 replace it one at a
	// time — so the final rev is exactly the number of writes, with none lost.
	rec, err := col.GetByKey("k")
	if err != nil {
		t.Fatalf("GetByKey: %v", err)
	}
	if rec.Rev != goroutines {
		t.Fatalf("final rev = %d, want %d (contiguous, no lost updates)", rec.Rev, goroutines)
	}
	if _, ok := rec.Data["worker"]; !ok {
		t.Fatalf("final data = %+v, want a worker's value", rec.Data)
	}
}

// Upsert survives reopen: a keyed record written via Upsert is retrievable by
// key with its revision intact after the collection is closed and reopened.
func TestUpsertSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	func() {
		col := openUniqueCollection(t, dir)
		if _, err := col.Upsert("a", map[string]any{"v": 1}); err != nil {
			t.Fatalf("Upsert insert: %v", err)
		}
		if _, err := col.Upsert("a", map[string]any{"v": 2}); err != nil {
			t.Fatalf("Upsert replace: %v", err)
		}
		col.Close()
	}()

	col := openUniqueCollection(t, dir)
	rec, err := col.GetByKey("a")
	if err != nil {
		t.Fatalf("GetByKey after reopen: %v", err)
	}
	if rec.Rev != 2 || rec.Data["v"] != float64(2) {
		t.Fatalf("after reopen = %+v, want rev 2 v 2", rec)
	}
	// A subsequent upsert continues the revision sequence.
	if next, err := col.Upsert("a", map[string]any{"v": 3}); err != nil || next.Rev != 3 {
		t.Fatalf("post-reopen upsert = (%+v, %v), want rev 3", next, err)
	}
}
