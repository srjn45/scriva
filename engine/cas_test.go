//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// ---- Revision tracking on the plain (non-keyed) path ------------------------

func TestRevIncrementsOnInsertAndUpdate(t *testing.T) {
	col := openTestCollection(t)

	id, _, err := col.Insert(map[string]any{"n": 1})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if rec, err := col.Get(id); err != nil || rec.Rev != 1 {
		t.Fatalf("after insert: rev=%d err=%v, want rev 1", rec.Rev, err)
	}

	for want := uint64(2); want <= 5; want++ {
		if _, err := col.Update(id, map[string]any{"n": int(want)}); err != nil {
			t.Fatalf("Update: %v", err)
		}
		rec, err := col.Get(id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if rec.Rev != want {
			t.Fatalf("after update: rev=%d, want %d", rec.Rev, want)
		}
	}

	// Scan surfaces the same revision.
	results, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 1 || results[0].Rev != 5 {
		t.Fatalf("scan rev = %v, want a single result at rev 5", results)
	}
}

// GetByKey exposes the revision for a keyed record.
func TestRevExposedViaGetByKey(t *testing.T) {
	col := openTestCollection(t)

	if _, _, err := col.InsertWithKey("k", map[string]any{"v": 1}); err != nil {
		t.Fatalf("InsertWithKey: %v", err)
	}
	rec, err := col.GetByKey("k")
	if err != nil {
		t.Fatalf("GetByKey: %v", err)
	}
	if rec.Rev != 1 || rec.Key != "k" {
		t.Errorf("GetByKey = rev %d key %q, want rev 1 key k", rec.Rev, rec.Key)
	}
	if _, err := col.UpdateByKey("k", map[string]any{"v": 2}); err != nil {
		t.Fatalf("UpdateByKey: %v", err)
	}
	if rec, err := col.GetByKey("k"); err != nil || rec.Rev != 2 {
		t.Errorf("after update GetByKey = rev %d err %v, want rev 2", rec.Rev, err)
	}
	if _, err := col.GetByKey("absent"); err == nil {
		t.Errorf("GetByKey absent: expected error, got nil")
	}
}

// ---- UpdateIfRev ------------------------------------------------------------

func TestUpdateIfRev(t *testing.T) {
	col := openTestCollection(t)

	col.InsertWithKey("k", map[string]any{"status": "open"}) // rev 1

	// Stale rev is a clean no-op — no error, no write.
	applied, err := col.UpdateIfRev("k", 99, map[string]any{"status": "closed"})
	if err != nil {
		t.Fatalf("UpdateIfRev stale: unexpected err %v", err)
	}
	if applied {
		t.Fatalf("UpdateIfRev with stale rev applied; want no-op")
	}
	if rec, _ := col.GetByKey("k"); rec.Data["status"] != "open" || rec.Rev != 1 {
		t.Fatalf("stale CAS mutated the record: %+v", rec)
	}

	// Correct rev applies and bumps the revision.
	applied, err = col.UpdateIfRev("k", 1, map[string]any{"status": "closed"})
	if err != nil {
		t.Fatalf("UpdateIfRev match: %v", err)
	}
	if !applied {
		t.Fatalf("UpdateIfRev with correct rev did not apply")
	}
	rec, _ := col.GetByKey("k")
	if rec.Data["status"] != "closed" || rec.Rev != 2 {
		t.Fatalf("after CAS: %+v, want status closed rev 2", rec)
	}
	// The key is preserved across the swap.
	if rec.Data[KeyField] != "k" {
		t.Errorf("CAS dropped the _key: %v", rec.Data[KeyField])
	}

	// Re-applying the now-stale rev 1 is again a no-op.
	if applied, _ := col.UpdateIfRev("k", 1, map[string]any{"status": "reopened"}); applied {
		t.Fatalf("second CAS on rev 1 applied; want no-op")
	}
}

// CAS on a missing key is a clean no-op: (false, nil), never an error.
func TestUpdateIfRevMissingKey(t *testing.T) {
	col := openTestCollection(t)

	// Before any keyed write the _key index does not exist yet.
	if applied, err := col.UpdateIfRev("nope", 1, map[string]any{"x": 1}); applied || err != nil {
		t.Fatalf("CAS on empty collection = (%v, %v), want (false, nil)", applied, err)
	}

	col.InsertWithKey("present", map[string]any{"x": 1})
	if applied, err := col.UpdateIfRev("absent", 1, map[string]any{"x": 2}); applied || err != nil {
		t.Fatalf("CAS on absent key = (%v, %v), want (false, nil)", applied, err)
	}
}

// ---- UpdateIfMatch ----------------------------------------------------------

func TestUpdateIfMatch(t *testing.T) {
	col := openTestCollection(t)

	col.InsertWithKey("sess", map[string]any{"status": "running"}) // rev 1

	// Predicate false → no-op.
	applied, err := col.UpdateIfMatch("sess",
		func(cur map[string]any) bool { return cur["status"] == "done" },
		map[string]any{"status": "exited"})
	if err != nil || applied {
		t.Fatalf("false-predicate CAS = (%v, %v), want (false, nil)", applied, err)
	}
	if rec, _ := col.GetByKey("sess"); rec.Data["status"] != "running" {
		t.Fatalf("false-predicate CAS mutated the record: %+v", rec)
	}

	// Predicate true → applies, rev bumps.
	applied, err = col.UpdateIfMatch("sess",
		func(cur map[string]any) bool { return cur["status"] == "running" },
		map[string]any{"status": "exited"})
	if err != nil || !applied {
		t.Fatalf("true-predicate CAS = (%v, %v), want (true, nil)", applied, err)
	}
	if rec, _ := col.GetByKey("sess"); rec.Data["status"] != "exited" || rec.Rev != 2 {
		t.Fatalf("after CAS: %+v, want status exited rev 2", rec)
	}

	// Missing key → no-op.
	if applied, err := col.UpdateIfMatch("gone",
		func(map[string]any) bool { return true },
		map[string]any{"status": "x"}); applied || err != nil {
		t.Fatalf("CAS on missing key = (%v, %v), want (false, nil)", applied, err)
	}
}

// Supplying the reserved _key field inside CAS data is rejected.
func TestCASRejectsReservedField(t *testing.T) {
	col := openTestCollection(t)
	col.InsertWithKey("k", map[string]any{"v": 1})

	if _, err := col.UpdateIfRev("k", 1, map[string]any{KeyField: "other"}); err == nil {
		t.Errorf("UpdateIfRev with _key in data: expected ErrReservedField, got nil")
	}
	if _, err := col.UpdateIfMatch("k", func(map[string]any) bool { return true },
		map[string]any{KeyField: "other"}); err == nil {
		t.Errorf("UpdateIfMatch with _key in data: expected ErrReservedField, got nil")
	}
}

// ---- Concurrency: exactly one racing CAS applies ----------------------------

// Two (and more) goroutines racing the same rev-CAS: exactly one must apply.
// Run under -race to catch any unsynchronised read-check-write.
func TestUpdateIfRevRaceExactlyOneApplies(t *testing.T) {
	col := openTestCollection(t)
	col.InsertWithKey("k", map[string]any{"n": 0}) // rev 1

	const goroutines = 16
	var applied atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ok, err := col.UpdateIfRev("k", 1, map[string]any{"n": i + 1})
			if err != nil {
				t.Errorf("UpdateIfRev: %v", err)
			}
			if ok {
				applied.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := applied.Load(); got != 1 {
		t.Fatalf("expected exactly one CAS to apply, got %d", got)
	}
	if rec, _ := col.GetByKey("k"); rec.Rev != 2 {
		t.Fatalf("after race rev = %d, want 2 (a single applied swap)", rec.Rev)
	}
}

// The predicate form claims a key exactly once under contention.
func TestUpdateIfMatchRaceExactlyOneApplies(t *testing.T) {
	col := openTestCollection(t)
	col.InsertWithKey("job", map[string]any{"claimed": false})

	const goroutines = 16
	var applied atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			ok, err := col.UpdateIfMatch("job",
				func(cur map[string]any) bool { return cur["claimed"] == false },
				map[string]any{"claimed": true, "worker": worker})
			if err != nil {
				t.Errorf("UpdateIfMatch: %v", err)
			}
			if ok {
				applied.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := applied.Load(); got != 1 {
		t.Fatalf("expected exactly one worker to claim the job, got %d", got)
	}
	if rec, _ := col.GetByKey("job"); rec.Data["claimed"] != true {
		t.Fatalf("job not claimed after race: %+v", rec)
	}
}

// ---- Rev survives compaction and reopen -------------------------------------

func TestRevSurvivesCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()

	func() {
		col := openUniqueCollection(t, dir)

		col.InsertWithKey("a", map[string]any{"v": 0}) // rev 1
		for i := 1; i <= 4; i++ {                       // rev 2..5
			if _, err := col.UpdateByKey("a", map[string]any{"v": i}); err != nil {
				t.Fatalf("UpdateByKey: %v", err)
			}
		}
		if rec, _ := col.GetByKey("a"); rec.Rev != 5 {
			t.Fatalf("before compaction rev = %d, want 5", rec.Rev)
		}

		// Force a compaction: the many stale versions collapse into one line that
		// must still carry rev 5.
		col.rotateSegment()
		if err := col.compact(false); err != nil {
			t.Fatalf("compact: %v", err)
		}
		if rec, _ := col.GetByKey("a"); rec.Rev != 5 {
			t.Fatalf("after compaction rev = %d, want 5 (compaction must preserve rev)", rec.Rev)
		}

		// CAS off the preserved rev still works and bumps to 6.
		if ok, err := col.UpdateIfRev("a", 5, map[string]any{"v": 99}); err != nil || !ok {
			t.Fatalf("post-compaction CAS = (%v, %v), want (true, nil)", ok, err)
		}
		if rec, _ := col.GetByKey("a"); rec.Rev != 6 {
			t.Fatalf("post-compaction CAS rev = %d, want 6", rec.Rev)
		}
		col.Close()
	}()

	// Reopen from disk: the persisted index carries the revision.
	col := openUniqueCollection(t, dir)
	rec, err := col.GetByKey("a")
	if err != nil {
		t.Fatalf("GetByKey after reopen: %v", err)
	}
	if rec.Rev != 6 {
		t.Fatalf("after reopen rev = %d, want 6", rec.Rev)
	}
	// A stale CAS after reopen is a no-op; the live rev still applies.
	if ok, _ := col.UpdateIfRev("a", 5, map[string]any{"v": 1}); ok {
		t.Fatalf("stale CAS after reopen applied; want no-op")
	}
	if ok, err := col.UpdateIfRev("a", 6, map[string]any{"v": 2}); err != nil || !ok {
		t.Fatalf("live CAS after reopen = (%v, %v), want (true, nil)", ok, err)
	}
	if rec, _ := col.GetByKey("a"); rec.Rev != 7 {
		t.Fatalf("after reopen CAS rev = %d, want 7", rec.Rev)
	}
}

// Rev is recomputed by replay order after an index rebuild (crash recovery).
func TestRevRecomputedOnRebuild(t *testing.T) {
	dir := t.TempDir()
	col := openUniqueCollection(t, dir)

	col.InsertWithKey("a", map[string]any{"v": 0})
	col.UpdateByKey("a", map[string]any{"v": 1})
	col.UpdateByKey("a", map[string]any{"v": 2}) // rev 3
	col.Close()

	// Remove the persisted index so load() must rebuild from segments, then
	// reopen and confirm the revision was recomputed by replay order.
	if err := os.Remove(filepath.Join(dir, "test", "index.json")); err != nil {
		t.Fatalf("remove index.json: %v", err)
	}
	col2 := openUniqueCollection(t, dir)
	if rec, err := col2.GetByKey("a"); err != nil || rec.Rev != 3 {
		t.Fatalf("after rebuild rev = %d err = %v, want 3", rec.Rev, err)
	}
}

// ---- Backward compatibility: pre-existing rev-less segments -----------------

// A segment written before revisions existed (no "rev" field, crc computed
// without it) must load cleanly, and its records must be assigned revisions by
// replay order on rebuild.
func TestRevlessSegmentLoadsFine(t *testing.T) {
	dir := t.TempDir()
	segDir := filepath.Join(dir, "test")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Hand-write an old-format segment: NewInsert/NewUpdate leave Rev == 0, so
	// Encode omits the field and checksums the entry exactly as legacy code did.
	var buf []byte
	for _, e := range []store.Entry{
		store.NewInsert(1, map[string]any{"name": "alice"}),
		store.NewInsert(2, map[string]any{"name": "bob"}),
		store.NewUpdate(1, map[string]any{"name": "alice2"}),
	} {
		e.Ts = time.Now().UTC()
		line, err := store.Encode(e)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		buf = append(buf, line...)
	}
	if err := os.WriteFile(filepath.Join(segDir, "seg_000001.ndjson"), buf, 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	// Open the collection over the legacy segment: no index.json exists, so the
	// index (and revisions) are rebuilt from the rev-less lines.
	col := openTestCollection2(t, dir)

	rec, err := col.Get(1)
	if err != nil {
		t.Fatalf("Get(1) over legacy segment: %v", err)
	}
	if rec.Data["name"] != "alice2" {
		t.Errorf("Get(1) data = %v, want latest alice2", rec.Data["name"])
	}
	// id 1 had insert+update ⇒ rev 2; id 2 had a single insert ⇒ rev 1.
	if rec.Rev != 2 {
		t.Errorf("Get(1) rev = %d, want 2 (replay-order count)", rec.Rev)
	}
	if rec2, _ := col.Get(2); rec2.Rev != 1 {
		t.Errorf("Get(2) rev = %d, want 1", rec2.Rev)
	}

	// A fresh update continues the sequence from the recomputed rev.
	if _, err := col.Update(1, map[string]any{"name": "alice3"}); err != nil {
		t.Fatalf("Update after legacy load: %v", err)
	}
	if rec, _ := col.Get(1); rec.Rev != 3 {
		t.Errorf("post-legacy update rev = %d, want 3", rec.Rev)
	}
}

// openTestCollection2 opens a collection named "test" rooted at an explicit dir
// (openTestCollection always uses a fresh t.TempDir()). The background compactor
// is stopped so the test controls timing.
func openTestCollection2(t *testing.T, dir string) *Collection {
	t.Helper()
	col, err := OpenCollection("test", dir, CollectionConfig{
		SegmentMaxSize:  512,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	col.closeOnce.Do(func() { close(col.closed) })
	select {
	case <-col.compactC:
	default:
	}
	t.Cleanup(func() { col.Close() })
	return col
}
