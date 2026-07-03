//nolint:errcheck
package engine

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// openUniqueCollection creates a test collection rooted at dir with a unique
// secondary index on "email". The background compactor is stopped so tests
// control timing. Passing an existing dir reopens a previously persisted
// collection.
func openUniqueCollection(t *testing.T, dir string) *Collection {
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

// ---- Insert enforcement -----------------------------------------------------

func TestUniqueIndex_RejectsDuplicateInsert(t *testing.T) {
	col := openTestCollection(t)
	if err := col.EnsureUniqueIndex("email"); err != nil {
		t.Fatalf("EnsureUniqueIndex: %v", err)
	}

	if _, _, err := col.Insert(map[string]any{"email": "a@x.com"}); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	_, _, err := col.Insert(map[string]any{"email": "a@x.com"})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey on duplicate insert, got %v", err)
	}

	// The rejected insert must not have appended a record or mutated the index.
	if got := col.Stats().RecordCount; got != 1 {
		t.Errorf("expected 1 record after rejected insert, got %d", got)
	}
	if ids, _ := col.IndexLookup("email", "a@x.com"); len(ids) != 1 {
		t.Errorf("expected exactly 1 id for email=a@x.com, got %v", ids)
	}
}

func TestUniqueIndex_AllowsDistinctInserts(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	if _, _, err := col.Insert(map[string]any{"email": "a@x.com"}); err != nil {
		t.Fatalf("insert a: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"email": "b@x.com"}); err != nil {
		t.Fatalf("insert b: %v", err)
	}
	// A record missing the indexed field is not constrained.
	if _, _, err := col.Insert(map[string]any{"name": "no-email"}); err != nil {
		t.Fatalf("insert without email: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"name": "also-no-email"}); err != nil {
		t.Fatalf("second insert without email: %v", err)
	}
}

// ---- Update enforcement -----------------------------------------------------

func TestUniqueIndex_RejectsDuplicateUpdate(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	id1, _, _ := col.Insert(map[string]any{"email": "a@x.com"})
	id2, _, _ := col.Insert(map[string]any{"email": "b@x.com"})

	// Updating id2 to id1's value must be rejected.
	_, err := col.Update(id2, map[string]any{"email": "a@x.com"})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey on colliding update, got %v", err)
	}

	// id2 must still hold its original value.
	if ids, _ := col.IndexLookup("email", "b@x.com"); len(ids) != 1 || ids[0] != id2 {
		t.Errorf("id2 should still map to b@x.com, got %v", ids)
	}
	if ids, _ := col.IndexLookup("email", "a@x.com"); len(ids) != 1 || ids[0] != id1 {
		t.Errorf("a@x.com should still map only to id1, got %v", ids)
	}
}

func TestUniqueIndex_AllowsSelfUpdate(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	id, _, _ := col.Insert(map[string]any{"email": "a@x.com", "n": 1})

	// Re-writing the same record with its own existing value is allowed.
	if _, err := col.Update(id, map[string]any{"email": "a@x.com", "n": 2}); err != nil {
		t.Fatalf("self-update with same value should be allowed, got %v", err)
	}
	// Moving to a fresh value is allowed.
	if _, err := col.Update(id, map[string]any{"email": "c@x.com"}); err != nil {
		t.Fatalf("update to new value should be allowed, got %v", err)
	}
}

// ---- Non-unique indexes are unaffected --------------------------------------

func TestNonUniqueIndex_AllowsDuplicates(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("tag") // non-unique

	if _, _, err := col.Insert(map[string]any{"tag": "go"}); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"tag": "go"}); err != nil {
		t.Fatalf("duplicate on non-unique index must be allowed, got %v", err)
	}
	if ids, _ := col.IndexLookup("tag", "go"); len(ids) != 2 {
		t.Errorf("expected 2 ids for tag=go on non-unique index, got %v", ids)
	}
}

// ---- Transaction enforcement ------------------------------------------------

func TestUniqueIndex_CommitTxRejectsDuplicate(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	col.Insert(map[string]any{"email": "a@x.com"})

	// Staged op colliding with committed data.
	now := time.Now().UTC()
	ops := []txOp{{kind: txOpInsert, id: col.ReserveID(), data: map[string]any{"email": "a@x.com"}, ts: now}}
	if err := col.CommitTx(ops); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey from CommitTx vs committed data, got %v", err)
	}
	// Nothing should have been written.
	if got := col.Stats().RecordCount; got != 1 {
		t.Errorf("expected 1 record after rejected commit, got %d", got)
	}
}

func TestUniqueIndex_CommitTxRejectsIntraBatchDuplicate(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	now := time.Now().UTC()
	ops := []txOp{
		{kind: txOpInsert, id: col.ReserveID(), data: map[string]any{"email": "dup@x.com"}, ts: now},
		{kind: txOpInsert, id: col.ReserveID(), data: map[string]any{"email": "dup@x.com"}, ts: now},
	}
	if err := col.CommitTx(ops); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey from intra-batch duplicate, got %v", err)
	}
	if got := col.Stats().RecordCount; got != 0 {
		t.Errorf("expected 0 records after rejected commit, got %d", got)
	}
}

func TestUniqueIndex_CommitTxAllowsDistinct(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	now := time.Now().UTC()
	ops := []txOp{
		{kind: txOpInsert, id: col.ReserveID(), data: map[string]any{"email": "a@x.com"}, ts: now},
		{kind: txOpInsert, id: col.ReserveID(), data: map[string]any{"email": "b@x.com"}, ts: now},
	}
	if err := col.CommitTx(ops); err != nil {
		t.Fatalf("distinct batch should commit, got %v", err)
	}
	if got := col.Stats().RecordCount; got != 2 {
		t.Errorf("expected 2 records after commit, got %d", got)
	}
}

// ---- Persistence & rebuild --------------------------------------------------

func TestUniqueIndex_FlagSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	func() {
		col := openUniqueCollection(t, dir)
		if err := col.EnsureUniqueIndex("email"); err != nil {
			t.Fatalf("EnsureUniqueIndex: %v", err)
		}
		col.Insert(map[string]any{"email": "a@x.com"})
		col.Close()
	}()

	// Reopen: the unique flag must be restored from disk and still enforced.
	col := openUniqueCollection(t, dir)
	_, _, err := col.Insert(map[string]any{"email": "a@x.com"})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected uniqueness enforced after reopen, got %v", err)
	}
}

func TestUniqueIndex_FlagSurvivesRebuildOnStaleFile(t *testing.T) {
	dir := t.TempDir()

	func() {
		col := openUniqueCollection(t, dir)
		col.EnsureUniqueIndex("email")
		col.Insert(map[string]any{"email": "a@x.com"})
		col.Close()
	}()

	// Overwrite the sidx file with one whose checksum does not match its
	// buckets, forcing a rebuild from segments on the next open. The unique flag
	// is stored as metadata outside the checksum, so it must survive the rebuild.
	path := sidxFilePath(filepath.Join(dir, "test"), "email")
	stale := sidxFile{
		Field:    "email",
		Unique:   true,
		Buckets:  map[string][]uint64{"a@x.com": {1}},
		Checksum: "deadbeef", // deliberately wrong → triggers rebuild on load
	}
	b, err := json.Marshal(stale)
	if err != nil {
		t.Fatalf("marshal stale sidx: %v", err)
	}
	if err := writeFileAtomic(path, b, 0o644); err != nil {
		t.Fatalf("write stale sidx: %v", err)
	}

	col := openUniqueCollection(t, dir)
	_, _, err = col.Insert(map[string]any{"email": "a@x.com"})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected uniqueness enforced after rebuild-on-stale, got %v", err)
	}
}

// ---- Rebuild tolerates historical duplicates --------------------------------

func TestUniqueIndex_RebuildToleratesHistoricalDuplicates(t *testing.T) {
	col := openTestCollection(t)

	// Two live records share a value before any unique index exists.
	col.Insert(map[string]any{"email": "dup@x.com"})
	col.Insert(map[string]any{"email": "dup@x.com"})

	// Making the index unique must not fail on the pre-existing duplicate.
	if err := col.EnsureUniqueIndex("email"); err != nil {
		t.Fatalf("EnsureUniqueIndex over historical duplicates should not error, got %v", err)
	}
	if ids, _ := col.IndexLookup("email", "dup@x.com"); len(ids) != 2 {
		t.Errorf("expected historical duplicates preserved (2 ids), got %v", ids)
	}

	// But new writes of that value are now rejected.
	if _, _, err := col.Insert(map[string]any{"email": "dup@x.com"}); !errors.Is(err, ErrDuplicateKey) {
		t.Errorf("expected new duplicate to be rejected, got %v", err)
	}
}

// ---- Concurrency ------------------------------------------------------------

func TestUniqueIndex_ConcurrentInsertsExactlyOneWins(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureUniqueIndex("email")

	const n = 32
	var wg sync.WaitGroup
	var wins, dups atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _, err := col.Insert(map[string]any{"email": "race@x.com"})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrDuplicateKey):
				dups.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if wins.Load() != 1 {
		t.Errorf("expected exactly 1 winning insert, got %d", wins.Load())
	}
	if dups.Load() != n-1 {
		t.Errorf("expected %d duplicate rejections, got %d", n-1, dups.Load())
	}
	if ids, _ := col.IndexLookup("email", "race@x.com"); len(ids) != 1 {
		t.Errorf("expected exactly 1 id in the index bucket, got %v", ids)
	}
}
