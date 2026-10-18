package engine

import (
	"errors"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// Outcome contract (§2.3) for the background and replication mutation paths
// beyond the single-target baseline in write_outcome_baseline_test.go: a
// multi-tombstone reaper pass and a follower re-apply after reopen.

// seedExpired inserts n records already past their TTL and returns their ids.
func seedExpired(t *testing.T, col *Collection, n int) []uint64 {
	t.Helper()
	ids := make([]uint64, n)
	exp := time.Now().Add(-time.Hour).UnixNano()
	for i := range ids {
		id, _, err := col.insert(map[string]any{"tag": "old", "v": i}, exp)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	return ids
}

func drainEvents(ch <-chan WatchEvent) []WatchEvent {
	var evs []WatchEvent
	for {
		select {
		case ev := <-ch:
			evs = append(evs, ev)
		default:
			return evs
		}
	}
}

// A reaper pass whose single sync fails publishes none of its tombstones: the
// error is Unknown, counted once, every id stays in the indexes, no event, no
// LSN; the next pass is refused and the background reaper stops. Reopen shows
// all tombstones (page cache) or none (power loss) — never a subset.
func TestReap_SyncFailureFencesWholePass(t *testing.T) {
	t.Parallel()
	db, col, fs, u, dir := openFenceTest(t, woConfig(CollectionConfig{SyncMode: SyncModeAlways}))
	if err := col.EnsureIndex("tag"); err != nil {
		t.Fatal(err)
	}
	ids := seedExpired(t, col, 3)
	_, events, cancel := col.Subscribe()
	defer cancel()
	lsn0 := woLSN(db)

	fs.failSyncAt(fs.scopedCount("sync")+1, syscall.EIO)
	err := col.reapExpired()
	if !errors.Is(err, ErrWriteOutcomeUnknown) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("reap err = %v, want ErrWriteOutcomeUnknown wrapping EIO", err)
	}
	for _, id := range ids {
		if _, ok := col.index.Get(id); !ok {
			t.Errorf("id %d dropped from the primary index before its tombstone was durable", id)
		}
	}
	// Read the tag index before the fence check below would refuse it.
	if tagged, _ := col.IndexLookup("tag", "old"); tagged != nil {
		t.Errorf("IndexLookup on a fenced collection = %v, want refused", tagged)
	}
	col.sidxMu.RLock()
	tagged := col.sidxMap["tag"].Lookup("old")
	col.sidxMu.RUnlock()
	if len(tagged) != len(ids) {
		t.Errorf("tag index holds %v, want all of %v", tagged, ids)
	}
	if evs := drainEvents(events); len(evs) != 0 {
		t.Errorf("watch events for an unknown reap: %+v", evs)
	}
	if d := woLSN(db) - lsn0; d != 0 {
		t.Errorf("replication LSN advanced by %d for an unknown reap", d)
	}
	if got := u.get(); !slices.Equal(got, []string{"reap"}) {
		t.Errorf("OnWriteOutcomeUnknown calls = %v, want exactly [reap]", got)
	}
	if err := col.reapExpired(); !errors.Is(err, ErrCollectionFenced) {
		t.Errorf("second reap err = %v, want ErrCollectionFenced", err)
	}
	if col.backgroundReap() {
		t.Error("backgroundReap reported a fenced collection as runnable")
	}

	crash, power := t.TempDir(), t.TempDir()
	copyTree(t, dir, crash)
	powerLossImage(t, fs, dir, power)
	for _, img := range []struct {
		name string
		dir  string
		want int // ids still indexed
	}{{"page-cache", crash, 0}, {"power-loss", power, len(ids)}} {
		for range 2 {
			rdb, err := Open(img.dir, woConfig(CollectionConfig{ReplicationRingSize: -1}))
			if err != nil {
				t.Fatalf("%s reopen: %v", img.name, err)
			}
			rc, err := rdb.Collection("c")
			if err != nil {
				t.Fatal(err)
			}
			var n int
			for _, id := range ids {
				if _, ok := rc.index.Get(id); ok {
					n++
				}
			}
			if n != img.want {
				t.Errorf("%s reopen: %d of %d reaped ids still indexed, want %d", img.name, n, len(ids), img.want)
			}
			_ = rdb.Close()
		}
	}
}

// An append failure mid-pass whose rollback succeeds is NotApplied for that
// tombstone only: the earlier ones are synced, dropped, published and emitted,
// the collection is not fenced, and the next pass reaps the rest.
func TestReap_ShortWriteMidPassCommitsPrefix(t *testing.T) {
	t.Parallel()
	db, col, fs, u, _ := openFenceTest(t, woConfig(CollectionConfig{SyncMode: SyncModeAlways}))
	ids := seedExpired(t, col, 2)
	_, events, cancel := col.Subscribe()
	defer cancel()
	lsn0 := woLSN(db)

	fs.failWriteAt(fs.scopedCount("write")+2, 7, syscall.ENOSPC)
	err := col.reapExpired()
	if !errors.Is(err, syscall.ENOSPC) || errors.Is(err, ErrWriteOutcomeUnknown) {
		t.Fatalf("reap err = %v, want plain ENOSPC (NotApplied)", err)
	}
	if col.Fenced() != nil {
		t.Fatalf("NotApplied reap fenced the collection: %v", col.Fenced())
	}
	var indexed int
	for _, id := range ids {
		if _, ok := col.index.Get(id); ok {
			indexed++
		}
	}
	if indexed != 1 {
		t.Errorf("%d of 2 ids still indexed after a half pass, want 1", indexed)
	}
	if evs := drainEvents(events); len(evs) != 1 {
		t.Errorf("watch events = %+v, want 1", evs)
	}
	if d := woLSN(db) - lsn0; d != 1 {
		t.Errorf("replication LSN advanced by %d, want 1", d)
	}

	if err := col.reapExpired(); err != nil {
		t.Fatalf("retry reap: %v", err)
	}
	for _, id := range ids {
		if _, ok := col.index.Get(id); ok {
			t.Errorf("id %d still indexed after the retry pass", id)
		}
	}
	if d := woLSN(db) - lsn0; d != 2 {
		t.Errorf("replication LSN advanced by %d after both passes, want 2", d)
	}
	if got := u.get(); len(got) != 0 {
		t.Errorf("OnWriteOutcomeUnknown calls = %v, want none", got)
	}
}

// A follower apply whose sync fails is Unknown and fences the collection with
// nothing applied; after a page-cache reopen the persisted entry is found, so
// re-applying it from the follower's unadvanced LSN is an idempotent no-op.
func TestApplyEntry_SyncFailureReapplyAfterReopen(t *testing.T) {
	t.Parallel()
	_, col, fs, u, dir := openFenceTest(t, woConfig(CollectionConfig{SyncMode: SyncModeAlways}))
	id, _, err := col.Insert(map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	en := store.NewUpdate(id, map[string]any{"v": 2})
	en.Rev = 2
	en.Ts = time.Now().UTC()

	fs.failSyncAt(fs.scopedCount("sync")+1, syscall.EIO)
	if err := col.applyEntry(en); !errors.Is(err, ErrWriteOutcomeUnknown) {
		t.Fatalf("apply err = %v, want ErrWriteOutcomeUnknown", err)
	}
	if loc, _ := col.index.Get(id); loc.Rev != 1 {
		t.Errorf("index rev = %d after an unknown apply, want 1 (not applied)", loc.Rev)
	}
	if err := col.applyEntry(en); !errors.Is(err, ErrCollectionFenced) {
		t.Errorf("re-apply on the fenced collection err = %v, want ErrCollectionFenced", err)
	}
	if got := u.get(); !slices.Equal(got, []string{"apply"}) {
		t.Errorf("OnWriteOutcomeUnknown calls = %v, want exactly [apply]", got)
	}

	crash := t.TempDir()
	copyTree(t, dir, crash)
	rdb, err := Open(crash, woConfig(CollectionConfig{ReplicationRingSize: -1}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()
	rc, err := rdb.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	_, events, cancel := rc.Subscribe()
	defer cancel()
	if err := rc.applyEntry(en); err != nil {
		t.Fatalf("re-apply after reopen: %v", err)
	}
	if evs := drainEvents(events); len(evs) != 0 {
		t.Errorf("re-apply of a persisted entry emitted %+v, want nothing", evs)
	}
	rec, err := rc.Get(id)
	if err != nil || rec.Rev != 2 || woInt(rec.Data["v"]) != 2 {
		t.Errorf("after reopen Get = %+v, %v; want rev 2 v 2", rec, err)
	}
}
