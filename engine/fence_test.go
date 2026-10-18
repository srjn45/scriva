package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"

	"github.com/srjn45/scriva/store"
)

// unknownCounter records OnWriteOutcomeUnknown calls.
type unknownCounter struct {
	mu  sync.Mutex
	ops []string
}

func (u *unknownCounter) hook(_, op string) {
	u.mu.Lock()
	u.ops = append(u.ops, op)
	u.mu.Unlock()
}

func (u *unknownCounter) get() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.ops)
}

// openFenceTest opens a faulty single-collection DB with the unknown-outcome
// hook wired and the faultFS scoped to the active segment.
func openFenceTest(t *testing.T, cfg CollectionConfig) (*DB, *Collection, *faultFS, *unknownCounter, string) {
	t.Helper()
	dir := t.TempDir()
	fs := newFaultFS()
	u := &unknownCounter{}
	cfg.OnWriteOutcomeUnknown = u.hook
	db, col := openFaulty(t, dir, fs, cfg)
	t.Cleanup(func() { _ = db.Close() })
	col.mu.RLock()
	fs.scopeTo(col.active.Path())
	col.mu.RUnlock()
	return db, col, fs, u, dir
}

// S1.1: an fsync failure is Unknown, counted once, and every later read and
// write is refused with ErrCollectionFenced. Close, Stats and Verify still work;
// Snapshot is refused.
func TestFence_SyncFailureFencesReadsAndWrites(t *testing.T) {
	t.Parallel()
	db, col, fs, u, _ := openFenceTest(t, CollectionConfig{SyncMode: SyncModeAlways})
	seed, _, err := col.InsertWithKey("k1", map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}

	fs.failSyncAt(fs.scopedCount("sync")+1, syscall.EIO)
	_, _, err = col.Insert(map[string]any{"v": 2})
	if !errors.Is(err, ErrWriteOutcomeUnknown) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("insert err = %v, want ErrWriteOutcomeUnknown wrapping EIO", err)
	}
	if errors.Is(err, ErrCollectionFenced) {
		t.Fatalf("the failing write itself must report Unknown, not fenced: %v", err)
	}
	syncs := fs.scopedCount("sync")

	fenced := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, ErrCollectionFenced) {
			t.Errorf("%s: err = %v, want ErrCollectionFenced", what, err)
		}
	}
	_, _, err = col.Insert(map[string]any{"v": 3})
	fenced("Insert", err)
	_, err = col.Update(seed, map[string]any{"v": 4})
	fenced("Update", err)
	fenced("Delete", col.Delete(seed))
	_, err = col.Upsert("k1", map[string]any{"v": 5})
	fenced("Upsert", err)
	_, err = col.UpdateIfRev("k1", 1, map[string]any{"v": 6})
	fenced("UpdateIfRev", err)
	_, err = col.Get(seed)
	fenced("Get", err)
	_, _, err = col.FindByKey("k1")
	fenced("FindByKey", err)
	_, err = col.Scan(nil)
	fenced("Scan", err)
	_, err = col.Count(nil)
	fenced("Count", err)
	_, err = col.Exists("k1")
	fenced("Exists", err)
	fenced("Aggregate", col.Aggregate(context.Background(), AggregateSpec{}, func(GroupResult) error { return nil }))
	fenced("EnsureIndex", col.EnsureIndex("v"))
	fenced("CompactNow", col.CompactNow())
	if ids, ok := col.IndexLookup(KeyField, "k1"); ok || ids != nil {
		t.Errorf("IndexLookup on a fenced collection = %v, %v; want nil, false", ids, ok)
	}
	fenced("Snapshot", db.SnapshotTo(io.Discard))
	if err := col.Fenced(); !errors.Is(err, ErrCollectionFenced) {
		t.Errorf("Fenced() = %v", err)
	}

	if got := u.get(); !slices.Equal(got, []string{"insert"}) {
		t.Errorf("OnWriteOutcomeUnknown calls = %v, want exactly [insert]", got)
	}

	// Still allowed: Stats and Verify.
	if st := col.Stats(); st.Name != "c" {
		t.Errorf("Stats = %+v", st)
	}
	if _, err := col.Verify(context.Background(), VerifyOptions{}); err != nil {
		t.Errorf("Verify on a fenced collection: %v", err)
	}
	// Close is allowed and never fsyncs the segment again (no fsync retry).
	if err := db.Close(); err != nil {
		t.Errorf("Close on a fenced collection: %v", err)
	}
	if n := fs.scopedCount("sync"); n != syncs {
		t.Errorf("segment fsynced %d more time(s) after the failure", n-syncs)
	}
}

// Write + truncate failure: the poison and the write's own Unknown count once.
// A short write whose rollback succeeds is NotApplied and counts nothing.
func TestFence_TruncateFailureCountsOnce(t *testing.T) {
	t.Parallel()
	_, col, fs, u, _ := openFenceTest(t, CollectionConfig{SyncMode: SyncModeAlways})

	fs.failWriteAt(fs.scopedCount("write")+1, 7, syscall.ENOSPC)
	if _, _, err := col.Insert(map[string]any{"v": 1}); err == nil || errors.Is(err, ErrWriteOutcomeUnknown) {
		t.Fatalf("short write with rollback: err = %v, want NotApplied", err)
	}
	if col.Fenced() != nil || len(u.get()) != 0 {
		t.Fatalf("NotApplied fenced the collection (%v) or counted %v", col.Fenced(), u.get())
	}

	fs.failWriteAt(fs.scopedCount("write")+1, 7, syscall.ENOSPC)
	fs.failTruncateAt(fs.scopedCount("truncate")+1, syscall.EIO)
	_, _, err := col.Insert(map[string]any{"v": 2})
	if !errors.Is(err, ErrWriteOutcomeUnknown) || !errors.Is(err, ErrSegmentPoisoned) {
		t.Fatalf("err = %v, want ErrWriteOutcomeUnknown wrapping ErrSegmentPoisoned", err)
	}
	if got := u.get(); len(got) != 1 {
		t.Fatalf("OnWriteOutcomeUnknown calls = %v, want exactly one", got)
	}
}

// S1.5 / §3.6: a Seal sync failure after a committed insert returns success
// plus *PostCommitError, still delivers the event, fences the next write, and
// leaves the segment active — not sealed-and-active.
func TestFence_SealSyncFailureIsPostCommit(t *testing.T) {
	t.Parallel()
	db, col, fs, u, dir := openFenceTest(t, CollectionConfig{SyncMode: SyncModeAlways, SegmentMaxSize: 1, ReplicationRingSize: 64})
	_, events, cancel := col.Subscribe()
	defer cancel()
	col.mu.RLock()
	active := col.active
	nSealed := len(col.sealed)
	col.mu.RUnlock()
	lsn0 := woLSN(db)

	// Sync #1 is the commit fsync, #2 the seal fsync during rotation.
	fs.failSyncAt(fs.scopedCount("sync")+2, syscall.EIO)
	id, _, err := col.Insert(map[string]any{"v": 1})
	var pce *PostCommitError
	if !errors.Is(err, ErrPostCommit) || !errors.As(err, &pce) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("err = %v, want *PostCommitError wrapping EIO", err)
	}
	if errors.Is(err, ErrWriteOutcomeUnknown) || id == 0 || pce.Op != "insert" || pce.Stage != "rotate" {
		t.Fatalf("committed insert misreported: id=%d err=%v", id, err)
	}
	select {
	case ev := <-events:
		if ev.ID != id || ev.Op != store.OpInsert {
			t.Fatalf("event %+v, want insert of %d", ev, id)
		}
	default:
		t.Fatal("no watch event for a committed insert")
	}
	if d := woLSN(db) - lsn0; d != 1 {
		t.Fatalf("replication LSN advanced by %d, want 1", d)
	}

	col.mu.RLock()
	stillActive, sealedNow := col.active == active, len(col.sealed)
	col.mu.RUnlock()
	if !stillActive || active.IsSealed() || sealedNow != nSealed {
		t.Fatalf("segment left sealed-and-active or rotated: active kept=%v sealed=%v sealed segs %d->%d",
			stillActive, active.IsSealed(), nSealed, sealedNow)
	}
	if _, _, err := col.Insert(map[string]any{"v": 2}); !errors.Is(err, ErrCollectionFenced) {
		t.Fatalf("next write err = %v, want ErrCollectionFenced", err)
	}
	if got := u.get(); !slices.Equal(got, []string{"seal"}) {
		t.Fatalf("OnWriteOutcomeUnknown calls = %v, want [seal]", got)
	}

	// The commit was fsynced: it survives a power loss and a crash reopen.
	for _, power := range []bool{false, true} {
		img := t.TempDir()
		if power {
			powerLossImage(t, fs, dir, img)
		} else {
			copyTree(t, dir, img)
		}
		for range 2 {
			if v := openImage(t, img, id, ""); v.OpenErr != nil || !v.State.Visible {
				t.Fatalf("power=%v reopen: %+v", power, v)
			}
		}
	}
}

// §3.2: an XTx naming a fenced participant aborts before any byte or decision
// is written; the other participant is untouched.
func TestFence_XTxFencedParticipantAbortsPreDecision(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fs := newFaultFS()
	db, cols := openFaultyMulti(t, dir, fs, CollectionConfig{SyncMode: SyncModeAlways}, "a", "b")
	defer func() { _ = db.Close() }()
	a, b := cols["a"], cols["b"]
	if _, err := db.CommitXTx("warm", []XTxOp{{Collection: "a", Op: store.OpInsert, ID: a.ReserveID(), Data: map[string]any{"v": 0}}, {Collection: "b", Op: store.OpInsert, ID: b.ReserveID(), Data: map[string]any{"v": 0}}}); err != nil {
		t.Fatal(err)
	}

	b.mu.RLock()
	fs.scopeTo(b.active.Path())
	b.mu.RUnlock()
	fs.failSyncAt(fs.scopedCount("sync")+1, syscall.EIO)
	if _, _, err := b.Insert(map[string]any{"v": 1}); !errors.Is(err, ErrWriteOutcomeUnknown) {
		t.Fatalf("fence b: %v", err)
	}

	aSize := a.Stats().SizeBytes
	journal := func() int64 {
		st, err := os.Stat(filepath.Join(dir, xtxJournalFile))
		if err != nil {
			return -1
		}
		return st.Size()
	}
	j0 := journal()
	aID := a.ReserveID()
	_, err := db.CommitXTx("tx", []XTxOp{
		{Collection: "a", Op: store.OpInsert, ID: aID, Data: map[string]any{"v": 2}},
		{Collection: "b", Op: store.OpInsert, ID: b.ReserveID(), Data: map[string]any{"v": 2}},
	})
	if !errors.Is(err, ErrXTxDurability) || !errors.Is(err, ErrCollectionFenced) {
		t.Fatalf("CommitXTx err = %v, want ErrXTxDurability wrapping ErrCollectionFenced", err)
	}
	if got := a.Stats().SizeBytes; got != aSize {
		t.Fatalf("participant a grew %d -> %d bytes", aSize, got)
	}
	if got := journal(); got != j0 {
		t.Fatalf("journal changed %d -> %d bytes", j0, got)
	}
	if _, err := a.Get(aID); err == nil {
		t.Fatal("aborted XTx left its insert visible in a")
	}
}
