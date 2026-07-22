package engine

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

// replCfg is a replication-enabled engine config for leader-side tests.
func replCfg() CollectionConfig {
	return CollectionConfig{
		SegmentMaxSize:      4 * 1024 * 1024,
		CompactInterval:     time.Hour,
		CompactDirtyPct:     0.30,
		ReplicationRingSize: 1024,
	}
}

// scanState reads a collection into an id→data map so two DBs can be compared.
func scanState(t *testing.T, db *DB, coll string) map[uint64]map[string]any {
	t.Helper()
	c, err := db.Collection(coll)
	if err != nil {
		return map[uint64]map[string]any{}
	}
	out := make(map[uint64]map[string]any)
	_, err = c.ScanStream(context.Background(), ScanOptions{Filter: query.MatchAll}, func(r ScanResult) error {
		out[r.ID] = r.Data
		return nil
	})
	if err != nil {
		t.Fatalf("scan %q: %v", coll, err)
	}
	return out
}

// drainBacklog subscribes from fromLSN and returns the buffered backlog, closing
// the subscription immediately (no live tailing needed for these tests).
func drainBacklog(t *testing.T, db *DB, fromLSN uint64) []ReplicationEntry {
	t.Helper()
	stream, backlog, ok := db.SubscribeReplication(fromLSN, "test")
	if !ok {
		t.Fatalf("SubscribeReplication(%d) not ok", fromLSN)
	}
	stream.Close()
	return backlog
}

func TestReplication_LSNMonotonicAndOrdered(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := Open(dir, replCfg())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		if _, _, err := col.Insert(map[string]any{"i": float64(i)}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if got := db.CurrentLSN(); got != n {
		t.Fatalf("CurrentLSN = %d, want %d", got, n)
	}

	backlog := drainBacklog(t, db, 0)
	if len(backlog) != n {
		t.Fatalf("backlog len = %d, want %d", len(backlog), n)
	}
	for i, re := range backlog {
		if re.LSN != uint64(i+1) {
			t.Fatalf("backlog[%d].LSN = %d, want %d", i, re.LSN, i+1)
		}
		if re.Collection != "c" {
			t.Fatalf("backlog[%d].Collection = %q", i, re.Collection)
		}
	}
}

func TestReplication_ResumeFromLSN(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := Open(dir, replCfg())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	col, _ := db.CreateCollection("c")
	for i := 0; i < 10; i++ {
		col.Insert(map[string]any{"i": float64(i)})
	}
	// Resuming from LSN 6 must yield only entries 7..10.
	backlog := drainBacklog(t, db, 6)
	if len(backlog) != 4 {
		t.Fatalf("resume backlog len = %d, want 4", len(backlog))
	}
	if backlog[0].LSN != 7 {
		t.Fatalf("first resumed LSN = %d, want 7", backlog[0].LSN)
	}
}

func TestReplication_TooFarBehindRequiresResync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{
		SegmentMaxSize:      4 * 1024 * 1024,
		CompactInterval:     time.Hour,
		ReplicationRingSize: 4, // tiny ring so old entries are evicted
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	col, _ := db.CreateCollection("c")
	for i := 0; i < 20; i++ {
		col.Insert(map[string]any{"i": float64(i)})
	}
	// LSN 1 has long since aged out of the 4-entry ring → resync required.
	if _, _, ok := db.SubscribeReplication(0, "t"); ok {
		t.Fatal("expected resync-required (ok=false) for an evicted LSN, got ok=true")
	}
	// A recent LSN is still servable.
	if _, _, ok := db.SubscribeReplication(18, "t"); !ok {
		t.Fatal("expected recent LSN to be servable")
	}
}

// TestReplication_ApplyEqualsReplay verifies that applying the shipped entries to
// a fresh follower reproduces the leader's exact query state, and that applying
// them again is a no-op (idempotent) — covering "snapshot-bootstrap + tail equals
// a full replay" and the no-duplication guarantee at the engine level.
func TestReplication_ApplyEqualsReplay(t *testing.T) {
	t.Parallel()
	leaderDir := t.TempDir()
	leader, err := Open(leaderDir, replCfg())
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leader.Close()

	lc, _ := leader.CreateCollection("c")
	// A mix of insert / update / delete / keyed writes.
	id0, _, _ := lc.Insert(map[string]any{"n": float64(1)})
	id1, _, _ := lc.Insert(map[string]any{"n": float64(2)})
	lc.Update(id1, map[string]any{"n": float64(22)})
	lc.Delete(id0)
	lc.InsertWithKey("alpha", map[string]any{"v": "a"})
	lc.Upsert("alpha", map[string]any{"v": "a2"})

	entries := drainBacklog(t, leader, 0)
	if len(entries) == 0 {
		t.Fatal("no replicated entries")
	}

	followerDir := t.TempDir()
	follower, err := Open(followerDir, CollectionConfig{SegmentMaxSize: 4 << 20, CompactInterval: time.Hour})
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer follower.Close()

	apply := func() {
		for _, re := range entries {
			if err := follower.ApplyReplication(re.Collection, re.Entry); err != nil {
				t.Fatalf("apply lsn %d: %v", re.LSN, err)
			}
		}
	}
	apply()
	apply() // second pass must be idempotent

	want := scanState(t, leader, "c")
	got := scanState(t, follower, "c")
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("follower state != leader state\n leader=%v\n follower=%v", want, got)
	}

	// Keyed lookups must resolve on the follower (the _key index was rebuilt from
	// the applied entries).
	fc, _ := follower.Collection("c")
	rec, err := fc.GetByKey("alpha")
	if err != nil {
		t.Fatalf("follower GetByKey: %v", err)
	}
	if rec.Data["v"] != "a2" {
		t.Fatalf("follower key alpha = %v, want a2", rec.Data["v"])
	}
}

// TestReplication_AppliedLSNPersists checks the follower's applied-LSN survives a
// close/reopen, so a resumed follower does not replay from zero.
func TestReplication_AppliedLSNPersists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{SegmentMaxSize: 4 << 20, CompactInterval: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.SetAppliedLSN(42); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(dir, CollectionConfig{SegmentMaxSize: 4 << 20, CompactInterval: time.Hour})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := db2.AppliedLSN(); got != 42 {
		t.Fatalf("applied lsn after reopen = %d, want 42", got)
	}
}
