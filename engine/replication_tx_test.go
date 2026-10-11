package engine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srjn45/scriva/store"
)

func envOf(t *testing.T, txid string, n int) *TxEnvelope {
	t.Helper()
	var ops []TxEnvelopeOp
	for i := 0; i < n; i++ {
		ops = append(ops, TxEnvelopeOp{Collection: "c", Entry: store.Entry{ID: uint64(i + 1), Op: store.OpInsert, Rev: 1, Data: map[string]any{"i": i}}})
	}
	env, err := NewTxEnvelope(txid, "", ops)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func legacyE(id uint64) store.Entry { return store.NewInsert(id, map[string]any{"id": id}) }

// whole asserts every record in recs is intact (an envelope still verifies and
// has all its ops) — i.e. nothing was truncated or split.
func whole(t *testing.T, recs []ReplicationEntry) {
	t.Helper()
	for _, r := range recs {
		if r.IsTx() {
			if err := r.Tx.Verify(); err != nil {
				t.Fatalf("lsn %d: %v", r.LSN, err)
			}
		}
	}
}

func TestBroker_LSNMonotonicMixedAndOneLSNPerTx(t *testing.T) {
	t.Parallel()
	b := newReplicationBroker(64, 0)
	if l := b.publish("c", legacyE(1)); l != 1 {
		t.Fatal(l)
	}
	if l := b.publishTx(envOf(t, "t1", 5)); l != 2 {
		t.Fatalf("tx lsn %d, want 2 (one LSN for 5 ops)", l)
	}
	if l := b.publish("c", legacyE(2)); l != 3 {
		t.Fatal(l)
	}
	if b.currentLSN() != 3 {
		t.Fatal(b.currentLSN())
	}
	_, backlog, ok := b.subscribe(0, "f")
	if !ok || len(backlog) != 3 {
		t.Fatalf("ok=%v len=%d", ok, len(backlog))
	}
	for i, r := range backlog {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record %d has lsn %d", i, r.LSN)
		}
	}
	if !backlog[1].IsTx() || backlog[0].IsTx() || backlog[2].IsTx() {
		t.Fatal("kinds wrong")
	}
	whole(t, backlog)
}

func TestBroker_RingEvictsWholeEnvelopes(t *testing.T) {
	t.Parallel()
	b := newReplicationBroker(10, 0)
	b.publishTx(envOf(t, "t1", 6)) // lsn1 weight 6
	b.publish("c", legacyE(1))     // lsn2 weight 1 (total 7)
	b.publishTx(envOf(t, "t2", 5)) // lsn3: 7+5>10 -> evict t1 whole
	_, backlog, ok := b.subscribe(1, "f")
	if !ok {
		t.Fatal("resume from 1 should be serveable")
	}
	if len(backlog) != 2 || backlog[0].LSN != 2 || backlog[1].LSN != 3 || len(backlog[1].Tx.Ops) != 5 {
		t.Fatalf("backlog: %+v", backlog)
	}
	// LSN 1 (the evicted envelope) is gone: a follower at 0 must re-bootstrap,
	// it is never served a partial transaction.
	if _, _, ok := b.subscribe(0, "f2"); ok {
		t.Fatal("subscribe(0) should require bootstrap after eviction")
	}
	whole(t, backlog)
}

func TestBroker_OversizeEnvelopeRetainedAlone(t *testing.T) {
	t.Parallel()
	b := newReplicationBroker(4, 0)
	b.publish("c", legacyE(1))
	b.publish("c", legacyE(2))
	b.publishTx(envOf(t, "big", 9)) // weight 9 > cap 4
	_, backlog, ok := b.subscribe(2, "f")
	if !ok || len(backlog) != 1 || len(backlog[0].Tx.Ops) != 9 {
		t.Fatalf("ok=%v backlog=%+v", ok, backlog)
	}
	b.publish("c", legacyE(3)) // evicts the big one, whole
	if _, _, ok := b.subscribe(2, "f"); ok {
		t.Fatal("expected resume point 2 to be evicted")
	}
}

func TestBroker_SlowSubscriberCutOffNotPartial(t *testing.T) {
	t.Parallel()
	b := newReplicationBroker(100, 10)
	sub, _, ok := b.subscribe(0, "slow")
	if !ok {
		t.Fatal()
	}
	b.publishTx(envOf(t, "t1", 6)) // queued weight 6 (admitted)
	select {
	case <-sub.dead:
		t.Fatal("subscriber cut off too early")
	default:
	}
	b.publishTx(envOf(t, "t2", 6)) // 6+6 > 10 -> cut off, t2 NOT delivered partially/at all
	select {
	case <-sub.dead:
	default:
		t.Fatal("slow subscriber should be cut off")
	}
	if len(sub.ch) != 1 {
		t.Fatalf("channel holds %d records, want only the first whole envelope", len(sub.ch))
	}
	got := <-sub.ch
	if got.Tx.TxID != "t1" || len(got.Tx.Ops) != 6 {
		t.Fatalf("unexpected record %+v", got)
	}
}

func TestBroker_HeavyEnvelopeAdmittedToEmptyBuffer(t *testing.T) {
	t.Parallel()
	b := newReplicationBroker(100, 4)
	sub, _, _ := b.subscribe(0, "f")
	b.publishTx(envOf(t, "big", 50)) // heavier than the sub weight, buffer empty
	select {
	case <-sub.dead:
		t.Fatal("heavy envelope must be deliverable to a caught-up subscriber")
	default:
	}
	r := <-sub.ch
	sub.queued.Add(-int64(r.Weight()))
	b.publish("c", legacyE(1)) // buffer drained: fine
	select {
	case <-sub.dead:
		t.Fatal("drained subscriber was cut off")
	default:
	}
}

func TestBroker_ResyncBarrier(t *testing.T) {
	t.Parallel()
	b := newReplicationBroker(16, 0)
	b.publish("c", legacyE(1))
	sub, _, _ := b.subscribe(1, "f")
	b.resyncBarrier()
	select {
	case <-sub.dead:
	default:
		t.Fatal("barrier must cut subscribers")
	}
	if b.currentLSN() != 2 {
		t.Fatalf("lsn %d", b.currentLSN())
	}
	if _, _, ok := b.subscribe(1, "f"); ok {
		t.Fatal("follower at pre-barrier LSN must re-bootstrap")
	}
	if _, _, ok := b.subscribe(2, "f"); !ok {
		t.Fatal("follower at the barrier LSN can resume")
	}
}

func TestBroker_LSNReservationSurvivesCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := Open(dir, replCfg())
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.CreateCollection("c")
	for i := 0; i < 3; i++ {
		if _, _, err := col.Insert(map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	// Mid-run (as after a crash) the persisted watermark is ahead of the
	// assigned LSN, never behind it.
	st, err := loadReplState(filepath.Join(dir, replStateFilename))
	if err != nil {
		t.Fatal(err)
	}
	if st.LeaderLSN < db.CurrentLSN() || st.LeaderLSN <= 3 {
		t.Fatalf("persisted leader_lsn %d must reserve ahead of LSN %d", st.LeaderLSN, db.CurrentLSN())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A clean close writes the exact value.
	data, _ := os.ReadFile(filepath.Join(dir, replStateFilename))
	var after replState
	_ = json.Unmarshal(data, &after)
	if after.LeaderLSN != 3 {
		t.Fatalf("clean close persisted %d, want 3", after.LeaderLSN)
	}
}

func TestXTxReplication_AbortedTxNeverPublished(t *testing.T) {
	cfg := defaultConfig()
	cfg.ReplicationRingSize = 100
	db := openTestDB(t, cfg)
	colA, _ := db.CreateCollection("colA")
	_, _ = db.CreateCollection("colB")
	if _, _, err := colA.Insert(map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	before := db.CurrentLSN()

	// Update of a document that does not exist aborts at validation.
	_, err := db.CommitXTx("", []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"a": 1}},
		{Collection: "colB", Op: store.OpUpdate, ID: 9999, Data: map[string]any{"b": 1}},
	})
	if err == nil {
		t.Fatal("expected conflict")
	}
	if db.CurrentLSN() != before {
		t.Fatalf("aborted tx consumed LSN: %d -> %d", before, db.CurrentLSN())
	}
	// A later legacy write continues the sequence with no gap.
	if _, _, err := colA.Insert(map[string]any{"x": 2}); err != nil {
		t.Fatal(err)
	}
	if db.CurrentLSN() != before+1 {
		t.Fatalf("lsn %d, want %d", db.CurrentLSN(), before+1)
	}
}

func TestXTxReplication_MixedStreamAndOneLSNPerTx(t *testing.T) {
	cfg := defaultConfig()
	cfg.ReplicationRingSize = 100
	db := openTestDB(t, cfg)
	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")

	if _, _, err := colA.Insert(map[string]any{"x": 1}); err != nil { // lsn 1
		t.Fatal(err)
	}
	res, err := db.CommitXTx("k", []XTxOp{ // lsn 2
		{Collection: "colB", Op: store.OpInsert, ID: colB.ReserveID(), Data: map[string]any{"b": 1}},
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"a": 1}},
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"a": 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := colB.Insert(map[string]any{"y": 1}); err != nil { // lsn 3
		t.Fatal(err)
	}
	recs := drainBacklog(t, db, 0)
	if len(recs) != 3 || db.CurrentLSN() != 3 {
		t.Fatalf("got %d records, lsn %d", len(recs), db.CurrentLSN())
	}
	tx := recs[1]
	if !tx.IsTx() || tx.LSN != 2 || tx.Tx.TxID != res.TxID || tx.Tx.Key != "k" {
		t.Fatalf("bad envelope record: %+v", tx)
	}
	var got []string
	for _, o := range tx.Tx.Ops {
		got = append(got, o.Collection)
	}
	if strings.Join(got, ",") != "colA,colA,colB" {
		t.Fatalf("not canonical order: %v", got)
	}
	whole(t, recs)
	// Resume from inside the stream replays whole records only.
	tail := drainBacklog(t, db, 1)
	if len(tail) != 2 || !tail[0].IsTx() || len(tail[0].Tx.Ops) != 3 {
		t.Fatalf("tail: %+v", tail)
	}
}

func TestXTxReplication_EnvelopeTooLargeRejectedBeforeWrite(t *testing.T) {
	cfg := defaultConfig()
	cfg.ReplicationRingSize = 100
	db := openTestDB(t, cfg)
	colA, _ := db.CreateCollection("colA")
	colB, _ := db.CreateCollection("colB")
	big := strings.Repeat("x", DefaultMaxEnvelopeBytes+1)
	_, err := db.CommitXTx("", []XTxOp{
		{Collection: "colA", Op: store.OpInsert, ID: colA.ReserveID(), Data: map[string]any{"v": big}},
		{Collection: "colB", Op: store.OpInsert, ID: colB.ReserveID(), Data: map[string]any{"v": 1}},
	})
	if !errors.Is(err, ErrXTxTooLarge) {
		t.Fatalf("got %v, want ErrXTxTooLarge", err)
	}
	if db.CurrentLSN() != 0 {
		t.Fatal("rejected tx consumed an LSN")
	}
}
