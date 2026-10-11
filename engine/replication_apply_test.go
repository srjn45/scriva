package engine

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// replEnv builds a two-collection envelope: insert (a,1) + insert (b,1) at rev
// 1, or — when update — update both at rev 2.
func replEnv(t *testing.T, txid, key string, rev uint64, v string) *TxEnvelope {
	t.Helper()
	op := store.OpInsert
	if rev > 1 {
		op = store.OpUpdate
	}
	env, err := NewTxEnvelope(txid, key, []TxEnvelopeOp{
		{Collection: "a", Entry: store.Entry{ID: 1, Op: op, Rev: rev, Ts: time.Unix(1700000000, 5).UTC(), Data: map[string]any{"v": v}}},
		{Collection: "b", Entry: store.Entry{ID: 1, Op: op, Rev: rev, Ts: time.Unix(1700000000, 5).UTC(), Data: map[string]any{"v": v}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// view reports (a,b) as "rev:v" strings, "-" when absent.
func view(t *testing.T, db *DB) (string, string) {
	t.Helper()
	get := func(name string) string {
		c, err := db.Collection(name)
		if err != nil {
			return "-"
		}
		r, err := c.Get(1)
		if err != nil {
			return "-"
		}
		return string(rune('0'+r.Rev)) + ":" + r.Data["v"].(string)
	}
	return get("a"), get("b")
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		if strings.HasSuffix(d.Name(), ".lock") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func openFollower(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(dir, followerCfg())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestApplyReplicatedTx_AppliesAtomicallyAndAdvancesLSN(t *testing.T) {
	db := openFollower(t, t.TempDir())
	applied, err := db.ApplyReplicatedTx(7, replEnv(t, "tx-1", "k", 1, "x"))
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if a, b := view(t, db); a != "1:x" || b != "1:x" {
		t.Fatalf("view %q %q", a, b)
	}
	if db.AppliedLSN() != 7 {
		t.Fatalf("applied lsn %d", db.AppliedLSN())
	}
	// Missing collections were created; ids and revs are the leader's.
	c, _ := db.Collection("a")
	if r, _ := c.Get(1); r.Rev != 1 {
		t.Fatal("rev")
	}
}

func TestApplyReplicatedTx_IdempotentRedelivery(t *testing.T) {
	dir := t.TempDir()
	db := openFollower(t, dir)
	env := replEnv(t, "tx-1", "k", 1, "x")
	if _, err := db.ApplyReplicatedTx(3, env); err != nil {
		t.Fatal(err)
	}
	applied, err := db.ApplyReplicatedTx(3, env)
	if err != nil || applied {
		t.Fatalf("redelivery: applied=%v err=%v", applied, err)
	}
	// Redelivery after restart and with a stale applied LSN file.
	_ = db.Close()
	_ = os.Remove(filepath.Join(dir, replStateFilename))
	db2 := openFollower(t, dir)
	// The journal binding restored the position even without replication.json.
	if db2.AppliedLSN() != 3 {
		t.Fatalf("recovered applied lsn %d, want 3", db2.AppliedLSN())
	}
	applied, err = db2.ApplyReplicatedTx(3, env)
	if err != nil || applied {
		t.Fatalf("post-restart redelivery: applied=%v err=%v", applied, err)
	}
	if a, b := view(t, db2); a != "1:x" || b != "1:x" {
		t.Fatalf("view %q %q", a, b)
	}
}

func TestApplyReplicatedTx_DivergentRedeliveryIsTyped(t *testing.T) {
	db := openFollower(t, t.TempDir())
	if _, err := db.ApplyReplicatedTx(3, replEnv(t, "tx-1", "k", 1, "x")); err != nil {
		t.Fatal(err)
	}
	// Same LSN, different content.
	_, err := db.ApplyReplicatedTx(3, replEnv(t, "tx-1", "k", 1, "DIFFERENT"))
	if !errors.Is(err, ErrReplicationDivergence) {
		t.Fatalf("same lsn: %v", err)
	}
	// Same txid at a different LSN, different content.
	_, err = db.ApplyReplicatedTx(4, replEnv(t, "tx-1", "k", 1, "DIFFERENT"))
	if !errors.Is(err, ErrReplicationDivergence) {
		t.Fatalf("same txid: %v", err)
	}
	if a, _ := view(t, db); a != "1:x" {
		t.Fatalf("state changed: %q", a)
	}
	if db.AppliedLSN() != 3 {
		t.Fatalf("applied lsn moved: %d", db.AppliedLSN())
	}
}

// Crash at every coordinator step of a follower apply: after reopen the
// transaction is all-or-nothing and the applied LSN never covers a missing or
// partial transaction.
func TestApplyReplicatedTx_CrashAtEachStep(t *testing.T) {
	steps := []struct {
		name      string
		point     XTxHookPoint
		wantTx    bool // transaction must be present after recovery
		wantLSN   uint64
		wantRedel bool // redelivery is applied (true) or recognized as done
	}{
		{"before-prepare", XTxHookLocked, false, 5, true},
		{"after-prepare", XTxHookPrepared, false, 5, true},
		{"after-commit-decision", XTxHookDecided, true, 6, false},
		{"after-visible-before-lsn-persist", XTxHookReplApplied, true, 6, false},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			crash := t.TempDir()
			cfg := followerCfg()
			var snap atomic.Bool
			cfg.xtxHook = func(p xtxHookPoint, key, txid string) {
				if key == "k2" && XTxHookPoint(p) == st.point && snap.CompareAndSwap(false, true) {
					copyTree(t, dir, crash)
				}
			}
			db, err := Open(dir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.ApplyReplicatedTx(5, replEnv(t, "tx-1", "k1", 1, "one")); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ApplyReplicatedTx(6, replEnv(t, "tx-2", "k2", 2, "two")); err != nil {
				t.Fatal(err)
			}
			if !snap.Load() {
				t.Fatal("hook never fired")
			}

			re := openFollower(t, crash)
			a, b := view(t, re)
			want := "1:one"
			if st.wantTx {
				want = "2:two"
			}
			if a != want || b != want {
				t.Fatalf("partial or wrong state: a=%q b=%q want %q", a, b, want)
			}
			if re.AppliedLSN() != st.wantLSN {
				t.Fatalf("applied lsn %d, want %d", re.AppliedLSN(), st.wantLSN)
			}
			// Resume redelivers the envelope: applied now, or recognized.
			applied, err := re.ApplyReplicatedTx(6, replEnv(t, "tx-2", "k2", 2, "two"))
			if err != nil {
				t.Fatal(err)
			}
			if applied != st.wantRedel {
				t.Fatalf("redelivery applied=%v want %v", applied, st.wantRedel)
			}
			if a, b := view(t, re); a != "2:two" || b != "2:two" {
				t.Fatalf("after redelivery a=%q b=%q", a, b)
			}
			if re.AppliedLSN() != 6 {
				t.Fatalf("applied lsn %d", re.AppliedLSN())
			}
		})
	}
}

// Readers never observe a subset of a replicated transaction.
func TestApplyReplicatedTx_ConcurrentReadersSeeAllOrNothing(t *testing.T) {
	db := openFollower(t, t.TempDir())
	var stop atomic.Bool
	var wg sync.WaitGroup
	var bad atomic.Value
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				ca, errA := db.Collection("a")
				cb, errB := db.Collection("b")
				if errA != nil || errB != nil {
					continue
				}
				// Read b then a: if b already shows the revision, a must too.
				rb, errb := cb.Get(1)
				ra, erra := ca.Get(1)
				if errb == nil && (erra != nil || ra.Rev < rb.Rev) {
					bad.Store("b ahead of a")
				}
			}
		}()
	}
	for i := uint64(1); i <= 60; i++ {
		env := replEnv(t, "tx-"+string(rune('A'+i%26))+string(rune('a'+i/26)), "", i, "v")
		_ = env
	}
	for i := uint64(1); i <= 60; i++ {
		op := store.OpUpdate
		if i == 1 {
			op = store.OpInsert
		}
		env, err := NewTxEnvelope("t"+time.Now().Format("150405.000000000")+string(rune('a'+i%26))+string(rune('0'+i/26)), "", []TxEnvelopeOp{
			{Collection: "a", Entry: store.Entry{ID: 1, Op: op, Rev: i, Data: map[string]any{"v": "x"}}},
			{Collection: "b", Entry: store.Entry{ID: 1, Op: op, Rev: i, Data: map[string]any{"v": "x"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ApplyReplicatedTx(i, env); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	if v := bad.Load(); v != nil {
		t.Fatal(v)
	}
}

// A failed apply leaves nothing visible and does not advance the LSN.
func TestApplyReplicatedTx_FailureLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	cfg := followerCfg()
	fs := newFaultFS()
	cfg.wrapFile, cfg.renameFn = fs.wrap, fs.rename
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ApplyReplicatedTx(1, replEnv(t, "tx-1", "k1", 1, "one")); err != nil {
		t.Fatal(err)
	}
	fs.failSyncAt(fs.count("sync")+1, errors.New("injected sync failure"))
	if _, err := db.ApplyReplicatedTx(2, replEnv(t, "tx-2", "k2", 2, "two")); err == nil {
		t.Fatal("expected failure")
	}
	if a, b := view(t, db); a != "1:one" || b != "1:one" {
		t.Fatalf("partial state a=%q b=%q", a, b)
	}
	if db.AppliedLSN() != 1 {
		t.Fatalf("applied lsn %d", db.AppliedLSN())
	}
}

// Promote waits for an in-flight replicated apply and then refuses further
// ones: it never exposes (or loses) a partial transaction.
func TestPromote_InterlocksWithReplicatedApply(t *testing.T) {
	dir := t.TempDir()
	cfg := followerCfg()
	inApply := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	cfg.xtxHook = func(p xtxHookPoint, key, txid string) {
		if XTxHookPoint(p) == XTxHookPrepared {
			once.Do(func() { close(inApply); <-release })
		}
	}
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	applyErr := make(chan error, 1)
	go func() {
		_, err := db.ApplyReplicatedTx(9, replEnv(t, "tx-1", "k", 1, "x"))
		applyErr <- err
	}()
	<-inApply
	db.NoteLeaderLSN(9)

	promoted := make(chan PromoteResult, 1)
	go func() {
		res, _ := db.Promote(0, true)
		promoted <- res
	}()
	select {
	case <-promoted:
		t.Fatal("Promote returned while a replicated apply was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-applyErr; err != nil {
		t.Fatal(err)
	}
	res := <-promoted
	if res.LSN != 9 {
		t.Fatalf("promoted LSN %d, want 9 (covers the whole tx)", res.LSN)
	}
	if a, b := view(t, db); a != "1:x" || b != "1:x" {
		t.Fatalf("view %q %q", a, b)
	}
	if _, err := db.ApplyReplicatedTx(10, replEnv(t, "tx-2", "k2", 2, "y")); !errors.Is(err, ErrReplicationPromoted) {
		t.Fatalf("apply after promote: %v", err)
	}
}

// A transaction already contained in the bootstrap snapshot (leader journal
// shipped with it, applied LSN behind) is recognized, not re-applied.
func TestApplyReplicatedTx_SnapshotOverlapIsNoOp(t *testing.T) {
	ldir := t.TempDir()
	lcfg := defaultConfig()
	lcfg.ReplicationRingSize = 64
	leader, err := Open(ldir, lcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	la, _ := leader.CreateCollection("a")
	lb, _ := leader.CreateCollection("b")
	ida, idb := la.ReserveID(), lb.ReserveID()
	if _, err := leader.CommitXTx("k", []XTxOp{
		{Collection: "a", Op: store.OpInsert, ID: ida, Data: map[string]any{"v": "x"}},
		{Collection: "b", Op: store.OpInsert, ID: idb, Data: map[string]any{"v": "x"}},
	}); err != nil {
		t.Fatal(err)
	}
	stream, backlog, ok := leader.SubscribeReplication(0, "f")
	if !ok || len(backlog) != 1 || !backlog[0].IsTx() {
		t.Fatalf("backlog %v ok=%v", backlog, ok)
	}
	defer stream.Close()

	// "Snapshot": a copy of the leader's directory.
	fdir := t.TempDir()
	if err := leader.xtxJournal.f.Sync(); err != nil {
		t.Fatal(err)
	}
	copyTree(t, ldir, fdir)
	_ = os.Remove(filepath.Join(fdir, replStateFilename))
	f := openFollower(t, fdir)
	applied, err := f.ApplyReplicatedTx(backlog[0].LSN, backlog[0].Tx)
	if err != nil || applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if f.AppliedLSN() != backlog[0].LSN {
		t.Fatalf("applied lsn %d", f.AppliedLSN())
	}
}
