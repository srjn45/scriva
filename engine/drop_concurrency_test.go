package engine

// Concurrency, snapshot and crash tests for DropCollection against the
// cross-collection transaction journal (hardening §3.3, acceptance P3.x).
// Every race is driven by the commit-phase hook (barriers, not sleeps); the
// only timing used is a deadlock guard, and a short negative probe that can
// miss a bug but never fail a correct implementation.

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

const dropTestGuard = 20 * time.Second

// recv waits for ch or fails the test with a goroutine dump (deadlock guard).
func recv[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(dropTestGuard):
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
		t.Fatalf("deadlock guard: %s did not complete in %s\n%s", what, dropTestGuard, buf.String())
		panic("unreachable")
	}
}

// docCount counts the live records of a collection (-1 when it is absent).
func docCount(t *testing.T, db *DB, name string) int {
	t.Helper()
	c, err := db.Collection(name)
	if err != nil {
		return -1
	}
	res, err := c.Scan(nil)
	if err != nil {
		t.Fatalf("scan %s: %v", name, err)
	}
	return len(res)
}

func TestDropVsCommitXTx_AtEveryPhase(t *testing.T) {
	cases := []struct {
		name  string
		point xtxHookPoint
		// dropFirst: the hook fires before the commit holds DB.mu, so Drop
		// completes first and the commit must fail cleanly.
		dropFirst bool
	}{
		{"staged_before_lock", xtxHookBeforeLocks, true},
		{"locked", xtxHookLocked, false},
		{"prepared", xtxHookPrepared, false},
		{"decided_before_apply", xtxHookDecided, false},
		{"applied", xtxHookApplied, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			hit := make(chan struct{})
			release := make(chan struct{})
			cfg := defaultConfig()
			cfg.xtxHook = func(p xtxHookPoint, key, txid string) {
				if key == "race" && p == tc.point {
					close(hit)
					<-release
				}
			}
			db, err := Open(dir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = db.Close()
				}
			}()
			ops, ids := xtxInsertOps(t, db, "a", "b")

			commitDone := make(chan error, 1)
			go func() { _, err := db.CommitXTx("race", ops); commitDone <- err }()
			recv(t, "commit to reach hook", hit)

			dropDone := make(chan error, 1)
			go func() { dropDone <- db.DropCollection("a") }()

			var dropErr, commitErr error
			if tc.dropFirst {
				dropErr = recv(t, "drop", dropDone)
				close(release)
				commitErr = recv(t, "commit", commitDone)
			} else {
				select {
				case err := <-dropDone:
					close(release)
					t.Fatalf("drop completed (%v) while the commit held DB.mu", err)
				case <-time.After(50 * time.Millisecond):
				}
				close(release)
				commitErr = recv(t, "commit", commitDone)
				dropErr = recv(t, "drop", dropDone)
			}

			if tc.dropFirst {
				if dropErr != nil {
					t.Fatalf("drop = %v, want success", dropErr)
				}
				if !errors.Is(commitErr, ErrCollectionNotFound) {
					t.Fatalf("commit = %v, want ErrCollectionNotFound", commitErr)
				}
				if got := db.xtxJournal.retainedFor("a"); len(got) != 0 {
					t.Fatalf("journal evidence names dropped a: %v", got)
				}
				if st, _ := db.XTxStatus("race"); st == XTxCommitted {
					t.Fatalf("failed commit reported committed")
				}
			} else {
				if commitErr != nil {
					t.Fatalf("commit = %v", commitErr)
				}
				if !errors.Is(dropErr, ErrParticipantRetained) {
					t.Fatalf("drop = %v, want ErrParticipantRetained", dropErr)
				}
				before := snapshotTree(t, dir)
				if err := db.DropCollection("a"); !errors.Is(err, ErrParticipantRetained) {
					t.Fatalf("second drop = %v", err)
				}
				sameTree(t, "refused drop", before, snapshotTree(t, dir))
			}

			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			db, err = Open(dir, defaultConfig())
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer db.Close()
			if tc.dropFirst {
				if n := docCount(t, db, "a"); n != -1 {
					t.Fatalf("a present after drop (%d docs)", n)
				}
				if n := docCount(t, db, "b"); n != 0 {
					t.Fatalf("b has %d docs from a failed commit", n)
				}
				return
			}
			for _, n := range []string{"a", "b"} {
				c, err := db.Collection(n)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.Get(ids[n]); err != nil {
					t.Errorf("%s: committed record missing after reopen: %v", n, err)
				}
			}
		})
	}
}

func snapshotBytes(t *testing.T, db *DB) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	return buf.Bytes()
}

func restoreSnapshot(t *testing.T, data []byte) *DB {
	t.Helper()
	dst := t.TempDir()
	extractTarGz(t, data, dst)
	db, err := Open(dst, defaultConfig())
	if err != nil {
		t.Fatalf("open restored snapshot: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSnapshotVsDrop_SerializeWithInFlightCommit(t *testing.T) {
	hit := make(chan struct{})
	release := make(chan struct{})
	cfg := defaultConfig()
	cfg.xtxHook = func(p xtxHookPoint, key, txid string) {
		if key == "race" && p == xtxHookPrepared {
			close(hit)
			<-release
		}
	}
	db := openTestDB(t, cfg)
	ops, ids := xtxInsertOps(t, db, "a", "b")

	commitDone := make(chan error, 1)
	go func() { _, err := db.CommitXTx("race", ops); commitDone <- err }()
	recv(t, "commit to reach hook", hit)

	snapDone := make(chan []byte, 1)
	snapErr := make(chan error, 1)
	go func() {
		var buf bytes.Buffer
		err := db.SnapshotTo(&buf)
		snapErr <- err
		snapDone <- buf.Bytes()
	}()
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.DropCollection("a") }()

	close(release)
	if err := recv(t, "commit", commitDone); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, "snapshot", snapErr); err != nil {
		t.Fatal(err)
	}
	data := recv(t, "snapshot data", snapDone)
	if err := recv(t, "drop", dropDone); !errors.Is(err, ErrParticipantRetained) {
		t.Fatalf("drop = %v, want ErrParticipantRetained", err)
	}

	restored := restoreSnapshot(t, data)
	for _, n := range []string{"a", "b"} {
		c, err := restored.Collection(n)
		if err != nil {
			t.Fatalf("restored %s: %v", n, err)
		}
		if _, err := c.Get(ids[n]); err != nil {
			t.Errorf("restored %s lost the committed record: %v", n, err)
		}
	}
}

func TestSnapshot_BeforeAndAfterRefusedDrop(t *testing.T) {
	db := openTestDB(t, defaultConfig())
	ops, ids := xtxInsertOps(t, db, "a", "b")
	if _, err := db.CommitXTx("k1", ops); err != nil {
		t.Fatal(err)
	}
	before := snapshotBytes(t, db)
	if err := db.DropCollection("a"); !errors.Is(err, ErrParticipantRetained) {
		t.Fatalf("drop = %v", err)
	}
	after := snapshotBytes(t, db)

	for name, data := range map[string][]byte{"before": before, "after": after} {
		r := restoreSnapshot(t, data)
		for _, n := range []string{"a", "b"} {
			c, err := r.Collection(n)
			if err != nil {
				t.Fatalf("%s: %s: %v", name, n, err)
			}
			if _, err := c.Get(ids[n]); err != nil {
				t.Errorf("%s: %s record: %v", name, n, err)
			}
		}
		// Retained evidence travels with the snapshot.
		if err := r.DropCollection("a"); !errors.Is(err, ErrParticipantRetained) {
			t.Errorf("%s: restored drop = %v, want ErrParticipantRetained", name, err)
		}
	}
}

func TestSnapshot_UnreferencedDropBeforeAfter(t *testing.T) {
	db := openTestDB(t, defaultConfig())
	ops, ids := xtxInsertOps(t, db, "a", "b")
	if _, err := db.CommitXTx("k1", ops); err != nil {
		t.Fatal(err)
	}
	u, err := db.CreateCollection("u")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := u.Insert(map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	before := snapshotBytes(t, db)
	if err := db.DropCollection("u"); err != nil {
		t.Fatal(err)
	}
	after := snapshotBytes(t, db)

	rb := restoreSnapshot(t, before)
	if n := docCount(t, rb, "u"); n != 5 {
		t.Errorf("before-drop snapshot: u has %d docs, want 5", n)
	}
	ra := restoreSnapshot(t, after)
	if n := docCount(t, ra, "u"); n != -1 {
		t.Errorf("after-drop snapshot: u present with %d docs", n)
	}
	for name, r := range map[string]*DB{"before": rb, "after": ra} {
		for _, n := range []string{"a", "b"} {
			c, err := r.Collection(n)
			if err != nil {
				t.Fatalf("%s: %s: %v", name, n, err)
			}
			if _, err := c.Get(ids[n]); err != nil {
				t.Errorf("%s: %s: %v", name, n, err)
			}
		}
	}
}

func TestDropRetained_EvidencePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ops, ids := xtxInsertOps(t, db, "a", "b")
	res, err := db.CommitXTx("k1", ops)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("a"); !errors.Is(err, ErrParticipantRetained) {
		t.Fatalf("drop = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 2; round++ {
		db, err = Open(dir, defaultConfig())
		if err != nil {
			t.Fatalf("round %d: reopen: %v", round, err)
		}
		for _, n := range []string{"a", "b"} {
			c, err := db.Collection(n)
			if err != nil {
				t.Fatalf("round %d: %s: %v", round, n, err)
			}
			if _, err := c.Get(ids[n]); err != nil {
				t.Errorf("round %d: %s record: %v", round, n, err)
			}
		}
		before := snapshotTree(t, dir)
		err = db.DropCollection("a")
		var pr *ParticipantRetainedError
		if !errors.As(err, &pr) || len(pr.TxIDs) != 1 || pr.TxIDs[0] != res.TxID {
			t.Fatalf("round %d: drop after reopen = %v, want ErrParticipantRetained(%s)", round, err, res.TxID)
		}
		sameTree(t, "refused drop after reopen", before, snapshotTree(t, dir))
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// An aborted transaction that had really staged participant runs (S1 done, S2
// fsync failed) must not block dropping a participant.
func TestDropCollection_AbortedWithStagedParticipantsDoesNotBlock(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	cfg := defaultConfig()
	cfg.wrapFile = fs.wrap
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ops, _ := xtxInsertOps(t, db, "a", "b")

	// Create the journal first so the armed fault lands on the participant
	// fsync (S2), after both runs were appended (S1).
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}
	appendsBefore := fs.count("write")
	fs.failSyncAt(fs.count("sync")+1, errors.New("injected sync failure"))
	_, err = db.CommitXTx("k1", ops)
	if !errors.Is(err, ErrXTxDurability) {
		t.Fatalf("commit = %v, want ErrXTxDurability", err)
	}
	if fs.count("write") == appendsBefore {
		t.Fatal("no participant run was staged before the abort")
	}
	if st, _ := db.XTxStatus("k1"); st == XTxCommitted {
		t.Fatalf("aborted tx reported committed")
	}
	if got := db.xtxJournal.retainedFor("a"); len(got) != 0 {
		t.Fatalf("retainedFor(a) = %v after abort", got)
	}
	if err := db.DropCollection("a"); err != nil {
		t.Fatalf("drop of aborted-tx participant: %v", err)
	}
	if _, err := db.Collection("a"); err == nil {
		t.Fatal("a still present")
	}
}

// Crash during drops of unreferenced collections: after SIGKILL each such
// collection is fully present (opens, all docs) or fully absent, the root
// opens, and the retained XTx participants are untouched.
func TestKill9_DropUnreferencedCollection(t *testing.T) {
	const dropDocs = 8
	if os.Getenv(envKillChild) == "1" && os.Getenv(envKillMode) == "drop" {
		dir := os.Getenv(envKillDir)
		db, err := Open(dir, killChildCfg("writer"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "child open:", err)
			os.Exit(1)
		}
		if _, err := db.Collection("keep_a"); err != nil {
			ops, _ := xtxInsertOps(t, db, "keep_a", "keep_b")
			if _, err := db.CommitXTx("seed", ops); err != nil {
				fmt.Fprintln(os.Stderr, "child seed:", err)
				os.Exit(1)
			}
		}
		os.Stdout.WriteString("READY\n")
		for n := 0; ; n++ {
			name := fmt.Sprintf("d%d_%d", os.Getpid(), n)
			c, err := db.CreateCollection(name)
			if err != nil {
				fmt.Fprintln(os.Stderr, "child create:", err)
				os.Exit(1)
			}
			for i := 0; i < dropDocs; i++ {
				if _, _, err := c.Insert(map[string]any{"i": i}); err != nil {
					fmt.Fprintln(os.Stderr, "child insert:", err)
					os.Exit(1)
				}
			}
			fmt.Printf("S %s\n", name)
			if err := db.DropCollection(name); err != nil {
				fmt.Fprintln(os.Stderr, "child drop:", err)
				os.Exit(1)
			}
			fmt.Printf("A %s\n", name)
		}
	}

	dir := t.TempDir()
	for _, seed := range []int64{1, 2, 3, 4, 5, 6} {
		rng := rand.New(rand.NewSource(seed))
		cmd, lines := startKillChild(t, dir, "drop", seed)
		started := map[string]bool{}
		acked := map[string]bool{}
		target := 1 + rng.Intn(25)
		jitter := time.Duration(rng.Intn(1500)) * time.Microsecond
		timeout := time.After(30 * time.Second)
		for len(acked) < target {
			select {
			case l, ok := <-lines:
				if !ok {
					t.Fatalf("seed %d: child exited early", seed)
				}
				if strings.HasPrefix(l, "S ") {
					started[l[2:]] = true
				} else if strings.HasPrefix(l, "A ") {
					acked[l[2:]] = true
				}
			case <-timeout:
				_ = cmd.Process.Kill()
				t.Fatalf("seed %d: timed out", seed)
			}
		}
		time.Sleep(jitter)
		sigkill(t, cmd)
		for l := range lines {
			if strings.HasPrefix(l, "S ") {
				started[l[2:]] = true
			} else if strings.HasPrefix(l, "A ") {
				acked[l[2:]] = true
			}
		}

		db, err := Open(dir, killChildCfg("writer"))
		if err != nil {
			t.Fatalf("seed %d: reopen after kill: %v", seed, err)
		}
		for name := range started {
			n := docCount(t, db, name)
			switch {
			case acked[name] && n != -1:
				t.Errorf("seed %d: acked drop of %s but it is present (%d docs)", seed, name, n)
			case n != -1 && n != dropDocs:
				t.Errorf("seed %d: %s partially dropped: %d of %d docs", seed, name, n, dropDocs)
			}
		}
		for _, n := range []string{"keep_a", "keep_b"} {
			if docCount(t, db, n) != 1 {
				t.Errorf("seed %d: retained participant %s damaged", seed, n)
			}
		}
		if err := db.DropCollection("keep_a"); !errors.Is(err, ErrParticipantRetained) {
			t.Errorf("seed %d: drop(keep_a) = %v", seed, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Faultfs crash image: a drop is atomic with respect to what the engine
// itself can observe on reopen when the process dies right after the
// directory is removed or before anything happens (refusal).
func TestDropCollection_CrashImageAroundDrop(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ops, _ := xtxInsertOps(t, db, "a", "b")
	if _, err := db.CommitXTx("k1", ops); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("u"); err != nil {
		t.Fatal(err)
	}
	// Crash image before the drop (copy of the live dir), then after.
	pre := copyDirImage(t, dir)
	if err := db.DropCollection("u"); err != nil {
		t.Fatal(err)
	}
	post := copyDirImage(t, dir)
	_ = db.Close()
	for name, img := range map[string]string{"pre": pre, "post": post} {
		r, err := Open(img, defaultConfig())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_, uerr := r.Collection("u")
		if (name == "pre") != (uerr == nil) {
			t.Errorf("%s: u presence wrong (err=%v)", name, uerr)
		}
		if docCount(t, r, "a") != 1 || docCount(t, r, "b") != 1 {
			t.Errorf("%s: participants damaged", name)
		}
		_ = r.Close()
	}
}

func copyDirImage(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(dst + "/LOCK")
	return dst
}

// Commit, drop, snapshot and compaction hammer the same DB concurrently. The
// goroutine mix is seeded; the assertion is liveness (deadlock guard), no
// panics/data races (-race), and a consistent reopen.
func TestLockOrder_CommitDropSnapshotCompactConcurrent(t *testing.T) {
	const seed = 20240601
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.SegmentMaxSize = 4 << 10
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"s1", "s2"} {
		if _, err := db.CreateCollection(n); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var commits int
	var mu sync.Mutex
	errc := make(chan error, 16)
	fail := func(err error) {
		select {
		case errc <- err:
		default:
		}
	}
	worker := func(id int64, body func(rng *rand.Rand, i int) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + id))
			for i := 0; i < 40; i++ {
				if err := body(rng, i); err != nil {
					fail(err)
					return
				}
			}
		}()
	}

	for w := int64(0); w < 2; w++ {
		worker(w, func(rng *rand.Rand, i int) error {
			s1, _ := db.Collection("s1")
			s2, _ := db.Collection("s2")
			if s1 == nil || s2 == nil {
				return errors.New("participant vanished")
			}
			ops := []XTxOp{
				{Collection: "s1", Op: store.OpInsert, ID: s1.ReserveID(), Data: map[string]any{"v": i}},
				{Collection: "s2", Op: store.OpInsert, ID: s2.ReserveID(), Data: map[string]any{"v": i}},
			}
			if _, err := db.CommitXTx(fmt.Sprintf("w%d-%d", w, i), ops); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			mu.Lock()
			commits++
			mu.Unlock()
			return nil
		})
	}
	worker(10, func(rng *rand.Rand, i int) error {
		name := fmt.Sprintf("tmp%d", i)
		c, err := db.CreateCollection(name)
		if err != nil {
			return err
		}
		_, _, _ = c.Insert(map[string]any{"i": i})
		if err := db.DropCollection(name); err != nil {
			return fmt.Errorf("drop unreferenced: %w", err)
		}
		// Referenced participant: refused once any commit landed, otherwise it
		// would drop the participant under the committers; only attempt it
		// when we know the outcome is a refusal.
		mu.Lock()
		have := commits > 0
		mu.Unlock()
		if have {
			if err := db.DropCollection("s1"); !errors.Is(err, ErrParticipantRetained) {
				return fmt.Errorf("drop(s1) = %w, want ErrParticipantRetained", err)
			}
		}
		return nil
	})
	worker(20, func(rng *rand.Rand, i int) error {
		var buf bytes.Buffer
		if err := db.SnapshotTo(&buf); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		if rng.Intn(8) == 0 {
			dst := t.TempDir()
			extractTarGz(t, buf.Bytes(), dst)
			r, err := Open(dst, defaultConfig())
			if err != nil {
				return fmt.Errorf("open snapshot: %w", err)
			}
			a, b := docCount(t, r, "s1"), docCount(t, r, "s2")
			_ = r.Close()
			if a != b {
				return fmt.Errorf("snapshot straddled a commit: s1=%d s2=%d", a, b)
			}
		}
		return nil
	})
	worker(30, func(rng *rand.Rand, i int) error {
		if err := db.Compact([]string{"s1", "s2"}[rng.Intn(2)]); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		return nil
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	recv(t, "concurrent workload", done)
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, defaultConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	a, b := docCount(t, db, "s1"), docCount(t, db, "s2")
	if a != b || a != commits {
		t.Fatalf("after reopen s1=%d s2=%d, want %d each", a, b, commits)
	}
}
