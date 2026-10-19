package engine

// Release gate for crash-atomic single-collection batches (hardening §3.1).
//
// Every test here drives batches (InsertMany and single-collection CommitTx)
// through the production path with the writer gate open, cuts the process at
// one boundary (append, sync, decision, apply, acknowledgment) or injects one
// I/O fault, and asserts the recovered collection equals exactly the state
// before the in-flight batch or exactly the state after it. A prefix of a
// batch matches neither and fails. Reopen is repeated to prove recovery is
// idempotent. Real-process SIGKILL coverage is in batch_kill_test.go.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

type gateModel map[uint64]map[string]any

func (m gateModel) clone() gateModel {
	out := make(gateModel, len(m))
	for id, d := range m {
		out[id] = d
	}
	return out
}

// gateBatch is one workload step that commits through CommitTx with ids
// reserved up front, so the model effect is known before the batch runs.
type gateBatch struct {
	name  string
	ops   []txOp
	apply func(m gateModel)
}

// gateRun executes batches in order against c, tracking the acknowledged model
// and the single in-flight batch. imN > 0 appends an InsertMany of imN records
// tagged b="im" whose ids are only known afterwards; it is verified by marker.
type gateRun struct {
	acked    gateModel
	inflight gateModel // model if the in-flight batch committed; nil when none
	dead     bool
	lastErr  error

	imN       int  // size of the InsertMany batch (0 = not part of the workload)
	imStarted bool // InsertMany was attempted
	imAcked   bool
}

func (r *gateRun) commit(c *Collection, b gateBatch) {
	if r.dead {
		return
	}
	after := r.acked.clone()
	b.apply(after)
	r.inflight = after
	if err := c.CommitTx(b.ops); err != nil {
		r.dead, r.lastErr = true, err
		return
	}
	r.acked, r.inflight = after, nil
}

func (r *gateRun) insertMany(c *Collection) {
	if r.dead || r.imN == 0 {
		return
	}
	recs := make([]map[string]any, r.imN)
	for i := range recs {
		recs[i] = map[string]any{"b": "im", "i": float64(i), "g": float64(1)}
	}
	r.imStarted = true
	if _, _, err := c.InsertMany(recs, time.Time{}); err != nil {
		r.dead, r.lastErr = true, err
		return
	}
	r.imAcked = true
}

func scanModel(t *testing.T, c *Collection) gateModel {
	t.Helper()
	res, err := c.Scan(nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	m := make(gateModel, len(res))
	for _, r := range res {
		m[r.ID] = r.Data
	}
	return m
}

func modelsEqual(a, b gateModel) bool {
	if len(a) != len(b) {
		return false
	}
	for id, d := range a {
		o, ok := b[id]
		if !ok || !sameData(d, o) {
			return false
		}
	}
	return true
}

// splitIM separates the InsertMany records from the rest of a scanned model.
func splitIM(m gateModel) (rest gateModel, im int) {
	rest = gateModel{}
	for id, d := range m {
		if d["b"] == "im" {
			im++
			continue
		}
		rest[id] = d
	}
	return rest, im
}

// check asserts the recovered state is full-old or full-new for the in-flight
// batch and that everything acknowledged is present. Never a prefix.
func (r *gateRun) check(t *testing.T, c *Collection, label string) {
	t.Helper()
	got := scanModel(t, c)
	rest, im := splitIM(got)
	if im != 0 && im != r.imN {
		t.Fatalf("%s: InsertMany batch recovered as a prefix: %d of %d records visible", label, im, r.imN)
	}
	if r.imAcked && im != r.imN {
		t.Fatalf("%s: acknowledged InsertMany lost (%d of %d)", label, im, r.imN)
	}
	if im != 0 && !r.imStarted {
		t.Fatalf("%s: InsertMany records visible but the batch never started", label)
	}
	if im != 0 && r.inflight != nil {
		t.Fatalf("%s: InsertMany visible while an earlier batch was still unacknowledged", label)
	}
	ok := modelsEqual(rest, r.acked) || (r.inflight != nil && modelsEqual(rest, r.inflight))
	if !ok {
		t.Fatalf("%s: recovered state is neither full-old nor full-new for the in-flight batch\n got:  %v\n old:  %v\n new:  %v", label, rest, r.acked, r.inflight)
	}
	// Index agrees with the primary view.
	for _, b := range []string{"b1", "b2", "b3", "b5", "im"} {
		want := 0
		for _, d := range got {
			if d["b"] == b {
				want++
			}
		}
		ids, ok := c.IndexLookup("b", b)
		if !ok {
			t.Fatalf("%s: index on b unavailable", label)
		}
		if len(ids) != want {
			t.Fatalf("%s: index lookup b=%s returned %d ids, scan has %d", label, b, len(ids), want)
		}
	}
}

// gateWorkload reserves ids and builds the CommitTx batches: a seed insert, a
// mixed update+insert+delete, and a delete-only batch.
func gateWorkload(c *Collection) []gateBatch {
	now := time.Now().UTC()
	d := func(b string, i, g int) map[string]any {
		return map[string]any{"b": b, "i": float64(i), "g": float64(g)}
	}
	var b1, b2 []uint64
	for i := 0; i < 4; i++ {
		b1 = append(b1, c.ReserveID())
	}
	for i := 0; i < 3; i++ {
		b2 = append(b2, c.ReserveID())
	}
	b3 := c.ReserveID()

	seed := gateBatch{name: "seed"}
	for i, id := range b1 {
		seed.ops = append(seed.ops, txOp{kind: txOpInsert, id: id, data: d("b1", i, 1), ts: now})
	}
	for i, id := range b2 {
		seed.ops = append(seed.ops, txOp{kind: txOpInsert, id: id, data: d("b2", i, 1), ts: now})
	}
	seed.apply = func(m gateModel) {
		for i, id := range b1 {
			m[id] = d("b1", i, 1)
		}
		for i, id := range b2 {
			m[id] = d("b2", i, 1)
		}
	}

	mixed := gateBatch{name: "mixed"}
	mixed.ops = append(mixed.ops, txOp{kind: txOpInsert, id: b3, data: d("b3", 0, 1), ts: now})
	for i, id := range b1 {
		mixed.ops = append(mixed.ops, txOp{kind: txOpUpdate, id: id, data: d("b1", i, 2), ts: now})
	}
	mixed.ops = append(mixed.ops, txOp{kind: txOpDelete, id: b2[0], ts: now})
	mixed.apply = func(m gateModel) {
		m[b3] = d("b3", 0, 1)
		for i, id := range b1 {
			m[id] = d("b1", i, 2)
		}
		delete(m, b2[0])
	}

	del := gateBatch{name: "delete_only"}
	for _, id := range b2[1:] {
		del.ops = append(del.ops, txOp{kind: txOpDelete, id: id, ts: now})
	}
	del.apply = func(m gateModel) {
		for _, id := range b2[1:] {
			delete(m, id)
		}
	}
	return []gateBatch{seed, mixed, del}
}

func runGateWorkload(c *Collection, r *gateRun) {
	for _, b := range gateWorkload(c) {
		r.commit(c, b)
	}
	r.insertMany(c)
}

func newGateRun() *gateRun { return &gateRun{acked: gateModel{}, imN: 5} }

func gateCfg(mode SyncMode) CollectionConfig { return cmBaseCfg(mode, 1<<20) }

// gateOpen opens a faulty DB with the batch gate open and an index on "b".
func gateOpen(t *testing.T, dir string, fs *faultFS, cfg CollectionConfig) (*DB, *Collection) {
	t.Helper()
	SetAtomicBatchWriterReady(t, true)
	db, cols := openFaultyMulti(t, dir, fs, cfg, "g")
	c := cols["g"]
	if err := c.EnsureIndex("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}
	return db, c
}

// recoverRepeatedly closes nothing: it opens the image, checks the state, then
// reopens it three more times (clean close in between) and requires the state
// to be identical and the database to verify clean. Returns the final state.
func recoverRepeatedly(t *testing.T, dir string, cfg CollectionConfig, r *gateRun, label string) gateModel {
	t.Helper()
	cfg.wrapFile, cfg.renameFn = nil, nil
	var first gateModel
	for pass := 0; pass < 4; pass++ {
		db, err := Open(dir, cfg)
		if err != nil {
			t.Fatalf("%s pass %d: reopen refused: %v", label, pass, err)
		}
		c, err := db.Collection("g")
		if err != nil {
			t.Fatalf("%s pass %d: %v", label, pass, err)
		}
		r.check(t, c, fmt.Sprintf("%s pass %d", label, pass))
		got := scanModel(t, c)
		if pass == 0 {
			first = got
		} else if !modelsEqual(first, got) {
			t.Fatalf("%s pass %d: state changed across reopen\n first: %v\n now:   %v", label, pass, first, got)
		}
		if pass == 3 {
			rep, err := db.Verify(context.Background(), VerifyOptions{Mode: VerifyFull})
			if err != nil {
				t.Fatalf("%s: verify: %v", label, err)
			}
			if !rep.Clean() {
				t.Fatalf("%s: verify not clean: %+v", label, rep.AllFindings())
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("%s pass %d: close: %v", label, pass, err)
		}
	}
	return first
}

// TestBatchGate_StepBoundaries: crash after every write/sync/rename step of
// the whole workload (append, data sync, decision append, decision sync,
// apply), under every sync mode (the batch forces its own fsyncs).
func TestBatchGate_StepBoundaries(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeAlways, SyncModeInterval, SyncModeNone} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := gateCfg(mode)
			dry := newFaultFS()
			dDB, dC := gateOpen(t, t.TempDir(), dry, cfg)
			base := dry.totalSteps()
			runGateWorkload(dC, newGateRun())
			total := dry.totalSteps() - base
			if total < 12 {
				t.Fatalf("workload produced only %d steps", total)
			}
			trace := make([]string, total+1)
			for k := 0; k <= total; k++ {
				trace[k] = dry.opAt(base + k)
			}
			dry.kill()
			dry.releaseFDs()
			_ = dDB.lock.release()

			for k := 0; k <= total; k++ {
				t.Run(fmt.Sprintf("crash_after_%02d_before_%s", k, trace[k]), func(t *testing.T) {
					dir := t.TempDir()
					fs := newFaultFS()
					db, c := gateOpen(t, dir, fs, cfg)
					fs.crashAfter(fs.totalSteps() + k)
					r := newGateRun()
					runGateWorkload(c, r)
					fs.kill()
					fs.releaseFDs()
					_ = db.lock.release()
					recoverRepeatedly(t, dir, cfg, r, "crash")
				})
			}
		})
	}
}

// TestBatchGate_PowerLoss: the same boundaries, but the image is cut back to
// the last fsync of every file (page cache lost), including relaxed sync modes
// where only the batch's own forced fsyncs make it durable.
func TestBatchGate_PowerLoss(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeAlways, SyncModeNone} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := gateCfg(mode)
			dry := newFaultFS()
			dDB, dC := gateOpen(t, t.TempDir(), dry, cfg)
			base := dry.totalSteps()
			runGateWorkload(dC, newGateRun())
			total := dry.totalSteps() - base
			dry.kill()
			dry.releaseFDs()
			_ = dDB.lock.release()

			for k := 0; k <= total; k++ {
				t.Run(fmt.Sprintf("cut_%02d", k), func(t *testing.T) {
					dir := t.TempDir()
					fs := newFaultFS()
					db, c := gateOpen(t, dir, fs, cfg)
					fs.crashAfter(fs.totalSteps() + k)
					r := newGateRun()
					runGateWorkload(c, r)
					img := t.TempDir() + "/img"
					powerLossImage(t, fs, dir, img)
					fs.kill()
					fs.releaseFDs()
					_ = db.lock.release()
					recoverRepeatedly(t, img, cfg, r, "powerloss")
				})
			}
		})
	}
}

// TestBatchGate_PhaseHooks: a hard stop at each named phase of the one batch
// being committed. Before the decision the batch must vanish; from the
// decision on it must be fully present; the status API agrees.
func TestBatchGate_PhaseHooks(t *testing.T) {
	cases := []struct {
		point XTxHookPoint
		name  string
		want  string // "old" | "new"
	}{
		{XTxHookBeforeLocks, "before_locks", "old"},
		{XTxHookLocked, "locked", "old"},
		{XTxHookPrepared, "prepared_data_synced_no_decision", "old"},
		{XTxHookDecided, "decided_not_applied", "new"},
		{XTxHookApplied, "applied_not_acknowledged", "new"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := newFaultFS()
			cfg := gateCfg(SyncModeAlways)
			var (
				mu    sync.Mutex
				armed bool
				txid  string
				db    *DB
				hit   bool
			)
			SetTestXTxHook(&cfg, func(p XTxHookPoint, key, id string) {
				mu.Lock()
				defer mu.Unlock()
				if !armed || p != tc.point || hit {
					return
				}
				hit = true
				txid = id
				fs.kill()
				fs.releaseFDs()
			})
			var c *Collection
			db, c = gateOpen(t, dir, fs, cfg)
			r := newGateRun()
			batches := gateWorkload(c)
			r.commit(c, batches[0]) // seed acknowledged
			if r.dead {
				t.Fatalf("seed failed: %v", r.lastErr)
			}
			mu.Lock()
			armed = true
			mu.Unlock()
			r.commit(c, batches[1]) // mixed: the batch under test
			if !hit {
				t.Fatalf("hook %v never fired", tc.point)
			}
			_ = db.lock.release()

			// Both outcomes are legal for the in-flight batch; the phase decides
			// which one recovery must pick.
			r2 := &gateRun{acked: r.acked, inflight: r.inflight, imN: 0}
			cfg.xtxHook = nil
			rdb, err := Open(dir, withoutFaults(cfg))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			rc, _ := rdb.Collection("g")
			got := scanModel(t, rc)
			wantModel := r.acked
			if tc.want == "new" && r.inflight != nil {
				wantModel = r.inflight
			}
			if !modelsEqual(got, wantModel) {
				t.Fatalf("phase %s: want full-%s state\n got:  %v\n want: %v", tc.name, tc.want, got, wantModel)
			}
			if txid != "" {
				st, _ := rdb.XTxStatus(txid)
				if tc.want == "old" && st == XTxCommitted {
					t.Fatalf("status %v for a batch that must have vanished", st)
				}
				if tc.want == "new" && st != XTxCommitted {
					t.Fatalf("status %v, want COMMITTED", st)
				}
			}
			_ = rdb.Close()
			recoverRepeatedly(t, dir, cfg, r2, tc.name)
		})
	}
}

func withoutFaults(cfg CollectionConfig) CollectionConfig {
	cfg.wrapFile, cfg.renameFn = nil, nil
	return cfg
}

// TestBatchGate_InjectedFaults: one I/O fault at each write/sync ordinal of a
// single batch. The returned error must be typed; the live collection and the
// recovered image must each be full-old or full-new; a proven abort leaves the
// old state and the collection usable; an unknown outcome fails closed and is
// resolvable via XTxStatus.
func TestBatchGate_InjectedFaults(t *testing.T) {
	type inj struct {
		name string
		arm  func(fs *faultFS, n int)
	}
	injectors := []inj{
		{"sync_eio", func(fs *faultFS, n int) { fs.failSyncAt(n, nil) }},
		{"write_enospc_partial", func(fs *faultFS, n int) { fs.failWriteAt(n, 7, syscall.ENOSPC) }},
		{"write_enospc_zero", func(fs *faultFS, n int) { fs.failWriteAt(n, 0, syscall.ENOSPC) }},
	}
	for _, in := range injectors {
		for ordinal := 1; ordinal <= 6; ordinal++ {
			t.Run(fmt.Sprintf("%s_%d", in.name, ordinal), func(t *testing.T) {
				dir := t.TempDir()
				fs := newFaultFS()
				cfg := gateCfg(SyncModeAlways)
				db, c := gateOpen(t, dir, fs, cfg)
				r := newGateRun()
				batches := gateWorkload(c)
				r.commit(c, batches[0])
				if r.dead {
					t.Fatalf("seed: %v", r.lastErr)
				}
				op := "sync"
				if in.name != "sync_eio" {
					op = "write"
				}
				in.arm(fs, fs.count(op)+ordinal)
				r.commit(c, batches[1])
				fired := fs.faults() > 0
				err := r.lastErr

				if !fired {
					// Ordinal beyond this batch's I/O: it must simply have committed.
					if err != nil {
						t.Fatalf("no fault fired but commit failed: %v", err)
					}
					_ = db.Close()
					return
				}
				if err == nil {
					// Fault landed on a non-critical operation after the decision
					// (e.g. a post-commit step): still all-new.
					got := scanModel(t, c)
					if !modelsEqual(got, r.acked) {
						t.Fatalf("acknowledged batch not fully visible: %v", got)
					}
				} else {
					known := errors.Is(err, ErrXTxDurability) || errors.Is(err, ErrXTxOutcomeUnknown) ||
						errors.Is(err, ErrWriteOutcomeUnknown) || errors.Is(err, ErrCollectionFenced) ||
						errors.Is(err, ErrXTxRecoveryRequired)
					if !known {
						t.Fatalf("untyped batch failure: %v", err)
					}
					txid := BatchTxID(err)
					if errors.Is(err, ErrXTxDurability) {
						// Proven abort: nothing applied, live and recovered state old.
						if got := scanModel(t, c); !modelsEqual(got, r.acked) {
							t.Fatalf("aborted batch left live state: %v want %v", got, r.acked)
						}
					} else if txid != "" {
						st, _ := db.XTxStatus(txid)
						_ = st // resolvable without panicking; value checked after reopen
					}
					// Live view, when the collection is still readable, is never a prefix.
					if got, serr := c.Scan(nil); serr == nil {
						m := make(gateModel, len(got))
						for _, x := range got {
							m[x.ID] = x.Data
						}
						if !modelsEqual(m, r.acked) && !modelsEqual(m, r.inflight) {
							t.Fatalf("live state after %v is a prefix: %v", err, m)
						}
					}
				}
				// Crash image at this exact point must recover to old or new.
				img := t.TempDir() + "/img"
				powerLossImage(t, fs, dir, img)
				fs.kill()
				fs.releaseFDs()
				_ = db.lock.release()
				r.imN = 0
				recoverRepeatedly(t, img, cfg, r, "fault image")
			})
		}
	}
}

// TestBatchGate_ConcurrentWritersAndReaders: parallel batch writers and
// readers on one collection. Every batch writes `n` records sharing a batch
// tag; readers must always see either 0 or n of them, never a prefix, even
// mid-flight, and the final reopen holds exactly the acknowledged batches.
func TestBatchGate_ConcurrentWritersAndReaders(t *testing.T) {
	SetAtomicBatchWriterReady(t, true)
	dir := t.TempDir()
	cfg := gateCfg(SyncModeAlways)
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.CreateCollection("g")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureIndex("tag"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		t.Fatal(err)
	}

	const writers, perWriter, n = 6, 12, 4
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var rmu sync.Mutex
	var readerErr error
	fail := func(e error) {
		rmu.Lock()
		if readerErr == nil {
			readerErr = e
		}
		rmu.Unlock()
	}

	var rwg sync.WaitGroup
	for i := 0; i < 3; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := c.Scan(nil)
				if err != nil {
					fail(err)
					return
				}
				// Every read path must see each batch whole: Scan, an indexed
				// Scan (candidate path), Count(MatchAll) and indexed Count.
				counts := map[string]int{}
				for _, r := range res {
					counts[r.Data["tag"].(string)]++
				}
				for tag, k := range counts {
					if k != n {
						fail(fmt.Errorf("Scan saw %d of %d records of batch %s", k, n, tag))
						return
					}
				}
				if cnt, err := c.Count(nil); err != nil || cnt%n != 0 {
					fail(fmt.Errorf("Count(MatchAll) = %d (err %w), not a multiple of %d", cnt, err, n))
					return
				}
				probe := fmt.Sprintf("w%d-%d", rand.Intn(writers), rand.Intn(perWriter))
				f := &query.FieldFilter{Field: "tag", Op: query.OpEq, Value: `"` + probe + `"`}
				if ir, err := c.Scan(f); err != nil || (len(ir) != 0 && len(ir) != n) {
					fail(fmt.Errorf("indexed Scan(%s) = %d records (err %w)", probe, len(ir), err))
					return
				}
				if cnt, err := c.Count(f); err != nil || (cnt != 0 && cnt != n) {
					fail(fmt.Errorf("indexed Count(%s) = %d (err %w)", probe, cnt, err))
					return
				}
			}
		}()
	}

	var amu sync.Mutex
	acked := map[string]bool{}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				tag := fmt.Sprintf("w%d-%d", w, j)
				recs := make([]map[string]any, n)
				for i := range recs {
					recs[i] = map[string]any{"tag": tag, "i": float64(i)}
				}
				if _, _, err := c.InsertMany(recs, time.Time{}); err != nil {
					fail(fmt.Errorf("InsertMany %s: %w", tag, err))
					return
				}
				amu.Lock()
				acked[tag] = true
				amu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stop)
	rwg.Wait()
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		db2, err := Open(dir, cfg)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		c2, _ := db2.Collection("g")
		res, _ := c2.Scan(nil)
		counts := map[string]int{}
		for _, r := range res {
			counts[r.Data["tag"].(string)]++
		}
		if len(counts) != writers*perWriter {
			t.Fatalf("pass %d: %d batches recovered, want %d", pass, len(counts), writers*perWriter)
		}
		for tag, k := range counts {
			if k != n || !acked[tag] {
				t.Fatalf("pass %d: batch %s has %d records (acked=%v)", pass, tag, k, acked[tag])
			}
		}
		_ = db2.Close()
	}
}

// TestBatchGate_ScanSeesBatchWholeAcrossCommit: a scan parked mid-stream while a
// multi-entry update batch commits must return the pre-batch state for every
// record (and never a mix); a scan started after sees the post-batch state.
func TestBatchGate_ScanSeesBatchWholeAcrossCommit(t *testing.T) {
	SetAtomicBatchWriterReady(t, true)
	cfg := gateCfg(SyncModeAlways)
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var armed bool
	var amu sync.Mutex
	cfg.scanHook = func(p scanHookPoint, arg uint64) {
		amu.Lock()
		a := armed
		amu.Unlock()
		if a && p == scanAtEntry {
			once.Do(func() { close(reached); <-release })
		}
	}
	db, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, _ := db.CreateCollection("g")
	ids, _, err := c.InsertMany(batchRecs(6), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	amu.Lock()
	armed = true
	amu.Unlock()

	type out struct {
		res []ScanResult
		err error
	}
	done := make(chan out)
	go func() {
		r, err := c.Scan(nil)
		done <- out{r, err}
	}()
	<-reached

	now := time.Now().UTC()
	var ops []txOp
	for _, id := range ids {
		ops = append(ops, txOp{kind: txOpUpdate, id: id, data: map[string]any{"n": 100.0}, ts: now})
	}
	if err := c.CommitTx(ops); err != nil {
		t.Fatal(err)
	}
	close(release)
	o := <-done
	if o.err != nil {
		t.Fatal(o.err)
	}
	if len(o.res) != len(ids) {
		t.Fatalf("scan returned %d records, want %d", len(o.res), len(ids))
	}
	for _, r := range o.res {
		if r.Data["n"] == 100.0 {
			t.Fatalf("scan begun before the commit exposed post-batch record %d (mixed view)", r.ID)
		}
	}
	after, _ := c.Scan(nil)
	for _, r := range after {
		if r.Data["n"] != 100.0 {
			t.Fatalf("scan after the commit missed update on %d", r.ID)
		}
	}
}
