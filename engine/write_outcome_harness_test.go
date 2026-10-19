package engine

// Write-outcome harness (test-only) for the single-record durability contract
// in docs/design-database-integrity-hardening.md §2.3 / §3.2.
//
// HOW FOLLOW-UP TASKS USE THIS
//
//	obs := observeWriteOutcome(t, op, fault, CollectionConfig{SyncMode: SyncModeAlways})
//	assertWriteOutcome(t, op, obs, exp)
//
// observeWriteOutcome opens a faulty DB (openFaulty) with replication enabled
// and a secondary index on "tag", seeds one keyed record ("k1", tag=old, v=1),
// scopes the faultFS to the collection's active segment, arms the fault,
// runs the op, and captures:
//
//   - the returned error and the number of armed faults consumed;
//   - the target record as seen right away through the raw primary index,
//     Get, the "tag" secondary index and FindByKey (a recState);
//   - watch events emitted for the call and the replication LSN delta;
//   - a probe Insert issued after the op (fenced / poisoned / ok, and that the
//     probe id was not reused);
//   - the target after reopening two images of the data dir taken before the
//     probe, each opened twice to prove recovery is idempotent: the bytes as
//     they are (process crash: the page cache survives) and with every wrapped
//     file cut back to its last fsync (power loss: unsynced tail dropped).
//
// Expectations are outcomeExpect values. Baseline tables keep a `today` and a
// `want` (ratified) expectation per case; an op whose path implements the
// contract sets ratified and is asserted against `want`, the rest against
// `today` with the gap logged. ErrCollectionFenced on a read or the probe is
// the label "fenced" (classifyReadErr / classifyProbe).
//
// To cover a new mutation path (TTL reap, key mutation, replication apply,
// ...) add a writeOutcomeOp with its run func and its old/new recState.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// recState is the target record as one handle sees it. Two recStates compare
// equal iff every observation path agrees.
type recState struct {
	Indexed bool   // present in the raw primary index (ignores TTL)
	Rev     uint64 // revision in the primary index (0 when not indexed)
	Visible bool   // Get returned the record
	V       int    // data["v"] when visible
	Tag     string // membership in the "tag" secondary index: old|new|none|both|noindex
	ByKey   int    // FindByKey(op.key) data["v"]; -1 not found; 0 when the op has no key
}

// writeOutcomeOp is one mutation under test. old/new are the target's state
// before the call and after a durable apply.
type writeOutcomeOp struct {
	name     string
	followUp string // TODO tag of the task that ratifies this path
	ratified bool   // the path implements §2.3: assert `want`, not `today`
	key      string // key observed with FindByKey ("" = none)
	expired  bool   // seed the base record already expired (reaper)
	old, new recState
	events   int    // watch events a durable apply emits
	lsn      uint64 // replication LSNs a durable apply publishes
	run      func(e *woEnv) (target uint64, err error)
}

// woEnv is the live handle an op runs against.
type woEnv struct {
	db   *DB
	col  *Collection
	base uint64 // the seeded "k1" record
}

// writeFault arms one deterministic fault on the active segment. arm runs
// before the op (with the faultFS already scoped to the active segment);
// settle, when set, runs after it (background faults such as syncLoop).
type writeFault struct {
	name   string
	faults int // armed faults the scenario must consume
	arm    func(fs *faultFS)
	settle func(t *testing.T, fs *faultFS, col *Collection)
}

// writeObservation is everything observeWriteOutcome captured for one call.
type writeObservation struct {
	Err       error
	Target    uint64
	Faults    int // armed faults consumed by the op (and settle)
	Immediate recState
	ReadErr   error // non-not-found error from Get/FindByKey right after the op
	Events    []WatchEvent
	LSNDelta  uint64
	Probe     error  // a fresh Insert after the op
	ProbeID   uint64 // id the probe got (0 on error)
	Reopen    [2]imageView
	PowerLoss [2]imageView
}

// imageView is the target after opening an image of the data dir.
type imageView struct {
	BadLines int // undecodable segment lines before open (torn tails)
	OpenErr  error
	State    recState
	ReadErr  error
}

// outcomeExpect is the expected observation, by state label ("old" / "new"
// of the op, "fenced", "error").
type outcomeExpect struct {
	outcome   string   // durable | not-applied | unknown (documentation)
	errIs     []error  // each must match the returned error; empty => nil
	immediate string   // target right after the call
	events    int      // watch events for the call
	lsn       uint64   // replication LSN delta
	probe     string   // ok | poisoned | fenced | error
	reopen    []string // allowed labels after the page-cache reopen
	powerLoss []string // allowed labels after the power-loss reopen
}

func woData() map[string]any { return map[string]any{"tag": "new", "v": 2} }

// writeOutcomeOps is every single-record mutation path covered by §2.3.
func writeOutcomeOps() []writeOutcomeOp {
	base := recState{Indexed: true, Rev: 1, Visible: true, V: 1, Tag: "old"}
	keyed := func(s recState, v int) recState { s.ByKey = v; return s }
	updated := recState{Indexed: true, Rev: 2, Visible: true, V: 2, Tag: "new"}
	gone := recState{Tag: "none"}
	const core, bg = "core-mutation-outcomes", "background-and-replication-writes"
	return []writeOutcomeOp{
		{name: "Insert", followUp: core, ratified: true, old: gone,
			new: recState{Indexed: true, Rev: 1, Visible: true, V: 2, Tag: "new"}, events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				next := e.col.idSeq.Load() + 1
				_, _, err := e.col.Insert(woData())
				return next, err
			}},
		{name: "InsertWithKey", followUp: core, ratified: true, key: "k2", old: keyed(gone, -1),
			new: recState{Indexed: true, Rev: 1, Visible: true, V: 2, Tag: "new", ByKey: 2}, events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				next := e.col.idSeq.Load() + 1
				_, _, err := e.col.InsertWithKey("k2", woData())
				return next, err
			}},
		{name: "Update", followUp: core, ratified: true, old: base, new: updated, events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				_, err := e.col.Update(e.base, woData())
				return e.base, err
			}},
		{name: "Delete", followUp: core, ratified: true, old: base, new: gone, events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) { return e.base, e.col.Delete(e.base) }},
		{name: "Upsert", followUp: core, ratified: true, key: "k1", old: keyed(base, 1), new: keyed(updated, 2), events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				_, err := e.col.Upsert("k1", woData())
				return e.base, err
			}},
		{name: "UpdateIfRev", followUp: core, ratified: true, key: "k1", old: keyed(base, 1), new: keyed(updated, 2), events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				_, err := e.col.UpdateIfRev("k1", 1, woData())
				return e.base, err
			}},
		{name: "UpdateIfMatch", followUp: core, ratified: true, key: "k1", old: keyed(base, 1), new: keyed(updated, 2), events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				_, err := e.col.UpdateIfMatch("k1", func(map[string]any) bool { return true }, woData())
				return e.base, err
			}},
		// UpdateByKey and DeleteByKey run through update/Delete, so they inherit
		// the contract; their own key-level work stays with the follow-up task.
		{name: "UpdateByKey", followUp: bg, ratified: true, key: "k1", old: keyed(base, 1), new: keyed(updated, 2), events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) {
				_, err := e.col.UpdateByKey("k1", woData())
				return e.base, err
			}},
		{name: "DeleteByKey", followUp: bg, ratified: true, key: "k1", old: keyed(base, 1), new: keyed(gone, -1), events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) { return e.base, e.col.DeleteByKey("k1") }},
		// The reaper's tombstone is committed like a Delete: synced, then
		// dropped from the indexes, published and emitted. The expired base
		// record is hidden before the pass (also from IndexLookup, §6); the delete shows in the raw index.
		{name: "reapExpired", followUp: bg, ratified: true, key: "k1", expired: true,
			old:    recState{Indexed: true, Rev: 1, Tag: "none", ByKey: -1},
			new:    keyed(gone, -1),
			events: 1, lsn: 1,
			run: func(e *woEnv) (uint64, error) { return e.base, e.col.reapExpired() }},
		// A follower apply writes verbatim and never re-publishes.
		{name: "applyEntry", followUp: bg, ratified: true, key: "k1", old: keyed(base, 1), new: keyed(updated, 2), events: 1, lsn: 0,
			run: func(e *woEnv) (uint64, error) {
				en := store.NewUpdate(e.base, stampKey(woData(), "k1"))
				en.Rev = 2
				en.Ts = time.Now().UTC()
				return e.base, e.col.applyEntry(en)
			}},
	}
}

// Faults on the active segment under SyncModeAlways (plus a no-fault control).
var (
	faultNone = writeFault{name: "none", arm: func(*faultFS) {}}
	// The write persists a 7-byte torn prefix; the rollback truncate succeeds.
	faultShortWrite = writeFault{name: "short-write", faults: 1, arm: func(fs *faultFS) {
		fs.failWriteAt(fs.scopedCount("write")+1, 7, syscall.ENOSPC)
	}}
	// The write persists a torn prefix and the rollback truncate fails.
	faultWriteTruncate = writeFault{name: "write+truncate-fail", faults: 2, arm: func(fs *faultFS) {
		fs.failWriteAt(fs.scopedCount("write")+1, 7, syscall.ENOSPC)
		fs.failTruncateAt(fs.scopedCount("truncate")+1, syscall.EIO)
	}}
	// The append succeeds; the foreground fsync fails.
	faultSync = writeFault{name: "sync-fail", faults: 1, arm: func(fs *faultFS) {
		fs.failSyncAt(fs.scopedCount("sync")+1, syscall.EIO)
	}}
	// SyncModeInterval: the op succeeds unsynced, then one syncLoop fsync
	// fails; settle waits for that failure and the fence, then checks the loop
	// never retries the fsync.
	faultSyncLoop = writeFault{name: "syncloop-fail", faults: 1, arm: func(*faultFS) {}, settle: func(t *testing.T, fs *faultFS, col *Collection) {
		before := fs.faults()
		fs.failNextSync(syscall.EIO)
		waitFor(t, "syncLoop fsync failure", func() bool { return fs.faults() > before })
		waitFor(t, "fence after syncLoop failure", func() bool { return col.Fenced() != nil })
		failed := fs.scopedCount("sync")
		time.Sleep(10 * col.cfg.SyncInterval)
		if n := fs.scopedCount("sync"); n != failed {
			t.Errorf("syncLoop retried fsync after failure: %d syncs, want %d", n, failed)
		}
	}}
)

// woConfig is the harness's collection config on top of cfg: replication on
// (so the LSN is observable) and no periodic index persists (so index files
// only change at deterministic points).
func woConfig(cfg CollectionConfig) CollectionConfig {
	if cfg.ReplicationRingSize == 0 {
		cfg.ReplicationRingSize = 64
	}
	if cfg.IndexPersistInterval == 0 {
		cfg.IndexPersistInterval = -1
	}
	if cfg.CompactInterval == 0 {
		cfg.CompactInterval = 24 * time.Hour
	}
	return cfg
}

// observeWriteOutcome runs op once against a fresh DB with fault armed on the
// active segment and returns what every observation path saw.
func observeWriteOutcome(t *testing.T, op writeOutcomeOp, fault writeFault, cfg CollectionConfig) writeObservation {
	t.Helper()
	cfg = woConfig(cfg)
	dir := t.TempDir()
	fs := newFaultFS()
	db, col := openFaulty(t, dir, fs, cfg)
	t.Cleanup(func() { _ = db.Close() }) // may fail on a poisoned segment

	if err := col.EnsureIndex("tag"); err != nil {
		t.Fatal(err)
	}
	if err := col.ensureKeyIndex(); err != nil {
		t.Fatal(err)
	}
	var exp int64
	if op.expired {
		exp = time.Now().Add(-time.Hour).UnixNano()
	}
	base, _, err := col.insert(stampKey(map[string]any{"tag": "old", "v": 1}, "k1"), exp)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	env := &woEnv{db: db, col: col, base: base}

	col.mu.RLock()
	active := col.active.Path()
	col.mu.RUnlock()
	fs.scopeTo(active)

	_, events, cancel := col.Subscribe()
	defer cancel()
	lsn0 := woLSN(db)
	faults0 := fs.faults()

	fault.arm(fs)
	var obs writeObservation
	obs.Target, obs.Err = op.run(env)
	if fault.settle != nil {
		fault.settle(t, fs, col)
	}
	obs.Faults = fs.faults() - faults0
	if obs.Faults != fault.faults {
		t.Errorf("harness: %s consumed %d armed faults, want %d", fault.name, obs.Faults, fault.faults)
	}
	obs.Immediate, obs.ReadErr = readState(col, obs.Target, op.key)
	obs.LSNDelta = woLSN(db) - lsn0
	for drained := false; !drained; {
		select {
		case ev := <-events:
			obs.Events = append(obs.Events, ev)
		default:
			drained = true
		}
	}

	// Both images are taken before the probe so they show only this op.
	crash, power := t.TempDir(), t.TempDir()
	copyTree(t, dir, crash)
	powerLossImage(t, fs, dir, power)

	obs.ProbeID, _, obs.Probe = col.Insert(map[string]any{"probe": true})

	for i := range 2 {
		obs.Reopen[i] = openImage(t, crash, obs.Target, op.key)
		obs.PowerLoss[i] = openImage(t, power, obs.Target, op.key)
	}
	return obs
}

func woLSN(db *DB) uint64 {
	if db.broker == nil {
		return 0
	}
	return db.broker.currentLSN()
}

// powerLossImage copies the data dir src to dst and cuts every file opened
// through fs back to its last-fsynced length, modeling a power loss in which
// only explicitly synced bytes survive. Files written outside the faultFS
// (meta.json, index snapshots: written via temp+fsync+rename) are copied as
// they are.
func powerLossImage(t *testing.T, fs *faultFS, src, dst string) {
	t.Helper()
	copyTree(t, src, dst)
	if r, err := filepath.EvalSymlinks(src); err == nil {
		src = r
	}
	for p, n := range fs.durableSizes() {
		rel, err := filepath.Rel(src, p)
		if err != nil || strings.HasPrefix(rel, "..") {
			continue
		}
		cp := filepath.Join(dst, rel)
		st, err := os.Stat(cp)
		if err != nil || st.Size() <= n {
			continue
		}
		if err := os.Truncate(cp, n); err != nil {
			t.Fatal(err)
		}
	}
}

// openImage opens a copy of a data dir with a plain (fault-free) handle and
// reads the target, then closes it.
func openImage(t *testing.T, dir string, target uint64, key string) imageView {
	t.Helper()
	var v imageView
	v.BadLines = len(diskState(t, colDirOf(dir)).Bad)
	db, err := Open(dir, woConfig(CollectionConfig{ReplicationRingSize: -1}))
	if err != nil {
		v.OpenErr = err
		return v
	}
	defer func() { _ = db.Close() }()
	col, err := db.Collection("c")
	if err != nil {
		v.OpenErr = err
		return v
	}
	v.State, v.ReadErr = readState(col, target, key)
	return v
}

// readState reads id through every observation path. The error is the first
// read failure that is not a plain not-found.
func readState(col *Collection, id uint64, key string) (recState, error) {
	var s recState
	var readErr error
	if loc, ok := col.index.Get(id); ok {
		s.Indexed, s.Rev = true, loc.Rev
	}
	switch rec, err := col.Get(id); {
	case err == nil:
		s.Visible, s.V = true, woInt(rec.Data["v"])
	case !isNotFound(err):
		readErr = err
	}
	oldIDs, okOld := col.IndexLookup("tag", "old")
	newIDs, okNew := col.IndexLookup("tag", "new")
	inOld, inNew := slices.Contains(oldIDs, id), slices.Contains(newIDs, id)
	switch {
	case !okOld || !okNew:
		s.Tag = "noindex"
	case inOld && inNew:
		s.Tag = "both"
	case inOld:
		s.Tag = "old"
	case inNew:
		s.Tag = "new"
	default:
		s.Tag = "none"
	}
	if key != "" {
		data, _, err := col.FindByKey(key)
		switch {
		case err == nil:
			s.ByKey = woInt(data["v"])
		case errors.Is(err, ErrKeyNotFound) || isNotFound(err):
			s.ByKey = -1
		default:
			s.ByKey = -1
			if readErr == nil {
				readErr = err
			}
		}
	}
	return s, readErr
}

func isNotFound(err error) bool { return strings.Contains(err.Error(), "not found") }

func woInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case int64:
		return int(n)
	}
	return -100
}

// stateLabel names a recState relative to op: "old", "new", or "other(...)".
// A read error classifies first (classifyReadErr).
func stateLabel(op writeOutcomeOp, s recState, readErr error) string {
	if readErr != nil {
		return classifyReadErr(readErr)
	}
	switch s {
	case op.old:
		return "old"
	case op.new:
		return "new"
	}
	return fmt.Sprintf("other(%+v)", s)
}

// classifyReadErr maps a read failure to a label.
func classifyReadErr(err error) string {
	if errors.Is(err, ErrCollectionFenced) {
		return "fenced"
	}
	return "error"
}

// classifyProbe maps the probe write's result to a label.
func classifyProbe(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrCollectionFenced):
		return "fenced"
	case errors.Is(err, ErrSegmentPoisoned):
		return "poisoned"
	}
	return "error"
}

// assertWriteOutcome checks obs against exp.
func assertWriteOutcome(t *testing.T, op writeOutcomeOp, obs writeObservation, exp outcomeExpect) {
	t.Helper()
	if len(exp.errIs) == 0 && obs.Err != nil {
		t.Errorf("err = %v, want nil", obs.Err)
	}
	for _, want := range exp.errIs {
		if !errors.Is(obs.Err, want) {
			t.Errorf("err = %v, want errors.Is %v", obs.Err, want)
		}
	}
	if got := stateLabel(op, obs.Immediate, obs.ReadErr); got != exp.immediate {
		t.Errorf("immediate state %s (%+v, readErr=%v), want %s", got, obs.Immediate, obs.ReadErr, exp.immediate)
	}
	var evs int
	for _, ev := range obs.Events {
		if ev.ID == obs.Target {
			evs++
		}
	}
	if evs != len(obs.Events) || evs != exp.events {
		t.Errorf("watch events %+v, want %d for id %d", obs.Events, exp.events, obs.Target)
	}
	if obs.LSNDelta != exp.lsn {
		t.Errorf("replication LSN advanced by %d, want %d", obs.LSNDelta, exp.lsn)
	}
	if got := classifyProbe(obs.Probe); got != exp.probe {
		t.Errorf("probe write %s (%v), want %s", got, obs.Probe, exp.probe)
	}
	if obs.Probe == nil && obs.ProbeID <= obs.Target {
		t.Errorf("probe got id %d, not above target %d (id reused)", obs.ProbeID, obs.Target)
	}
	assertImage(t, op, "page-cache reopen", obs.Reopen, exp.reopen)
	assertImage(t, op, "power-loss reopen", obs.PowerLoss, exp.powerLoss)
}

func assertImage(t *testing.T, op writeOutcomeOp, what string, v [2]imageView, allowed []string) {
	t.Helper()
	for i, iv := range v {
		if iv.OpenErr != nil {
			t.Errorf("%s #%d: open: %v", what, i+1, iv.OpenErr)
			return
		}
		if got := stateLabel(op, iv.State, iv.ReadErr); !slices.Contains(allowed, got) {
			t.Errorf("%s #%d: state %s (%+v), want one of %v", what, i+1, got, iv.State, allowed)
		}
	}
	if v[0].State != v[1].State {
		t.Errorf("%s not idempotent: %+v then %+v", what, v[0].State, v[1].State)
	}
	if v[1].BadLines != 0 {
		t.Errorf("%s: first open left %d bad lines", what, v[1].BadLines)
	}
}

// reportOutcomeGap logs where today's behavior differs from the ratified one,
// tagged with the follow-up task that closes the gap.
func reportOutcomeGap(t *testing.T, followUp string, today, want outcomeExpect) {
	t.Helper()
	if d, w := today.String(), want.String(); d != w {
		t.Logf("TODO(%s): today {%s}; ratified §2.3 {%s}", followUp, d, w)
	}
}

func (e outcomeExpect) String() string {
	errs := make([]string, len(e.errIs))
	for i, err := range e.errIs {
		errs[i] = err.Error()
	}
	return fmt.Sprintf("%s err=%v immediate=%s events=%d lsn=%d probe=%s reopen=%v powerLoss=%v",
		e.outcome, errs, e.immediate, e.events, e.lsn, e.probe, e.reopen, e.powerLoss)
}
