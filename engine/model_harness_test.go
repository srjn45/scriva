package engine

// Canonical seeded model-test harness for the index/data integrity hardening
// (issue #107, phase 2). A deterministic random op sequence is applied to both
// a Collection and an in-memory model; after every op the engine must agree
// with the model (Get, full Scan, secondary-index scan) and, after every clean
// reopen, the on-disk segments (diskState) must agree with both.
//
// Controls (environment):
//
//	SCRIVA_MODEL_SEED   comma-separated seeds to run (default: modelDefaultSeeds)
//	SCRIVA_MODEL_OPS    comma-separated op names to enable (default: the
//	                    enabled-by-default ops). Naming a disabled op opts in,
//	                    which is how a fix task turns its operation on.
//	SCRIVA_MODEL_STEPS  ops per seed (default 200)
//	SCRIVA_MODEL_SEED_COUNT  soak: run this many consecutive seeds starting at
//	                    SCRIVA_MODEL_SEED_BASE (default 1000); ignored when
//	                    SCRIVA_MODEL_SEED is set. See `make test-soak`.
//
// On failure the last modelTraceTail ops are printed together with the seed so
// the run can be replayed with SCRIVA_MODEL_SEED=<n>.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

const (
	modelTraceTail    = 25
	modelDefaultSteps = 200
	modelIndexField   = "tag"
	modelSegmentMax   = 1024
)

// modelDefaultSeeds are fixed so CI is deterministic.
var modelDefaultSeeds = []int64{1, 2, 3, 42, 1337}

// modelOp is a registered model operation.
type modelOp struct {
	name   string
	weight int
	// disabledBy is empty for enabled-by-default ops; otherwise it names the
	// known bug / task whose fix should flip the op on.
	disabledBy string
	run        func(m *modelRun) error
}

// modelOps is the op registry. Known-bug operations are registered but
// disabled; enable one with SCRIVA_MODEL_OPS=<name> to reproduce, and flip
// disabledBy to "" when its fixing task lands.
var modelOps = []modelOp{
	{name: "insert", weight: 30, run: (*modelRun).opInsert},
	{name: "update", weight: 20, run: (*modelRun).opUpdate},
	{name: "delete", weight: 15, run: (*modelRun).opDelete},
	{name: "get", weight: 15, run: (*modelRun).opGet},
	{name: "scan", weight: 8, run: (*modelRun).opScan},
	{name: "scan-index", weight: 8, run: (*modelRun).opScanIndex},
	{name: "reopen", weight: 4, run: (*modelRun).opReopen},

	{name: "crash-reopen", weight: 4, run: (*modelRun).opCrashReopen},
	{name: "torn-write", weight: 4, run: (*modelRun).opTornWrite},
	{name: "compact", weight: 3, run: (*modelRun).opCompact},
	{name: "commit-tx", weight: 4, run: (*modelRun).opCommitTx},
	{name: "image-recover", weight: 2, run: (*modelRun).opImageRecover},
	{name: "verify", weight: 3, run: (*modelRun).opVerify},
	{name: "repair-noop", weight: 1, run: (*modelRun).opRepairNoop},
}

// modelRun is the state of one seeded run.
type modelRun struct {
	t     *testing.T
	seed  int64
	rng   *rand.Rand
	dir   string
	fs    *faultFS // fault seam of the current handle; replaced on every open
	db    *DB
	col   *Collection
	state map[uint64]map[string]any // expected live records
	gone  []uint64                  // ids that must be absent
	trace []string
	step  int
}

// modelRaceCompaction (SCRIVA_MODEL_RACE_COMPACTION=1) lets the rotation-
// triggered background compaction run concurrently with the next op. It is
// off by default so ordinary model runs stay deterministic.
var modelRaceCompaction = os.Getenv("SCRIVA_MODEL_RACE_COMPACTION") == "1"

// settleCompaction waits for background compaction triggered by the last op
// (segment rotation) to finish when deterministic model sequencing is wanted.
func (m *modelRun) settleCompaction() {
	for i := 0; i < 3; i++ {
		for len(m.col.compactC) > 0 {
			time.Sleep(time.Millisecond)
		}
		m.col.compactMu.Lock()   // waits out a pass in flight
		m.col.compactMu.Unlock() //nolint:staticcheck // barrier only
		// The compactor goroutine may have taken the signal but not yet the
		// lock; give it time to get there so the next barrier waits for it.
		time.Sleep(5 * time.Millisecond)
	}
}

func (m *modelRun) tracef(format string, a ...any) {
	m.trace = append(m.trace, fmt.Sprintf("#%d "+format, append([]any{m.step}, a...)...))
}

// fail reports err with the replay seed and the trace tail, then stops.
func (m *modelRun) fail(format string, a ...any) {
	m.t.Helper()
	tail := m.trace
	if len(tail) > modelTraceTail {
		tail = tail[len(tail)-modelTraceTail:]
	}
	m.t.Fatalf("model mismatch (replay: SCRIVA_MODEL_SEED=%d) at step %d: %s\ntrace tail (%d ops):\n  %s",
		m.seed, m.step, fmt.Sprintf(format, a...), len(tail), strings.Join(tail, "\n  "))
}

func (m *modelRun) randData() map[string]any {
	return map[string]any{
		modelIndexField: "t" + strconv.Itoa(m.rng.Intn(5)),
		"n":             float64(m.rng.Intn(1000)),
	}
}

// put records id as live with data d. An id that was reserved by a failed
// insert/transaction is not durable and may legitimately be handed out again
// after a reopen, so it stops being "must be absent" once it is live again.
func (m *modelRun) put(id uint64, d map[string]any) {
	m.state[id] = d
	kept := m.gone[:0]
	for _, g := range m.gone {
		if g != id { // an id can be listed more than once
			kept = append(kept, g)
		}
	}
	m.gone = kept
}

func (m *modelRun) pickID() (uint64, bool) {
	if len(m.state) == 0 {
		return 0, false
	}
	ids := make([]uint64, 0, len(m.state))
	for id := range m.state {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] }) // map order is random
	return ids[m.rng.Intn(len(ids))], true
}

func (m *modelRun) open() {
	m.t.Helper()
	m.fs = newFaultFS()
	// A tiny segment size forces frequent rotation (and the background
	// compaction it can trigger) to interleave with every other op.
	db, err := Open(m.dir, CollectionConfig{
		CompactInterval: 24 * 3600 * 1e9,
		SegmentMaxSize:  modelSegmentMax,
		wrapFile:        m.fs.wrap,
		renameFn:        m.fs.rename,
	})
	if err != nil {
		m.fail("open: %v", err)
	}
	m.db = db
	col, err := db.Collection("c")
	if err != nil {
		if col, err = db.CreateCollection("c"); err != nil {
			m.fail("create collection: %v", err)
		}
		if err := col.EnsureIndex(modelIndexField); err != nil {
			m.fail("ensure index: %v", err)
		}
	}
	m.col = col
}

func (m *modelRun) opInsert() error {
	d := m.randData()
	id, _, err := m.col.Insert(d)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	if _, dup := m.state[id]; dup {
		return fmt.Errorf("insert reused live id %d", id)
	}
	m.put(id, d)
	m.tracef("insert id=%d %v", id, d)
	return nil
}

func (m *modelRun) opUpdate() error {
	id, ok := m.pickID()
	if !ok {
		return nil
	}
	d := m.randData()
	if _, err := m.col.Update(id, d); err != nil {
		return fmt.Errorf("update %d: %w", id, err)
	}
	m.put(id, d)
	m.tracef("update id=%d %v", id, d)
	return nil
}

func (m *modelRun) opDelete() error {
	id, ok := m.pickID()
	if !ok {
		return nil
	}
	if err := m.col.Delete(id); err != nil {
		return fmt.Errorf("delete %d: %w", id, err)
	}
	delete(m.state, id)
	m.gone = append(m.gone, id)
	m.tracef("delete id=%d", id)
	return nil
}

func (m *modelRun) opGet() error {
	id, ok := m.pickID()
	if !ok {
		return nil
	}
	m.tracef("get id=%d", id)
	rec, err := m.col.Get(id)
	if err != nil {
		return fmt.Errorf("get %d: %w", id, err)
	}
	if !sameData(rec.Data, m.state[id]) {
		return fmt.Errorf("get %d = %v, model %v", id, rec.Data, m.state[id])
	}
	return nil
}

func (m *modelRun) opScan() error {
	m.tracef("scan")
	return m.checkScan(nil, func(map[string]any) bool { return true }, false)
}

func (m *modelRun) opScanIndex() error {
	tag := "t" + strconv.Itoa(m.rng.Intn(5))
	m.tracef("scan-index %s=%s", modelIndexField, tag)
	v, _ := json.Marshal(tag)
	f := &query.FieldFilter{Field: modelIndexField, Op: query.OpEq, Value: string(v)}
	return m.checkScan(f, func(d map[string]any) bool { return d[modelIndexField] == tag }, true)
}

// checkScan runs a filtered scan and compares it to the model's expectation.
func (m *modelRun) checkScan(f query.Filter, want func(map[string]any) bool, wantIndex bool) error {
	got := map[uint64]map[string]any{}
	stats, err := m.col.ScanStream(context.Background(), ScanOptions{Filter: f}, func(r ScanResult) error {
		if _, dup := got[r.ID]; dup {
			return fmt.Errorf("scan returned id %d twice", r.ID)
		}
		got[r.ID] = r.Data
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	if wantIndex && !stats.IndexUsed {
		return fmt.Errorf("equality scan on indexed field %q did not use the secondary index", modelIndexField)
	}
	exp := map[uint64]map[string]any{}
	for id, d := range m.state {
		if want(d) {
			exp[id] = d
		}
	}
	for id, d := range exp {
		g, ok := got[id]
		if !ok {
			return fmt.Errorf("id %d in model but missing from scan (index=%v)", id, stats.IndexUsed)
		}
		if !sameData(g, d) {
			return fmt.Errorf("id %d scan data %v != model %v", id, g, d)
		}
	}
	for id := range got {
		if _, ok := exp[id]; !ok {
			return fmt.Errorf("id %d returned by scan (index=%v) but not expected by model", id, stats.IndexUsed)
		}
	}
	return nil
}

// opReopen cleanly closes and reopens the DB, then checks disk ground truth.
func (m *modelRun) opReopen() error {
	m.tracef("reopen")
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	m.open()
	return m.checkDisk()
}

// checkDisk compares the on-disk segments with the model (call only when the
// segments are quiescent: after Close/reopen).
func (m *modelRun) checkDisk() error {
	rep := diskState(m.t, colDirOf(m.dir))
	if len(rep.Bad) != 0 {
		return fmt.Errorf("disk has undecodable lines: %v", rep.Bad)
	}
	disk := rep.model()
	if len(disk) != len(m.state) {
		return fmt.Errorf("disk has %d live records, model %d", len(disk), len(m.state))
	}
	for id, d := range m.state {
		dd, ok := disk[id]
		if !ok {
			return fmt.Errorf("id %d in model but not live on disk", id)
		}
		if !sameData(dd, d) {
			return fmt.Errorf("id %d disk data %v != model %v", id, dd, d)
		}
	}
	for _, id := range m.gone {
		if _, ok := disk[id]; ok {
			return fmt.Errorf("deleted id %d still live on disk", id)
		}
	}
	return nil
}

// verifyAll is the full post-op invariant: lookups, scan and absent ids.
func (m *modelRun) verifyAll() error { return m.verifyCol(m.col) }

// verifyCol checks col against the model. It is also used on a recovered copy
// of the database, which must satisfy the same invariants as the live handle.
func (m *modelRun) verifyCol(col *Collection) error {
	c := *m
	c.col = col
	m = &c
	for id, d := range m.state {
		rec, err := m.col.Get(id)
		if err != nil {
			return fmt.Errorf("get(%d): %w (model has it)", id, err)
		}
		if !sameData(rec.Data, d) {
			return fmt.Errorf("get(%d) = %v, model %v", id, rec.Data, d)
		}
	}
	for _, id := range m.gone {
		if _, err := m.col.Get(id); err == nil {
			return fmt.Errorf("get(%d) succeeded but id was deleted", id)
		}
	}
	if err := m.checkScan(nil, func(map[string]any) bool { return true }, false); err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		tag := "t" + strconv.Itoa(i)
		v, _ := json.Marshal(tag)
		f := &query.FieldFilter{Field: modelIndexField, Op: query.OpEq, Value: string(v)}
		if err := m.checkScan(f, func(d map[string]any) bool { return d[modelIndexField] == tag }, true); err != nil {
			return err
		}
		// Direct secondary-index lookup must agree with the model too.
		ids, ok := m.col.IndexLookup(modelIndexField, tag)
		if !ok {
			return fmt.Errorf("IndexLookup: no index on %q", modelIndexField)
		}
		var want []uint64
		for id, d := range m.state {
			if d[modelIndexField] == tag {
				want = append(want, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		if fmt.Sprint(ids) != fmt.Sprint(want) {
			return fmt.Errorf("IndexLookup(%s=%s) = %v, model %v", modelIndexField, tag, ids, want)
		}
	}
	return nil
}

// disarmWrite clears an armed write fault that was not consumed.
func (m *modelRun) disarmWrite() { m.fs.failWriteAt(0, 0, nil) }

// opCrashReopen abandons the handle without Close (kill -9), optionally with a
// torn partial line left at the end of the newest segment, and reopens. Every
// acknowledged write must survive and the torn tail must not.
func (m *modelRun) opCrashReopen() error {
	torn := m.rng.Intn(3) == 0
	m.tracef("crash-reopen torn=%v", torn)
	m.fs.kill()
	// A real kill -9 stops every goroutine. Stop the abandoned handle's
	// background loops (without Close's flush/persist) and wait them out so they
	// cannot touch the directory the new handle is about to own.
	m.col.closeOnce.Do(func() { close(m.col.closed) })
	m.col.persistWG.Wait()
	m.col.compactMu.Lock()
	m.col.compactMu.Unlock() //nolint:staticcheck // barrier only
	m.fs.releaseFDs()
	if err := m.db.lock.release(); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	if torn {
		segs, _ := filepath.Glob(filepath.Join(colDirOf(m.dir), "seg_*.ndjson"))
		if len(segs) > 0 {
			sort.Strings(segs)
			f, err := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return err
			}
			_, werr := f.WriteString(`{"id":999999,"op":"ins`)
			cerr := f.Close()
			if err := errors.Join(werr, cerr); err != nil {
				return fmt.Errorf("write torn tail: %w", err)
			}
		}
	}
	m.open()
	return m.checkDisk()
}

// opTornWrite makes one write persist only a prefix and fail. A failed op must
// leave the model untouched; if the armed fault was consumed elsewhere (a
// background compaction) the op succeeded and is applied.
func (m *modelRun) opTornWrite() error {
	kind := m.rng.Intn(3)
	prefix := m.rng.Intn(14)
	before := m.fs.faults()
	m.fs.failWriteAt(m.fs.count("write")+1, prefix, syscall.ENOSPC)
	defer m.disarmWrite()
	switch kind {
	case 0:
		d := m.randData()
		id, _, err := m.col.Insert(d)
		m.tracef("torn insert prefix=%d err=%v", prefix, err)
		if err == nil {
			m.put(id, d)
		}
		return m.tornResult(err, before)
	case 1:
		id, ok := m.pickID()
		if !ok {
			return nil
		}
		d := m.randData()
		_, err := m.col.Update(id, d)
		m.tracef("torn update id=%d prefix=%d err=%v", id, prefix, err)
		if err == nil {
			m.put(id, d)
		}
		return m.tornResult(err, before)
	default:
		id, ok := m.pickID()
		if !ok {
			return nil
		}
		err := m.col.Delete(id)
		m.tracef("torn delete id=%d prefix=%d err=%v", id, prefix, err)
		if err == nil {
			delete(m.state, id)
			m.gone = append(m.gone, id)
		}
		return m.tornResult(err, before)
	}
}

func (m *modelRun) tornResult(err error, faultsBefore int) error {
	if err != nil && !errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("unexpected error (want ENOSPC or success): %w", err)
	}
	if err != nil && m.fs.faults() == faultsBefore {
		return fmt.Errorf("op failed without an injected fault: %w", err)
	}
	return nil
}

func (m *modelRun) opCompact() error {
	m.tracef("compact")
	if err := m.col.CompactNow(); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	return nil
}

// opCommitTx commits a small random transaction, half the time with an
// injected write failure. A failed commit must be all-or-nothing.
func (m *modelRun) opCommitTx() error {
	n := 1 + m.rng.Intn(4)
	used := map[uint64]bool{}
	var ops []txOp
	var inserted []uint64
	next := map[uint64]map[string]any{} // post-commit data; nil = deleted
	for i := 0; i < n; i++ {
		now := time.Now().UTC()
		switch k := m.rng.Intn(3); {
		case k == 0 || len(m.state) == 0:
			id := m.col.ReserveID()
			d := m.randData()
			ops = append(ops, txOp{kind: txOpInsert, id: id, data: d, ts: now})
			inserted = append(inserted, id)
			next[id] = d
		default:
			id, _ := m.pickID()
			if used[id] {
				continue
			}
			used[id] = true
			if k == 1 {
				d := m.randData()
				ops = append(ops, txOp{kind: txOpUpdate, id: id, data: d, ts: now})
				next[id] = d
			} else {
				ops = append(ops, txOp{kind: txOpDelete, id: id, ts: now})
				next[id] = nil
			}
		}
	}
	inject := m.rng.Intn(2) == 0
	before := m.fs.faults()
	if inject {
		m.fs.failWriteAt(m.fs.count("write")+1+m.rng.Intn(len(ops)), m.rng.Intn(10), syscall.ENOSPC)
		defer m.disarmWrite()
	}
	err := m.col.CommitTx(ops)
	m.tracef("commit-tx ops=%d inject=%v err=%v", len(ops), inject, err)
	if err != nil {
		if !errors.Is(err, syscall.ENOSPC) || m.fs.faults() == before {
			return fmt.Errorf("commit: %w", err)
		}
		m.gone = append(m.gone, inserted...) // reserved ids must stay absent
		return nil
	}
	for id, d := range next {
		if d == nil {
			delete(m.state, id)
			m.gone = append(m.gone, id)
		} else {
			m.put(id, d)
		}
	}
	return nil
}

// opImageRecover opens a copy of the live directory (a crash image: nothing
// flushed, index.json/sidx files possibly stale) and requires the recovered
// copy to agree with the model, including secondary-index lookups.
func (m *modelRun) opImageRecover() error {
	m.tracef("image-recover")
	m.col.compactMu.Lock() // no segment swap mid-copy
	img := copyDataDir(m.t, m.dir)
	m.col.compactMu.Unlock()
	db, err := Open(img, CollectionConfig{CompactInterval: 24 * 3600 * 1e9, SegmentMaxSize: modelSegmentMax})
	if err != nil {
		return fmt.Errorf("open crash image: %w", err)
	}
	defer db.Close()
	col, err := db.Collection("c")
	if err != nil {
		return fmt.Errorf("image collection: %w", err)
	}
	return m.verifyCol(col)
}

// opVerify runs the online full Verify; a healthy live DB has no findings
// above informational severity.
func (m *modelRun) opVerify() error {
	m.tracef("verify")
	rep, err := m.db.Verify(context.Background(), VerifyOptions{Mode: VerifyFull})
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	return noSevereFindings(rep)
}

func noSevereFindings(rep *IntegrityReport) error {
	for _, c := range rep.Collections {
		for _, f := range c.Findings {
			if f.Severity.rank() > SeverityInfo.rank() {
				return fmt.Errorf("finding in %s: %+v", c.Name, f)
			}
		}
	}
	return nil
}

// opRepairNoop closes cleanly, runs Repair on a copy of the healthy directory
// and requires it to change nothing, then reopens the original.
func (m *modelRun) opRepairNoop() error {
	m.tracef("repair-noop")
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	img := copyDataDir(m.t, m.dir)
	before := segHashes(m.t, colDirOf(img))
	rep, err := Repair(context.Background(), img, RepairOptions{BackupDir: m.t.TempDir()})
	if err != nil {
		return fmt.Errorf("repair: %w", err)
	}
	if rep.BackupDir != "" {
		return fmt.Errorf("repair of a healthy copy took a backup: %s", rep.BackupDir)
	}
	for _, c := range rep.Collections {
		if c.Status != RepairUnchanged || len(c.Actions) != 0 {
			return fmt.Errorf("repair of a healthy copy not a no-op: %s status=%s actions=%+v", c.Name, c.Status, c.Actions)
		}
	}
	if after := segHashes(m.t, colDirOf(img)); after != before {
		return fmt.Errorf("repair changed segment bytes of a healthy copy")
	}
	vrep, err := VerifyDir(context.Background(), img, VerifyOptions{Mode: VerifyFull})
	if err != nil {
		return fmt.Errorf("verify copy: %w", err)
	}
	if err := noSevereFindings(vrep); err != nil {
		return fmt.Errorf("copy after repair: %w", err)
	}
	m.open()
	return m.checkDisk()
}

// parseModelSeeds reads SCRIVA_MODEL_SEED (comma-separated) or returns the defaults.
func parseModelSeeds(env string) ([]int64, error) {
	if strings.TrimSpace(env) == "" {
		return modelDefaultSeeds, nil
	}
	var out []int64
	for _, s := range strings.Split(env, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad SCRIVA_MODEL_SEED %q: %w", s, err)
		}
		out = append(out, n)
	}
	return out, nil
}

// selectModelOps returns the ops to run: the enabled-by-default ones, or
// exactly those named in env (which may include disabled ops).
func selectModelOps(env string) ([]modelOp, error) {
	if strings.TrimSpace(env) == "" {
		var out []modelOp
		for _, op := range modelOps {
			if op.disabledBy == "" {
				out = append(out, op)
			}
		}
		return out, nil
	}
	var out []modelOp
	for _, name := range strings.Split(env, ",") {
		name = strings.TrimSpace(name)
		found := false
		for _, op := range modelOps {
			if op.name == name {
				out, found = append(out, op), true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown op %q in SCRIVA_MODEL_OPS", name)
		}
	}
	return out, nil
}

// soakSeeds returns count consecutive seeds starting at base (default 1000).
func soakSeeds(count, base string) ([]int64, error) {
	n, err := strconv.Atoi(count)
	if err != nil || n < 1 {
		return nil, fmt.Errorf("bad SCRIVA_MODEL_SEED_COUNT %q", count)
	}
	start := int64(1000)
	if base != "" {
		if start, err = strconv.ParseInt(base, 10, 64); err != nil {
			return nil, fmt.Errorf("bad SCRIVA_MODEL_SEED_BASE %q", base)
		}
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = start + int64(i)
	}
	return out, nil
}

func pickOp(rng *rand.Rand, ops []modelOp) modelOp {
	total := 0
	for _, op := range ops {
		total += op.weight
	}
	n := rng.Intn(total)
	for _, op := range ops {
		if n < op.weight {
			return op
		}
		n -= op.weight
	}
	return ops[len(ops)-1]
}

func TestModel(t *testing.T) {
	seeds, err := parseModelSeeds(os.Getenv("SCRIVA_MODEL_SEED"))
	if err != nil {
		t.Fatal(err)
	}
	if n := os.Getenv("SCRIVA_MODEL_SEED_COUNT"); n != "" && os.Getenv("SCRIVA_MODEL_SEED") == "" {
		if seeds, err = soakSeeds(n, os.Getenv("SCRIVA_MODEL_SEED_BASE")); err != nil {
			t.Fatal(err)
		}
	}
	ops, err := selectModelOps(os.Getenv("SCRIVA_MODEL_OPS"))
	if err != nil {
		t.Fatal(err)
	}
	steps := modelDefaultSteps
	if s := os.Getenv("SCRIVA_MODEL_STEPS"); s != "" {
		if steps, err = strconv.Atoi(s); err != nil || steps < 1 {
			t.Fatalf("bad SCRIVA_MODEL_STEPS %q", s)
		}
	}
	for _, seed := range seeds {
		t.Run("seed-"+strconv.FormatInt(seed, 10), func(t *testing.T) {
			t.Parallel()
			runModel(t, seed, steps, ops)
		})
	}
}

func runModel(t *testing.T, seed int64, steps int, ops []modelOp) {
	m := &modelRun{
		t: t, seed: seed, rng: rand.New(rand.NewSource(seed)),
		dir: t.TempDir(), state: map[uint64]map[string]any{},
	}
	m.open()
	t.Cleanup(func() { _ = m.db.Close() })
	for m.step = 1; m.step <= steps; m.step++ {
		op := pickOp(m.rng, ops)
		seq := m.col.segSeq.Load()
		if err := op.run(m); err != nil {
			m.fail("%s: %v", op.name, err)
		}
		if !modelRaceCompaction && m.col.segSeq.Load() != seq {
			m.settleCompaction() // only a rotation can have signalled the compactor
		}
		if err := m.verifyAll(); err != nil {
			m.fail("after %s: %v", op.name, err)
		}
	}
	if err := m.opReopen(); err != nil { // final clean reopen + disk agreement
		m.fail("final reopen: %v", err)
	}
	if err := m.verifyAll(); err != nil {
		m.fail("after final reopen: %v", err)
	}
}

// TestModelHarnessSelfCheck proves the harness detects divergence and that
// env parsing / op gating behave.
func TestModelHarnessSelfCheck(t *testing.T) {
	if s, _ := parseModelSeeds(""); len(s) == 0 {
		t.Fatal("no default seeds")
	}
	if s, err := parseModelSeeds("7, 9"); err != nil || len(s) != 2 || s[1] != 9 {
		t.Fatalf("seed parse: %v %v", s, err)
	}
	if _, err := parseModelSeeds("x"); err == nil {
		t.Fatal("want error for bad seed")
	}
	def, _ := selectModelOps("")
	for _, op := range def {
		if op.disabledBy != "" {
			t.Fatalf("disabled op %s enabled by default", op.name)
		}
	}
	if o, err := soakSeeds("3", "10"); err != nil || len(o) != 3 || o[2] != 12 {
		t.Fatalf("soak seeds: %v %v", o, err)
	}
	if o, err := selectModelOps("crash-reopen"); err != nil || len(o) != 1 {
		t.Fatalf("explicit opt-in: %v %v", o, err)
	}
	if _, err := selectModelOps("nope"); err == nil {
		t.Fatal("want error for unknown op")
	}
	for _, op := range modelOps {
		if op.disabledBy == "" && op.run == nil {
			t.Fatalf("op %s has no run", op.name)
		}
	}

	// Divergence is detected: corrupt the model behind the engine's back.
	m := &modelRun{t: t, rng: rand.New(rand.NewSource(1)), dir: t.TempDir(), state: map[uint64]map[string]any{}}
	m.open()
	defer func() { _ = m.db.Close() }()
	if err := m.opInsert(); err != nil {
		t.Fatal(err)
	}
	for id := range m.state {
		m.state[id]["n"] = float64(-1)
	}
	if err := m.verifyAll(); err == nil {
		t.Fatal("verifyAll missed a data divergence")
	}
}
