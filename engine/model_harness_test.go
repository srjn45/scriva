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
//
// On failure the last modelTraceTail ops are printed together with the seed so
// the run can be replayed with SCRIVA_MODEL_SEED=<n>.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/srjn45/scriva/query"
)

const (
	modelTraceTail    = 25
	modelDefaultSteps = 200
	modelIndexField   = "tag"
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

	{name: "crash-reopen", weight: 4, disabledBy: "stale-index-after-crash (TestCrashStaleIndex): index-vs-segment reconciliation task, #107 phase 2", run: (*modelRun).opUnimplemented},
	{name: "torn-write", weight: 3, disabledBy: "partial-write offsets (TestPartialWriteOffsets): write-failure rollback task, #107 phase 2", run: (*modelRun).opUnimplemented},
	{name: "compact", weight: 3, disabledBy: "compaction vs secondary-index desync: compaction/index task, #107 phase 2", run: (*modelRun).opUnimplemented},
	{name: "commit-tx", weight: 3, disabledBy: "CommitTx atomicity (TestCommitTxAtomicity): transaction atomicity task, #107 phase 2", run: (*modelRun).opUnimplemented},
}

// modelRun is the state of one seeded run.
type modelRun struct {
	t     *testing.T
	seed  int64
	rng   *rand.Rand
	dir   string
	db    *DB
	col   *Collection
	state map[uint64]map[string]any // expected live records
	gone  []uint64                  // ids that must be absent
	trace []string
	step  int
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
	db, err := Open(m.dir, CollectionConfig{CompactInterval: 24 * 3600 * 1e9})
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
	m.state[id] = d
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
	m.state[id] = d
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
func (m *modelRun) verifyAll() error {
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
	}
	return nil
}

func (m *modelRun) opUnimplemented() error {
	return fmt.Errorf("op is registered but has no implementation yet; its fixing task must provide one")
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
		if err := op.run(m); err != nil {
			m.fail("%s: %v", op.name, err)
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
