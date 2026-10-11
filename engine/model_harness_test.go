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
	"bytes"
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
	{name: "insert", weight: 25, run: (*modelRun).opInsert},
	{name: "update", weight: 15, run: (*modelRun).opUpdate},
	{name: "delete", weight: 10, run: (*modelRun).opDelete},
	{name: "get", weight: 10, run: (*modelRun).opGet},
	{name: "scan", weight: 6, run: (*modelRun).opScan},
	{name: "scan-index", weight: 6, run: (*modelRun).opScanIndex},
	{name: "reopen", weight: 3, run: (*modelRun).opReopen},

	{name: "crash-reopen", weight: 3, run: (*modelRun).opCrashReopen},
	{name: "torn-write", weight: 3, run: (*modelRun).opTornWrite},
	{name: "compact", weight: 3, run: (*modelRun).opCompact},
	{name: "commit-tx", weight: 3, run: (*modelRun).opCommitTx},
	{name: "image-recover", weight: 2, run: (*modelRun).opImageRecover},
	{name: "verify", weight: 2, run: (*modelRun).opVerify},
	{name: "repair-noop", weight: 1, run: (*modelRun).opRepairNoop},

	{name: "xtx-commit", weight: 6, run: (*modelRun).opXTxCommit},
	{name: "xtx-rollback", weight: 4, run: (*modelRun).opXTxRollback},
	{name: "snapshot", weight: 2, run: (*modelRun).opSnapshot},
	{name: "index-persist", weight: 3, run: (*modelRun).opIndexPersist},
}

// modelRun is the state of one seeded run.
type modelRun struct {
	t      *testing.T
	seed   int64
	rng    *rand.Rand
	dir    string
	fs     *faultFS // fault seam of the current handle; replaced on every open
	db     *DB
	col    *Collection
	col2   *Collection
	state  map[uint64]map[string]any // expected live records in col
	state2 map[uint64]map[string]any // expected live records in col2
	gone   []uint64                  // ids that must be absent in col
	gone2  []uint64                  // ids that must be absent in col2
	trace  []string
	step   int
}

// modelRaceCompaction (SCRIVA_MODEL_RACE_COMPACTION=1) lets the rotation-
// triggered background compaction run concurrently with the next op. It is
// off by default so ordinary model runs stay deterministic.
var modelRaceCompaction = os.Getenv("SCRIVA_MODEL_RACE_COMPACTION") == "1"

// settleCompaction waits for background compaction triggered by the last op
// (segment rotation) to finish when deterministic model sequencing is wanted.
func (m *modelRun) settleCompaction() {
	for i := 0; i < 3; i++ {
		for len(m.col.compactC) > 0 || len(m.col2.compactC) > 0 {
			time.Sleep(time.Millisecond)
		}
		m.col.compactMu.Lock()   // waits out a pass in flight
		m.col.compactMu.Unlock() //nolint:staticcheck // barrier only
		m.col2.compactMu.Lock()
		m.col2.compactMu.Unlock() //nolint:staticcheck // barrier only
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

// putCol records id as live in col with data d. An id that was reserved by a failed
// insert/transaction is not durable and may legitimately be handed out again
// after a reopen, so it stops being "must be absent" once it is live again.
func (m *modelRun) putCol(col *Collection, id uint64, d map[string]any) {
	if col == m.col {
		m.state[id] = d
		kept := m.gone[:0]
		for _, g := range m.gone {
			if g != id { // an id can be listed more than once
				kept = append(kept, g)
			}
		}
		m.gone = kept
	} else {
		m.state2[id] = d
		kept := m.gone2[:0]
		for _, g := range m.gone2 {
			if g != id {
				kept = append(kept, g)
			}
		}
		m.gone2 = kept
	}
}

func (m *modelRun) pickIDFrom(st map[uint64]map[string]any) (uint64, bool) {
	if len(st) == 0 {
		return 0, false
	}
	ids := make([]uint64, 0, len(st))
	for id := range st {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] }) // map order is random
	return ids[m.rng.Intn(len(ids))], true
}

func (m *modelRun) pickColTarget() (*Collection, map[uint64]map[string]any, *[]uint64) {
	if m.rng.Intn(2) == 0 {
		return m.col, m.state, &m.gone
	}
	return m.col2, m.state2, &m.gone2
}

func (m *modelRun) pickColWithData() (*Collection, map[uint64]map[string]any, *[]uint64, uint64, bool) {
	has1 := len(m.state) > 0
	has2 := len(m.state2) > 0
	if !has1 && !has2 {
		return nil, nil, nil, 0, false
	}
	use1 := false
	if has1 && has2 {
		use1 = m.rng.Intn(2) == 0
	} else if has1 {
		use1 = true
	}
	if use1 {
		id, ok := m.pickIDFrom(m.state)
		return m.col, m.state, &m.gone, id, ok
	}
	id, ok := m.pickIDFrom(m.state2)
	return m.col2, m.state2, &m.gone2, id, ok
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
			m.fail("create collection c: %v", err)
		}
		if err := col.EnsureIndex(modelIndexField); err != nil {
			m.fail("ensure index c: %v", err)
		}
	}
	m.col = col

	col2, err := db.Collection("c2")
	if err != nil {
		if col2, err = db.CreateCollection("c2"); err != nil {
			m.fail("create collection c2: %v", err)
		}
		if err := col2.EnsureIndex(modelIndexField); err != nil {
			m.fail("ensure index c2: %v", err)
		}
	}
	m.col2 = col2
}

func (m *modelRun) opInsert() error {
	col, _, _ := m.pickColTarget()
	d := m.randData()
	id, _, err := col.Insert(d)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	st := m.state
	if col == m.col2 {
		st = m.state2
	}
	if _, dup := st[id]; dup {
		return fmt.Errorf("insert reused live id %d", id)
	}
	m.putCol(col, id, d)
	m.tracef("insert col=%s id=%d %v", col.name, id, d)
	return nil
}

func (m *modelRun) opUpdate() error {
	col, _, _, id, ok := m.pickColWithData()
	if !ok {
		return nil
	}
	d := m.randData()
	if _, err := col.Update(id, d); err != nil {
		return fmt.Errorf("update %d: %w", id, err)
	}
	m.putCol(col, id, d)
	m.tracef("update col=%s id=%d %v", col.name, id, d)
	return nil
}

func (m *modelRun) opDelete() error {
	col, st, gone, id, ok := m.pickColWithData()
	if !ok {
		return nil
	}
	if err := col.Delete(id); err != nil {
		return fmt.Errorf("delete %d: %w", id, err)
	}
	delete(st, id)
	*gone = append(*gone, id)
	m.tracef("delete col=%s id=%d", col.name, id)
	return nil
}

func (m *modelRun) opGet() error {
	col, st, _, id, ok := m.pickColWithData()
	if !ok {
		return nil
	}
	m.tracef("get col=%s id=%d", col.name, id)
	rec, err := col.Get(id)
	if err != nil {
		return fmt.Errorf("get %d: %w", id, err)
	}
	if !sameData(rec.Data, st[id]) {
		return fmt.Errorf("get %d = %v, model %v", id, rec.Data, st[id])
	}
	return nil
}

func (m *modelRun) opScan() error {
	col, st, _ := m.pickColTarget()
	m.tracef("scan col=%s", col.name)
	return m.checkScan(col, st, nil, func(map[string]any) bool { return true }, false)
}

func (m *modelRun) opScanIndex() error {
	col, st, _ := m.pickColTarget()
	tag := "t" + strconv.Itoa(m.rng.Intn(5))
	m.tracef("scan-index col=%s %s=%s", col.name, modelIndexField, tag)
	v, _ := json.Marshal(tag)
	f := &query.FieldFilter{Field: modelIndexField, Op: query.OpEq, Value: string(v)}
	return m.checkScan(col, st, f, func(d map[string]any) bool { return d[modelIndexField] == tag }, true)
}

// checkScan runs a filtered scan on col and compares it to the model's expectation.
func (m *modelRun) checkScan(col *Collection, st map[uint64]map[string]any, f query.Filter, want func(map[string]any) bool, wantIndex bool) error {
	got := map[uint64]map[string]any{}
	stats, err := col.ScanStream(context.Background(), ScanOptions{Filter: f}, func(r ScanResult) error {
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
	for id, d := range st {
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
	if err := m.checkDiskCol("c", m.state, m.gone); err != nil {
		return fmt.Errorf("col c: %w", err)
	}
	if err := m.checkDiskCol("c2", m.state2, m.gone2); err != nil {
		return fmt.Errorf("col c2: %w", err)
	}
	return nil
}

func (m *modelRun) checkDiskCol(colName string, st map[uint64]map[string]any, gone []uint64) error {
	rep := diskState(m.t, filepath.Join(m.dir, colName))
	if len(rep.Bad) != 0 {
		return fmt.Errorf("disk has undecodable lines: %v", rep.Bad)
	}
	disk := rep.model()
	if len(disk) != len(st) {
		return fmt.Errorf("disk has %d live records, model %d", len(disk), len(st))
	}
	for id, d := range st {
		dd, ok := disk[id]
		if !ok {
			return fmt.Errorf("id %d in model but not live on disk", id)
		}
		if !sameData(dd, d) {
			return fmt.Errorf("id %d disk data %v != model %v", id, dd, d)
		}
	}
	for _, id := range gone {
		if _, ok := disk[id]; ok {
			return fmt.Errorf("deleted id %d still live on disk", id)
		}
	}
	return nil
}

// verifyAll is the full post-op invariant: lookups, scan and absent ids for all collections.
func (m *modelRun) verifyAll() error {
	if err := m.verifyCol(m.col, m.state, m.gone); err != nil {
		return fmt.Errorf("col c: %w", err)
	}
	if err := m.verifyCol(m.col2, m.state2, m.gone2); err != nil {
		return fmt.Errorf("col c2: %w", err)
	}
	return nil
}

// verifyCol checks col against the model. It is also used on a recovered copy
// of the database, which must satisfy the same invariants as the live handle.
func (m *modelRun) verifyCol(col *Collection, st map[uint64]map[string]any, gone []uint64) error {
	for id, d := range st {
		rec, err := col.Get(id)
		if err != nil {
			return fmt.Errorf("get(%d): %w (model has it)", id, err)
		}
		if !sameData(rec.Data, d) {
			return fmt.Errorf("get(%d) = %v, model %v", id, rec.Data, d)
		}
	}
	for _, id := range gone {
		if _, err := col.Get(id); err == nil {
			return fmt.Errorf("get(%d) succeeded but id was deleted", id)
		}
	}
	if err := m.checkScan(col, st, nil, func(map[string]any) bool { return true }, false); err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		tag := "t" + strconv.Itoa(i)
		v, _ := json.Marshal(tag)
		f := &query.FieldFilter{Field: modelIndexField, Op: query.OpEq, Value: string(v)}
		if err := m.checkScan(col, st, f, func(d map[string]any) bool { return d[modelIndexField] == tag }, true); err != nil {
			return err
		}
		// Direct secondary-index lookup must agree with the model too.
		ids, ok := col.IndexLookup(modelIndexField, tag)
		if !ok {
			return fmt.Errorf("IndexLookup: no index on %q", modelIndexField)
		}
		var want []uint64
		for id, d := range st {
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
	m.col2.closeOnce.Do(func() { close(m.col2.closed) })
	m.col.persistWG.Wait()
	m.col2.persistWG.Wait()
	m.col.compactMu.Lock()
	m.col.compactMu.Unlock() //nolint:staticcheck // barrier only
	m.col2.compactMu.Lock()
	m.col2.compactMu.Unlock() //nolint:staticcheck // barrier only
	m.fs.releaseFDs()
	if err := m.db.lock.release(); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	if torn {
		targetCol := "c"
		if m.rng.Intn(2) == 1 {
			targetCol = "c2"
		}
		segs, _ := filepath.Glob(filepath.Join(m.dir, targetCol, "seg_*.ndjson"))
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
	col, st, gone := m.pickColTarget()
	kind := m.rng.Intn(3)
	prefix := m.rng.Intn(14)
	before := m.fs.faults()
	m.fs.failWriteAt(m.fs.count("write")+1, prefix, syscall.ENOSPC)
	defer m.disarmWrite()
	switch kind {
	case 0:
		d := m.randData()
		id, _, err := col.Insert(d)
		m.tracef("torn insert col=%s prefix=%d err=%v", col.name, prefix, err)
		if err == nil {
			m.putCol(col, id, d)
		}
		return m.tornResult(err, before)
	case 1:
		id, ok := m.pickIDFrom(st)
		if !ok {
			return nil
		}
		d := m.randData()
		_, err := col.Update(id, d)
		m.tracef("torn update col=%s id=%d prefix=%d err=%v", col.name, id, prefix, err)
		if err == nil {
			m.putCol(col, id, d)
		}
		return m.tornResult(err, before)
	default:
		id, ok := m.pickIDFrom(st)
		if !ok {
			return nil
		}
		err := col.Delete(id)
		m.tracef("torn delete col=%s id=%d prefix=%d err=%v", col.name, id, prefix, err)
		if err == nil {
			delete(st, id)
			*gone = append(*gone, id)
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
	col, _, _ := m.pickColTarget()
	m.tracef("compact col=%s", col.name)
	if err := col.CompactNow(); err != nil {
		return fmt.Errorf("compact col %s: %w", col.name, err)
	}
	return nil
}

// opCommitTx commits a small random transaction, half the time with an
// injected write failure. A failed commit must be all-or-nothing.
func (m *modelRun) opCommitTx() error {
	col, st, gone := m.pickColTarget()
	n := 1 + m.rng.Intn(4)
	used := map[uint64]bool{}
	var ops []txOp
	var inserted []uint64
	next := map[uint64]map[string]any{} // post-commit data; nil = deleted
	for i := 0; i < n; i++ {
		now := time.Now().UTC()
		switch k := m.rng.Intn(3); {
		case k == 0 || len(st) == 0:
			id := col.ReserveID()
			d := m.randData()
			ops = append(ops, txOp{kind: txOpInsert, id: id, data: d, ts: now})
			inserted = append(inserted, id)
			next[id] = d
		default:
			id, ok := m.pickIDFrom(st)
			if !ok || used[id] {
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
	err := col.CommitTx(ops)
	m.tracef("commit-tx col=%s ops=%d inject=%v err=%v", col.name, len(ops), inject, err)
	if err != nil {
		if !errors.Is(err, syscall.ENOSPC) || m.fs.faults() == before {
			return fmt.Errorf("commit: %w", err)
		}
		*gone = append(*gone, inserted...) // reserved ids must stay absent
		return nil
	}
	for id, d := range next {
		if d == nil {
			delete(st, id)
			*gone = append(*gone, id)
		} else {
			m.putCol(col, id, d)
		}
	}
	return nil
}

// opXTxRollback stages operations across multiple collections and rolls them back.
func (m *modelRun) opXTxRollback() error {
	m.tracef("xtx-rollback")
	ctx := context.Background()
	tx, err := m.db.BeginXTx(ctx, []string{"c", "c2"}, XTxOptions{})
	if err != nil {
		return fmt.Errorf("begin xtx: %w", err)
	}

	n := 1 + m.rng.Intn(4)
	usedC := map[uint64]bool{}
	usedC2 := map[uint64]bool{}
	var provC, provC2 []uint64
	for i := 0; i < n; i++ {
		which := m.rng.Intn(2)
		colName := "c"
		st := m.state
		used := usedC
		if which == 1 {
			colName = "c2"
			st = m.state2
			used = usedC2
		}

		switch k := m.rng.Intn(3); {
		case k == 0 || len(st) == 0:
			d := m.randData()
			id, err := tx.Insert(colName, d)
			if err != nil {
				return fmt.Errorf("stage insert: %w", err)
			}
			if which == 0 {
				provC = append(provC, id)
			} else {
				provC2 = append(provC2, id)
			}
			rec, gerr := tx.Get(colName, id)
			if gerr != nil || !sameData(rec.Data, d) {
				return fmt.Errorf("tx get staged insert mismatch: rec=%v err=%v", rec, gerr)
			}
		default:
			id, ok := m.pickIDFrom(st)
			if !ok || used[id] {
				continue
			}
			used[id] = true
			if k == 1 {
				d := m.randData()
				if err := tx.Update(colName, id, d); err != nil {
					return fmt.Errorf("stage update: %w", err)
				}
				rec, gerr := tx.Get(colName, id)
				if gerr != nil || !sameData(rec.Data, d) {
					return fmt.Errorf("tx get staged update mismatch: rec=%v err=%v", rec, gerr)
				}
			} else {
				if err := tx.Delete(colName, id); err != nil {
					return fmt.Errorf("stage delete: %w", err)
				}
				if _, gerr := tx.Get(colName, id); gerr == nil {
					return fmt.Errorf("tx get staged delete should have failed")
				}
			}
		}
	}

	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}
	m.gone = append(m.gone, provC...)
	m.gone2 = append(m.gone2, provC2...)
	return nil
}

// opXTxCommit commits a cross-collection transaction across c and c2, optionally
// with write fault injection. A failed commit must be all-or-nothing.
func (m *modelRun) opXTxCommit() error {
	ctx := context.Background()
	key := fmt.Sprintf("model-xtx-%d-%d", m.seed, m.step)
	tx, err := m.db.BeginXTx(ctx, []string{"c", "c2"}, XTxOptions{Key: key})
	if err != nil {
		return fmt.Errorf("begin xtx: %w", err)
	}

	n := 1 + m.rng.Intn(4)
	usedC := map[uint64]bool{}
	usedC2 := map[uint64]bool{}
	var provC, provC2 []uint64
	nextC := map[uint64]map[string]any{}
	nextC2 := map[uint64]map[string]any{}

	for i := 0; i < n; i++ {
		which := m.rng.Intn(2)
		colName := "c"
		st := m.state
		used := usedC
		next := nextC
		prov := &provC
		if which == 1 {
			colName = "c2"
			st = m.state2
			used = usedC2
			next = nextC2
			prov = &provC2
		}

		switch k := m.rng.Intn(3); {
		case k == 0 || len(st) == 0:
			d := m.randData()
			id, err := tx.Insert(colName, d)
			if err != nil {
				return fmt.Errorf("stage insert: %w", err)
			}
			*prov = append(*prov, id)
			next[id] = d
		default:
			id, ok := m.pickIDFrom(st)
			if !ok || used[id] {
				continue
			}
			used[id] = true
			if k == 1 {
				d := m.randData()
				if err := tx.Update(colName, id, d); err != nil {
					return fmt.Errorf("stage update: %w", err)
				}
				next[id] = d
			} else {
				if err := tx.Delete(colName, id); err != nil {
					return fmt.Errorf("stage delete: %w", err)
				}
				next[id] = nil
			}
		}
	}

	inject := m.rng.Intn(3) == 0
	before := m.fs.faults()
	if inject {
		m.fs.failWriteAt(m.fs.count("write")+1+m.rng.Intn(3), m.rng.Intn(10), syscall.ENOSPC)
		defer m.disarmWrite()
	}

	res, err := tx.Commit(ctx)
	m.tracef("xtx-commit ops=%d inject=%v err=%v", n, inject, err)
	if err != nil {
		if m.fs.faults() == before {
			return fmt.Errorf("xtx commit failed without an injected fault: %w", err)
		}
		if errors.Is(err, ErrXTxOutcomeUnknown) {
			st, _ := m.db.XTxStatus(key)
			if st == XTxCommitted {
				for id, d := range nextC {
					if d == nil {
						delete(m.state, id)
						m.gone = append(m.gone, id)
					} else {
						m.putCol(m.col, id, d)
					}
				}
				for id, d := range nextC2 {
					if d == nil {
						delete(m.state2, id)
						m.gone2 = append(m.gone2, id)
					} else {
						m.putCol(m.col2, id, d)
					}
				}
				return nil
			}
		}
		m.gone = append(m.gone, provC...)
		m.gone2 = append(m.gone2, provC2...)
		return nil
	}

	_ = res
	for id, d := range nextC {
		if d == nil {
			delete(m.state, id)
			m.gone = append(m.gone, id)
		} else {
			m.putCol(m.col, id, d)
		}
	}
	for id, d := range nextC2 {
		if d == nil {
			delete(m.state2, id)
			m.gone2 = append(m.gone2, id)
		} else {
			m.putCol(m.col2, id, d)
		}
	}
	return nil
}

// opSnapshot creates a full database snapshot archive, restores it into a fresh
// directory, opens it, and requires the restored DB to agree with the model.
func (m *modelRun) opSnapshot() error {
	m.tracef("snapshot")
	var buf bytes.Buffer
	if err := m.db.SnapshotTo(&buf); err != nil {
		return fmt.Errorf("snapshot to: %w", err)
	}
	dst := m.t.TempDir()
	extractTarGz(m.t, buf.Bytes(), dst)

	db, err := Open(dst, CollectionConfig{CompactInterval: 24 * 3600 * 1e9, SegmentMaxSize: modelSegmentMax})
	if err != nil {
		return fmt.Errorf("open restored snapshot: %w", err)
	}
	defer db.Close()

	col1, err := db.Collection("c")
	if err != nil {
		return fmt.Errorf("snapshot collection c: %w", err)
	}
	if err := m.verifyCol(col1, m.state, m.gone); err != nil {
		return fmt.Errorf("snapshot verify c: %w", err)
	}

	col2, err := db.Collection("c2")
	if err != nil {
		return fmt.Errorf("snapshot collection c2: %w", err)
	}
	if err := m.verifyCol(col2, m.state2, m.gone2); err != nil {
		return fmt.Errorf("snapshot verify c2: %w", err)
	}
	return nil
}

// opIndexPersist explicitly flushes index persistence on all collections.
func (m *modelRun) opIndexPersist() error {
	m.tracef("index-persist")
	if err := m.col.persistIndexes(); err != nil {
		return fmt.Errorf("persist indexes c: %w", err)
	}
	if err := m.col2.persistIndexes(); err != nil {
		return fmt.Errorf("persist indexes c2: %w", err)
	}
	return nil
}

// opImageRecover opens a copy of the live directory (a crash image: nothing
// flushed, index.json/sidx files possibly stale) and requires the recovered
// copy to agree with the model, including secondary-index lookups.
func (m *modelRun) opImageRecover() error {
	m.tracef("image-recover")
	m.col.compactMu.Lock() // no segment swap mid-copy
	m.col2.compactMu.Lock()
	img := copyDataDir(m.t, m.dir)
	m.col2.compactMu.Unlock()
	m.col.compactMu.Unlock()
	db, err := Open(img, CollectionConfig{CompactInterval: 24 * 3600 * 1e9, SegmentMaxSize: modelSegmentMax})
	if err != nil {
		return fmt.Errorf("open crash image: %w", err)
	}
	defer db.Close()
	col, err := db.Collection("c")
	if err != nil {
		return fmt.Errorf("image collection c: %w", err)
	}
	if err := m.verifyCol(col, m.state, m.gone); err != nil {
		return fmt.Errorf("image collection c: %w", err)
	}
	col2, err := db.Collection("c2")
	if err != nil {
		return fmt.Errorf("image collection c2: %w", err)
	}
	return m.verifyCol(col2, m.state2, m.gone2)
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
	before1 := segHashes(m.t, filepath.Join(img, "c"))
	before2 := segHashes(m.t, filepath.Join(img, "c2"))
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
	if after1 := segHashes(m.t, filepath.Join(img, "c")); after1 != before1 {
		return fmt.Errorf("repair changed segment bytes of a healthy copy (c)")
	}
	if after2 := segHashes(m.t, filepath.Join(img, "c2")); after2 != before2 {
		return fmt.Errorf("repair changed segment bytes of a healthy copy (c2)")
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
		state2: map[uint64]map[string]any{},
	}
	m.open()
	t.Cleanup(func() { _ = m.db.Close() })
	for m.step = 1; m.step <= steps; m.step++ {
		op := pickOp(m.rng, ops)
		seq := m.col.segSeq.Load()
		seq2 := m.col2.segSeq.Load()
		if err := op.run(m); err != nil {
			m.fail("%s: %v", op.name, err)
		}
		if !modelRaceCompaction && (m.col.segSeq.Load() != seq || m.col2.segSeq.Load() != seq2) {
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
	m := &modelRun{t: t, rng: rand.New(rand.NewSource(1)), dir: t.TempDir(), state: map[uint64]map[string]any{}, state2: map[uint64]map[string]any{}}
	m.open()
	defer func() { _ = m.db.Close() }()
	if err := m.opInsert(); err != nil {
		t.Fatal(err)
	}
	for id := range m.state {
		m.state[id]["n"] = float64(-1)
	}
	for id := range m.state2 {
		m.state2[id]["n"] = float64(-1)
	}
	if err := m.verifyAll(); err == nil {
		t.Fatal("verifyAll missed a data divergence")
	}
}
