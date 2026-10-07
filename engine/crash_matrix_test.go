package engine

// Single-writer crash matrix.
//
// Every case names the boundary it exercises, runs a workload while tracking
// the *acknowledged* state (an op is acked only when the call returned nil),
// simulates a crash at that boundary, reopens the directory with a fresh
// handle and asserts:
//
//   - every acknowledged write is present, with its latest value;
//   - no deleted record is resurrected, no unknown record appears;
//   - at most the single in-flight op (the one that was running when the crash
//     hit) is visible, and then fully (never torn);
//   - Get == Scan == on-disk truth, and the secondary index agrees with Scan.
//
// Boundaries come from three mechanisms:
//
//  1. Step enumeration: the faultFS counts every segment write / fsync /
//     truncate / compactor rename; the workload is first dry-run to learn how
//     many steps it takes, then re-run once per step with crashAfter(k). Each
//     subtest is named after the exact next op that never happened.
//  2. Directory snapshots at named points (compaction hooks, rotation, Close)
//     for boundaries the faultFS does not intercept (manifest, index persist,
//     file removal, meta persist). The snapshot is what a kill -9 at that
//     instant leaves on disk; it is reopened as a fresh directory.
//  3. Hand-built states for the remaining sub-steps (partial removals, torn
//     index files, index/sidx persisted at different times).
//
// SyncModeNone: a process crash (kill -9) keeps every write() the kernel
// accepted, so the simulated crash is state-equivalent and the same assertions
// hold; the weaker "may lose the tail" contract applies to power loss, which
// this suite does not simulate.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

type cmPend struct {
	kind     string // insert | update | delete
	id       uint64 // 0 for insert (unknown)
	old, new map[string]any
}

// cmRun tracks the acknowledged state while a workload drives a collection.
type cmRun struct {
	t          *testing.T
	col        *Collection
	live       map[uint64]map[string]any
	deleted    map[uint64]bool
	vals       map[string]bool
	pend       *cmPend
	dead       bool // a non-definitive error stopped the workload
	definitive bool // errors are clean rejections (no crash): keep going
	seq        int
}

func newCmRun(t *testing.T, col *Collection, definitive bool) *cmRun {
	return &cmRun{t: t, col: col, live: map[uint64]map[string]any{}, deleted: map[uint64]bool{},
		vals: map[string]bool{}, definitive: definitive}
}

func (r *cmRun) data(v string) map[string]any {
	r.seq++
	r.vals[v] = true
	return map[string]any{"v": v, "i": fmt.Sprint(r.seq)}
}

func (r *cmRun) fail() {
	if r.definitive {
		r.pend = nil
	} else {
		r.dead = true // pend stays: the op is in flight at the crash
	}
}

func (r *cmRun) insert(v string) uint64 {
	if r.dead {
		return 0
	}
	d := r.data(v)
	r.pend = &cmPend{kind: "insert", new: d}
	id, _, err := r.col.Insert(d)
	if err != nil {
		if r.definitive {
			r.pend = nil
		} else {
			r.dead = true
		}
		return 0
	}
	r.pend = nil
	r.live[id] = d
	return id
}

func (r *cmRun) update(id uint64, v string) {
	if r.dead || id == 0 {
		return
	}
	d := r.data(v)
	r.pend = &cmPend{kind: "update", id: id, old: r.live[id], new: d}
	if _, err := r.col.Update(id, d); err != nil {
		r.fail()
		return
	}
	r.pend = nil
	r.live[id] = d
}

func (r *cmRun) del(id uint64) {
	if r.dead || id == 0 {
		return
	}
	r.pend = &cmPend{kind: "delete", id: id, old: r.live[id]}
	if err := r.col.Delete(id); err != nil {
		r.fail()
		return
	}
	r.pend = nil
	delete(r.live, id)
	r.deleted[id] = true
}

func (r *cmRun) step(err error) {
	if err != nil && !r.definitive {
		r.dead = true
	}
}

// ---- verification -----------------------------------------------------------

// cmVerify checks the recovered collection in db (rooted at dir) against the
// acknowledged state in r.
func cmVerify(t *testing.T, db *DB, dir string, r *cmRun, allowBad bool) {
	t.Helper()
	col, err := db.Collection("c")
	if err != nil {
		t.Fatalf("collection: %v", err)
	}
	res, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	scan := map[uint64]map[string]any{}
	for _, rec := range res {
		scan[rec.ID] = rec.Data
	}
	extras := 0
	for id, want := range r.live {
		got, ok := scan[id]
		p := r.pend
		switch {
		case p != nil && p.id == id && p.kind == "update":
			if !ok || !(sameData(got, p.old) || sameData(got, p.new)) {
				t.Errorf("id %d: want old %v or in-flight new %v, got %v (present=%v)", id, p.old, p.new, got, ok)
			}
		case p != nil && p.id == id && p.kind == "delete":
			if ok && !sameData(got, p.old) {
				t.Errorf("id %d: in-flight delete left torn data %v", id, got)
			}
		default:
			if !ok {
				t.Errorf("LOST ACK: id %d %v missing after recovery", id, want)
			} else if !sameData(got, want) {
				t.Errorf("STALE: id %d got %v want %v", id, got, want)
			}
		}
	}
	for id, got := range scan {
		if _, ok := r.live[id]; ok {
			continue
		}
		if r.deleted[id] {
			t.Errorf("RESURRECTED: deleted id %d reappeared as %v", id, got)
			continue
		}
		if p := r.pend; p != nil && p.kind == "insert" && sameData(got, p.new) {
			extras++
			continue
		}
		if p := r.pend; p != nil && p.kind == "delete" && p.id == id {
			continue // handled above
		}
		t.Errorf("GHOST: unexpected id %d %v", id, got)
	}
	if extras > 1 {
		t.Errorf("more than one in-flight insert visible")
	}
	var deleted []uint64
	for id := range r.deleted {
		deleted = append(deleted, id)
	}
	assertLookupScanAgree(t, col, scan, deleted...)

	// Disk truth.
	rep := diskState(t, colDirOf(dir))
	if len(rep.Bad) != 0 && !allowBad {
		t.Errorf("undecodable lines on disk: %v", rep.Bad)
	}
	for id := range scan {
		if _, ok := rep.Live[id]; !ok {
			t.Errorf("id %d served but not live on disk", id)
		}
	}
	for id := range rep.Live {
		if _, ok := scan[id]; !ok {
			t.Errorf("id %d live on disk but not served", id)
		}
	}
	// Secondary index == scan, for every value ever written.
	for v := range r.vals {
		var want []uint64
		for id, d := range scan {
			if d["v"] == v {
				want = append(want, id)
			}
		}
		got, ok := col.IndexLookup("v", v)
		if !ok {
			t.Errorf("secondary index on v missing after recovery")
			break
		}
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("sidx v=%q lookup %v != scan %v", v, got, want)
		}
	}
}

// ---- workloads ---------------------------------------------------------------

type cmCase struct {
	name string
	cfg  CollectionConfig
	work func(r *cmRun)
}

func cmBaseCfg(mode SyncMode, segMax int64) CollectionConfig {
	return CollectionConfig{
		SyncMode:             mode,
		SegmentMaxSize:       segMax,
		CompactInterval:      24 * time.Hour,
		CompactDirtyPct:      2, // unforced background passes never run
		IndexPersistInterval: -1,
	}
}

func workAppend(r *cmRun) {
	r.step(r.col.EnsureIndex("v"))
	a := r.insert("a")
	b := r.insert("b")
	r.insert("a")
	r.update(a, "c")
	r.del(b)
	d := r.insert("b")
	r.update(d, "a")
	r.update(a, "b")
	r.del(a)
	r.insert("c")
}

func workRotation(r *cmRun) {
	r.step(r.col.EnsureIndex("v"))
	var ids []uint64
	for i := 0; i < 6; i++ {
		ids = append(ids, r.insert(fmt.Sprint("v", i%3)))
	}
	if !r.dead {
		r.step(r.col.rotateSegment())
	}
	r.update(ids[0], "v9")
	r.del(ids[1])
	if !r.dead {
		r.step(r.col.rotateSegment())
	}
	r.update(ids[2], "v1")
	r.insert("v2")
}

func workCompaction(r *cmRun) {
	r.step(r.col.EnsureIndex("v"))
	var ids []uint64
	for i := 0; i < 5; i++ {
		ids = append(ids, r.insert("old"))
	}
	if !r.dead {
		r.step(r.col.rotateSegment())
	}
	for _, id := range ids {
		r.update(id, "new")
	}
	r.del(ids[0])
	if !r.dead {
		r.step(r.col.rotateSegment())
	}
	r.insert("tail")
	if !r.dead {
		r.step(r.col.CompactNow())
	}
	r.insert("after")
}

// ---- step-enumeration matrix ---------------------------------------------------

func TestCrashMatrix_StepBoundaries(t *testing.T) {
	cases := []cmCase{
		{"append", cmBaseCfg(SyncModeAlways, 1<<20), workAppend},
		{"rotation", cmBaseCfg(SyncModeAlways, 1<<20), workRotation},
		{"rotation_small_segments", cmBaseCfg(SyncModeAlways, 170), workRotation},
		{"compaction", cmBaseCfg(SyncModeAlways, 1<<20), workCompaction},
		{"compaction_small_segments", cmBaseCfg(SyncModeAlways, 150), workCompaction},
		{"append_syncnone", cmBaseCfg(SyncModeNone, 1<<20), workAppend},
		{"rotation_syncnone", cmBaseCfg(SyncModeNone, 170), workRotation},
		{"compaction_syncnone", cmBaseCfg(SyncModeNone, 150), workCompaction},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Dry run: learn the step trace of the workload.
			dry := newFaultFS()
			_, dcol := openFaulty(t, t.TempDir(), dry, tc.cfg)
			base := dry.totalSteps()
			tc.work(newCmRun(t, dcol, false))
			total := dry.totalSteps() - base
			if total < 3 {
				t.Fatalf("workload produced only %d steps", total)
			}
			trace := make([]string, total+1)
			for k := 0; k <= total; k++ {
				trace[k] = dry.opAt(base + k)
			}
			dry.kill()
			dry.releaseFDs()

			for k := 0; k <= total; k++ {
				name := fmt.Sprintf("crash_after_%02d_steps_before_%s", k, trace[k])
				t.Run(name, func(t *testing.T) {
					dir := t.TempDir()
					fs := newFaultFS()
					db, col := openFaulty(t, dir, fs, tc.cfg)
					fs.crashAfter(fs.totalSteps() + k)
					r := newCmRun(t, col, false)
					tc.work(r)
					// Background activity can shift step counts slightly; a crash
					// point past the end degenerates to a clean abandon, which
					// must satisfy the same invariants.
					cfg := tc.cfg
					cfg.wrapFile, cfg.renameFn = nil, nil
					db2 := reopenAfterCrash(t, db, fs, dir, cfg)
					cmVerify(t, db2, dir, r, false)
				})
			}
		})
	}
}

// ---- append: short writes (rejected, rolled back, writer keeps going) --------------

func TestCrashMatrix_ShortWrite(t *testing.T) {
	cfg := cmBaseCfg(SyncModeAlways, 1<<20)
	dry := newFaultFS()
	_, dcol := openFaulty(t, t.TempDir(), dry, cfg)
	base := dry.count("write")
	workAppend(newCmRun(t, dcol, true))
	nWrites := dry.count("write") - base
	if nWrites < 5 {
		t.Fatalf("only %d writes", nWrites)
	}
	dry.kill()
	dry.releaseFDs()

	for n := 1; n <= nWrites; n++ {
		for _, prefix := range []int{0, 1, 9, 25, 10000} { // 10000 = whole line, then ENOSPC
			t.Run(fmt.Sprintf("write%02d_persist%d_bytes", n, prefix), func(t *testing.T) {
				dir := t.TempDir()
				fs := newFaultFS()
				db, col := openFaulty(t, dir, fs, cfg)
				fs.failWriteAt(fs.count("write")+n, prefix, syscall.ENOSPC)
				r := newCmRun(t, col, true)
				workAppend(r) // failed ops are rejected; the rest continue
				if fs.faults() != 1 {
					t.Fatalf("fault not injected (%d)", fs.faults())
				}
				c2 := cfg
				db2 := reopenAfterCrash(t, db, fs, dir, c2)
				cmVerify(t, db2, dir, r, false)
			})
		}
	}
}

// A process killed mid-write leaves a torn final line. Whatever prefix of the
// next record reached the file, reopen serves exactly the acked state and the
// collection stays writable.
func TestCrashMatrix_TornTail(t *testing.T) {
	line, err := store.Encode(store.NewInsert(999, map[string]any{"v": "torn", "i": "x"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{1, 2, 7, len(line) / 2, len(line) - 2, len(line) - 1} {
		t.Run(fmt.Sprintf("prefix_%d_of_%d", cut, len(line)), func(t *testing.T) {
			dir := t.TempDir()
			fs := newFaultFS()
			cfg := cmBaseCfg(SyncModeAlways, 1<<20)
			db, col := openFaulty(t, dir, fs, cfg)
			r := newCmRun(t, col, false)
			workAppend(r)
			fs.kill()
			fs.releaseFDs()
			if err := db.lock.release(); err != nil {
				t.Fatal(err)
			}
			segs, _ := filepath.Glob(filepath.Join(colDirOf(dir), "seg_*.ndjson"))
			sort.Strings(segs)
			f, err := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(line[:cut]); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()

			db2, err := Open(dir, cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			col2, _ := db2.Collection("c")
			// The torn bytes may remain on disk, but must not be served.
			cmVerify(t, db2, dir, r, true)
			// And the collection must accept (and keep) new writes.
			r.col = col2
			id := r.insert("post")
			if id == 0 {
				t.Fatal("insert after torn-tail recovery failed")
			}
			if err := db2.Close(); err != nil {
				t.Fatal(err)
			}
			db3, err := Open(dir, cfg)
			if err != nil {
				t.Fatalf("second reopen: %v", err)
			}
			t.Cleanup(func() { _ = db3.Close() })
			cmVerify(t, db3, dir, r, true)
		})
	}
}

// ---- directory-state helpers ---------------------------------------------------

// openSnapshot opens a copy of the collection directory src (as it was at the
// snapshot instant) under a fresh root and returns it with its root.
func openSnapshot(t *testing.T, src string, cfg CollectionConfig) (*DB, string) {
	t.Helper()
	root := t.TempDir()
	copyDir(t, src, colDirOf(root))
	cfg.wrapFile, cfg.renameFn, cfg.preSwapHook, cfg.preSidxRebuildHook = nil, nil, nil, nil
	db, err := Open(root, cfg)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, root
}

func snapshotOf(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	copyDir(t, src, dst)
	return dst
}

func abandon(t *testing.T, db *DB, fs *faultFS) {
	t.Helper()
	fs.kill()
	fs.releaseFDs()
	_ = db.lock.release()
}

func copyFileIn(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ---- rotation ------------------------------------------------------------------

func TestCrashMatrix_RotationBoundaries(t *testing.T) {
	cfg := cmBaseCfg(SyncModeAlways, 1<<20)
	setup := func(t *testing.T) (*DB, *faultFS, string, *cmRun) {
		dir := t.TempDir()
		fs := newFaultFS()
		db, col := openFaulty(t, dir, fs, cfg)
		r := newCmRun(t, col, false)
		workAppend(r)
		return db, fs, dir, r
	}
	t.Run("before_seal", func(t *testing.T) {
		db, fs, dir, r := setup(t)
		snap := snapshotOf(t, colDirOf(dir))
		abandon(t, db, fs)
		db2, root := openSnapshot(t, snap, cfg)
		cmVerify(t, db2, root, r, false)
	})
	t.Run("after_seal_before_new_segment", func(t *testing.T) {
		db, fs, dir, r := setup(t)
		if err := r.col.rotateSegment(); err != nil {
			t.Fatal(err)
		}
		snap := snapshotOf(t, colDirOf(dir))
		abandon(t, db, fs)
		segs, _ := filepath.Glob(filepath.Join(snap, "seg_*.ndjson"))
		sort.Strings(segs)
		if len(segs) < 2 {
			t.Fatalf("expected a new segment, got %v", segs)
		}
		if err := os.Remove(segs[len(segs)-1]); err != nil { // new segment not created yet
			t.Fatal(err)
		}
		db2, root := openSnapshot(t, snap, cfg)
		cmVerify(t, db2, root, r, false)
		r.col, _ = db2.Collection("c")
		if r.insert("c") == 0 {
			t.Fatal("cannot write after recovering from a missing active segment")
		}
		cmVerify(t, db2, root, r, false)
	})
	t.Run("after_new_segment_before_meta_persist", func(t *testing.T) {
		db, fs, dir, r := setup(t)
		oldMeta := snapshotOf(t, colDirOf(dir))
		if err := r.col.rotateSegment(); err != nil {
			t.Fatal(err)
		}
		snap := snapshotOf(t, colDirOf(dir))
		abandon(t, db, fs)
		if _, err := os.Stat(filepath.Join(oldMeta, metaFilename)); err == nil {
			copyFileIn(t, filepath.Join(oldMeta, metaFilename), filepath.Join(snap, metaFilename))
		} else {
			_ = os.Remove(filepath.Join(snap, metaFilename))
		}
		db2, root := openSnapshot(t, snap, cfg)
		cmVerify(t, db2, root, r, false)
		// ids must not be reused after the stale-meta recovery.
		r.col, _ = db2.Collection("c")
		before := len(r.live)
		if r.insert("c") == 0 || len(r.live) != before+1 {
			t.Fatal("insert after stale meta lost or clobbered a record")
		}
		cmVerify(t, db2, root, r, false)
	})
}

// ---- compaction swap -------------------------------------------------------------

func TestCrashMatrix_CompactionSwap(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeAlways, SyncModeNone} {
		t.Run(fmt.Sprint("mode", mode), func(t *testing.T) {
			cfg := cmBaseCfg(mode, 1<<20)
			dir := t.TempDir()
			fs := newFaultFS()
			var preSwap, postPrimary string
			cfg.preSwapHook = func() { preSwap = snapshotOf(t, colDirOf(dir)) }
			cfg.preSidxRebuildHook = func() { postPrimary = snapshotOf(t, colDirOf(dir)) }
			db, col := openFaulty(t, dir, fs, cfg)
			r := newCmRun(t, col, false)
			// workCompaction ends with CompactNow then an insert; run it as-is.
			workCompaction(r)
			if r.dead || preSwap == "" || postPrimary == "" {
				t.Fatalf("setup failed dead=%v hooks=%q %q", r.dead, preSwap, postPrimary)
			}
			finalSnap := snapshotOf(t, colDirOf(dir))
			abandon(t, db, fs)

			// Compaction acks nothing new: the insert following it ("after") is
			// acked after the swap, so earlier-state snapshots must be compared
			// without it.
			rBefore := *r
			rBefore.live = map[uint64]map[string]any{}
			for id, d := range r.live {
				if d["v"] != "after" {
					rBefore.live[id] = d
				}
			}
			checkSnap := func(t *testing.T, snap string) {
				db2, root := openSnapshot(t, snap, cfg)
				cmVerify(t, db2, root, &rBefore, false)
				// The recovered collection must stay writable and compactable.
				c2, _ := db2.Collection("c")
				rr := newCmRun(t, c2, false)
				rr.live, rr.deleted, rr.vals = copyLive(rBefore.live), rBefore.deleted, rBefore.vals
				if rr.insert("post") == 0 {
					t.Fatal("insert after recovery failed")
				}
				if err := c2.CompactNow(); err != nil {
					t.Fatalf("compact after recovery: %v", err)
				}
				cmVerify(t, db2, root, rr, false)
			}

			t.Run("after_manifest_before_first_rename", func(t *testing.T) { checkSnap(t, preSwap) })

			m := readManifestOf(t, preSwap)
			// After each rename (renames applied in order), before removals.
			var srcs []string
			for src := range m.Renames {
				srcs = append(srcs, src)
			}
			sort.Strings(srcs)
			for i := 1; i <= len(srcs); i++ {
				t.Run(fmt.Sprintf("after_rename_%d_of_%d", i, len(srcs)), func(t *testing.T) {
					snap := snapshotOf(t, preSwap)
					applyManifestPartial(t, snap, m, srcs, i, 0)
					checkSnap(t, snap)
				})
			}
			for j := 1; j <= len(m.Removals); j++ {
				t.Run(fmt.Sprintf("after_removal_%d_of_%d", j, len(m.Removals)), func(t *testing.T) {
					snap := snapshotOf(t, preSwap)
					applyManifestPartial(t, snap, m, srcs, len(srcs), j)
					checkSnap(t, snap)
				})
			}
			t.Run("after_primary_persist_before_sidx_persist", func(t *testing.T) { checkSnap(t, postPrimary) })
			t.Run("after_sidx_persist_before_manifest_clear", func(t *testing.T) {
				snap := snapshotOf(t, finalSnap)
				copyFileIn(t, filepath.Join(preSwap, compactManifestFilename), filepath.Join(snap, compactManifestFilename))
				// The snapshot also contains the post-swap "after" insert; drop
				// to the pre-"after" logical state is not possible here, so
				// verify against the full acked state.
				db2, root := openSnapshot(t, snap, cfg)
				cmVerify(t, db2, root, r, false)
			})
			t.Run("after_manifest_clear", func(t *testing.T) {
				db2, root := openSnapshot(t, finalSnap, cfg)
				cmVerify(t, db2, root, r, false)
			})
		})
	}
}

func copyLive(m map[uint64]map[string]any) map[uint64]map[string]any {
	o := make(map[uint64]map[string]any, len(m))
	for k, v := range m {
		o[k] = v
	}
	return o
}

func readManifestOf(t *testing.T, dir string) compactManifest {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, compactManifestFilename))
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	var m compactManifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// applyManifestPartial rewrites paths in m (recorded relative to the original
// directory) onto snap, then applies the first nRen renames and nRem removals,
// leaving the manifest in place — the state a crash mid-swap leaves.
func applyManifestPartial(t *testing.T, snap string, m compactManifest, srcs []string, nRen, nRem int) {
	t.Helper()
	at := func(p string) string { return filepath.Join(snap, filepath.Base(p)) }
	// The manifest in the snapshot names the original directory; rewrite it so
	// recovery in the copied location resolves.
	nm := compactManifest{Renames: map[string]string{}}
	for s, d := range m.Renames {
		nm.Renames[at(s)] = at(d)
	}
	for _, p := range m.Removals {
		nm.Removals = append(nm.Removals, at(p))
	}
	if err := writeCompactManifest(snap, nm); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < nRen; i++ {
		if err := os.Rename(at(srcs[i]), at(m.Renames[srcs[i]])); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < nRem; i++ {
		_ = os.Remove(at(m.Removals[i]))
	}
}

// ---- index persist -----------------------------------------------------------------

func TestCrashMatrix_IndexPersist(t *testing.T) {
	cfg := cmBaseCfg(SyncModeAlways, 1<<20)
	build := func(t *testing.T) (string, *cmRun) {
		dir := t.TempDir()
		db, col := openFaulty(t, dir, newFaultFS(), cfg)
		r := newCmRun(t, col, false)
		workAppend(r)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return dir, r
	}
	for _, file := range []string{"index.json", "sidx_v.json"} {
		for _, frac := range []float64{0, 0.1, 0.5, 0.9, -1} { // -1 = drop last byte
			t.Run(fmt.Sprintf("torn_%s_%v", file, frac), func(t *testing.T) {
				dir, r := build(t)
				p := filepath.Join(colDirOf(dir), file)
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				n := int(float64(len(b)) * frac)
				if frac < 0 {
					n = len(b) - 1
				}
				if err := os.WriteFile(p, b[:n], 0o644); err != nil {
					t.Fatal(err)
				}
				db2, err := Open(dir, cfg)
				if err != nil {
					t.Fatalf("reopen with torn %s: %v", file, err)
				}
				t.Cleanup(func() { _ = db2.Close() })
				cmVerify(t, db2, dir, r, false)
			})
		}
		t.Run("garbage_"+file, func(t *testing.T) {
			dir, r := build(t)
			if err := os.WriteFile(filepath.Join(colDirOf(dir), file), []byte("{\"not\":\"an index\""), 0o644); err != nil {
				t.Fatal(err)
			}
			db2, err := Open(dir, cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = db2.Close() })
			cmVerify(t, db2, dir, r, false)
		})
	}

	// Crash between the primary and secondary persist: one is from an earlier
	// moment than the other, in both directions.
	for _, newer := range []string{"primary_newer_than_sidx", "sidx_newer_than_primary"} {
		t.Run(newer, func(t *testing.T) {
			dir := t.TempDir()
			db, col := openFaulty(t, dir, newFaultFS(), cfg)
			r := newCmRun(t, col, false)
			r.step(col.EnsureIndex("v"))
			a := r.insert("a")
			r.insert("b")
			if err := col.persistIndexes(); err != nil {
				t.Fatal(err)
			}
			old := snapshotOf(t, colDirOf(dir))
			r.update(a, "c")
			r.insert("a")
			r.del(a)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cd := colDirOf(dir)
			if newer == "primary_newer_than_sidx" {
				copyFileIn(t, filepath.Join(old, "sidx_v.json"), filepath.Join(cd, "sidx_v.json"))
			} else {
				copyFileIn(t, filepath.Join(old, "index.json"), filepath.Join(cd, "index.json"))
			}
			db2, err := Open(dir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db2.Close() })
			cmVerify(t, db2, dir, r, false)
		})
	}
}

// ---- shutdown -----------------------------------------------------------------------

func TestCrashMatrix_Shutdown(t *testing.T) {
	cfg := cmBaseCfg(SyncModeAlways, 150)
	// Close persists, in order: index.json, sidx files, meta. A crash inside
	// Close leaves any prefix of that sequence.
	steps := []struct {
		name  string
		files []string // files taken from the post-Close directory
	}{
		{"before_close", nil},
		{"after_index_persist", []string{"index.json"}},
		{"after_sidx_persist", []string{"index.json", "sidx_v.json"}},
		{"after_meta_persist", []string{"index.json", "sidx_v.json", metaFilename}},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := newFaultFS()
			db, col := openFaulty(t, dir, fs, cfg)
			r := newCmRun(t, col, false)
			workCompaction(r)
			if r.dead {
				t.Fatal("workload died")
			}
			snap := snapshotOf(t, colDirOf(dir))
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, f := range st.files {
				if _, err := os.Stat(filepath.Join(colDirOf(dir), f)); err != nil {
					continue
				}
				copyFileIn(t, filepath.Join(colDirOf(dir), f), filepath.Join(snap, f))
			}
			db2, root := openSnapshot(t, snap, cfg)
			cmVerify(t, db2, root, r, false)
		})
	}

	// Close racing an in-flight compaction: Close must wait for the pass, and
	// the directory must reopen to exactly the acked state.
	t.Run("close_racing_compaction", func(t *testing.T) {
		dir := t.TempDir()
		reached, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		c := cfg
		c.preSwapHook = func() {
			once.Do(func() { close(reached) })
			<-release
		}
		db, col := openFaulty(t, dir, newFaultFS(), c)
		r := newCmRun(t, col, false)
		r.step(col.EnsureIndex("v"))
		var ids []uint64
		for i := 0; i < 6; i++ {
			ids = append(ids, r.insert("old"))
		}
		r.step(col.rotateSegment())
		for _, id := range ids {
			r.update(id, "new")
		}
		r.step(col.rotateSegment())
		r.del(ids[0])

		compErr := make(chan error, 1)
		go func() { compErr <- col.CompactNow() }()
		<-reached
		closeErr := make(chan error, 1)
		go func() { closeErr <- db.Close() }()
		select {
		case err := <-closeErr:
			t.Fatalf("Close returned while a compaction pass was mid-swap: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		close(release)
		if err := <-compErr; err != nil {
			t.Fatalf("compaction: %v", err)
		}
		if err := <-closeErr; err != nil {
			t.Fatalf("close: %v", err)
		}
		db2, err := Open(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db2.Close() })
		cmVerify(t, db2, dir, r, false)
	})

	// Close racing a pass that has not started its swap: also a crash of the
	// process right after Close returns must leave a consistent directory.
	t.Run("kill_during_close_wait", func(t *testing.T) {
		dir := t.TempDir()
		reached, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		c := cfg
		c.preSwapHook = func() {
			once.Do(func() { close(reached) })
			<-release
		}
		fs := newFaultFS()
		db, col := openFaulty(t, dir, fs, c)
		r := newCmRun(t, col, false)
		workCompactionNoCompact(r)
		compErr := make(chan error, 1)
		go func() { compErr <- col.CompactNow() }()
		<-reached
		// Kill while the pass holds its manifest and Close is still waiting.
		snap := snapshotOf(t, colDirOf(dir))
		close(release)
		<-compErr
		_ = db.Close()
		db2, root := openSnapshot(t, snap, cfg)
		cmVerify(t, db2, root, r, false)
	})
}

func workCompactionNoCompact(r *cmRun) {
	r.step(r.col.EnsureIndex("v"))
	var ids []uint64
	for i := 0; i < 5; i++ {
		ids = append(ids, r.insert("old"))
	}
	r.step(r.col.rotateSegment())
	for _, id := range ids {
		r.update(id, "new")
	}
	r.step(r.col.rotateSegment())
	r.del(ids[1])
}
