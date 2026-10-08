package engine

// Fault-injection / crash-simulation harness (test-only).
//
// HOW LATER TASKS USE THIS
//
//	fs := newFaultFS()
//	fs.failWriteAt(3, 10, syscall.ENOSPC) // 3rd write: persist 10 bytes, return ENOSPC
//	fs.failSyncAt(1, nil)                 // 1st fsync fails (nil => EIO)
//	fs.failRenameAt(1, nil)               // 1st compactor rename fails
//	fs.crashAfter(5)                      // ops 1..5 succeed; op 6+ never reaches disk
//	db, col := openFaulty(t, dir, fs, CollectionConfig{...})
//	... drive writes / col.CompactNow() ...
//	db2 := reopenAfterCrash(t, db, fs, dir, cfg) // fresh handle, as after kill -9
//	st := diskState(t, colDir)                   // ground truth from the files
//	assertLookupScanAgree(t, col2, st.model())   // Get(id) and Scan() match the model
//
// Seams used (all unexported, nil/no-op in production): segFile / fileWrapper
// in segment.go (Write, Sync, Truncate, Close) via CollectionConfig.wrapFile,
// and CollectionConfig.renameFn for the compactor's segment renames. Not yet
// intercepted: index-persist renames (fsync.go), compaction manifest roll-forward
// renames and rebalance merge temp files; extend the seams when a task needs them.
//
// Findings for later tasks: (1) after crashAfter + reopenAfterCrash, a
// collection whose persisted index.json is empty/old (written at create time)
// reopens with that index even though the segment holds records, so Get/Scan
// miss them (index.json checksum + segmentsValid do not detect a stale-but-valid
// index). (2) Background compaction runs on segment rotation, so armed faults
// may fire there; assert with fs.faults().
//
// Op counting: "steps" count write, sync, truncate and rename calls (not close,
// stat, read, seek) across every file of the faultFS, in call order.

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"

	"github.com/srjn45/scriva/store"
)

// errCrash is returned by every operation at or after the simulated crash point.
var errCrash = errors.New("faultfs: simulated crash")

type faultFS struct {
	mu    sync.Mutex
	steps int // write/sync/truncate/rename calls seen so far
	ops   map[string]int

	writeN, syncN, renameN int // 1-based; 0 = disabled
	writePrefix            int // bytes persisted by the failing write
	writeErr, syncErr      error
	renameErr              error

	crashAt int // steps allowed before the crash; -1 = never
	dead    bool

	injected int // faults fired (failed write/sync/rename; not crashes)

	files []segFile // real files, so tests can release fds after a crash
	trace []string  // op name of every counted step, in order
}

func newFaultFS() *faultFS { return &faultFS{crashAt: -1, ops: map[string]int{}} }

// failWriteAt makes the nth write persist only the first prefix bytes of its
// buffer (0 = nothing) and return err (nil => ENOSPC).
func (f *faultFS) failWriteAt(n, prefix int, err error) {
	if err == nil {
		err = syscall.ENOSPC
	}
	f.mu.Lock()
	f.writeN, f.writePrefix, f.writeErr = n, prefix, err
	f.mu.Unlock()
}

func (f *faultFS) failSyncAt(n int, err error) {
	if err == nil {
		err = syscall.EIO
	}
	f.mu.Lock()
	f.syncN, f.syncErr = n, err
	f.mu.Unlock()
}

func (f *faultFS) failRenameAt(n int, err error) {
	if err == nil {
		err = syscall.EIO
	}
	f.mu.Lock()
	f.renameN, f.renameErr = n, err
	f.mu.Unlock()
}

// crashAfter lets `step` operations complete, then freezes the filesystem: the
// next op and everything after it fails with errCrash without touching disk
// (no sync, no truncate, no close), mimicking kill -9 at that instant.
func (f *faultFS) crashAfter(step int) {
	f.mu.Lock()
	f.crashAt = step
	f.mu.Unlock()
}

// crashed reports whether the crash point has been reached.
func (f *faultFS) crashed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dead
}

// count returns how many times the named op ("write","sync","truncate","rename") ran.
func (f *faultFS) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ops[op]
}

// faults returns how many armed faults have fired. Use it to assert a fault was
// actually consumed: background compaction (triggered by segment rotation) can
// take an armed rename/write/sync fault before the foreground call does.
func (f *faultFS) faults() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.injected
}

// totalSteps returns the number of counted operations so far; run a workload
// once with no faults to learn N, then sweep crashAfter(0..N).
func (f *faultFS) totalSteps() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.steps
}

// kill freezes the filesystem now (equivalent to crashAfter(current steps)).
func (f *faultFS) kill() {
	f.mu.Lock()
	f.dead = true
	f.mu.Unlock()
}

// opAt returns the op name of the i-th (0-based) counted step, or "end".
func (f *faultFS) opAt(i int) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i < 0 || i >= len(f.trace) {
		return "end"
	}
	return f.trace[i]
}

// begin registers one op. It returns the fault to inject (nil = proceed) and,
// for writes, whether this is the faulty write.
func (f *faultFS) begin(op string) (err error, hit bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead {
		return errCrash, false
	}
	if f.crashAt >= 0 && f.steps >= f.crashAt {
		f.dead = true
		return errCrash, false
	}
	f.steps++
	f.trace = append(f.trace, op)
	f.ops[op]++
	n := f.ops[op]
	switch {
	case op == "write" && f.writeN == n:
		f.injected++
		return f.writeErr, true
	case op == "sync" && f.syncN == n:
		f.injected++
		return f.syncErr, true
	case op == "rename" && f.renameN == n:
		f.injected++
		return f.renameErr, true
	}
	return nil, false
}

// wrap is a fileWrapper for CollectionConfig.wrapFile.
func (f *faultFS) wrap(_ string, real segFile) segFile {
	f.mu.Lock()
	f.files = append(f.files, real)
	f.mu.Unlock()
	return &faultFile{fs: f, segFile: real}
}

// rename is a renameFunc for CollectionConfig.renameFn.
func (f *faultFS) rename(oldpath, newpath string) error {
	if err, _ := f.begin("rename"); err != nil {
		return err
	}
	return os.Rename(oldpath, newpath)
}

// releaseFDs closes the real descriptors of an abandoned handle (test hygiene;
// performs no writes and no sync).
func (f *faultFS) releaseFDs() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, fl := range f.files {
		_ = fl.Close()
	}
	f.files = nil
}

type faultFile struct {
	fs *faultFS
	segFile
}

func (ff *faultFile) Write(b []byte) (int, error) {
	err, hit := ff.fs.begin("write")
	if err == nil {
		return ff.segFile.Write(b)
	}
	if !hit {
		return 0, err // crashed: nothing reaches disk
	}
	n := ff.fs.writePrefix
	if n > len(b) {
		n = len(b)
	}
	if n > 0 {
		if _, werr := ff.segFile.Write(b[:n]); werr != nil {
			return 0, werr
		}
	}
	return n, err
}

func (ff *faultFile) Sync() error {
	if err, _ := ff.fs.begin("sync"); err != nil {
		return err
	}
	return ff.segFile.Sync()
}

func (ff *faultFile) Truncate(size int64) error {
	if err, _ := ff.fs.begin("truncate"); err != nil {
		return err
	}
	return ff.segFile.Truncate(size)
}

func (ff *faultFile) Close() error {
	ff.fs.mu.Lock()
	dead := ff.fs.dead
	ff.fs.mu.Unlock()
	if dead {
		return errCrash // an abandoned handle must not flush or close
	}
	return ff.segFile.Close()
}

// openFaulty opens a DB at dir whose segment files and compactor renames go
// through fs. A collection named "c" is created and returned. Compaction is
// only run on demand (CompactInterval is long).
func openFaulty(t *testing.T, dir string, fs *faultFS, cfg CollectionConfig) (*DB, *Collection) {
	t.Helper()
	cfg.wrapFile, cfg.renameFn = fs.wrap, fs.rename
	if cfg.CompactInterval == 0 {
		cfg.CompactInterval = 24 * 3600 * 1e9
	}
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open faulty: %v", err)
	}
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	return db, col
}

// reopenAfterCrash simulates a restart after kill -9: it abandons old WITHOUT
// Close() (no index persist, no fsync) and opens dir with a fresh handle.
//
// Safe in tests only because the faultFS is frozen (kill) so the old handle
// performs no further writes, and its real descriptors are closed without
// flushing. The only thing "released" is the exclusive directory lock, which a
// real crash would drop with the process; we call the lock's release directly
// instead of DB.Close. Background goroutines of the old handle may linger but
// every file op they attempt fails with errCrash. cfg must not carry the old
// faultFS seams unless you want faults on the new handle too.
func reopenAfterCrash(t *testing.T, old *DB, fs *faultFS, dir string, cfg CollectionConfig) *DB {
	t.Helper()
	fs.kill()
	fs.releaseFDs()
	if err := old.lock.release(); err != nil {
		t.Fatalf("release old lock: %v", err)
	}
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// badLine is a line in a segment file that did not decode.
type badLine struct {
	File string
	Line int
	Err  error
	Raw  string
}

// diskReport is ground truth derived purely from the segment files.
type diskReport struct {
	Live       map[uint64]store.Entry // latest entry per id where it is not a delete
	Tombstones map[uint64]store.Entry // latest entry per id where it is a delete
	Bad        []badLine              // undecodable lines (a torn tail shows up here)
}

// diskState scans every seg_*.ndjson in a collection directory in name order,
// tolerating (but recording) undecodable lines. Later lines win. A final line
// lacking '\n' is a torn write and is reported in Bad.
func diskState(t *testing.T, colDir string) diskReport {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(colDir, "seg_*.ndjson"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	sort.Strings(paths)
	latest := map[uint64]store.Entry{}
	rep := diskReport{Live: map[uint64]store.Entry{}, Tombstones: map[uint64]store.Entry{}}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		line := 0
		for len(b) > 0 {
			line++
			end := indexByte(b, '\n')
			raw, torn := b, end < 0
			if !torn {
				raw = b[:end]
				b = b[end+1:]
			} else {
				b = nil
			}
			if len(raw) == 0 {
				continue
			}
			e, derr := store.Decode(raw)
			if derr == nil && torn {
				derr = errors.New("missing trailing newline")
			}
			if derr != nil {
				rep.Bad = append(rep.Bad, badLine{File: filepath.Base(p), Line: line, Err: derr, Raw: string(raw)})
				continue
			}
			latest[e.ID] = e
		}
	}
	for id, e := range latest {
		if e.Op == store.OpDelete {
			rep.Tombstones[id] = e
		} else {
			rep.Live[id] = e
		}
	}
	return rep
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

// model returns the expected id -> data view (live records only).
func (r diskReport) model() map[uint64]map[string]any {
	m := make(map[uint64]map[string]any, len(r.Live))
	for id, e := range r.Live {
		m[id] = e.Data
	}
	return m
}

// assertLookupScanAgree checks that, for the given model, point lookups (Get)
// and a full Scan both return exactly the model's records, with equal data.
// Ids absent from the model but present in Scan, or Get-able, are failures.
// Model ids listed in `absent` must not be gettable (e.g. deleted ids).
func assertLookupScanAgree(t *testing.T, col *Collection, model map[uint64]map[string]any, absent ...uint64) {
	t.Helper()
	res, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	scanned := map[uint64]map[string]any{}
	for _, r := range res {
		if _, dup := scanned[r.ID]; dup {
			t.Errorf("scan returned id %d twice", r.ID)
		}
		scanned[r.ID] = r.Data
	}
	for id, want := range model {
		got, ok := scanned[id]
		if !ok {
			t.Errorf("id %d in model but missing from scan", id)
		} else if !sameData(got, want) {
			t.Errorf("id %d scan data %v != model %v", id, got, want)
		}
		rec, err := col.Get(id)
		if err != nil {
			t.Errorf("get(%d): %v (model has it)", id, err)
		} else if !sameData(rec.Data, want) {
			t.Errorf("id %d get data %v != model %v", id, rec.Data, want)
		}
	}
	for id := range scanned {
		if _, ok := model[id]; !ok {
			t.Errorf("id %d returned by scan but not in model", id)
		}
	}
	for _, id := range absent {
		if _, err := col.Get(id); err == nil {
			t.Errorf("get(%d) succeeded but id must be absent", id)
		}
	}
}

func sameData(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w, ok := b[k]
		if !ok || !equalAny(v, w) {
			return false
		}
	}
	return true
}

// equalAny compares decoded JSON values, treating all numbers as float64.
func equalAny(a, b any) bool {
	toF := func(v any) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case int:
			return float64(n), true
		case int64:
			return float64(n), true
		case uint64:
			return float64(n), true
		}
		return 0, false
	}
	if fa, ok := toF(a); ok {
		fb, ok := toF(b)
		return ok && fa == fb
	}
	if ma, ok := a.(map[string]any); ok {
		mb, ok := b.(map[string]any)
		return ok && sameData(ma, mb)
	}
	return a == b
}
