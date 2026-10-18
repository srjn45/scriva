package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// errCrash is returned by every operation at or after the simulated crash point.
var errCrash = errors.New("faultfs: simulated crash")

type faultFS struct {
	mu    sync.Mutex
	steps int // write/sync/truncate/rename calls seen so far
	ops   map[string]int

	writeN, syncN, renameN, truncateN int // 1-based; 0 = disabled
	writePrefix                       int // bytes persisted by the failing write
	writeErr, syncErr                 error
	renameErr, truncateErr            error

	// scope, when set, restricts the write/sync/truncate fault ordinals to
	// ops on the file at exactly this path (e.g. a collection's active
	// segment), counted in scoped. Steps, trace and ops stay global.
	scope  string
	scoped map[string]int

	// durable is the size of each wrapped file as of its last successful Sync
	// (or when it was opened), i.e. what survives a power loss. Keyed by the
	// resolved path: segment paths may name the directory through a
	// /proc/self/fd/N link that only means something while the handle lives.
	durable map[string]int64

	crashAt int // steps allowed before the crash; -1 = never
	dead    bool

	injected int // faults fired (failed write/sync/rename; not crashes)

	files []segFile // real files, so tests can release fds after a crash
	trace []string  // op name of every counted step, in order
}

func newFaultFS() *faultFS {
	return &faultFS{crashAt: -1, ops: map[string]int{}, scoped: map[string]int{}, durable: map[string]int64{}}
}

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

// failNextSync arms a failure (nil => EIO) on the very next fsync of the
// scoped file (or of any file when unscoped), computing the ordinal under the
// same lock that counts syncs so a concurrent syncLoop cannot slip past it.
func (f *faultFS) failNextSync(err error) {
	if err == nil {
		err = syscall.EIO
	}
	f.mu.Lock()
	n := f.ops["sync"]
	if f.scope != "" {
		n = f.scoped["sync"]
	}
	f.syncN, f.syncErr = n+1, err
	f.mu.Unlock()
}

// failTruncateAt makes the nth truncate (the segment rollback after a failed
// write) return err (nil => EIO) without changing the file.
func (f *faultFS) failTruncateAt(n int, err error) {
	if err == nil {
		err = syscall.EIO
	}
	f.mu.Lock()
	f.truncateN, f.truncateErr = n, err
	f.mu.Unlock()
}

// scopeTo restricts the write/sync/truncate fault ordinals to ops on the file
// at path ("" = every file) and resets the scoped counters, so
// fs.failSyncAt(fs.scopedCount("sync")+1, nil) arms exactly the next fsync of
// that file regardless of journal or other-file traffic.
func (f *faultFS) scopeTo(path string) {
	f.mu.Lock()
	f.scope = path
	f.scoped = map[string]int{}
	f.mu.Unlock()
}

// scopedCount returns how many times op ran on the scoped file since scopeTo.
func (f *faultFS) scopedCount(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scoped[op]
}

// durableSizes returns, per wrapped file path, the byte length that a power
// loss would preserve: the size at the file's last successful Sync (or at
// open), lowered by later truncates. Files not opened through wrap (meta,
// index snapshots) are not tracked.
func (f *faultFS) durableSizes() map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int64, len(f.durable))
	for p, n := range f.durable {
		out[p] = n
	}
	return out
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

// begin registers one op on the file at path ("" for renames). It returns the
// fault to inject (nil = proceed) and, for writes, whether this is the faulty
// write.
func (f *faultFS) begin(op, path string) (err error, hit bool) {
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
	if op != "rename" && f.scope != "" {
		if path != f.scope {
			return nil, false
		}
		f.scoped[op]++
		n = f.scoped[op]
	}
	switch {
	case op == "write" && f.writeN == n:
		f.injected++
		return f.writeErr, true
	case op == "sync" && f.syncN == n:
		f.injected++
		return f.syncErr, true
	case op == "truncate" && f.truncateN == n:
		f.injected++
		return f.truncateErr, false
	case op == "rename" && f.renameN == n:
		f.injected++
		return f.renameErr, true
	}
	return nil, false
}

// wrap is a fileWrapper for CollectionConfig.wrapFile.
func (f *faultFS) wrap(path string, real segFile) segFile {
	var size int64
	if st, err := real.Stat(); err == nil {
		size = st.Size()
	}
	key := path
	if d, err := filepath.EvalSymlinks(filepath.Dir(path)); err == nil {
		key = filepath.Join(d, filepath.Base(path))
	}
	f.mu.Lock()
	f.files = append(f.files, real)
	f.durable[key] = size
	f.mu.Unlock()
	return &faultFile{fs: f, path: path, durKey: key, segFile: real}
}

// synced records a successful fsync of the file keyed path at its current size.
func (f *faultFS) synced(path string, real segFile) {
	st, err := real.Stat()
	if err != nil {
		return
	}
	f.mu.Lock()
	f.durable[path] = st.Size()
	f.mu.Unlock()
}

// truncated lowers the power-loss size of the file keyed path after a successful truncate.
func (f *faultFS) truncated(path string, size int64) {
	f.mu.Lock()
	if d, ok := f.durable[path]; ok && size < d {
		f.durable[path] = size
	}
	f.mu.Unlock()
}

// rename is a renameFunc for CollectionConfig.renameFn.
func (f *faultFS) rename(oldpath, newpath string) error {
	if err, _ := f.begin("rename", ""); err != nil {
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
	fs     *faultFS
	path   string // as opened (scope matches on this)
	durKey string // resolved path (durable map key)
	segFile
}

func (ff *faultFile) Write(b []byte) (int, error) {
	err, hit := ff.fs.begin("write", ff.path)
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
	if err, _ := ff.fs.begin("sync", ff.path); err != nil {
		return err
	}
	if err := ff.segFile.Sync(); err != nil {
		return err
	}
	ff.fs.synced(ff.durKey, ff.segFile)
	return nil
}

func (ff *faultFile) Truncate(size int64) error {
	if err, _ := ff.fs.begin("truncate", ff.path); err != nil {
		return err
	}
	if err := ff.segFile.Truncate(size); err != nil {
		return err
	}
	ff.fs.truncated(ff.durKey, size)
	return nil
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
