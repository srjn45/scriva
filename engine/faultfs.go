package engine

import (
	"errors"
	"os"
	"sync"
	"syscall"
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
