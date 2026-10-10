package engine

import "os"

// XTxHookPoint names a phase of the cross-collection commit path.
type XTxHookPoint int

const (
	// XTxHookBeforeLocks: the commit is admitted and about to wait for locks.
	XTxHookBeforeLocks XTxHookPoint = iota
	// XTxHookLockNext: about to acquire one collection's lock.
	XTxHookLockNext
	// XTxHookLocked: every participant lock is held; nothing validated yet.
	XTxHookLocked
	// XTxHookPrepared: stamped runs are appended and fsynced on participants.
	XTxHookPrepared
	// XTxHookDecided: COMMIT record is durable; not yet applied to indexes.
	XTxHookDecided
	// XTxHookApplied: applied to indexes and locks released.
	XTxHookApplied
)

// SetTestXTxHook installs a test hook for cross-collection commit phases.
func SetTestXTxHook(cfg *CollectionConfig, h func(point XTxHookPoint, key, txid string)) {
	cfg.xtxHook = func(p xtxHookPoint, key, txid string) {
		if h != nil {
			h(XTxHookPoint(p), key, txid)
		}
	}
}

// SetTestFileWrapper installs a file wrapper on CollectionConfig for fault injection.
func SetTestFileWrapper(cfg *CollectionConfig, wrap func(path string, f *os.File) *os.File) {
	if wrap == nil {
		cfg.wrapFile = nil
		return
	}
	cfg.wrapFile = func(path string, real segFile) segFile {
		if osf, ok := real.(*os.File); ok {
			wrapped := wrap(path, osf)
			if wrapped != nil {
				return wrapped
			}
		}
		return real
	}
}

// EnsureDBXTxJournal creates or opens the coordinator journal on db.
func EnsureDBXTxJournal(db *DB) (any, error) {
	return db.ensureXTxJournal()
}

// TestFaultFS is an exported adapter for faultFS in tests across packages.
type TestFaultFS struct {
	inner *faultFS
}

// NewFaultFS creates a new TestFaultFS.
func NewFaultFS() *TestFaultFS {
	return &TestFaultFS{inner: newFaultFS()}
}

// FailSyncAt arms the nth sync to fail with err.
func (f *TestFaultFS) FailSyncAt(n int, err error) {
	f.inner.failSyncAt(n, err)
}

// Count returns the number of times op was executed.
func (f *TestFaultFS) Count(op string) int {
	return f.inner.count(op)
}

// Wrap is a fileWrapper adapter for CollectionConfig.
func (f *TestFaultFS) Wrap(cfg *CollectionConfig) {
	cfg.wrapFile = f.inner.wrap
}
