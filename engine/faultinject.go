package engine

// FaultInjector fails fsyncs of a collection's active segment on demand. It is
// a test hook for packages outside engine (the server's integration tests)
// that need a real engine on a real disk whose sync can fail, to observe the
// write-outcome contract (§2.3) end to end. Production code never constructs
// one; a DB opened without it pays nothing.
//
// It installs the same file seam the engine's own fault tests use
// (CollectionConfig.wrapFile), so it must be applied before the DB is opened:
//
//	fi := engine.NewFaultInjector()
//	db, _ := engine.Open(dir, fi.Install(cfg))
//	col, _ := db.Collection("c")
//	fi.FailSync(col, 1, nil) // the next fsync of c's active segment fails with EIO
//
// Faults are not durable: reopen the directory without the injector to observe
// recovery. A real file system keeps the page cache across that reopen, so a
// write whose bytes fully reached the file recovers as committed.
type FaultInjector struct{ fs *faultFS }

// NewFaultInjector returns an injector with no fault armed.
func NewFaultInjector() *FaultInjector { return &FaultInjector{fs: newFaultFS()} }

// Install returns cfg with the injector wired in. Pass the result to Open.
func (fi *FaultInjector) Install(cfg CollectionConfig) CollectionConfig {
	cfg.wrapFile = fi.fs.wrap
	return cfg
}

// FailSync arms the nth next fsync (1 = the very next) of c's current active
// segment to fail with err (nil = EIO). Under SyncModeAlways the first fsync
// after a write's append is its commit sync; when that write also rotates the
// segment, the second is the seal sync of the rotation. Arming again replaces
// the previous fault.
func (fi *FaultInjector) FailSync(c *Collection, n int, err error) {
	c.mu.RLock()
	path := c.active.Path()
	c.mu.RUnlock()
	fi.fs.scopeTo(path)
	fi.fs.failSyncAt(n, err)
}

// Injected reports how many faults have fired.
func (fi *FaultInjector) Injected() int { return fi.fs.faults() }
