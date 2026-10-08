package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// A full scan takes a segment-layout snapshot. Compaction replaces and removes
// files in that layout, so it must wait until the scan has opened and walked
// them all. In particular, waiting only while the snapshot is copied leaves a
// scan able to open a path that a swap has since unlinked.
func TestScanStream_HoldsLayoutStableAgainstCompaction(t *testing.T) {
	entered := make(chan struct{})
	allowScan := make(chan struct{})
	var paused atomic.Bool

	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.SegmentMaxSize = 256
	cfg.scanHook = func(point scanHookPoint, _ uint64) {
		if point != scanAtEntry {
			return
		}
		if paused.CompareAndSwap(false, true) {
			close(entered)
			<-allowScan
		}
	}
	col, err := OpenCollection("race", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer col.Close()

	for i := 0; i < 80; i++ {
		if _, _, err := col.Insert(map[string]any{"v": i}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	// Rotations signal the background compactor. Let any such pass finish and
	// drain its signal so this test controls the one compaction under test.
	col.compactMu.Lock()
	for {
		select {
		case <-col.compactC:
		default:
			col.compactMu.Unlock()
			goto settled
		}
	}

settled:

	scanDone := make(chan error, 1)
	go func() {
		_, err := col.ScanStream(context.Background(), ScanOptions{}, func(ScanResult) error { return nil })
		scanDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("scan did not begin")
	}

	compactDone := make(chan error, 1)
	go func() { compactDone <- col.CompactNow() }()
	// The pass must defer instead of waiting as an RWMutex writer: a waiting
	// writer would block this re-entrant/new scan behind the slow first stream.
	select {
	case err := <-compactDone:
		if !errors.Is(err, ErrCompactionDeferred) {
			t.Fatalf("deferred compaction = %v, want ErrCompactionDeferred", err)
		}
	case <-time.After(time.Second):
		t.Fatal("compaction waited for the scan instead of deferring")
	}
	// A kill now cannot roll forward an abandoned swap: deferral precedes both
	// the temp output and the durable manifest.
	if _, err := os.Stat(compactManifestPath(col.dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deferred compaction left manifest: %v", err)
	}
	if temps, _ := filepath.Glob(filepath.Join(col.dir, ".compact_*")); len(temps) != 0 {
		t.Fatalf("deferred compaction left temps: %v", temps)
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := col.ScanStream(context.Background(), ScanOptions{}, func(ScanResult) error { return nil })
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second ScanStream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("new scan blocked behind deferred compaction")
	}

	close(allowScan)
	select {
	case err := <-scanDone:
		if err != nil {
			t.Fatalf("ScanStream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scan did not finish")
	}
	if err := col.CompactNow(); err != nil {
		t.Fatalf("CompactNow after scan: %v", err)
	}
}

// A failed swap must release the lease too: otherwise all later full scans
// would block forever behind a compaction that has already returned an error.
func TestCompactionFailedSwapReleasesLayoutLease(t *testing.T) {
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.SegmentMaxSize = 128
	cfg.renameFn = func(_, _ string) error { return errors.New("injected rename failure") }
	col, err := OpenCollection("race", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer col.Close()
	for i := 0; i < 30; i++ {
		if _, _, err := col.Insert(map[string]any{"v": i}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	if err := col.CompactNow(); err == nil {
		t.Fatal("CompactNow succeeded with injected rename failure")
	}
	if !col.layoutMu.TryLock() {
		t.Fatal("failed compaction retained layout lease")
	}
	col.layoutMu.Unlock()
}

func TestCompactionSecondLeaseDeferralLeavesNoRecoveryIntent(t *testing.T) {
	entered, allow := make(chan struct{}), make(chan struct{})
	var paused atomic.Bool
	var col *Collection
	cfg := defaultConfig()
	cfg.CompactInterval, cfg.SegmentMaxSize = time.Hour, 128
	cfg.scanHook = func(p scanHookPoint, _ uint64) {
		if p == scanAtEntry && paused.CompareAndSwap(false, true) {
			close(entered)
			<-allow
		}
	}
	cfg.postWriteCompactedHook = func() {
		go func() {
			_, _ = col.ScanStream(context.Background(), ScanOptions{}, func(ScanResult) error { return nil })
		}()
		<-entered
	}
	var err error
	col, err = OpenCollection("race", t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()
	for i := 0; i < 30; i++ {
		if _, _, err := col.Insert(map[string]any{"v": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := col.CompactNow(); !errors.Is(err, ErrCompactionDeferred) {
		t.Fatalf("CompactNow = %v, want deferred", err)
	}
	if _, err := os.Stat(compactManifestPath(col.dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest after second deferral: %v", err)
	}
	if temps, _ := filepath.Glob(filepath.Join(col.dir, ".compact_*")); len(temps) != 0 {
		t.Fatalf("temps after second deferral: %v", temps)
	}
	close(allow)
}

// Close has no reason to take the layout lease: closing a handle while a
// client is paused in its scan callback must not wait for that client.
func TestCloseDoesNotWaitForScanLayoutLease(t *testing.T) {
	entered := make(chan struct{})
	allowScan := make(chan struct{})
	var paused atomic.Bool
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.scanHook = func(point scanHookPoint, _ uint64) {
		if point == scanAtEntry && paused.CompareAndSwap(false, true) {
			close(entered)
			<-allowScan
		}
	}
	col, err := OpenCollection("race", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"v": 1}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	scanDone := make(chan error, 1)
	go func() {
		_, err := col.ScanStream(context.Background(), ScanOptions{}, func(ScanResult) error { return nil })
		scanDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("scan did not begin")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- col.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for a scan layout lease")
	}
	close(allowScan)
	select {
	case err := <-scanDone:
		if err != nil {
			t.Fatalf("ScanStream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scan did not finish")
	}
}
