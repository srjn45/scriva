package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

// Barrier regression tests for the stable scan view (hardening §3.5, ratified
// storage-read contract §5): a scan emits exactly the records, revisions and
// data that were live when it captured its view, however writers, rotation and
// compaction interleave afterwards. Schedules are driven by scanHook; nothing
// here sleeps to order events.

// knownScanViewBug gates assertions for ScanViewMaxAge / MaxScanViews, which the
// engine does not implement yet. Flip to false when the limits land.
const knownScanViewBug = true

var allV = &query.FieldFilter{Field: "v", Op: query.OpGte, Value: "0"}

// schedCol opens a collection whose background compactor is stopped, with a
// hook installed that the test can set after construction through *hook.
func schedCol(t *testing.T, hook *func(scanHookPoint, uint64), index bool) *Collection {
	t.Helper()
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.SegmentMaxSize = 1 << 20 // rotate only when the test says so
	cfg.AllowNonAtomicBatch = true
	cfg.CompactDirtyPct = 2 // background passes never qualify; only CompactNow runs
	cfg.scanHook = func(p scanHookPoint, a uint64) {
		if h := *hook; h != nil {
			h(p, a)
		}
	}
	col, err := OpenCollection("sched", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	t.Cleanup(func() { col.Close() })
	if index {
		if err := col.EnsureIndex("v"); err != nil {
			t.Fatalf("EnsureIndex: %v", err)
		}
	}
	return col
}

func seed(t *testing.T, col *Collection, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, _, err := col.Insert(map[string]any{"v": float64(i)}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
}

// onceAtStart returns a hook that runs fn exactly once, at the first point of
// the scan after its view was captured (first segment, or first index candidate).
func onceAtStart(fn func()) func(scanHookPoint, uint64) {
	var done atomic.Bool
	return func(p scanHookPoint, a uint64) {
		if (p == scanAtSegment && a == 0) || p == scanAtCandidate {
			if done.CompareAndSwap(false, true) {
				fn()
			}
		}
	}
}

func scanAll(t *testing.T, col *Collection) []ScanResult {
	t.Helper()
	out, err := col.Scan(allV)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return out
}

func sameResults(t *testing.T, got, want []ScanResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("emitted %d records, want %d: got %v want %v", len(got), len(want), ids(got), ids(want))
	}
	for i := range got {
		if got[i].ID != want[i].ID || got[i].Rev != want[i].Rev || got[i].Data["v"] != want[i].Data["v"] {
			t.Fatalf("record %d: got id=%d rev=%d v=%v, want id=%d rev=%d v=%v",
				i, got[i].ID, got[i].Rev, got[i].Data["v"], want[i].ID, want[i].Rev, want[i].Data["v"])
		}
	}
}

func ids(rs []ScanResult) []uint64 {
	out := make([]uint64, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

// modes: full segment sweep, and index-served candidate walk.
var schedModes = []struct {
	name  string
	index bool
}{{"segments", false}, {"index", true}}

// V5.1 + V5.3: updating a record (across a forced rotation, and twice) while
// the scan is paused never omits or duplicates it, and the scan emits its
// start version. R=2 lives in a sealed segment, R=5 in the captured active tail.
func TestScanSchedule_UpdateAcrossRotation(t *testing.T) {
	for _, m := range schedModes {
		for _, r := range []uint64{2, 5} {
			t.Run(fmt.Sprintf("%s/R%d", m.name, r), func(t *testing.T) {
				var hook func(scanHookPoint, uint64)
				col := schedCol(t, &hook, m.index)
				seed(t, col, 3)
				if err := col.rotateSegment(); err != nil {
					t.Fatal(err)
				}
				for i := 4; i <= 5; i++ {
					col.Insert(map[string]any{"v": float64(i)})
				}
				want := scanAll(t, col)

				hook = onceAtStart(func() {
					if _, err := col.Update(r, map[string]any{"v": float64(100 + r)}); err != nil {
						t.Errorf("Update #1: %v", err)
					}
					if err := col.rotateSegment(); err != nil { // R's new version lands in a new segment
						t.Errorf("rotate: %v", err)
					}
					if _, err := col.Update(r, map[string]any{"v": float64(200 + r)}); err != nil {
						t.Errorf("Update #2: %v", err)
					}
				})
				got := scanAll(t, col)
				sameResults(t, got, want)
			})
		}
	}
}

// V5.2: a delete of a not-yet-visited record does not hide it; an insert after
// capture is not emitted.
func TestScanSchedule_DeleteAndInsertAfterCapture(t *testing.T) {
	for _, m := range schedModes {
		t.Run(m.name, func(t *testing.T) {
			var hook func(scanHookPoint, uint64)
			col := schedCol(t, &hook, m.index)
			seed(t, col, 5)
			want := scanAll(t, col)

			var newID uint64
			hook = onceAtStart(func() {
				if err := col.Delete(5); err != nil {
					t.Errorf("Delete: %v", err)
				}
				id, _, err := col.Insert(map[string]any{"v": float64(99)})
				if err != nil {
					t.Errorf("Insert: %v", err)
				}
				newID = id
			})
			got := scanAll(t, col)
			sameResults(t, got, want)
			for _, r := range got {
				if r.ID == newID {
					t.Fatalf("scan emitted record %d inserted after capture", newID)
				}
			}
			hook = nil
			if n := len(scanAll(t, col)); n != 5 { // 4 survivors + N
				t.Fatalf("post-scan state has %d records, want 5", n)
			}
		})
	}
}

// V5.4: compaction requested mid-scan defers, leaves the scan output equal to a
// no-compaction control, and succeeds once the scan released the lease.
func TestScanSchedule_CompactionDeferredUntilRelease(t *testing.T) {
	var hook func(scanHookPoint, uint64)
	col := schedCol(t, &hook, false)
	seed(t, col, 4)
	col.rotateSegment()
	for i := 1; i <= 4; i++ { // make sealed bytes stale
		col.Update(uint64(i), map[string]any{"v": float64(10 * i)})
	}
	col.rotateSegment()
	control := scanAll(t, col)

	var deferred error
	hook = onceAtStart(func() { deferred = col.CompactNow() })
	got := scanAll(t, col)
	if !errors.Is(deferred, ErrCompactionDeferred) {
		t.Fatalf("mid-scan CompactNow = %v, want ErrCompactionDeferred", deferred)
	}
	sameResults(t, got, control)

	hook = nil
	if err := col.CompactNow(); err != nil {
		t.Fatalf("CompactNow after release: %v", err)
	}
	sameResults(t, scanAll(t, col), control)
}

// V5.5: a cancelled scan, one aborted by its consumer, and one abandoned by a
// slow consumer all release the layout lease so compaction proceeds.
func TestScanSchedule_AbortReleasesLease(t *testing.T) {
	boom := errors.New("consumer failed")
	cases := []struct {
		name string
		run  func(col *Collection) error
		want error
	}{
		{"ctx-cancel-mid-scan", func(col *Collection) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n := 0
			_, err := col.ScanStream(ctx, ScanOptions{Filter: allV}, func(ScanResult) error {
				if n++; n == 2 {
					cancel()
				}
				return nil
			})
			return err
		}, context.Canceled},
		{"consumer-error", func(col *Collection) error {
			_, err := col.ScanStream(context.Background(), ScanOptions{Filter: allV}, func(ScanResult) error { return boom })
			return err
		}, boom},
		{"slow-consumer-cancelled", func(col *Collection) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inYield, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				_, err := col.ScanStream(ctx, ScanOptions{Filter: allV}, func(ScanResult) error {
					select {
					case <-inYield:
					default:
						close(inYield)
					}
					<-release
					return nil
				})
				done <- err
			}()
			<-inYield
			// While the consumer is stuck the lease is held: compaction defers.
			if err := col.CompactNow(); !errors.Is(err, ErrCompactionDeferred) {
				t.Errorf("CompactNow with slow consumer = %v, want deferred", err)
			}
			cancel()
			close(release)
			return <-done
		}, context.Canceled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hook func(scanHookPoint, uint64)
			col := schedCol(t, &hook, false)
			seed(t, col, 4)
			col.rotateSegment()
			col.Update(1, map[string]any{"v": 11.0})
			col.rotateSegment()

			if err := tc.run(col); !errors.Is(err, tc.want) {
				t.Fatalf("scan error = %v, want %v", err, tc.want)
			}
			if !col.layoutMu.TryLock() {
				t.Fatal("aborted scan retained the layout lease")
			}
			col.layoutMu.Unlock()
			if err := col.CompactNow(); err != nil {
				t.Fatalf("CompactNow after abort: %v", err)
			}
		})
	}
}

// holdScans starts n scans and returns once every one has captured its view and
// is parked; the returned func releases them and reports their errors.
func holdScans(t *testing.T, col *Collection, hook *func(scanHookPoint, uint64), n int) (release func() []error) {
	t.Helper()
	var arrived sync.WaitGroup
	arrived.Add(n)
	gate := make(chan struct{})
	var seen atomic.Int32
	*hook = func(p scanHookPoint, a uint64) {
		if !((p == scanAtSegment && a == 0) || p == scanAtCandidate) {
			return
		}
		if int(seen.Add(1)) <= n {
			arrived.Done()
			<-gate
		}
	}
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := col.ScanStream(context.Background(), ScanOptions{Filter: allV}, func(ScanResult) error { return nil })
			errs <- err
		}()
	}
	arrived.Wait()
	return func() []error {
		close(gate)
		out := make([]error, n)
		for i := range out {
			out[i] = <-errs
		}
		return out
	}
}

// V5.6: fixture for ScanViewMaxAge / MaxScanViews. Concurrent views must coexist
// and all release; the limit assertions wait for the feature.
func TestScanSchedule_ConcurrentViewsAndLimits(t *testing.T) {
	var hook func(scanHookPoint, uint64)
	col := schedCol(t, &hook, false)
	seed(t, col, 3)

	release := holdScans(t, col, &hook, 4)
	if _, _, err := col.Insert(map[string]any{"v": 9.0}); err != nil {
		t.Fatalf("writer blocked behind open views: %v", err)
	}
	if err := col.CompactNow(); !errors.Is(err, ErrCompactionDeferred) && err != nil {
		t.Fatalf("CompactNow = %v", err)
	}
	for _, err := range release() {
		if err != nil {
			t.Fatalf("held scan: %v", err)
		}
	}
	if !col.layoutMu.TryLock() {
		t.Fatal("views released but lease retained")
	}
	col.layoutMu.Unlock()

	t.Run("limits", func(t *testing.T) {
		if knownScanViewBug {
			t.Skip("ScanViewMaxAge/MaxScanViews not implemented (hardening §3.5, SR-D5): " +
				"expect scan N+1 beyond MaxScanViews to fail fast and a view older than ScanViewMaxAge to be aborted with its lease released")
		}
	})
}

// V5.7: Count and Scan taken from views captured before concurrent writes both
// equal the model snapshot at the capture barrier. Count uses a predicate that
// takes the scan path (not the O(1) live-index fast paths).
func TestScanSchedule_CountEqualsScanUnderWrites(t *testing.T) {
	for _, m := range schedModes {
		t.Run(m.name, func(t *testing.T) {
			var hook func(scanHookPoint, uint64)
			col := schedCol(t, &hook, m.index)
			seed(t, col, 6)
			model := ids(scanAll(t, col)) // snapshot at the barrier

			var arrived atomic.Int32
			gate := make(chan struct{})
			hook = func(p scanHookPoint, a uint64) {
				// First point of each scan: segment 0, or candidate id 1.
				if !((p == scanAtSegment && a == 0) || (p == scanAtCandidate && a == 1)) {
					return
				}
				if arrived.Add(1) == 2 {
					// Both views are captured: mutate, rotate, mutate.
					col.Delete(1)
					col.Update(2, map[string]any{"v": 50.0})
					col.rotateSegment()
					col.Insert(map[string]any{"v": 60.0})
					close(gate)
				}
				<-gate
			}

			var wg sync.WaitGroup
			var scanned []ScanResult
			var counted uint64
			var scanErr, countErr error
			wg.Add(2)
			go func() { defer wg.Done(); scanned, scanErr = col.Scan(allV) }()
			go func() { defer wg.Done(); counted, countErr = col.Count(allV) }()
			wg.Wait()
			if scanErr != nil || countErr != nil {
				t.Fatalf("scan=%v count=%v", scanErr, countErr)
			}
			if got := ids(scanned); !equalIDs(got, model) {
				t.Fatalf("Scan ids = %v, want model %v", got, model)
			}
			if counted != uint64(len(model)) || counted != uint64(len(scanned)) {
				t.Fatalf("Count=%d len(Scan)=%d model=%d", counted, len(scanned), len(model))
			}
		})
	}
}
