package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Stress and model tests for the stable scan view (hardening §3.5, §4/§5):
// heavy concurrent writes, rotation, compaction and long streaming scans, with
// each scan checked against a model snapshot taken at its capture barrier.

type stressRec struct {
	rev uint64
	v   float64
}

// stressModel mirrors the collection. Writers apply each op to the collection
// and the model under mu.Lock; a scanner snapshots the model under mu.RLock and
// keeps the lock until its view is captured, so the snapshot is exactly the
// state the scan must return.
type stressModel struct {
	mu   sync.RWMutex
	recs map[uint64]stressRec
}

func (m *stressModel) snapshot() map[uint64]stressRec {
	out := make(map[uint64]stressRec, len(m.recs))
	for k, v := range m.recs {
		out[k] = v
	}
	return out
}

func stressCol(t *testing.T, hook *atomic.Pointer[func(scanHookPoint, uint64)], opts ...func(*CollectionConfig)) *Collection {
	t.Helper()
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.SegmentMaxSize = 8 << 10 // frequent rotation
	cfg.AllowNonAtomicBatch = true
	cfg.CompactDirtyPct = 2
	cfg.scanHook = func(p scanHookPoint, a uint64) {
		if h := hook.Load(); h != nil {
			(*h)(p, a)
		}
	}
	for _, o := range opts {
		o(&cfg)
	}
	col, err := OpenCollection("stress", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	t.Cleanup(func() { col.Close() })
	return col
}

func TestScanStress_ModelUnderWritesRotationCompaction(t *testing.T) {
	var hookp atomic.Pointer[func(scanHookPoint, uint64)]
	col := stressCol(t, &hookp)
	m := &stressModel{recs: map[uint64]stressRec{}}
	for i := 0; i < 150; i++ {
		id, _, err := col.Insert(map[string]any{"v": float64(i), "pad": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"})
		if err != nil {
			t.Fatal(err)
		}
		m.recs[id] = stressRec{rev: 1, v: float64(i)}
	}

	// Scans are started one at a time; the hook fires once per scan, at the
	// first segment after capture, and releases the model read lock.
	captured := make(chan struct{}, 1)
	h := func(p scanHookPoint, a uint64) {
		if p == scanAtSegment && a == 0 {
			captured <- struct{}{}
		}
	}
	hookp.Store(&h)

	deadline := time.Now().Add(1500 * time.Millisecond)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64
	var ops atomic.Int64
	errc := make(chan error, 64)
	fail := func(err error) {
		select {
		case errc <- err:
		default:
		}
	}

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.mu.Lock()
				var live []uint64
				for id := range m.recs {
					live = append(live, id)
				}
				var err error
				switch op := rng.Intn(10); {
				case op < 3 || len(live) < 20:
					v := float64(seq.Add(1) + 1000)
					var id uint64
					id, _, err = col.Insert(map[string]any{"v": v, "pad": "yyyyyyyyyyyyyyyyyyyyyyyyyyyy"})
					if err == nil {
						m.recs[id] = stressRec{1, v}
					}
				case op < 6:
					id := live[rng.Intn(len(live))]
					v := float64(seq.Add(1) + 1000)
					if _, err = col.Update(id, map[string]any{"v": v, "pad": "zzzzzzzzzzzzzzzzzzzzzzzzzzzz"}); err == nil {
						m.recs[id] = stressRec{m.recs[id].rev + 1, v}
					}
				case op < 8:
					id := live[rng.Intn(len(live))]
					if err = col.Delete(id); err == nil {
						delete(m.recs, id)
					}
				default:
					batch := []map[string]any{
						{"v": float64(seq.Add(1) + 1000)}, {"v": float64(seq.Add(1) + 1000)},
					}
					var bids []uint64
					bids, _, err = col.InsertMany(batch, time.Time{})
					if err == nil {
						for i, id := range bids {
							m.recs[id] = stressRec{1, batch[i]["v"].(float64)}
						}
					}
				}
				m.mu.Unlock()
				if err != nil {
					fail(fmt.Errorf("writer: %w", err))
					return
				}
				ops.Add(1)
			}
		}(w)
	}

	wg.Add(1)
	go func() { // rotation + compaction
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			m.mu.Lock()
			var err error
			if i%2 == 0 {
				err = col.rotateSegment()
			}
			m.mu.Unlock()
			if err != nil {
				fail(fmt.Errorf("rotate: %w", err))
				return
			}
			if err := col.CompactNow(); err != nil && !errors.Is(err, ErrCompactionDeferred) {
				fail(fmt.Errorf("compact: %w", err))
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var startMu sync.Mutex
	var scans atomic.Int64
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				startMu.Lock()
				m.mu.RLock()
				want := m.snapshot()
				done := make(chan error, 1)
				got := map[uint64]ScanResult{}
				slow := s%2 == 0
				go func() {
					_, err := col.ScanStream(context.Background(), ScanOptions{Filter: allV}, func(r ScanResult) error {
						if _, dup := got[r.ID]; dup {
							return fmt.Errorf("duplicate id %d", r.ID)
						}
						got[r.ID] = r
						if slow && len(got)%16 == 0 {
							time.Sleep(time.Millisecond) // long-lived streaming scan
						}
						return nil
					})
					done <- err
				}()
				select {
				case <-captured:
				case err := <-done:
					m.mu.RUnlock()
					startMu.Unlock()
					fail(fmt.Errorf("scan ended before capture: %w", err))
					return
				}
				m.mu.RUnlock()
				startMu.Unlock()
				if err := <-done; err != nil {
					fail(fmt.Errorf("scan: %w", err))
					return
				}
				if len(got) != len(want) {
					fail(fmt.Errorf("scan returned %d records, start view had %d", len(got), len(want)))
					return
				}
				for id, w := range want {
					r, ok := got[id]
					if !ok {
						fail(fmt.Errorf("id %d omitted", id))
						return
					}
					if r.Rev != w.rev || r.Data["v"] != w.v {
						fail(fmt.Errorf("id %d: got rev=%d v=%v, want rev=%d v=%v", id, r.Rev, r.Data["v"], w.rev, w.v))
						return
					}
				}
				scans.Add(1)
			}
		}(s)
	}

	time.Sleep(time.Until(deadline))
	// scanners exit on their own at the deadline; stop writers afterwards
	waitScanners := make(chan struct{})
	go func() {
		for scans.Load() < 1 && time.Now().Before(deadline.Add(5*time.Second)) {
			time.Sleep(10 * time.Millisecond)
		}
		close(waitScanners)
	}()
	<-waitScanners
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}
	if scans.Load() == 0 || ops.Load() == 0 {
		t.Fatalf("no progress: scans=%d writes=%d", scans.Load(), ops.Load())
	}
	t.Logf("verified %d scans against %d concurrent writes", scans.Load(), ops.Load())
	assertScanResourcesFree(t, col)
}

// assertScanResourcesFree checks that no lease or slot outlives its scan.
func assertScanResourcesFree(t *testing.T, col *Collection) {
	t.Helper()
	if !col.layoutMu.TryLock() {
		t.Fatal("layout lease leaked")
	}
	col.layoutMu.Unlock()
	if n := len(col.scanSlots); n != 0 {
		t.Fatalf("%d scan slots leaked", n)
	}
}

// Cancellation under load: scans cancelled at random points, plus scans
// queued behind a full slot semaphore, all release lease, slot and copy.
func TestScanStress_CancellationUnderLoad(t *testing.T) {
	var hookp atomic.Pointer[func(scanHookPoint, uint64)]
	col := stressCol(t, &hookp, func(c *CollectionConfig) { c.MaxScanViews = 3 })
	seed(t, col, 300)
	_ = col.rotateSegment()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // background writer must never be starved
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			col.Insert(map[string]any{"v": float64(i)})
			col.Update(uint64(i%300+1), map[string]any{"v": float64(-i)})
			time.Sleep(200 * time.Microsecond)
		}
	}()

	var sw sync.WaitGroup
	var cancelled atomic.Int64
	for g := 0; g < 12; g++ {
		sw.Add(1)
		go func(g int) {
			defer sw.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 12; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				cutoff := rng.Intn(300)
				if g%3 == 0 { // cancel while possibly queued for a slot
					delay := time.Duration(rng.Intn(300)) * time.Microsecond
					go func() { time.Sleep(delay); cancel() }()
				}
				n := 0
				_, err := col.ScanStream(ctx, ScanOptions{Filter: allV}, func(ScanResult) error {
					if n++; n == cutoff {
						cancel()
					}
					return nil
				})
				cancel()
				if errors.Is(err, context.Canceled) {
					cancelled.Add(1)
				} else if err != nil {
					t.Errorf("scan: %v", err)
					return
				}
			}
		}(g)
	}
	done := make(chan struct{})
	go func() { sw.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: scans did not finish")
	}
	close(stop)
	wg.Wait()
	if cancelled.Load() == 0 {
		t.Fatal("no scan was cancelled")
	}
	assertScanResourcesFree(t, col)
	if err := col.CompactNow(); err != nil {
		t.Fatalf("CompactNow after cancellations: %v", err)
	}
}

// Writers hold c.mu only for short appends while scans hold the layout lease
// read-side: parked and slow scans must not starve them, and pins stay bounded
// by MaxScanViews.
func TestScanStress_NoWriterStarvationBoundedPins(t *testing.T) {
	var hookp atomic.Pointer[func(scanHookPoint, uint64)]
	const maxViews = 4
	col := stressCol(t, &hookp, func(c *CollectionConfig) { c.MaxScanViews = maxViews })
	seed(t, col, 500)

	var arrived sync.WaitGroup
	arrived.Add(maxViews)
	gate := make(chan struct{})
	var seen atomic.Int32
	var inFlight, peak atomic.Int32
	h := func(p scanHookPoint, a uint64) {
		if p == scanAtSegment && a == 0 {
			n := inFlight.Add(1)
			for {
				pk := peak.Load()
				if n <= pk || peak.CompareAndSwap(pk, n) {
					break
				}
			}
			if int(seen.Add(1)) <= maxViews {
				arrived.Done()
				<-gate
			}
		}
	}
	hookp.Store(&h)

	errs := make(chan error, maxViews+4)
	for i := 0; i < maxViews+4; i++ { // more scans than slots
		go func() {
			_, err := col.ScanStream(context.Background(), ScanOptions{Filter: allV}, func(ScanResult) error {
				return nil
			})
			inFlight.Add(-1)
			errs <- err
		}()
	}
	arrived.Wait()

	start := time.Now()
	var worst time.Duration
	for i := 0; i < 300; i++ {
		t0 := time.Now()
		if _, _, err := col.Insert(map[string]any{"v": float64(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := col.Update(uint64(i%500+1), map[string]any{"v": -1.0}); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(t0); d > worst {
			worst = d
		}
	}
	if el := time.Since(start); el > 20*time.Second {
		t.Fatalf("writers starved by %d parked scans: 600 writes took %v", maxViews, el)
	}
	if pk := peak.Load(); pk > maxViews {
		t.Fatalf("%d scan views open, MaxScanViews=%d", pk, maxViews)
	}
	t.Logf("600 writes with %d parked scans in %v (worst pair %v)", maxViews, time.Since(start), worst)

	close(gate)
	for i := 0; i < maxViews+4; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if pk := peak.Load(); pk > maxViews {
		t.Fatalf("peak %d open views exceeded MaxScanViews=%d", pk, maxViews)
	}
	assertScanResourcesFree(t, col)
}

// Memory per captured view is O(live records): the cloned index stays within a
// small constant per record.
func TestScanStress_ViewMemoryBounded(t *testing.T) {
	var hookp atomic.Pointer[func(scanHookPoint, uint64)]
	col := stressCol(t, &hookp, func(c *CollectionConfig) { c.SegmentMaxSize = 1 << 20 })
	const n = 5000
	for i := 0; i < n; i++ {
		col.Insert(map[string]any{"v": float64(i)})
	}
	measure := func() uint64 {
		var a, b runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&a)
		col.layoutMu.RLock()
		col.mu.RLock()
		v := col.captureViewLocked(nil)
		col.mu.RUnlock()
		col.layoutMu.RUnlock()
		runtime.GC()
		runtime.ReadMemStats(&b)
		runtime.KeepAlive(v)
		return b.HeapAlloc - a.HeapAlloc
	}
	per := float64(measure()) / n
	t.Logf("captured view: %.1f bytes/record (%d records)", per, n)
	if per > 192 {
		t.Fatalf("view costs %.1f bytes/record, want O(64-ish) <= 192", per)
	}
	// released view leaves nothing behind
	runtime.GC()
	var base, after runtime.MemStats
	runtime.ReadMemStats(&base)
	for i := 0; i < 5; i++ {
		if _, err := col.Count(allV); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > base.HeapAlloc+uint64(n)*32 {
		t.Fatalf("heap grew %d bytes across finished scans", after.HeapAlloc-base.HeapAlloc)
	}
}

func benchCol(b *testing.B, n int) *Collection {
	b.Helper()
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.SegmentMaxSize = 8 << 20
	cfg.AllowNonAtomicBatch = true
	col, err := OpenCollection("bench", b.TempDir(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { col.Close() })
	for i := 0; i < n; i++ {
		col.Insert(map[string]any{"v": float64(i)})
	}
	return col
}

// BenchmarkScanViewCapture measures view capture cost and memory per record.
func BenchmarkScanViewCapture(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			col := benchCol(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				col.layoutMu.RLock()
				col.mu.RLock()
				v := col.captureViewLocked(nil)
				col.mu.RUnlock()
				col.layoutMu.RUnlock()
				_ = v
			}
		})
	}
}

// BenchmarkConcurrentScanWrite measures single-writer Insert latency with and
// without long streaming scans running against the same collection.
func BenchmarkConcurrentScanWrite(b *testing.B) {
	for _, scanners := range []int{0, 4} {
		b.Run(fmt.Sprintf("scanners=%d", scanners), func(b *testing.B) {
			col := benchCol(b, 20000)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < scanners; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						col.ScanStream(context.Background(), ScanOptions{Filter: allV}, func(ScanResult) error { return nil })
					}
				}()
			}
			var worst time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				t0 := time.Now()
				col.Insert(map[string]any{"v": float64(i)})
				if d := time.Since(t0); d > worst {
					worst = d
				}
			}
			b.StopTimer()
			close(stop)
			wg.Wait()
			b.ReportMetric(float64(worst.Microseconds()), "worst-write-µs")
		})
	}
}
