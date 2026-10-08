package engine

import (
	"sync/atomic"
	"testing"
	"time"
)

// A point read resolves the record's location from the primary index and then
// reads the segment without holding the collection lock. A compaction swap that
// lands in between replaces the sealed segment files and rewrites every index
// location, so the read used to follow a pre-swap location into the post-swap
// layout and fail with "segment not found" (the segment's name was not reused),
// a decode error, or an IntegrityError (the name was reused and the offset now
// holds another record). Seen as a TestKill9_DuringCompaction failure: the
// reopened handle's background compactor swapped between a Scan and a Get.
//
// The swap is forced deterministically at exactly that point for every live
// record of the collection.
func TestGet_CompactionSwapBetweenLocateAndRead(t *testing.T) {
	const n = 40
	build := func(t *testing.T, hook func()) (*Collection, map[uint64]int) {
		t.Helper()
		cfg := defaultConfig()
		cfg.CompactInterval = time.Hour // only the test compacts
		cfg.SegmentMaxSize = 256
		cfg.postLocateHook = hook
		col, err := OpenCollection("race", t.TempDir(), cfg)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		t.Cleanup(func() { _ = col.Close() })

		want := make(map[uint64]int, n)
		var ids []uint64
		for i := 0; i < n; i++ {
			id, _, err := col.Insert(map[string]any{"v": i})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			ids = append(ids, id)
			want[id] = i
		}
		// Garbage in the sealed segments: the compacted output is smaller, so
		// offsets shift and trailing segment names disappear.
		for i, id := range ids {
			switch i % 3 {
			case 0:
				if err := col.Delete(id); err != nil {
					t.Fatalf("Delete: %v", err)
				}
				delete(want, id)
			case 1:
				if _, err := col.Update(id, map[string]any{"v": 1000 + i}); err != nil {
					t.Fatalf("Update: %v", err)
				}
				want[id] = 1000 + i
			}
		}
		return col, want
	}

	// The record set is the same for every build; take the ids from a probe.
	_, probe := build(t, nil)
	for id := range probe {
		var armed atomic.Bool
		var col *Collection
		col, want := build(t, func() {
			if !armed.CompareAndSwap(true, false) {
				return
			}
			if err := col.CompactNow(); err != nil {
				t.Errorf("CompactNow: %v", err)
			}
		})
		before := col.layoutGen.Load()
		armed.Store(true)
		rec, err := col.Get(id)
		if err != nil {
			t.Fatalf("Get(%d) across a compaction swap: %v", id, err)
		}
		if got, _ := rec.Data["v"].(float64); int(got) != want[id] {
			t.Fatalf("Get(%d) across a compaction swap: v=%v, want %d", id, rec.Data["v"], want[id])
		}
		if armed.Load() || col.layoutGen.Load() == before {
			t.Fatalf("Get(%d): the swap did not run inside the read", id)
		}
	}
}

// When swaps keep overlapping the optimistic attempts, the read falls back to
// reading under the lock and still returns the right record.
func TestGet_FallsBackToLockedReadUnderRepeatedSwaps(t *testing.T) {
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour
	cfg.SegmentMaxSize = 256
	var col *Collection
	var hooks atomic.Int64
	cfg.postLocateHook = func() {
		hooks.Add(1)
		// Simulate a swap overlapping every optimistic attempt. The locked
		// attempt holds c.mu, which a real swap could not get past.
		col.layoutGen.Add(1)
	}
	col, err := OpenCollection("race", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	defer col.Close()
	id, _, err := col.Insert(map[string]any{"v": 7})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	rec, err := col.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, _ := rec.Data["v"].(float64); got != 7 {
		t.Fatalf("Get: v=%v, want 7", rec.Data["v"])
	}
	if got := hooks.Load(); got != 4 {
		t.Fatalf("read attempts = %d, want 4 (3 optimistic + 1 locked)", got)
	}
}
