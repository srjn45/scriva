//nolint:errcheck
package engine

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

// ttlClock is an injectable collection clock (CollectionConfig.nowFn).
type ttlClock struct{ ns atomic.Int64 }

func (k *ttlClock) set(t time.Time)     { k.ns.Store(t.UnixNano()) }
func (k *ttlClock) now() time.Time      { return time.Unix(0, k.ns.Load()) }
func (k *ttlClock) add(d time.Duration) { k.ns.Add(int64(d)) }
func openClockCollection(t *testing.T, dir string, k *ttlClock) *Collection {
	t.Helper()
	return openClockCollectionTTL(t, dir, k, time.Minute)
}

func openClockCollectionTTL(t *testing.T, dir string, k *ttlClock, ttl time.Duration) *Collection {
	t.Helper()
	col, err := OpenCollection("ttlvis", dir, CollectionConfig{
		SegmentMaxSize:  512,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
		DefaultTTL:      ttl,
		nowFn:           k.now,
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	col.closeOnce.Do(func() { close(col.closed) })
	select {
	case <-col.compactC:
	default:
	}
	return col
}

// visibleEverywhere reports (get, scan, count, countEq, indexLookup, exists)
// visibility of the single TTL record "k1" and one permanent record.
func ttlSurfaces(t *testing.T, c *Collection, id uint64) (vis [6]bool) {
	t.Helper()
	_, err := c.Get(id)
	vis[0] = err == nil
	res, err := c.Scan(&query.FieldFilter{Field: "tag", Op: query.OpEq, Value: `"x"`})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	vis[1] = len(res) == 1
	n, err := c.Count(query.MatchAll)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	vis[2] = n == 2 // permanent + ttl record
	ne, _ := c.Count(&query.FieldFilter{Field: "tag", Op: query.OpEq, Value: `"x"`})
	vis[3] = ne == 1
	ids, _ := c.IndexLookup("tag", "x")
	vis[4] = len(ids) == 1
	ok, _ := c.Exists("k1")
	vis[5] = ok
	return vis
}

func TestTTLVisibility_SurfacesAgreeAcrossLifecycle(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	dl := base.Add(time.Minute)
	k := &ttlClock{}
	k.set(base)
	dir := t.TempDir()
	c := openClockCollection(t, dir, k)
	defer func() { c.Close() }()
	if err := c.EnsureIndex("tag"); err != nil {
		t.Fatal(err)
	}
	c.InsertWithExpiry(map[string]any{"tag": "perm"}, base.Add(1000*time.Hour))
	id, _, err := c.InsertWithKey("k1", map[string]any{"tag": "x"})
	if err != nil {
		t.Fatal(err)
	}

	all := func(vis [6]bool, want bool, stage string) {
		t.Helper()
		for i, v := range vis {
			if v != want {
				t.Errorf("%s: surface %d visible=%v, want %v", stage, i, v, want)
			}
		}
	}
	all(ttlSurfaces(t, c, id), true, "before expiry")
	k.set(dl.Add(-time.Nanosecond))
	all(ttlSurfaces(t, c, id), true, "1ns before deadline")
	k.set(dl)
	all(ttlSurfaces(t, c, id), false, "at deadline exactly")
	k.set(dl.Add(time.Hour))
	all(ttlSurfaces(t, c, id), false, "after expiry before reaper")

	// Clock steps back: floor keeps it hidden.
	k.set(base)
	all(ttlSurfaces(t, c, id), false, "clock stepped back")
	k.set(dl.Add(time.Hour))

	if err := c.reapExpired(); err != nil {
		t.Fatal(err)
	}
	all(ttlSurfaces(t, c, id), false, "after reaper")
	c.rotateSegment()
	if err := c.compact(false); err != nil {
		t.Fatal(err)
	}
	all(ttlSurfaces(t, c, id), false, "after compaction")

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = openClockCollection(t, dir, k)
	all(ttlSurfaces(t, c, id), false, "after reopen")
}

// Reopen with an unreaped expired record: index load/rebuild and sidx rebuild
// must keep it hidden everywhere and Count must not count it.
func TestTTLVisibility_ReopenAndRebuildUnreaped(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	dl := base.Add(time.Minute)
	k := &ttlClock{}
	k.set(base)
	dir := t.TempDir()
	c := openClockCollection(t, dir, k)
	c.EnsureIndex("tag")
	c.InsertWithExpiry(map[string]any{"tag": "perm"}, base.Add(1000*time.Hour))
	id, _, _ := c.InsertWithKey("k1", map[string]any{"tag": "x"})
	c.Close()

	k.set(dl.Add(time.Second))
	c = openClockCollection(t, dir, k)
	defer c.Close()
	if v := ttlSurfaces(t, c, id); v != [6]bool{} {
		t.Fatalf("after reopen unreaped: %v", v)
	}
	// Force full primary + sidx rebuild.
	if err := c.index.Rebuild(c.allSegmentsForTest()); err != nil {
		t.Fatal(err)
	}
	if err := c.rebuildSidxForTest("tag"); err != nil {
		t.Fatal(err)
	}
	if v := ttlSurfaces(t, c, id); v != [6]bool{} {
		t.Fatalf("after rebuild unreaped: %v", v)
	}
}

func TestTTLVisibility_ExpiredUniqueKeyHolderIsVacant(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	dl := base.Add(time.Minute)
	k := &ttlClock{}
	k.set(base)
	c := openClockCollection(t, t.TempDir(), k)
	defer c.Close()
	_, _, _ = c.InsertWithKey("dup", map[string]any{"v": 1})
	if _, _, err := c.InsertWithKey("dup", map[string]any{"v": 2}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("live holder: want ErrDuplicateKey, got %v", err)
	}
	k.set(dl)
	id2, _, err := c.InsertWithKey("dup", map[string]any{"v": 2})
	if err != nil {
		t.Fatalf("expired holder should be vacant: %v", err)
	}
	if err := c.reapExpired(); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Exists("dup"); !ok {
		t.Fatal("new holder must stay visible after reaping the old one")
	}
	if _, err := c.Get(id2); err != nil {
		t.Fatal(err)
	}
}

// A scan whose clock crosses a deadline mid-scan still returns the record.
func TestTTLVisibility_ScanCutoffCapturedOnce(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	dl := base.Add(time.Minute)
	k := &ttlClock{}
	k.set(base)
	c := openClockCollection(t, t.TempDir(), k)
	defer c.Close()
	for i := 0; i < 5; i++ {
		c.InsertWithExpiry(map[string]any{"i": i}, dl)
	}
	var n int
	err := c.forEachMatch(t.Context(), query.MatchAll, &ScanStats{}, func(ScanResult) error {
		k.set(dl.Add(time.Hour)) // clock crosses the deadline mid-scan
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("scan returned %d, want 5 (cutoff captured at start)", n)
	}
}

// Count(MatchAll) keeps its O(1) path while no record carries a deadline.
func TestTTLVisibility_CountFastPathWithoutTTL(t *testing.T) {
	k := &ttlClock{}
	k.set(time.Unix(1_800_000_000, 0))
	c := openClockCollectionTTL(t, t.TempDir(), k, 0)
	defer c.Close()
	c.Insert(map[string]any{"a": 1})
	if c.index.mayExpire {
		t.Fatal("mayExpire set without any TTL record")
	}
	if n, _ := c.Count(query.MatchAll); n != 1 {
		t.Fatalf("count=%d", n)
	}
}

func (c *Collection) allSegmentsForTest() []*Segment {
	return append(append([]*Segment(nil), c.sealed...), c.active)
}

func (c *Collection) rebuildSidxForTest(field string) error {
	c.sidxMu.RLock()
	sidx := c.sidxMap[field]
	c.sidxMu.RUnlock()
	return sidx.rebuild(c.allSegmentsForTest(), false, c.currentDecisions())
}
