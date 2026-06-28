//nolint:errcheck
package engine

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/srjn45/filedbv2/internal/query"
)

// openCompactionCollection creates a collection tuned for compaction testing:
// tiny segments (so a short op stream still spans several segments), a low
// dirty threshold (so compaction actually runs), and a secondary index on
// "grp". The background compactor goroutine is stopped so the test drives
// compact() explicitly without races.
func openCompactionCollection(t *testing.T) *Collection {
	t.Helper()
	col, err := OpenCollection("test", t.TempDir(), CollectionConfig{
		SegmentMaxSize:  256,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.05,
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	col.closeOnce.Do(func() { close(col.closed) })
	select {
	case <-col.compactC:
	default:
	}
	if err := col.EnsureIndex("grp"); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	t.Cleanup(func() { col.Close() })
	return col
}

// liveModel mirrors the collection's intended live state: id -> latest data.
// Values are strings so they survive a JSON encode/decode round trip exactly.
type liveModel struct {
	data         map[uint64]map[string]any
	ids          []uint64        // live ids, for picking update/delete targets
	everInserted map[uint64]bool // every id ever inserted, for delete checks
}

func newLiveModel() *liveModel {
	return &liveModel{
		data:         make(map[uint64]map[string]any),
		everInserted: make(map[uint64]bool),
	}
}

func (m *liveModel) insert(id uint64, d map[string]any) {
	m.data[id] = d
	m.ids = append(m.ids, id)
	m.everInserted[id] = true
}

// deletedIDs returns ids that were inserted at some point but are no longer
// live — these must not resurface after compaction.
func (m *liveModel) deletedIDs() map[uint64]bool {
	out := make(map[uint64]bool)
	for id := range m.everInserted {
		if _, ok := m.data[id]; !ok {
			out[id] = true
		}
	}
	return out
}

func (m *liveModel) update(id uint64, d map[string]any) { m.data[id] = d }

func (m *liveModel) remove(id uint64) {
	delete(m.data, id)
	for i, x := range m.ids {
		if x == id {
			m.ids = append(m.ids[:i], m.ids[i+1:]...)
			break
		}
	}
}

// snapshotLive returns the collection's live records (id -> data) via a full
// scan, the path that actually reads from disk and reflects compaction output.
func snapshotLive(t *testing.T, col *Collection) map[uint64]map[string]any {
	t.Helper()
	results, err := col.Scan(query.MatchAll)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	out := make(map[uint64]map[string]any, len(results))
	for _, r := range results {
		out[r.ID] = r.Data
	}
	return out
}

func sameRecord(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if fmt.Sprint(b[k]) != fmt.Sprint(av) {
			return false
		}
	}
	return true
}

func sameLiveSet(a, b map[uint64]map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for id, av := range a {
		bv, ok := b[id]
		if !ok || !sameRecord(av, bv) {
			return false
		}
	}
	return true
}

// runOps applies a randomized Insert/Update/Delete mix driven by ops, rotating
// segments periodically so there is sealed data for compaction to merge. It
// keeps the model in lockstep and returns it.
func runOps(t *testing.T, col *Collection, rng *rand.Rand, ops int) *liveModel {
	t.Helper()
	m := newLiveModel()
	seq := 0
	mkData := func() map[string]any {
		seq++
		return map[string]any{
			"grp": fmt.Sprintf("g%d", rng.Intn(4)),
			"val": fmt.Sprintf("v%d", seq),
		}
	}

	for i := 0; i < ops; i++ {
		switch r := rng.Intn(10); {
		case r < 5 || len(m.ids) == 0: // insert (always possible)
			d := mkData()
			id, _, err := col.Insert(d)
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			m.insert(id, d)
		case r < 8: // update an existing id
			id := m.ids[rng.Intn(len(m.ids))]
			d := mkData()
			if _, err := col.Update(id, d); err != nil {
				t.Fatalf("Update %d: %v", id, err)
			}
			m.update(id, d)
		default: // delete an existing id
			id := m.ids[rng.Intn(len(m.ids))]
			if err := col.Delete(id); err != nil {
				t.Fatalf("Delete %d: %v", id, err)
			}
			m.remove(id)
		}

		if rng.Intn(4) == 0 {
			if err := col.rotateSegment(); err != nil {
				t.Fatalf("rotateSegment: %v", err)
			}
		}
	}
	// Seal whatever is in the active segment so compaction can see all data.
	if err := col.rotateSegment(); err != nil {
		t.Fatalf("final rotateSegment: %v", err)
	}
	return m
}

func segmentCount(col *Collection) int {
	col.mu.RLock()
	defer col.mu.RUnlock()
	return len(col.sealed) + 1
}

// expectedGroups derives the secondary-index buckets implied by the model:
// grp value -> sorted set of ids.
func expectedGroups(m *liveModel) map[string][]uint64 {
	groups := make(map[string][]uint64)
	for id, d := range m.data {
		g := fmt.Sprint(d["grp"])
		groups[g] = append(groups[g], id)
	}
	for _, ids := range groups {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	return groups
}

// verifyCollection asserts that the on-disk collection matches the model:
// FindByID resolves every live id to its latest data, deleted ids are gone,
// and the secondary index buckets match.
func verifyCollection(t *testing.T, col *Collection, m *liveModel, deleted map[uint64]bool) {
	t.Helper()

	for id, want := range m.data {
		got, _, err := col.FindByID(id)
		if err != nil {
			t.Fatalf("FindByID(%d): %v", id, err)
		}
		if !sameRecord(want, got) {
			t.Fatalf("FindByID(%d): got %v want %v", id, got, want)
		}
	}

	for id := range deleted {
		if _, ok := m.data[id]; ok {
			continue // re-inserted ids are not actually deleted; ignore
		}
		if _, _, err := col.FindByID(id); err == nil {
			t.Fatalf("FindByID(%d): deleted id still resolves", id)
		}
	}

	// Secondary index buckets must match the model exactly.
	for g, wantIDs := range expectedGroups(m) {
		got, hit := col.IndexLookup("grp", g)
		if !hit {
			t.Fatalf("IndexLookup(grp,%s): no index", g)
		}
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		if fmt.Sprint(got) != fmt.Sprint(wantIDs) {
			t.Fatalf("IndexLookup(grp,%s): got %v want %v", g, got, wantIDs)
		}
	}
}

// compactionRound runs one full property check for a given seed: apply ops,
// snapshot the live set, compact, and assert nothing changed.
func compactionRound(t *testing.T, seed int64, ops int) {
	t.Helper()
	col := openCompactionCollection(t)
	rng := rand.New(rand.NewSource(seed))

	m := runOps(t, col, rng, ops)
	deleted := m.deletedIDs()

	before := snapshotLive(t, col)
	verifyCollection(t, col, m, deleted)
	beforeSegs := segmentCount(col)

	if err := col.compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	after := snapshotLive(t, col)
	if !sameLiveSet(before, after) {
		t.Fatalf("seed=%d: live set changed across compaction\nbefore=%v\nafter=%v",
			seed, before, after)
	}

	verifyCollection(t, col, m, deleted)

	if afterSegs := segmentCount(col); afterSegs > beforeSegs {
		t.Fatalf("seed=%d: segment count grew across compaction: before=%d after=%d",
			seed, beforeSegs, afterSegs)
	}
}

// TestCompactionPreservesData runs the compaction invariant check across a
// spread of deterministic seeds.
func TestCompactionPreservesData(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			compactionRound(t, seed, 80)
		})
	}
}

// FuzzCompaction fuzzes the seed and op-count driving the randomized mix, so
// the corpus explores op orderings beyond the fixed table seeds.
func FuzzCompaction(f *testing.F) {
	f.Add(int64(1), uint16(40))
	f.Add(int64(7), uint16(120))
	f.Add(int64(99), uint16(5))

	f.Fuzz(func(t *testing.T, seed int64, opsRaw uint16) {
		ops := int(opsRaw%200) + 1
		compactionRound(t, seed, ops)
	})
}
