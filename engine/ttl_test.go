//nolint:errcheck
package engine

import (
	"testing"
	"time"

	"github.com/srjn45/filedbv2/query"
)

// openTTLCollection is openTestCollection with a collection-level DefaultTTL and
// the background compactor stopped so tests drive reaping explicitly.
func openTTLCollection(t *testing.T, ttl time.Duration) *Collection {
	t.Helper()
	dir := t.TempDir()
	col, err := OpenCollection("ttl", dir, CollectionConfig{
		SegmentMaxSize:  512,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
		DefaultTTL:      ttl,
	})
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	col.closeOnce.Do(func() { close(col.closed) })
	select {
	case <-col.compactC:
	default:
	}
	t.Cleanup(func() { col.Close() })
	return col
}

// TestTTL_NotExpiredIsVisible confirms a record with a future deadline reads
// normally through every access path.
func TestTTL_NotExpiredIsVisible(t *testing.T) {
	col := openTestCollection(t)
	future := time.Now().Add(time.Hour)
	id, _, err := col.InsertWithExpiry(map[string]any{"k": "v"}, future)
	if err != nil {
		t.Fatalf("InsertWithExpiry: %v", err)
	}
	if _, _, err := col.FindByID(id); err != nil {
		t.Errorf("FindByID on non-expired record: %v", err)
	}
	res, err := col.Scan(&query.FieldFilter{Field: "k", Op: query.OpEq, Value: `"v"`})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("Scan: got %d records, want 1", len(res))
	}
}

// TestTTL_ExpiredIsHidden pins the core acceptance property: a record past its
// deadline is invisible to every read path even before the reaper reclaims it.
func TestTTL_ExpiredIsHidden(t *testing.T) {
	col := openTestCollection(t)
	past := time.Now().Add(-time.Second)
	id, _, err := col.InsertWithExpiry(map[string]any{"k": "v"}, past)
	if err != nil {
		t.Fatalf("InsertWithExpiry: %v", err)
	}

	if _, _, err := col.FindByID(id); err == nil {
		t.Error("FindByID returned an expired record")
	}
	if _, err := col.Get(id); err == nil {
		t.Error("Get returned an expired record")
	}
	res, err := col.Scan(&query.FieldFilter{Field: "k", Op: query.OpEq, Value: `"v"`})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("Scan returned %d expired records, want 0", len(res))
	}
}

// TestTTL_DisappearsAfterDeadline is the literal "not before, gone after" check
// against a short real deadline.
func TestTTL_DisappearsAfterDeadline(t *testing.T) {
	col := openTestCollection(t)
	id, _, err := col.InsertWithExpiry(map[string]any{"k": "v"}, time.Now().Add(60*time.Millisecond))
	if err != nil {
		t.Fatalf("InsertWithExpiry: %v", err)
	}
	if _, _, err := col.FindByID(id); err != nil {
		t.Fatalf("record vanished before its deadline: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, _, err := col.FindByID(id); err == nil {
		t.Error("record still visible after its deadline")
	}
}

// TestTTL_DefaultApplied verifies a collection-level DefaultTTL stamps inserts
// that carry no explicit deadline.
func TestTTL_DefaultApplied(t *testing.T) {
	col := openTTLCollection(t, time.Hour)
	before := time.Now()
	id, _, err := col.Insert(map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	loc, ok := col.index.Get(id)
	if !ok {
		t.Fatal("record missing from index")
	}
	if loc.ExpiresAt == 0 {
		t.Fatal("DefaultTTL did not stamp an expiry")
	}
	got := time.Unix(0, loc.ExpiresAt)
	wantLo := before.Add(time.Hour - time.Minute)
	if got.Before(wantLo) {
		t.Errorf("expiry %v earlier than expected ~now+1h", got)
	}
}

// TestTTL_DefaultNoTTLNeverExpires confirms records never expire when no TTL is
// configured and none is supplied.
func TestTTL_DefaultNoTTLNeverExpires(t *testing.T) {
	col := openTestCollection(t)
	id, _, err := col.Insert(map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	loc, _ := col.index.Get(id)
	if loc.ExpiresAt != 0 {
		t.Errorf("record got an expiry (%d) with no TTL configured", loc.ExpiresAt)
	}
}

// TestTTL_UpdatePreservesDeadline verifies a data-only Update keeps a record's
// original expiry (sticky deadline), rather than clearing or refreshing it.
func TestTTL_UpdatePreservesDeadline(t *testing.T) {
	col := openTestCollection(t)
	deadline := time.Now().Add(time.Hour)
	id, _, err := col.InsertWithExpiry(map[string]any{"n": float64(1)}, deadline)
	if err != nil {
		t.Fatalf("InsertWithExpiry: %v", err)
	}
	orig, _ := col.index.Get(id)

	if _, err := col.Update(id, map[string]any{"n": float64(2)}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	after, _ := col.index.Get(id)
	if after.ExpiresAt != orig.ExpiresAt {
		t.Errorf("Update changed the deadline: %d → %d", orig.ExpiresAt, after.ExpiresAt)
	}
}

// TestTTL_UpdateWithExpiryOverrides verifies UpdateWithExpiry can move a
// record's deadline — here, expiring a previously-live record on the spot.
func TestTTL_UpdateWithExpiryOverrides(t *testing.T) {
	col := openTestCollection(t)
	id, _, err := col.InsertWithExpiry(map[string]any{"k": "v"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("InsertWithExpiry: %v", err)
	}
	if _, err := col.UpdateWithExpiry(id, map[string]any{"k": "v2"}, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("UpdateWithExpiry: %v", err)
	}
	if _, _, err := col.FindByID(id); err == nil {
		t.Error("record still visible after UpdateWithExpiry moved its deadline into the past")
	}
}

// TestTTL_ReapedByReaper verifies the reaper tombstones expired records and
// drops them from the index.
func TestTTL_ReapedByReaper(t *testing.T) {
	col := openTestCollection(t)
	for i := 0; i < 5; i++ {
		col.InsertWithExpiry(map[string]any{"n": float64(i)}, time.Now().Add(-time.Second))
	}
	live, _, _ := col.Insert(map[string]any{"n": float64(99)}) // never expires

	if err := col.reapExpired(); err != nil {
		t.Fatalf("reapExpired: %v", err)
	}
	if n := col.index.Len(); n != 1 {
		t.Errorf("after reap: index has %d entries, want 1", n)
	}
	if _, _, err := col.FindByID(live); err != nil {
		t.Errorf("reaper removed a live record: %v", err)
	}
}

// TestTTL_ReclaimedByCompaction verifies compaction drops expired records from
// the segment set even when the reaper has not tombstoned them.
func TestTTL_ReclaimedByCompaction(t *testing.T) {
	col := newCompactableCollection(t)
	for i := 0; i < 8; i++ {
		col.InsertWithExpiry(map[string]any{"n": float64(i)}, time.Now().Add(-time.Second))
	}
	col.Insert(map[string]any{"n": float64(100)}) // never expires
	col.rotateSegment()
	col.Insert(map[string]any{"n": float64(101)})
	col.rotateSegment()

	if err := col.compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Only the two non-expiring records should survive across all segments.
	all := &query.FieldFilter{Field: "n", Op: query.OpGte, Value: "0"}
	res, err := col.Scan(all)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 2 {
		t.Errorf("after compaction: %d records survive, want 2 (expired not reclaimed)", len(res))
	}
}

// TestTTL_SurvivesReopen confirms deadlines round-trip through index.json so a
// record's expiry is honored after a restart.
func TestTTL_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{SegmentMaxSize: 512, CompactInterval: 24 * time.Hour}

	col, err := OpenCollection("ttl", dir, cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	future := time.Now().Add(time.Hour)
	liveID, _, _ := col.InsertWithExpiry(map[string]any{"k": "keep"}, future)
	deadID, _, _ := col.InsertWithExpiry(map[string]any{"k": "gone"}, time.Now().Add(50*time.Millisecond))
	col.Close()

	time.Sleep(80 * time.Millisecond) // let the short deadline pass while closed

	reopened, err := OpenCollection("ttl", dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	loc, ok := reopened.index.Get(liveID)
	if !ok || loc.ExpiresAt == 0 {
		t.Fatalf("deadline not restored on reopen: ok=%v exp=%d", ok, loc.ExpiresAt)
	}
	if _, _, err := reopened.FindByID(liveID); err != nil {
		t.Errorf("live record not visible after reopen: %v", err)
	}
	if _, _, err := reopened.FindByID(deadID); err == nil {
		t.Error("record expired-while-closed is visible after reopen")
	}
}

// TestTTL_SecondaryIndexExcludesExpired confirms an indexed eq query does not
// surface an expired record, even before the reaper prunes the index bucket.
func TestTTL_SecondaryIndexExcludesExpired(t *testing.T) {
	col := openTestCollection(t)
	col.EnsureIndex("status")
	col.InsertWithExpiry(map[string]any{"status": "active"}, time.Now().Add(-time.Second))
	col.Insert(map[string]any{"status": "active"}) // never expires

	res, err := col.Scan(&query.FieldFilter{Field: "status", Op: query.OpEq, Value: `"active"`})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("indexed eq surfaced %d records, want 1 (expired excluded)", len(res))
	}
}
