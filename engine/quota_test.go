//nolint:errcheck
package engine

import (
	"errors"
	"testing"
	"time"
)

// quotaCfg returns a test config with the given record/byte caps and automatic
// compaction disabled.
func quotaCfg(maxRecords, maxBytes uint64) CollectionConfig {
	cfg := testCfg()
	cfg.MaxRecords = maxRecords
	cfg.MaxBytes = maxBytes
	return cfg
}

func TestQuotaMaxRecordsRefusesInsert(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("capped", dir, quotaCfg(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	if _, _, err := col.Insert(map[string]any{"n": float64(1)}); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"n": float64(2)}); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	// The third insert breaches MaxRecords=2.
	_, _, err = col.Insert(map[string]any{"n": float64(3)})
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("insert 3: got %v, want ErrResourceExhausted", err)
	}
	// Nothing was written for the refused insert.
	if got := col.Stats().RecordCount; got != 2 {
		t.Fatalf("RecordCount = %d, want 2 (refused insert must not persist)", got)
	}
}

func TestQuotaMaxRecordsAllowsUpdateAndDelete(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("capped", dir, quotaCfg(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	id, _, err := col.Insert(map[string]any{"n": float64(1)})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	// A record-count cap must not block an in-place update (no new record).
	if _, err := col.Update(id, map[string]any{"n": float64(2)}); err != nil {
		t.Fatalf("update at cap: %v", err)
	}
	// Nor a delete — a tenant at its limit must be able to recover.
	if err := col.Delete(id); err != nil {
		t.Fatalf("delete at cap: %v", err)
	}
	// After deleting, an insert fits again.
	if _, _, err := col.Insert(map[string]any{"n": float64(3)}); err != nil {
		t.Fatalf("insert after delete: %v", err)
	}
}

func TestQuotaMaxBytesRefusesInsert(t *testing.T) {
	dir := t.TempDir()
	// Give room for a couple of small records, then a tight byte budget.
	col, err := OpenCollection("bytecap", dir, quotaCfg(0, 200))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	// Insert until the byte quota refuses a write.
	var refused bool
	for i := 0; i < 100; i++ {
		_, _, err := col.Insert(map[string]any{"payload": "some data value here"})
		if err != nil {
			if !errors.Is(err, ErrResourceExhausted) {
				t.Fatalf("insert %d: unexpected error %v", i, err)
			}
			refused = true
			break
		}
	}
	if !refused {
		t.Fatal("expected a byte-quota refusal, got none")
	}
	// Usage must remain within the configured budget (nothing written past it).
	if got := col.Stats().SizeBytes; got > 200 {
		t.Fatalf("SizeBytes = %d, want <= 200", got)
	}
}

func TestQuotaInsertManyAtomicRejection(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("batch", dir, quotaCfg(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	if _, _, err := col.Insert(map[string]any{"n": float64(0)}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	// A batch of 3 would take the count to 4, past MaxRecords=3: reject the whole
	// batch atomically.
	batch := []map[string]any{{"n": float64(1)}, {"n": float64(2)}, {"n": float64(3)}}
	_, _, err = col.InsertMany(batch, time.Time{})
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("InsertMany: got %v, want ErrResourceExhausted", err)
	}
	if got := col.Stats().RecordCount; got != 1 {
		t.Fatalf("RecordCount = %d, want 1 (rejected batch must write nothing)", got)
	}
	// A batch that fits is applied in full.
	if ids, _, err := col.InsertMany([]map[string]any{{"n": float64(1)}, {"n": float64(2)}}, time.Time{}); err != nil {
		t.Fatalf("InsertMany within budget: %v", err)
	} else if len(ids) != 2 {
		t.Fatalf("InsertMany returned %d ids, want 2", len(ids))
	}
	if got := col.Stats().RecordCount; got != 3 {
		t.Fatalf("RecordCount = %d, want 3", got)
	}
}

func TestQuotaUnlimitedUnaffected(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("free", dir, quotaCfg(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	for i := 0; i < 50; i++ {
		if _, _, err := col.Insert(map[string]any{"n": float64(i)}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if got := col.Stats().RecordCount; got != 50 {
		t.Fatalf("RecordCount = %d, want 50", got)
	}
}

func TestQuotaUpsertInsertRefusedReplaceAllowed(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("kv", dir, quotaCfg(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	if _, err := col.Upsert("a", map[string]any{"v": float64(1)}); err != nil {
		t.Fatalf("upsert insert a: %v", err)
	}
	// Replacing an existing key adds no record — allowed at the cap.
	if _, err := col.Upsert("a", map[string]any{"v": float64(2)}); err != nil {
		t.Fatalf("upsert replace a at cap: %v", err)
	}
	// Upserting a new key would create a second record — refused.
	_, err = col.Upsert("b", map[string]any{"v": float64(3)})
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("upsert insert b: got %v, want ErrResourceExhausted", err)
	}
}

func TestQuotaMapOverlayByName(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.Quotas = map[string]Quota{"limited": {MaxRecords: 1}}

	limited, err := OpenCollection("limited", dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if got := limited.Config().MaxRecords; got != 1 {
		t.Fatalf("limited MaxRecords = %d, want 1 (Quotas overlay)", got)
	}

	// A collection not named in Quotas stays unlimited.
	other, err := OpenCollection("other", dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if got := other.Config().MaxRecords; got != 0 {
		t.Fatalf("other MaxRecords = %d, want 0 (unlisted stays unlimited)", got)
	}
}
