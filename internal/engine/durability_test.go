//nolint:errcheck
package engine

import (
	"testing"
	"time"
)

// syncCfg returns a test config with the given durability mode and a short
// flush interval, with automatic compaction disabled.
func syncCfg(mode SyncMode) CollectionConfig {
	cfg := testCfg()
	cfg.SyncMode = mode
	cfg.SyncInterval = 20 * time.Millisecond
	return cfg
}

// TestCRUDUnderEachSyncMode exercises the full write path under every sync mode
// and verifies records remain correct and durable across a reopen.
func TestCRUDUnderEachSyncMode(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeNone, SyncModeAlways, SyncModeInterval} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			col, err := OpenCollection("users", dir, syncCfg(mode))
			if err != nil {
				t.Fatal(err)
			}

			id, _, err := col.Insert(map[string]any{"name": "alice"})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if _, err := col.Update(id, map[string]any{"name": "alice2"}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			id2, _, err := col.Insert(map[string]any{"name": "bob"})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if err := col.Delete(id2); err != nil {
				t.Fatalf("Delete: %v", err)
			}

			// For interval mode, give the flush loop a chance to run.
			if mode == SyncModeInterval {
				time.Sleep(60 * time.Millisecond)
			}
			col.Close()

			// Reopen and verify state survived.
			col2, err := OpenCollection("users", dir, syncCfg(mode))
			if err != nil {
				t.Fatal(err)
			}
			defer col2.Close()

			data, _, err := col2.FindByID(id)
			if err != nil {
				t.Fatalf("FindByID after reopen: %v", err)
			}
			if data["name"] != "alice2" {
				t.Errorf("mode %s: expected name=alice2, got %v", mode, data["name"])
			}
			if _, _, err := col2.FindByID(id2); err == nil {
				t.Errorf("mode %s: deleted id %d should not be found", mode, id2)
			}
		})
	}
}

// TestZeroValueConfigDefaultsToNone verifies that a zero-valued CollectionConfig
// is normalized to a safe, working durability policy.
func TestZeroValueConfigDefaultsToNone(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("c", dir, CollectionConfig{}) // all zero values
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	if col.cfg.SyncMode != SyncModeNone {
		t.Errorf("expected SyncMode normalized to none, got %q", col.cfg.SyncMode)
	}
	if col.cfg.SyncInterval != DefaultSyncInterval {
		t.Errorf("expected SyncInterval normalized to default, got %v", col.cfg.SyncInterval)
	}
	if _, _, err := col.Insert(map[string]any{"k": "v"}); err != nil {
		t.Fatalf("Insert with zero config: %v", err)
	}
}

// TestSegmentSyncSafeOnSealed verifies Sync is a no-op (no panic/error) on a
// sealed segment that holds no open file handle.
func TestSegmentSyncSafeOnSealed(t *testing.T) {
	sealed := openSealedSegment("/nonexistent/seg.ndjson", 0)
	if err := sealed.Sync(); err != nil {
		t.Errorf("Sync on sealed segment should be a no-op, got %v", err)
	}
}

// TestSyncAlwaysCommitTx verifies a transaction commit succeeds under
// SyncModeAlways and the staged ops are persisted.
func TestSyncAlwaysCommitTx(t *testing.T) {
	dir := t.TempDir()
	col, err := OpenCollection("tx", dir, syncCfg(SyncModeAlways))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	id := col.ReserveID()
	now := time.Now().UTC()
	ops := []txOp{{kind: txOpInsert, id: id, data: map[string]any{"name": "carol"}, ts: now}}
	if err := col.CommitTx(ops); err != nil {
		t.Fatalf("CommitTx: %v", err)
	}

	data, _, err := col.FindByID(id)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if data["name"] != "carol" {
		t.Errorf("expected name=carol, got %v", data["name"])
	}
}
