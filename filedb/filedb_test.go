package filedb_test

import (
	"errors"
	"testing"
	"time"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/filedb"
)

// wardenStore stands up the warden-shaped collection set through the façade in a
// handful of lines — the EMB-2 acceptance shape. It returns the DB and its
// collections so tests can exercise them.
func wardenStore(t *testing.T, dir string) (*filedb.DB, map[string]*engine.Collection) {
	t.Helper()
	db, err := filedb.Open(dir)
	if err != nil {
		t.Fatalf("filedb.Open: %v", err)
	}
	cols := map[string]*engine.Collection{
		"sessions": db.MustCollection("sessions", filedb.WithUniqueIndex("name")),
		"events":   db.MustCollection("events"),
		"messages": db.MustCollection("messages"),
		"context":  db.MustCollection("context"),
		// spend needs an fsync on every write — opt out of the interval default.
		"spend": db.MustCollection("spend", filedb.WithCollectionSyncMode(engine.SyncModeAlways)),
	}
	return db, cols
}

func TestOpenCollectionsCRUDReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	db, cols := wardenStore(t, dir)

	// Keyed CRUD on sessions.
	if _, _, err := cols["sessions"].InsertWithKey("sess-1", map[string]any{"name": "alpha", "status": "open"}); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := cols["sessions"].UpdateByKey("sess-1", map[string]any{"name": "alpha", "status": "closed"}); err != nil {
		t.Fatalf("update session: %v", err)
	}
	// Plain inserts on the append-only collections.
	if _, _, err := cols["events"].Insert(map[string]any{"session": "sess-1", "kind": "tick"}); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if _, _, err := cols["messages"].Insert(map[string]any{"to": "agent-a", "body": "hi"}); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if _, err := cols["context"].Upsert("cfg-key", map[string]any{"v": 1}); err != nil {
		t.Fatalf("upsert context: %v", err)
	}
	if _, _, err := cols["spend"].Insert(map[string]any{"amount": 42}); err != nil {
		t.Fatalf("insert spend: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen recovers all data across every collection.
	db2, cols2 := wardenStore(t, dir)
	defer db2.Close()

	data, _, err := cols2["sessions"].FindByKey("sess-1")
	if err != nil {
		t.Fatalf("reopen find session: %v", err)
	}
	if data["status"] != "closed" {
		t.Fatalf("session status after reopen = %v, want closed", data["status"])
	}
	for _, name := range []string{"events", "messages", "context", "spend"} {
		got, err := cols2[name].Scan(nil)
		if err != nil {
			t.Fatalf("scan %q: %v", name, err)
		}
		if len(got) != 1 {
			t.Fatalf("collection %q after reopen has %d records, want 1", name, len(got))
		}
	}

	// The spend override must survive reopen — not be clobbered by the global
	// interval default.
	if got := cols2["spend"].Config().SyncMode; got != engine.SyncModeAlways {
		t.Fatalf("spend SyncMode after reopen = %q, want %q", got, engine.SyncModeAlways)
	}
}

func TestDefaultSyncModeInterval(t *testing.T) {
	t.Parallel()
	db, err := filedb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	col := db.MustCollection("plain")
	cfg := col.Config()
	if cfg.SyncMode != engine.SyncModeInterval {
		t.Fatalf("default SyncMode = %q, want %q", cfg.SyncMode, engine.SyncModeInterval)
	}
	if cfg.SyncInterval != time.Second {
		t.Fatalf("default SyncInterval = %v, want 1s", cfg.SyncInterval)
	}
}

func TestPerCollectionSyncAlwaysOverride(t *testing.T) {
	t.Parallel()
	db, err := filedb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ledger := db.MustCollection("spend", filedb.WithCollectionSyncMode(engine.SyncModeAlways))
	if got := ledger.Config().SyncMode; got != engine.SyncModeAlways {
		t.Fatalf("overridden SyncMode = %q, want %q", got, engine.SyncModeAlways)
	}

	// A sibling collection with no override keeps the interval default — proving
	// the override is scoped per collection, not global.
	plain := db.MustCollection("events")
	if got := plain.Config().SyncMode; got != engine.SyncModeInterval {
		t.Fatalf("sibling SyncMode = %q, want %q (interval default)", got, engine.SyncModeInterval)
	}
}

func TestPerCollectionQuotaOptions(t *testing.T) {
	t.Parallel()
	db, err := filedb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	capped := db.MustCollection("capped", filedb.WithMaxRecords(1), filedb.WithMaxBytes(4096))
	if cfg := capped.Config(); cfg.MaxRecords != 1 || cfg.MaxBytes != 4096 {
		t.Fatalf("capped quota cfg = {%d, %d}, want {1, 4096}", cfg.MaxRecords, cfg.MaxBytes)
	}

	// The cap is enforced through the embedded write path.
	if _, _, err := capped.Insert(map[string]any{"n": float64(1)}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, _, err = capped.Insert(map[string]any{"n": float64(2)})
	if !errors.Is(err, engine.ErrResourceExhausted) {
		t.Fatalf("over-quota insert: got %v, want ErrResourceExhausted", err)
	}

	// A sibling with no quota option stays unlimited.
	if cfg := db.MustCollection("free").Config(); cfg.MaxRecords != 0 || cfg.MaxBytes != 0 {
		t.Fatalf("sibling quota cfg = {%d, %d}, want {0, 0}", cfg.MaxRecords, cfg.MaxBytes)
	}
}

func TestOpenOptionOverridesDefault(t *testing.T) {
	t.Parallel()
	db, err := filedb.Open(t.TempDir(), filedb.WithSyncMode(engine.SyncModeNone))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if got := db.MustCollection("c").Config().SyncMode; got != engine.SyncModeNone {
		t.Fatalf("SyncMode with WithSyncMode(none) = %q, want %q", got, engine.SyncModeNone)
	}
}

func TestUniqueIndexAtOpen(t *testing.T) {
	t.Parallel()
	db, err := filedb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sessions := db.MustCollection("sessions", filedb.WithUniqueIndex("name"))
	if _, _, err := sessions.Insert(map[string]any{"name": "alpha"}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, _, err = sessions.Insert(map[string]any{"name": "alpha"})
	if !errors.Is(err, engine.ErrDuplicateKey) {
		t.Fatalf("duplicate insert err = %v, want ErrDuplicateKey", err)
	}
}

func TestCollectionFirstCallWinsAndCaches(t *testing.T) {
	t.Parallel()
	db, err := filedb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	first := db.MustCollection("c", filedb.WithCollectionSyncMode(engine.SyncModeAlways))
	// A second call for the same name returns the cached handle and ignores its
	// options (first call wins).
	second := db.MustCollection("c", filedb.WithCollectionSyncMode(engine.SyncModeNone))
	if first != second {
		t.Fatal("second Collection call returned a different handle; expected cached")
	}
	if got := second.Config().SyncMode; got != engine.SyncModeAlways {
		t.Fatalf("cached SyncMode = %q, want %q (first call wins)", got, engine.SyncModeAlways)
	}
}
