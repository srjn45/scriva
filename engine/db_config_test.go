package engine

import (
	"testing"
)

func TestCollectionWithConfigOpensWithConfig(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	col, err := db.CollectionWithConfig("ledger", CollectionConfig{SyncMode: SyncModeAlways})
	if err != nil {
		t.Fatalf("CollectionWithConfig: %v", err)
	}
	if got := col.Config().SyncMode; got != SyncModeAlways {
		t.Fatalf("SyncMode = %q, want %q", got, SyncModeAlways)
	}

	// Data written through the returned collection is durable and readable.
	id, _, err := col.Insert(map[string]any{"amount": 10})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, _, err := col.FindByID(id); err != nil {
		t.Fatalf("find: %v", err)
	}
}

func TestCollectionWithConfigReopensPreOpened(t *testing.T) {
	dir := t.TempDir()

	// First run: create the collection under the default (none) config and write
	// a record, then close so it lands on disk.
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col, err := db.CreateCollection("ledger")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"amount": 1}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: Open pre-opens "ledger" under the default config. Asking for it via
	// CollectionWithConfig must reopen it under the requested config (the override
	// takes effect) without losing the record.
	db2, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()

	if got := db2.mustCol(t, "ledger").Config().SyncMode; got != SyncModeNone {
		t.Fatalf("pre-open SyncMode = %q, want %q", got, SyncModeNone)
	}

	col2, err := db2.CollectionWithConfig("ledger", CollectionConfig{SyncMode: SyncModeAlways})
	if err != nil {
		t.Fatalf("CollectionWithConfig reopen: %v", err)
	}
	if got := col2.Config().SyncMode; got != SyncModeAlways {
		t.Fatalf("reopened SyncMode = %q, want %q", got, SyncModeAlways)
	}
	got, err := col2.Scan(nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("record count after reopen = %d, want 1", len(got))
	}
}

// mustCol fetches an already-open collection or fails the test.
func (db *DB) mustCol(t *testing.T, name string) *Collection {
	t.Helper()
	c, err := db.Collection(name)
	if err != nil {
		t.Fatalf("Collection(%q): %v", name, err)
	}
	return c
}
