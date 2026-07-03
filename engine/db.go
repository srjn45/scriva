// Package engine implements the core FileDB storage engine.
// A DB manages a set of named Collections, each stored as append-only
// NDJSON segment files in a dedicated sub-directory.
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DB is the top-level database handle. It owns a registry of collections and
// is the entry point for all engine operations.
type DB struct {
	dataDir     string
	defaultCfg  CollectionConfig
	mu          sync.RWMutex
	collections map[string]*Collection
}

// Open opens (or creates) the database rooted at dataDir.
// Existing collections are discovered and opened lazily on first access.
func Open(dataDir string, cfg CollectionConfig) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("db: mkdir %q: %w", dataDir, err)
	}
	db := &DB{
		dataDir:     dataDir,
		defaultCfg:  cfg,
		collections: make(map[string]*Collection),
	}
	// Pre-open existing collections.
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("db: read dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		col, err := OpenCollection(e.Name(), dataDir, cfg)
		if err != nil {
			return nil, fmt.Errorf("db: open collection %q: %w", e.Name(), err)
		}
		db.collections[e.Name()] = col
	}
	return db, nil
}

// CreateCollection creates a new collection with the given name.
// Returns an error if it already exists.
func (db *DB) CreateCollection(name string) (*Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.collections[name]; exists {
		return nil, fmt.Errorf("db: collection %q already exists", name)
	}
	col, err := OpenCollection(name, db.dataDir, db.defaultCfg)
	if err != nil {
		return nil, err
	}
	db.collections[name] = col
	return col, nil
}

// CollectionWithConfig returns the collection named name, opening it under cfg.
// Unlike Collection (which never creates) and CreateCollection (which fails if
// the collection already exists), this method is open-or-create: if the
// collection is not open it is opened under cfg; if it is already open — including
// collections Open discovered on disk and pre-opened under the DB's default
// config — it is closed and reopened under cfg so a per-collection override (for
// example SyncModeAlways on a ledger) actually takes effect. Reopening reloads
// from disk, so no committed data is lost.
//
// It is the entry point the embedded façade (the filedb package) uses to give
// each collection its own durability/compaction settings. The behavior of
// Collection and CreateCollection is unchanged — this method is additive.
func (db *DB) CollectionWithConfig(name string, cfg CollectionConfig) (*Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if existing, ok := db.collections[name]; ok {
		// Already open, possibly under a different (default) config. Close and
		// reopen under the requested config so the override takes effect.
		if err := existing.Close(); err != nil {
			return nil, fmt.Errorf("db: reopen collection %q: %w", name, err)
		}
		delete(db.collections, name)
	}
	col, err := OpenCollection(name, db.dataDir, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: open collection %q: %w", name, err)
	}
	db.collections[name] = col
	return col, nil
}

// DropCollection closes and deletes a collection and all its data.
func (db *DB) DropCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	col, exists := db.collections[name]
	if !exists {
		return fmt.Errorf("db: collection %q not found", name)
	}
	_ = col.Close()
	delete(db.collections, name)

	dir := filepath.Join(db.dataDir, name)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("db: drop %q: %w", name, err)
	}
	return nil
}

// Collection returns an existing collection or an error if it doesn't exist.
func (db *DB) Collection(name string) (*Collection, error) {
	db.mu.RLock()
	col, ok := db.collections[name]
	db.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("db: collection %q not found", name)
	}
	return col, nil
}

// Compact runs a synchronous, forced compaction pass on the named collection,
// returning only after it completes. It ignores the dirty-ratio gate so callers
// can reclaim space on demand.
func (db *DB) Compact(name string) error {
	col, err := db.Collection(name)
	if err != nil {
		return err
	}
	return col.CompactNow()
}

// ListCollections returns the names of all collections.
func (db *DB) ListCollections() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.collections))
	for n := range db.collections {
		names = append(names, n)
	}
	return names
}

// Close gracefully shuts down all collections.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var firstErr error
	for name, col := range db.collections {
		if err := col.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("db: close collection %q: %w", name, err)
		}
	}
	return firstErr
}
