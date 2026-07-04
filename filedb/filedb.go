// Package filedb is the embedded façade over the FileDB storage engine. It is
// the ergonomic, zero-server entry point for programs that want to compile
// FileDB in-process and host several collections — each with its own durability
// and compaction settings — from a single data directory.
//
// It is a thin convenience layer over engine.DB: Open returns a handle, and
// Collection/MustCollection lazily open-or-create a named collection under
// per-collection options. Anything the façade does not expose is reachable via
// Engine.
//
// # Embedded durability default (OPS-1)
//
// Unlike the raw engine — whose default is SyncModeNone (fastest, but a crash
// can lose recently acknowledged writes) — a DB opened through filedb.Open
// defaults every collection to SyncModeInterval at a 1s cadence. This trades a
// bounded (~1s) durability window for throughput: a crash can lose at most the
// last interval's writes, while the append-only, temp-then-rename segment format
// already rules out torn/partial records. It is the right default for a local,
// single-writer daemon that wants crash-safety without paying an fsync on every
// write.
//
// Write paths that genuinely need per-write durability (a spend/ledger
// collection, say) can opt back into SyncModeAlways per collection with
// WithCollectionSyncMode(engine.SyncModeAlways) — an explicit escape hatch that
// fsyncs before every write is acknowledged. Every other engine default (segment
// size, compaction cadence, watch buffer) is left untouched unless overridden.
package filedb

import (
	"fmt"
	"sync"
	"time"

	"github.com/srjn45/filedbv2/engine"
)

// Option configures the DB-wide defaults applied to every collection opened
// through the façade. Options mutate the base engine.CollectionConfig that is
// also handed to the engine when it pre-opens existing collections, so they take
// effect uniformly.
type Option func(*engine.CollectionConfig)

// WithSyncMode overrides the default durability policy for every collection.
// The façade default is engine.SyncModeInterval; pass engine.SyncModeAlways for
// strict per-write fsync or engine.SyncModeNone to match the raw engine.
func WithSyncMode(m engine.SyncMode) Option {
	return func(c *engine.CollectionConfig) { c.SyncMode = m }
}

// WithSyncInterval sets the flush cadence used under SyncModeInterval. The
// façade default is 1s.
func WithSyncInterval(d time.Duration) Option {
	return func(c *engine.CollectionConfig) { c.SyncInterval = d }
}

// WithSegmentMaxSize sets the maximum active-segment size before rotation.
func WithSegmentMaxSize(n int64) Option {
	return func(c *engine.CollectionConfig) { c.SegmentMaxSize = n }
}

// WithCompactInterval sets the background compaction cadence.
func WithCompactInterval(d time.Duration) Option {
	return func(c *engine.CollectionConfig) { c.CompactInterval = d }
}

// WithWatchBufferSize sets the per-subscriber Watch channel buffer.
func WithWatchBufferSize(n int) Option {
	return func(c *engine.CollectionConfig) { c.WatchBufferSize = n }
}

// collectionOptions accumulates a per-collection config override plus any unique
// indexes to ensure at open time.
type collectionOptions struct {
	cfg    engine.CollectionConfig
	unique []string
}

// CollectionOption overrides the DB-wide defaults for a single collection and/or
// declares unique indexes to ensure when the collection is opened.
type CollectionOption func(*collectionOptions)

// WithCollectionSyncMode overrides the durability policy for this collection
// only. The headline use is WithCollectionSyncMode(engine.SyncModeAlways) for a
// write path (a spend/ledger collection) that needs an fsync on every write,
// while the rest of the store keeps the interval default.
func WithCollectionSyncMode(m engine.SyncMode) CollectionOption {
	return func(o *collectionOptions) { o.cfg.SyncMode = m }
}

// WithCollectionSyncInterval overrides the flush cadence for this collection.
func WithCollectionSyncInterval(d time.Duration) CollectionOption {
	return func(o *collectionOptions) { o.cfg.SyncInterval = d }
}

// WithCollectionSegmentMaxSize overrides the segment rotation size for this
// collection.
func WithCollectionSegmentMaxSize(n int64) CollectionOption {
	return func(o *collectionOptions) { o.cfg.SegmentMaxSize = n }
}

// WithCollectionCompactInterval overrides the compaction cadence for this
// collection.
func WithCollectionCompactInterval(d time.Duration) CollectionOption {
	return func(o *collectionOptions) { o.cfg.CompactInterval = d }
}

// WithCollectionWatchBufferSize overrides the Watch buffer size for this
// collection.
func WithCollectionWatchBufferSize(n int) CollectionOption {
	return func(o *collectionOptions) { o.cfg.WatchBufferSize = n }
}

// WithMaxRecords caps this collection at n live records (S4): an insert, keyed
// insert, inserting upsert, batch, or transaction that would create a record
// beyond the cap is refused with engine.ErrResourceExhausted, before anything is
// written. An in-place update or a delete is never refused. Zero (the default)
// leaves the record count unlimited.
func WithMaxRecords(n uint64) CollectionOption {
	return func(o *collectionOptions) { o.cfg.MaxRecords = n }
}

// WithMaxBytes caps this collection's on-disk footprint at n bytes (S4, the
// summed size of its segment files): once the budget is reached, a write that
// would create a new record is refused with engine.ErrResourceExhausted. Like
// WithMaxRecords it gates only new-record creation, so a tenant at its limit can
// still update or delete to recover. Zero (the default) leaves it unlimited.
func WithMaxBytes(n uint64) CollectionOption {
	return func(o *collectionOptions) { o.cfg.MaxBytes = n }
}

// WithUniqueIndex ensures a unique secondary index on each named field when the
// collection is opened (via engine.Collection.EnsureUniqueIndex). Subsequent
// inserts or updates that would map a field's value to a different live record
// are rejected with engine.ErrDuplicateKey. Fields already indexed are left as
// they are.
func WithUniqueIndex(fields ...string) CollectionOption {
	return func(o *collectionOptions) { o.unique = append(o.unique, fields...) }
}

// DB is an embedded FileDB handle: a set of named collections rooted at one data
// directory, opened in-process with no server. It is safe for concurrent use.
type DB struct {
	edb  *engine.DB
	base engine.CollectionConfig

	mu   sync.Mutex
	cols map[string]*engine.Collection
}

// Open opens (or creates) an embedded database rooted at dir. Existing
// collections on disk are discovered automatically. Every collection defaults to
// SyncModeInterval at a 1s cadence (see the package doc for the OPS-1 rationale);
// pass Options to change the DB-wide defaults.
func Open(dir string, opts ...Option) (*DB, error) {
	// Embedded durability default (OPS-1): interval fsync ~1s, engine defaults
	// for everything else. Zero-valued fields are normalized to their engine
	// defaults inside OpenCollection.
	base := engine.CollectionConfig{
		SyncMode:     engine.SyncModeInterval,
		SyncInterval: engine.DefaultSyncInterval,
	}
	for _, o := range opts {
		o(&base)
	}

	edb, err := engine.Open(dir, base)
	if err != nil {
		return nil, err
	}
	return &DB{
		edb:  edb,
		base: base,
		cols: make(map[string]*engine.Collection),
	}, nil
}

// Collection opens (or creates) the named collection, applying the DB-wide
// defaults overlaid with any CollectionOption. The first call for a given name
// wins: later calls return the same handle and ignore their options, so the
// per-collection config is fixed at first open. Use it once per collection at
// startup.
func (db *DB) Collection(name string, opts ...CollectionOption) (*engine.Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if c, ok := db.cols[name]; ok {
		return c, nil
	}

	co := collectionOptions{cfg: db.base}
	for _, o := range opts {
		o(&co)
	}

	col, err := db.edb.CollectionWithConfig(name, co.cfg)
	if err != nil {
		return nil, err
	}
	for _, field := range co.unique {
		if err := col.EnsureUniqueIndex(field); err != nil {
			return nil, fmt.Errorf("filedb: collection %q: ensure unique index %q: %w", name, field, err)
		}
	}
	db.cols[name] = col
	return col, nil
}

// MustCollection is Collection that panics on error. It is a convenience for
// package/struct initialization, where a store's fixed set of collections is
// opened once and a failure is fatal.
func (db *DB) MustCollection(name string, opts ...CollectionOption) *engine.Collection {
	col, err := db.Collection(name, opts...)
	if err != nil {
		panic(fmt.Sprintf("filedb: MustCollection(%q): %v", name, err))
	}
	return col
}

// Engine returns the underlying engine.DB for operations the façade does not
// wrap (ListCollections, DropCollection, …). Collections opened directly on the
// returned handle bypass the façade's caching and per-collection option layer.
func (db *DB) Engine() *engine.DB { return db.edb }

// Close closes every open collection and flushes their indexes to disk.
func (db *DB) Close() error { return db.edb.Close() }
