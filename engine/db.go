// Package engine implements the core FileDB storage engine.
// A DB manages a set of named Collections, each stored as append-only
// NDJSON segment files in a dedicated sub-directory.
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// DB is the top-level database handle. It owns a registry of collections and
// is the entry point for all engine operations.
type DB struct {
	dataDir     string
	defaultCfg  CollectionConfig
	mu          sync.RWMutex
	collections map[string]*Collection

	// Replication (R1). broker is non-nil only when replication is enabled
	// (CollectionConfig.ReplicationRingSize > 0); it sequences committed entries
	// into a global LSN order and fans them out to followers. replMu guards the
	// follower-side applied-LSN watermark.
	broker     *replicationBroker
	replMu     sync.Mutex
	appliedLSN uint64

	// Role management (R3). role is the node's replication role (leader by
	// default; follower when opened with CollectionConfig.Follower). It is read
	// lock-free by the read-only guard on every RPC, so a Promote can lift the
	// guard at runtime. lastLeaderLSN is a follower's last-known upstream leader
	// LSN, kept only to compute the promotion lag guard. roleMu serialises a
	// promotion so the role flip, LSN reseed, and apply-loop stop are atomic; it
	// never guards the read path. onPromote, when set by the server, stops the
	// follower apply loop the moment the node is promoted.
	role          atomic.Int32
	lastLeaderLSN atomic.Uint64
	roleMu        sync.Mutex
	onPromote     func()

	// lock guarantees exclusive access to the database directory.
	lock *dirLock

	// xtxJournal is the root coordinator journal (cross-collection
	// transactions, docs/design-cross-collection-transactions.md §4). It is nil
	// for a legacy root that never had xtx.format / xtx.journal.
	xtxJournal  *xtxJournal
	xtxMu       sync.Mutex
	xtxInFlight map[string]struct{}

	// xtxSnapshotMu makes an XTx and an online snapshot mutually exclusive.
	// A collection-by-collection snapshot is otherwise allowed to copy one
	// participant before an XTx commits and another afterwards.
	xtxSnapshotMu sync.RWMutex
}

// Open opens (or creates) the database rooted at dataDir.
// Existing collections are discovered and opened lazily on first access.
func Open(dataDir string, cfg CollectionConfig) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("db: mkdir %q: %w", dataDir, err)
	}

	dl, err := lockDir(dataDir)
	if h := cfg.OnLock; h != nil {
		switch {
		case err == nil:
			h(dataDir, LockAcquired)
		case errors.Is(err, ErrDatabaseLocked):
			h(dataDir, LockContended)
		default:
			h(dataDir, LockFailed)
		}
	}
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataDir:     dataDir,
		defaultCfg:  cfg,
		collections: make(map[string]*Collection),
		lock:        dl,
	}
	// A node opened as a follower starts in the follower role so the read-only
	// guard rejects writes until an operator promotes it (R3).
	if cfg.Follower {
		db.role.Store(int32(RoleFollower))
	}
	// Build the replication broker (if enabled) and restore the LSN watermarks
	// before opening any collection, so collections open with a live broker.
	db.initReplication(cfg.ReplicationRingSize)

	// Coordinator recovery (design §7): validate format gate, open journal,
	// gather participant evidence, resolve truth table, truncate dead tail runs,
	// replay committed entries, and open collections under the decision table.
	if err := db.recoverCoordinator(cfg); err != nil {
		for _, c := range db.collections {
			_ = c.Close()
		}
		if db.xtxJournal != nil {
			_ = db.xtxJournal.close()
		}
		_ = dl.release()
		return nil, err
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
	colCfg := db.defaultCfg
	if db.xtxJournal != nil {
		colCfg.decisions = db.xtxJournal.decisionsMap()
	}
	col, err := OpenCollection(name, db.dataDir, colCfg)
	if err != nil {
		return nil, err
	}
	col.broker = db.broker
	db.collections[name] = col
	return col, nil
}

// CreateCollectionWithDefaultTTL creates a new collection whose records expire
// after defaultTTL by default (unless a write supplies its own deadline). The
// default is persisted per-collection, so it survives restarts and overrides
// the server-wide default. A non-positive defaultTTL behaves exactly like
// CreateCollection.
func (db *DB) CreateCollectionWithDefaultTTL(name string, defaultTTL time.Duration) (*Collection, error) {
	col, err := db.CreateCollection(name)
	if err != nil {
		return nil, err
	}
	if err := col.setDefaultTTL(defaultTTL); err != nil {
		return nil, fmt.Errorf("db: set default ttl on %q: %w", name, err)
	}
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
	if db.xtxJournal != nil {
		cfg.decisions = db.xtxJournal.decisionsMap()
	}
	col, err := OpenCollection(name, db.dataDir, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: open collection %q: %w", name, err)
	}
	col.broker = db.broker
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
	// Persist the replication watermarks (leader LSN / follower applied LSN) so
	// they survive a clean restart. Best-effort: apply is idempotent, so a lost
	// write only widens the resync window.
	if db.broker != nil || db.appliedLSN > 0 {
		if err := db.persistReplState(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("db: persist replication state: %w", err)
		}
	}

	if err := db.xtxJournal.close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("db: close xtx journal: %w", err)
	}

	if db.lock != nil {
		if err := db.lock.release(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("db: release directory lock: %w", err)
		}
	}

	return firstErr
}

// Results reported through CollectionConfig.OnLock.
const (
	LockAcquired  = "acquired"
	LockContended = "contended"
	LockFailed    = "failed"
)

func (db *DB) beginXTxInFlight(key string) error {
	if key == "" {
		return nil
	}
	db.xtxMu.Lock()
	defer db.xtxMu.Unlock()
	if db.xtxInFlight == nil {
		db.xtxInFlight = make(map[string]struct{})
	}
	if _, ok := db.xtxInFlight[key]; ok {
		return xtxErr("", ErrXTxInProgress, fmt.Errorf("transaction with key %q is in progress", key))
	}
	db.xtxInFlight[key] = struct{}{}
	return nil
}

func (db *DB) endXTxInFlight(key string) {
	if key == "" {
		return
	}
	db.xtxMu.Lock()
	delete(db.xtxInFlight, key)
	db.xtxMu.Unlock()
}

func (db *DB) ensureXTxJournal() (*xtxJournal, error) {
	db.xtxMu.Lock()
	defer db.xtxMu.Unlock()
	if db.xtxJournal != nil {
		return db.xtxJournal, nil
	}
	j, err := openOrCreateXTxJournal(db.dataDir, xtxOptions{wrapFile: db.defaultCfg.wrapFile, renameFn: db.defaultCfg.renameFn}, "v1.4.0")
	if err != nil {
		return nil, err
	}
	db.xtxJournal = j
	return j, nil
}

// XTxStatus returns the status for a given txid or idempotency key (§8.2).
func (db *DB) XTxStatus(txOrKey string) (XTxStatus, string) {
	db.xtxMu.Lock()
	j := db.xtxJournal
	db.xtxMu.Unlock()
	if j == nil {
		return XTxUnknown, ""
	}
	if _, _, err := parseTxID(txOrKey); err == nil {
		return j.status(txOrKey), txOrKey
	}
	return j.statusByKey(txOrKey)
}
