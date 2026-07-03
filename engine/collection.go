package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/srjn45/filedbv2/store"
)

// SyncMode controls how aggressively writes are flushed to stable storage.
type SyncMode string

const (
	// SyncModeNone never calls fsync explicitly; durability is left to the OS
	// page-cache flush. Fastest, but a power loss can lose recently
	// acknowledged writes. This is the default.
	SyncModeNone SyncMode = "none"
	// SyncModeAlways fsyncs after every write before the write is acknowledged.
	// Strongest durability, lowest throughput.
	SyncModeAlways SyncMode = "always"
	// SyncModeInterval fsyncs the active segment on a fixed timer. A crash can
	// lose at most one interval's worth of writes — a middle ground between
	// none and always.
	SyncModeInterval SyncMode = "interval"
)

// DefaultSyncInterval is the flush cadence used by SyncModeInterval when no
// interval is configured.
const DefaultSyncInterval = time.Second

// DefaultWatchBufferSize is the per-subscriber channel buffer used when no size
// is configured.
const DefaultWatchBufferSize = 64

// OpOverflow is a watch-only sentinel op delivered to a subscriber after the
// engine had to drop one or more events because that subscriber's buffer was
// full. It tells the consumer it missed writes and should resync. It is never
// written to a segment.
const OpOverflow store.Op = "overflow"

// CollectionConfig holds tunable parameters for a single collection.
type CollectionConfig struct {
	SegmentMaxSize  int64         // default: DefaultSegmentMaxSize
	CompactInterval time.Duration // default: 5m
	CompactDirtyPct float64       // default: 0.30 (30%)

	// SyncMode selects the durability policy. Default: SyncModeNone.
	SyncMode SyncMode
	// SyncInterval is the flush cadence for SyncModeInterval. Default: 1s.
	SyncInterval time.Duration

	// WatchBufferSize is the per-subscriber channel buffer for Watch. A slow
	// subscriber that fills its buffer receives an OpOverflow sentinel rather
	// than silently missing events. Default: DefaultWatchBufferSize.
	WatchBufferSize int

	// OnCompaction is called after each successful compaction run with the
	// collection name and elapsed wall-clock duration. May be nil.
	OnCompaction func(collection string, dur time.Duration)
}

func defaultConfig() CollectionConfig {
	return CollectionConfig{
		SegmentMaxSize:  DefaultSegmentMaxSize,
		CompactInterval: 5 * time.Minute,
		CompactDirtyPct: 0.30,
		SyncMode:        SyncModeNone,
		SyncInterval:    DefaultSyncInterval,
		WatchBufferSize: DefaultWatchBufferSize,
	}
}

// WatchEvent is emitted to Watch subscribers on every write.
type WatchEvent struct {
	Op   store.Op
	ID   uint64
	Data map[string]any
	Ts   time.Time
}

// Collection is a named set of records stored across one or more segment files.
// All exported methods are safe for concurrent use.
type Collection struct {
	name      string
	dir       string
	cfg       CollectionConfig
	createdAt time.Time
	mu        sync.RWMutex
	sealed    []*Segment
	active    *Segment
	index     *Index
	idSeq     atomic.Uint64 // monotonically increasing id counter

	// Watch subscribers.
	watchMu      sync.Mutex
	watchers     map[uint64]*watcher
	watcherIDSeq atomic.Uint64

	// Secondary indexes: field name → index.
	sidxMu  sync.RWMutex
	sidxMap map[string]*SecondaryIndex

	// Compactor control.
	compactC  chan struct{} // signal: run compaction now
	closeOnce sync.Once
	closed    chan struct{}
}

// OpenCollection opens or creates the collection rooted at dir.
// It loads the persisted index (rebuilding from segments if stale),
// and starts the background compactor goroutine.
func OpenCollection(name, dataDir string, cfg CollectionConfig) (*Collection, error) {
	dir := filepath.Join(dataDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("collection: mkdir %q: %w", dir, err)
	}

	// Normalize config so a zero-valued CollectionConfig is safe to use.
	def := defaultConfig()
	if cfg.SegmentMaxSize <= 0 {
		cfg.SegmentMaxSize = def.SegmentMaxSize
	}
	if cfg.CompactInterval <= 0 {
		cfg.CompactInterval = def.CompactInterval
	}
	if cfg.CompactDirtyPct <= 0 {
		cfg.CompactDirtyPct = def.CompactDirtyPct
	}
	if cfg.SyncMode == "" {
		cfg.SyncMode = SyncModeNone
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = DefaultSyncInterval
	}
	if cfg.WatchBufferSize <= 0 {
		cfg.WatchBufferSize = DefaultWatchBufferSize
	}

	c := &Collection{
		name:     name,
		dir:      dir,
		cfg:      cfg,
		index:    newIndex(),
		sidxMap:  make(map[string]*SecondaryIndex),
		watchers: make(map[uint64]*watcher),
		compactC: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}

	if err := c.load(); err != nil {
		return nil, err
	}

	go c.compactLoop()
	if c.cfg.SyncMode == SyncModeInterval {
		go c.syncLoop()
	}
	return c, nil
}

// syncLoop periodically fsyncs the active segment when SyncModeInterval is
// configured. It exits when the collection is closed.
func (c *Collection) syncLoop() {
	t := time.NewTicker(c.cfg.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			c.mu.RLock()
			active := c.active
			c.mu.RUnlock()
			_ = active.Sync()
		}
	}
}

// load reads existing segments from disk, restores the index, and opens
// or creates the active (write) segment.
func (c *Collection) load() error {
	// Discover sealed segment files.
	pattern := filepath.Join(c.dir, "seg_*.ndjson")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("collection: glob segments: %w", err)
	}
	sort.Strings(paths)

	// Identify the active (latest) segment — the one we'll append to.
	// All others are sealed.
	var activePath string
	if len(paths) == 0 {
		activePath = c.segmentPath(1)
	} else {
		activePath = paths[len(paths)-1]
		for _, p := range paths[:len(paths)-1] {
			info, err := os.Stat(p)
			if err != nil {
				return fmt.Errorf("collection: stat %q: %w", p, err)
			}
			c.sealed = append(c.sealed, openSealedSegment(p, info.Size()))
		}
	}

	active, err := openActiveSegment(activePath)
	if err != nil {
		return fmt.Errorf("collection: open active segment: %w", err)
	}
	c.active = active

	// Build the full segment list for index rebuild.
	all := make([]*Segment, 0, len(c.sealed)+1)
	all = append(all, c.sealed...)
	all = append(all, c.active)

	// Try loading the persisted index.
	indexPath := filepath.Join(c.dir, "index.json")
	err = c.index.Load(indexPath)
	if err != nil && !errors.Is(err, ErrIndexStale) && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("collection: load index: %w", err)
	}
	if err != nil {
		// Stale or missing — rebuild.
		if rbErr := c.index.Rebuild(all); rbErr != nil {
			return fmt.Errorf("collection: rebuild index: %w", rbErr)
		}
		_ = c.index.Persist(indexPath)
	}

	// Reload any previously persisted secondary indexes.
	if sidxPaths, _ := filepath.Glob(filepath.Join(c.dir, "sidx_*.json")); len(sidxPaths) > 0 {
		for _, p := range sidxPaths {
			// derive field name from filename: sidx_<field>.json
			base := filepath.Base(p)
			field := base[len("sidx_") : len(base)-len(".json")]
			// Load() restores the persisted unique flag; false is a placeholder.
			sidx := newSecondaryIndex(field, false)
			if err := sidx.Load(p); err != nil {
				// stale/corrupt — rebuild from segments
				if rbErr := sidx.rebuild(all); rbErr != nil {
					return fmt.Errorf("collection: rebuild secondary index %q: %w", field, rbErr)
				}
				_ = sidx.Persist(p)
			}
			c.sidxMap[field] = sidx
		}
	}

	// Restore the id counter.
	// Fast path: load from meta.json written by a previous clean run or
	// segment rotation. meta.json is no longer rewritten on every insert, so it
	// may trail the true counter after a crash — reconcile it against the
	// highest id present in the active segment, which always holds the most
	// recently assigned id (ids are monotonic and appended in order). This
	// scan is cheap: the active segment is bounded by SegmentMaxSize.
	metaPath := filepath.Join(c.dir, metaFilename)
	if meta, err := loadMeta(metaPath); err == nil {
		c.idSeq.Store(meta.IDCounter)
		c.createdAt = meta.CreatedAt
		if amax := c.activeMaxID(); amax > c.idSeq.Load() {
			c.idSeq.Store(amax)
		}
		return nil
	}

	// Slow path: meta.json is missing or corrupt — scan all entries for max id.
	var maxID uint64

	c.index.mu.RLock()
	for id := range c.index.entries {
		if id > maxID {
			maxID = id
		}
	}
	c.index.mu.RUnlock()

	for _, seg := range all {
		entries, err := seg.ScanAll()
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.ID > maxID {
				maxID = e.ID
			}
		}
	}
	c.idSeq.Store(maxID)
	c.createdAt = time.Now().UTC()

	// Write meta.json so the next startup can skip this scan.
	_ = persistMeta(metaPath, collectionMeta{IDCounter: maxID, CreatedAt: c.createdAt})

	return nil
}

// activeMaxID returns the highest entry id present in the active segment, or 0
// if it is empty or unreadable. Used to reconcile the id counter on load.
func (c *Collection) activeMaxID() uint64 {
	entries, err := c.active.ScanAll()
	if err != nil {
		return 0
	}
	var maxID uint64
	for _, e := range entries {
		if e.ID > maxID {
			maxID = e.ID
		}
	}
	return maxID
}

// syncActiveLocked fsyncs the active segment when SyncModeAlways is configured.
// The caller must hold c.mu so the active segment pointer is stable.
func (c *Collection) syncActiveLocked() error {
	if c.cfg.SyncMode != SyncModeAlways {
		return nil
	}
	return c.active.Sync()
}

// Insert adds a new record and returns its assigned id.
func (c *Collection) Insert(data map[string]any) (uint64, time.Time, error) {
	id := c.idSeq.Add(1)
	ts := time.Now().UTC()
	e := store.NewInsert(id, data)
	e.Ts = ts

	c.mu.Lock()
	// Enforce unique indexes before writing so a rejected insert appends nothing
	// and mutates no index.
	if err := c.sidxCheckUnique(id, data); err != nil {
		c.mu.Unlock()
		return 0, time.Time{}, err
	}
	offset, err := c.active.Append(e)
	if err != nil {
		c.mu.Unlock()
		return 0, time.Time{}, fmt.Errorf("collection: insert: %w", err)
	}
	c.index.Set(id, IndexEntry{SegmentPath: c.active.Path(), Offset: offset})
	c.sidxIndexEntry(id, data)
	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return 0, time.Time{}, fmt.Errorf("collection: insert: %w", err)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return id, ts, fmt.Errorf("collection: rotate after insert: %w", err)
		}
	}

	// meta.json is persisted on rotation and on Close rather than per insert;
	// the id counter is reconciled against the active segment on load, so a
	// crash between writes cannot cause id reuse.
	c.emit(WatchEvent{Op: store.OpInsert, ID: id, Data: data, Ts: ts})
	return id, ts, nil
}

// Update overwrites the data for an existing record.
func (c *Collection) Update(id uint64, data map[string]any) (time.Time, error) {
	ts := time.Now().UTC()
	e := store.NewUpdate(id, data)
	e.Ts = ts

	c.mu.Lock()
	if _, ok := c.index.Get(id); !ok {
		c.mu.Unlock()
		return time.Time{}, fmt.Errorf("collection: update: id %d not found", id)
	}
	// Enforce unique indexes before writing so a rejected update appends nothing
	// and mutates no index.
	if err := c.sidxCheckUnique(id, data); err != nil {
		c.mu.Unlock()
		return time.Time{}, err
	}
	offset, err := c.active.Append(e)
	if err != nil {
		c.mu.Unlock()
		return time.Time{}, fmt.Errorf("collection: update: %w", err)
	}
	c.index.Set(id, IndexEntry{SegmentPath: c.active.Path(), Offset: offset})
	c.sidxUpdateEntry(id, data)
	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return time.Time{}, fmt.Errorf("collection: update: %w", err)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return ts, fmt.Errorf("collection: rotate after update: %w", err)
		}
	}

	c.emit(WatchEvent{Op: store.OpUpdate, ID: id, Data: data, Ts: ts})
	return ts, nil
}

// Delete removes a record by id.
func (c *Collection) Delete(id uint64) error {
	e := store.NewDelete(id)

	c.mu.Lock()
	if _, ok := c.index.Get(id); !ok {
		c.mu.Unlock()
		return fmt.Errorf("collection: delete: id %d not found", id)
	}
	if _, err := c.active.Append(e); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("collection: delete: %w", err)
	}
	c.index.Delete(id)
	c.sidxRemoveEntry(id)
	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("collection: delete: %w", err)
	}
	c.mu.Unlock()

	c.emit(WatchEvent{Op: store.OpDelete, ID: id, Ts: e.Ts})
	return nil
}

// FindByID returns the data for the given id.
func (c *Collection) FindByID(id uint64) (map[string]any, time.Time, error) {
	c.mu.RLock()
	loc, ok := c.index.Get(id)
	c.mu.RUnlock()

	if !ok {
		return nil, time.Time{}, fmt.Errorf("collection: findById: id %d not found", id)
	}

	seg := c.segmentByPath(loc.SegmentPath)
	if seg == nil {
		return nil, time.Time{}, fmt.Errorf("collection: findById: segment not found for id %d", id)
	}

	e, err := seg.ReadAt(loc.Offset)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("collection: findById: %w", err)
	}
	return e.Data, e.Ts, nil
}

// Stats returns diagnostic information about the collection.
func (c *Collection) Stats() CollectionStats {
	c.mu.RLock()
	segCount := len(c.sealed) + 1
	var totalSize int64
	for _, s := range c.sealed {
		totalSize += s.Size()
	}
	totalSize += c.active.Size()
	c.mu.RUnlock()

	return CollectionStats{
		Name:         c.name,
		RecordCount:  uint64(c.index.Len()),
		SegmentCount: uint64(segCount),
		SizeBytes:    uint64(totalSize),
	}
}

// CollectionStats holds diagnostic data for a collection.
type CollectionStats struct {
	Name         string
	RecordCount  uint64
	SegmentCount uint64
	DirtyEntries uint64
	SizeBytes    uint64
}

// watcher is a single Watch subscription: a buffered channel plus a flag that
// records whether events were dropped because the channel was full.
type watcher struct {
	ch         chan WatchEvent
	overflowed bool // set when an event was dropped; cleared once the sentinel is delivered
}

// Subscribe registers a watcher channel and returns its id and a cancel func.
func (c *Collection) Subscribe() (uint64, <-chan WatchEvent, func()) {
	id := c.watcherIDSeq.Add(1)
	w := &watcher{ch: make(chan WatchEvent, c.cfg.WatchBufferSize)}

	c.watchMu.Lock()
	c.watchers[id] = w
	c.watchMu.Unlock()

	cancel := func() {
		c.watchMu.Lock()
		delete(c.watchers, id)
		c.watchMu.Unlock()
		close(w.ch)
	}
	return id, w.ch, cancel
}

// emit delivers ev to every subscriber. If a subscriber's buffer is full the
// event is dropped and the watcher is marked overflowed; once its channel
// drains, exactly one OpOverflow sentinel is delivered before normal events
// resume, so the consumer knows it missed writes and must resync.
func (c *Collection) emit(ev WatchEvent) {
	c.watchMu.Lock()
	for _, w := range c.watchers {
		if w.overflowed {
			// Flush the overflow sentinel before resuming normal delivery.
			// Until it lands, keep dropping events (continuity is already lost).
			select {
			case w.ch <- WatchEvent{Op: OpOverflow, Ts: time.Now().UTC()}:
				w.overflowed = false
			default:
				continue // still backed up; stay overflowed
			}
		}
		select {
		case w.ch <- ev:
		default:
			w.overflowed = true // subscriber too slow; signal once it drains
		}
	}
	c.watchMu.Unlock()
}

// rotateSegment seals the current active segment and opens a new one.
func (c *Collection) rotateSegment() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.active.Seal(); err != nil {
		return err
	}
	c.sealed = append(c.sealed, c.active)

	newPath := c.segmentPath(uint64(len(c.sealed) + 1))
	active, err := openActiveSegment(newPath)
	if err != nil {
		return err
	}
	c.active = active

	// Persist the newly created segment's directory entry so a crash cannot
	// lose the file. Skipped in SyncModeNone to preserve fast-mode throughput.
	if c.cfg.SyncMode != SyncModeNone {
		if err := fsyncDir(c.dir); err != nil {
			return fmt.Errorf("collection: fsync dir after rotate: %w", err)
		}
	}

	// Persist the id counter now that a segment boundary has been crossed.
	_ = persistMeta(filepath.Join(c.dir, metaFilename),
		collectionMeta{IDCounter: c.idSeq.Load(), CreatedAt: c.createdAt})

	// Signal the compactor.
	select {
	case c.compactC <- struct{}{}:
	default:
	}
	return nil
}

// segmentByPath finds a segment in the collection by its file path.
func (c *Collection) segmentByPath(path string) *Segment {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.sealed {
		if s.Path() == path {
			return s
		}
	}
	if c.active.Path() == path {
		return c.active
	}
	return nil
}

func (c *Collection) segmentPath(n uint64) string {
	return filepath.Join(c.dir, fmt.Sprintf("seg_%06d.ndjson", n))
}

// ReserveID atomically increments and returns the next id without writing
// anything to disk. Used by transaction staging so the caller receives the
// assigned id immediately; the actual segment write happens at CommitTx.
func (c *Collection) ReserveID() uint64 {
	return c.idSeq.Add(1)
}

// CommitTx applies a slice of staged transaction ops atomically under the
// collection write lock. It pre-validates all update/delete ops first — if any
// ID no longer exists the entire commit is rejected with no partial writes.
func (c *Collection) CommitTx(ops []txOp) error {
	if len(ops) == 0 {
		return nil
	}

	c.mu.Lock()

	// Pre-validate: ensure every update/delete target still exists.
	for _, op := range ops {
		if op.kind == txOpUpdate || op.kind == txOpDelete {
			if _, ok := c.index.Get(op.id); !ok {
				c.mu.Unlock()
				return fmt.Errorf("tx commit: id %d was deleted before commit", op.id)
			}
		}
	}

	// Pre-validate uniqueness across all staged ops before applying any, so a
	// violating commit writes nothing. Each insert/update is checked against the
	// committed data (a different live id) and against values claimed by earlier
	// ops in this same batch.
	if err := c.txCheckUnique(ops); err != nil {
		c.mu.Unlock()
		return err
	}

	// Apply all ops sequentially.
	var events []WatchEvent
	var maxInsertID uint64

	for _, op := range ops {
		switch op.kind {
		case txOpInsert:
			e := store.NewInsert(op.id, op.data)
			e.Ts = op.ts
			offset, err := c.active.Append(e)
			if err != nil {
				c.mu.Unlock()
				return fmt.Errorf("tx commit: insert id %d: %w", op.id, err)
			}
			c.index.Set(op.id, IndexEntry{SegmentPath: c.active.Path(), Offset: offset})
			c.sidxIndexEntry(op.id, op.data)
			if op.id > maxInsertID {
				maxInsertID = op.id
			}
			events = append(events, WatchEvent{Op: store.OpInsert, ID: op.id, Data: op.data, Ts: op.ts})

		case txOpUpdate:
			e := store.NewUpdate(op.id, op.data)
			e.Ts = op.ts
			offset, err := c.active.Append(e)
			if err != nil {
				c.mu.Unlock()
				return fmt.Errorf("tx commit: update id %d: %w", op.id, err)
			}
			c.index.Set(op.id, IndexEntry{SegmentPath: c.active.Path(), Offset: offset})
			c.sidxUpdateEntry(op.id, op.data)
			events = append(events, WatchEvent{Op: store.OpUpdate, ID: op.id, Data: op.data, Ts: op.ts})

		case txOpDelete:
			e := store.NewDelete(op.id)
			e.Ts = op.ts
			if _, err := c.active.Append(e); err != nil {
				c.mu.Unlock()
				return fmt.Errorf("tx commit: delete id %d: %w", op.id, err)
			}
			c.index.Delete(op.id)
			c.sidxRemoveEntry(op.id)
			events = append(events, WatchEvent{Op: store.OpDelete, ID: op.id, Ts: op.ts})
		}
	}

	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("tx commit: sync: %w", err)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return fmt.Errorf("collection: rotate after commit: %w", err)
		}
	}

	for _, ev := range events {
		c.emit(ev)
	}

	if maxInsertID > 0 {
		_ = persistMeta(filepath.Join(c.dir, metaFilename),
			collectionMeta{IDCounter: c.idSeq.Load(), CreatedAt: c.createdAt})
	}

	return nil
}

// Name returns the collection name.
func (c *Collection) Name() string { return c.name }

// Close shuts down the collection and its background goroutine.
func (c *Collection) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() { close(c.closed) })
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.active.Close(); err != nil {
		return err
	}
	if err := c.index.Persist(filepath.Join(c.dir, "index.json")); err != nil {
		return err
	}
	c.sidxMu.RLock()
	for field, sidx := range c.sidxMap {
		_ = sidx.Persist(sidxFilePath(c.dir, field))
	}
	c.sidxMu.RUnlock()
	return persistMeta(filepath.Join(c.dir, metaFilename),
		collectionMeta{IDCounter: c.idSeq.Load(), CreatedAt: c.createdAt})
}

// ---- Secondary index helpers (called under c.mu write lock) ----------------

// sidxCheckUnique verifies that applying data to id would not collide with a
// different live record on any unique secondary index. It returns an
// ErrDuplicateKey-wrapped error on the first violation. It must be called while
// c.mu (write lock) is held so the check and the subsequent index mutation are
// atomic with respect to other writers.
func (c *Collection) sidxCheckUnique(id uint64, data map[string]any) error {
	c.sidxMu.RLock()
	defer c.sidxMu.RUnlock()
	for field, sidx := range c.sidxMap {
		if !sidx.unique {
			continue
		}
		val, ok := data[field]
		if !ok {
			continue
		}
		key := toIndexKey(val)
		if sidx.conflict(key, id) {
			return fmt.Errorf("%w: field %q value %q", ErrDuplicateKey, field, key)
		}
	}
	return nil
}

// txCheckUnique pre-validates a batch of staged ops against every unique
// secondary index. It must be called while c.mu (write lock) is held. A
// violation — either against already-committed data or against another op in
// the same batch — returns an ErrDuplicateKey-wrapped error.
func (c *Collection) txCheckUnique(ops []txOp) error {
	c.sidxMu.RLock()
	defer c.sidxMu.RUnlock()

	// claimed[field][value] = id staked by an earlier op in this batch.
	claimed := make(map[string]map[string]uint64)
	for _, op := range ops {
		if op.kind != txOpInsert && op.kind != txOpUpdate {
			continue
		}
		for field, sidx := range c.sidxMap {
			if !sidx.unique {
				continue
			}
			val, ok := op.data[field]
			if !ok {
				continue
			}
			key := toIndexKey(val)
			if sidx.conflict(key, op.id) {
				return fmt.Errorf("%w: field %q value %q", ErrDuplicateKey, field, key)
			}
			byVal := claimed[field]
			if byVal == nil {
				byVal = make(map[string]uint64)
				claimed[field] = byVal
			}
			if prev, taken := byVal[key]; taken && prev != op.id {
				return fmt.Errorf("%w: field %q value %q", ErrDuplicateKey, field, key)
			}
			byVal[key] = op.id
		}
	}
	return nil
}

// sidxIndexEntry adds data[field] to every secondary index for a new id.
func (c *Collection) sidxIndexEntry(id uint64, data map[string]any) {
	c.sidxMu.RLock()
	for field, sidx := range c.sidxMap {
		if val, ok := data[field]; ok {
			sidx.add(toIndexKey(val), id)
		}
	}
	c.sidxMu.RUnlock()
}

// sidxUpdateEntry moves id to the new field values in every secondary index.
func (c *Collection) sidxUpdateEntry(id uint64, data map[string]any) {
	c.sidxMu.RLock()
	for field, sidx := range c.sidxMap {
		if val, ok := data[field]; ok {
			sidx.update(id, toIndexKey(val))
		} else {
			sidx.remove(id)
		}
	}
	c.sidxMu.RUnlock()
}

// sidxRemoveEntry removes id from every secondary index.
func (c *Collection) sidxRemoveEntry(id uint64) {
	c.sidxMu.RLock()
	for _, sidx := range c.sidxMap {
		sidx.remove(id)
	}
	c.sidxMu.RUnlock()
}

// ---- Secondary index management (public API) --------------------------------

// EnsureIndex creates a non-unique secondary index on field if one does not
// already exist. It immediately rebuilds the index from all current segments.
func (c *Collection) EnsureIndex(field string) error {
	return c.ensureIndex(field, false)
}

// EnsureUniqueIndex creates a unique secondary index on field: subsequent
// inserts or updates that would map field's value to a different live record
// are rejected with ErrDuplicateKey. Uniqueness is enforced going forward only;
// historical duplicates already present in the data are tolerated on rebuild.
// The unique flag is persisted so it survives reload.
func (c *Collection) EnsureUniqueIndex(field string) error {
	return c.ensureIndex(field, true)
}

// ensureIndex creates a secondary index on field if one does not already exist.
func (c *Collection) ensureIndex(field string, unique bool) error {
	c.sidxMu.Lock()
	if _, exists := c.sidxMap[field]; exists {
		c.sidxMu.Unlock()
		return nil
	}
	sidx := newSecondaryIndex(field, unique)
	c.sidxMap[field] = sidx
	c.sidxMu.Unlock()

	// Rebuild under collection read lock so we get a consistent snapshot.
	c.mu.RLock()
	all := make([]*Segment, 0, len(c.sealed)+1)
	all = append(all, c.sealed...)
	all = append(all, c.active)
	c.mu.RUnlock()

	if err := sidx.rebuild(all); err != nil {
		c.sidxMu.Lock()
		delete(c.sidxMap, field)
		c.sidxMu.Unlock()
		return fmt.Errorf("collection: ensure index %q: %w", field, err)
	}
	return sidx.Persist(sidxFilePath(c.dir, field))
}

// DropIndex removes the secondary index for field and deletes its file.
func (c *Collection) DropIndex(field string) error {
	c.sidxMu.Lock()
	if _, exists := c.sidxMap[field]; !exists {
		c.sidxMu.Unlock()
		return fmt.Errorf("collection: index on %q not found", field)
	}
	delete(c.sidxMap, field)
	c.sidxMu.Unlock()
	_ = os.Remove(sidxFilePath(c.dir, field))
	return nil
}

// ListIndexes returns the field names of all active secondary indexes.
func (c *Collection) ListIndexes() []string {
	c.sidxMu.RLock()
	fields := make([]string, 0, len(c.sidxMap))
	for f := range c.sidxMap {
		fields = append(fields, f)
	}
	c.sidxMu.RUnlock()
	sort.Strings(fields)
	return fields
}

// IndexLookup returns the IDs of records whose field equals value, using the
// secondary index. Returns nil if no index exists for field (caller falls back
// to Scan).
func (c *Collection) IndexLookup(field, value string) ([]uint64, bool) {
	c.sidxMu.RLock()
	sidx, ok := c.sidxMap[field]
	c.sidxMu.RUnlock()
	if !ok {
		return nil, false
	}
	return sidx.Lookup(value), true
}
