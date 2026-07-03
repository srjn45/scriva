package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/srjn45/filedbv2/store"
)

// KeyField is the reserved data field that stores a caller-supplied string
// primary key. It is populated only by the keyed API (InsertWithKey,
// UpdateByKey) and enforced unique by a mandatory secondary index created
// lazily on the first keyed write. Because the key lives in the record's data
// it round-trips through segments, compaction, index rebuild, and reopen for
// free, and surfaces naturally in Watch events.
const KeyField = "_key"

// ErrReservedField is returned by Insert and Update when the supplied data sets
// a reserved field (currently _key) directly. String keys are settable only
// through the keyed API. Callers can match it with errors.Is.
var ErrReservedField = errors.New("engine: reserved field")

// ErrKeyNotFound is returned by FindByKey, UpdateByKey, and DeleteByKey when no
// live record carries the given string key. Callers can match it with
// errors.Is.
var ErrKeyNotFound = errors.New("engine: key not found")

// reservedFieldErr builds the ErrReservedField error with guidance on the
// correct API to use.
func reservedFieldErr() error {
	return fmt.Errorf("%w: %q is settable only via InsertWithKey/UpdateByKey", ErrReservedField, KeyField)
}

// InsertWithKey inserts data under the caller-supplied string key. It stamps the
// key into the reserved _key field, ensures the collection's unique _key index
// exists (created lazily on the first keyed write), and appends the record. A
// key already held by a live record is rejected with ErrDuplicateKey. Supplying
// _key inside data is rejected with ErrReservedField — the key argument is the
// only way to set it.
func (c *Collection) InsertWithKey(key string, data map[string]any) (uint64, time.Time, error) {
	if _, ok := data[KeyField]; ok {
		return 0, time.Time{}, reservedFieldErr()
	}
	if err := c.ensureKeyIndex(); err != nil {
		return 0, time.Time{}, err
	}
	return c.insert(stampKey(data, key), c.resolveInsertExpiry(time.Time{}))
}

// FindByKey returns the data and timestamp for the record carrying key. The
// lookup is O(1) via the unique _key index. A missing key yields
// ErrKeyNotFound.
func (c *Collection) FindByKey(key string) (map[string]any, time.Time, error) {
	id, err := c.resolveKey(key)
	if err != nil {
		return nil, time.Time{}, err
	}
	return c.FindByID(id)
}

// UpdateByKey overwrites the data for the record carrying key, preserving the
// key itself. Supplying _key inside data is rejected with ErrReservedField; a
// missing key yields ErrKeyNotFound.
func (c *Collection) UpdateByKey(key string, data map[string]any) (time.Time, error) {
	if _, ok := data[KeyField]; ok {
		return time.Time{}, reservedFieldErr()
	}
	id, err := c.resolveKey(key)
	if err != nil {
		return time.Time{}, err
	}
	return c.update(id, stampKey(data, key), 0, true)
}

// DeleteByKey removes the record carrying key. A missing key yields
// ErrKeyNotFound.
func (c *Collection) DeleteByKey(key string) error {
	id, err := c.resolveKey(key)
	if err != nil {
		return err
	}
	return c.Delete(id)
}

// Upsert inserts data under key if no live record carries it, or replaces the
// existing record's data if one does — in a single c.mu.Lock critical section,
// so concurrent upserts on the same key serialise and cannot lose an update. The
// key is stamped into the reserved _key field either way; supplying _key inside
// data is rejected with ErrReservedField. Following the same revision convention
// as InsertWithKey/UpdateByKey, an insert starts the record at revision 1 and a
// replace bumps the current revision by one. It returns the resulting Record and
// emits the matching Watch event (OpInsert or OpUpdate).
func (c *Collection) Upsert(key string, data map[string]any) (Record, error) {
	if _, ok := data[KeyField]; ok {
		return Record{}, reservedFieldErr()
	}
	// Ensure the mandatory unique _key index exists before entering the critical
	// section: ensureKeyIndex acquires c.mu (read) internally, so it must not run
	// while we hold c.mu (write).
	if err := c.ensureKeyIndex(); err != nil {
		return Record{}, err
	}

	stamped := stampKey(data, key)
	ts := time.Now().UTC()

	c.mu.Lock()

	// Resolve the key under the write lock so the present/absent decision and the
	// append are atomic with respect to every other writer. IndexLookup takes
	// sidxMu (read) after c.mu (write), matching the insert/update lock order, so
	// there is no deadlock.
	var (
		id  uint64
		rev uint64
		op  store.Op
		e   store.Entry
		exp int64
	)
	if ids, hit := c.IndexLookup(KeyField, key); hit && len(ids) > 0 {
		if cur, ok := c.index.Get(ids[0]); ok {
			id, rev, op = ids[0], cur.Rev+1, store.OpUpdate
			e = store.NewUpdate(id, stamped)
			// A replace is a data-only write: preserve the record's existing
			// deadline (sticky), matching UpdateByKey/Update semantics.
			exp = cur.ExpiresAt
		}
	}
	if op == "" {
		// No live record carries the key → insert a fresh one at revision 1,
		// honoring the collection's default TTL if configured.
		id, rev, op = c.idSeq.Add(1), 1, store.OpInsert
		e = store.NewInsert(id, stamped)
		exp = c.resolveInsertExpiry(time.Time{})
	}
	e.Ts = ts
	e.Rev = rev
	e.ExpiresAt = exp

	// Enforce unique indexes before writing so a rejected upsert appends nothing
	// and mutates no index. The _key index never conflicts here (insert: no live
	// record holds key; replace: the key maps to id itself), but other unique
	// indexes on data fields still apply.
	if err := c.sidxCheckUnique(id, stamped); err != nil {
		c.mu.Unlock()
		return Record{}, err
	}
	offset, err := c.active.Append(e)
	if err != nil {
		c.mu.Unlock()
		return Record{}, fmt.Errorf("collection: upsert: %w", err)
	}
	c.index.Set(id, IndexEntry{SegmentPath: c.active.Path(), Offset: offset, Rev: rev, ExpiresAt: exp})
	if op == store.OpInsert {
		c.sidxIndexEntry(id, stamped)
	} else {
		c.sidxUpdateEntry(id, stamped)
	}
	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return Record{}, fmt.Errorf("collection: upsert: %w", err)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	rec := Record{ID: id, Key: key, Rev: rev, Ts: ts, Data: stamped}
	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return rec, fmt.Errorf("collection: rotate after upsert: %w", err)
		}
	}

	c.emit(WatchEvent{Op: op, ID: id, Data: stamped, Ts: ts})
	return rec, nil
}

// ensureKeyIndex lazily creates the mandatory unique index on the reserved _key
// field. EnsureUniqueIndex is idempotent, so this is cheap after the first call.
func (c *Collection) ensureKeyIndex() error {
	return c.EnsureUniqueIndex(KeyField)
}

// resolveKey returns the uint64 id of the live record carrying key. It relies on
// the unique _key index, which maps a key to at most one id, so the lookup is
// O(1). A missing key (or an index that does not exist because no keyed write
// has happened yet) yields ErrKeyNotFound.
func (c *Collection) resolveKey(key string) (uint64, error) {
	ids, ok := c.IndexLookup(KeyField, key)
	if !ok || len(ids) == 0 {
		return 0, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}
	// The _key index is unique, so at most one id maps to the key.
	return ids[0], nil
}

// stampKey returns a copy of data with the reserved _key field set to key. The
// input map is never mutated, so a keyed write does not leak the reserved field
// back into the caller's map.
func stampKey(data map[string]any, key string) map[string]any {
	out := make(map[string]any, len(data)+1)
	for k, v := range data {
		out[k] = v
	}
	out[KeyField] = key
	return out
}
