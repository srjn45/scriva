package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/srjn45/scriva/store"
)

// metaSnapshot builds the current collectionMeta for persistence, carrying the
// id counter, creation time, and any explicit per-collection default TTL.
func (c *Collection) metaSnapshot() collectionMeta {
	return collectionMeta{
		IDCounter:         c.idSeq.Load(),
		CreatedAt:         c.createdAt,
		DefaultTTLSeconds: c.explicitDefaultTTLSecs.Load(),
		// Carry the encryption block on every snapshot so no write path (rotate,
		// tx commit, close) silently drops it from meta.json.
		Encryption: c.encMeta.Load(),
	}
}

// applyPersistedDefaultTTL applies a per-collection default TTL loaded from
// meta.json. A positive value overrides the server-wide default for this
// collection; zero leaves the inherited global default in place.
func (c *Collection) applyPersistedDefaultTTL(secs int64) {
	if secs <= 0 {
		return
	}
	c.explicitDefaultTTLSecs.Store(secs)
}

// setDefaultTTL records an explicit per-collection default TTL, applies it to
// the running config, and persists it durably in meta.json. A non-positive ttl
// is a no-op (the collection keeps inheriting the global default).
func (c *Collection) setDefaultTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	secs := int64(ttl / time.Second)
	// Memory (explicitDefaultTTLSecs) changes only after meta.json is durable; a
	// failed persist leaves the previous default in force. No collection lock is
	// held: writeMeta's metaMu is the only serialization needed.
	return c.writeMetaThen("ttl",
		func(m *collectionMeta) { m.DefaultTTLSeconds = secs },
		func() { c.explicitDefaultTTLSecs.Store(secs) })
}

// resolveInsertExpiry returns the Unix-nano deadline to stamp on a record: the
// explicit instant when one is supplied, else now+DefaultTTL when a default TTL
// is configured, else 0 (the record never expires).
func (c *Collection) resolveInsertExpiry(explicit time.Time) int64 {
	return c.resolveInsertExpiryAt(explicit, c.now())
}

// resolveInsertExpiryAt is resolveInsertExpiry against a caller-captured now, so
// a batch stamps every record from one instant.
func (c *Collection) resolveInsertExpiryAt(explicit time.Time, now int64) int64 {
	if !explicit.IsZero() {
		return explicit.UnixNano()
	}
	if secs := c.explicitDefaultTTLSecs.Load(); secs > 0 {
		return now + int64(time.Duration(secs)*time.Second)
	}
	if c.cfg.DefaultTTL > 0 {
		return now + int64(c.cfg.DefaultTTL)
	}
	return 0
}

// now is the collection's TTL clock in Unix nanoseconds: the wall clock (or the
// nowFn test hook) floored by an in-process high-water mark, so a backwards wall
// step can never un-expire a record that was already hidden. Every operation
// calls it once and applies that single value to every row it examines.
func (c *Collection) now() int64 {
	var t int64
	if c.cfg.nowFn != nil {
		t = c.cfg.nowFn().UnixNano()
	} else {
		t = time.Now().UnixNano()
	}
	for {
		f := c.clockFloor.Load()
		if t <= f {
			return f
		}
		if c.clockFloor.CompareAndSwap(f, t) {
			return t
		}
	}
}

// expired reports whether a Unix-nano deadline has passed as of now. A zero
// deadline never expires.
func expired(deadline, now int64) bool {
	return deadline != 0 && now >= deadline
}

// isExpired reports whether the record located by loc has passed its TTL as of
// a fresh read of the collection clock. Single-record operations use it; any
// operation touching many rows captures now once and calls expired directly.
func (c *Collection) isExpired(loc IndexEntry) bool {
	return expired(loc.ExpiresAt, c.now())
}

// liveIDs returns ids filtered to those present in the primary index and not
// expired as of now. The shared visibility predicate for index-derived id sets.
func (c *Collection) liveIDs(ids []uint64, now int64) []uint64 {
	if len(ids) == 0 {
		return ids
	}
	out := ids[:0:0]
	c.index.mu.RLock()
	for _, id := range ids {
		if loc, ok := c.index.entries[id]; ok && !expired(loc.ExpiresAt, now) {
			out = append(out, id)
		}
	}
	c.index.mu.RUnlock()
	return out
}

// sidxConflict is SecondaryIndex.conflict with expired holders treated as vacant.
func (c *Collection) sidxConflict(sidx *SecondaryIndex, value string, id uint64, now int64) bool {
	for _, other := range sidx.Lookup(value) {
		if other == id {
			continue
		}
		if loc, ok := c.index.Get(other); ok && !expired(loc.ExpiresAt, now) {
			return true
		}
	}
	return false
}

// reapExpired tombstones every record whose TTL has passed and drops it from the
// primary and secondary indexes, reclaiming it on the next compaction. It is
// driven off the compactor cadence. Records are hidden from reads the instant
// they expire (see isExpired); this pass makes that reclamation durable.
//
// A pass follows the single-record outcome contract (§2.3) with one sync for
// all its tombstones: append every tombstone, sync once, and only then drop the
// ids from the indexes, publish each tombstone to replication and emit its
// watch event. A sync failure (or an append whose rollback failed) is Unknown:
// the collection is fenced and nothing from the pass is published. An append
// failure whose rollback succeeded stops the pass; the tombstones already
// appended are still committed and published, and the error is returned.
func (c *Collection) reapExpired() error {
	if err := c.checkFenced(); err != nil {
		return err
	}
	now := c.now()

	// Collect expired ids under the index read lock; do not mutate while iterating.
	c.index.mu.RLock()
	var expiredIDs []uint64
	for id, loc := range c.index.entries {
		if expired(loc.ExpiresAt, now) {
			expiredIDs = append(expiredIDs, id)
		}
	}
	c.index.mu.RUnlock()

	if len(expiredIDs) == 0 {
		return nil
	}

	c.mu.Lock()
	var (
		reaped  []store.Entry
		stopErr error
	)
	for _, id := range expiredIDs {
		loc, ok := c.index.Get(id)
		if !ok || !expired(loc.ExpiresAt, now) {
			continue // resurrected or refreshed since we snapshotted — leave it be
		}
		e := store.NewDelete(id)
		if _, err := c.appendLocked("reap", e); err != nil {
			if errors.Is(err, ErrWriteOutcomeUnknown) || errors.Is(err, ErrCollectionFenced) {
				c.mu.Unlock()
				return err
			}
			stopErr = fmt.Errorf("collection: reap id %d: %w", id, err)
			break
		}
		reaped = append(reaped, e)
	}
	if len(reaped) == 0 {
		c.mu.Unlock()
		return stopErr
	}
	if err := c.syncCommitLocked("reap"); err != nil {
		c.mu.Unlock()
		return err
	}
	for _, e := range reaped {
		c.index.Delete(e.ID)
		c.sidxRemoveEntry(e.ID)
		c.publishCommit(e)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	for _, e := range reaped {
		c.emit(WatchEvent{Op: store.OpDelete, ID: e.ID, Ts: e.Ts})
	}
	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return &PostCommitError{Op: "reap", Stage: "rotate", Err: err}
		}
	}
	return stopErr
}

// backgroundReap runs a reaper pass for the compactor loop. The pass has no
// caller to return to, so a failure is logged (an Unknown outcome is also
// reported through the fence's log line and OnWriteOutcomeUnknown hook). It
// reports false once the collection is fenced, so the loop stops.
func (c *Collection) backgroundReap() bool {
	err := c.reapExpired()
	if err != nil && !errors.Is(err, ErrCollectionFenced) {
		if l := c.cfg.Logger; l != nil {
			l.Error("ttl reaper pass failed", "collection", c.name, "err", err)
		}
	}
	return c.checkFenced() == nil
}
