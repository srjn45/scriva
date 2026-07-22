package engine

import (
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
		DefaultTTLSeconds: c.explicitDefaultTTLSecs,
	}
}

// applyPersistedDefaultTTL applies a per-collection default TTL loaded from
// meta.json. A positive value overrides the server-wide default for this
// collection; zero leaves the inherited global default in place.
func (c *Collection) applyPersistedDefaultTTL(secs int64) {
	if secs <= 0 {
		return
	}
	c.explicitDefaultTTLSecs = secs
	c.cfg.DefaultTTL = time.Duration(secs) * time.Second
}

// setDefaultTTL records an explicit per-collection default TTL, applies it to
// the running config, and persists it durably in meta.json. A non-positive ttl
// is a no-op (the collection keeps inheriting the global default).
func (c *Collection) setDefaultTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	c.mu.Lock()
	c.explicitDefaultTTLSecs = int64(ttl / time.Second)
	c.cfg.DefaultTTL = time.Duration(c.explicitDefaultTTLSecs) * time.Second
	snap := c.metaSnapshot()
	c.mu.Unlock()
	return persistMeta(metaPath(c.dir), snap)
}

// resolveInsertExpiry returns the Unix-nano deadline to stamp on a record: the
// explicit instant when one is supplied, else now+DefaultTTL when a default TTL
// is configured, else 0 (the record never expires).
func (c *Collection) resolveInsertExpiry(explicit time.Time) int64 {
	if !explicit.IsZero() {
		return explicit.UnixNano()
	}
	if c.cfg.DefaultTTL > 0 {
		return time.Now().UTC().Add(c.cfg.DefaultTTL).UnixNano()
	}
	return 0
}

// expired reports whether a Unix-nano deadline has passed as of now. A zero
// deadline never expires.
func expired(deadline, now int64) bool {
	return deadline != 0 && now >= deadline
}

// isExpired reports whether the record located by loc has passed its TTL as of
// the current wall-clock time. Reads consult this to hide expired records that
// the reaper has not yet reclaimed.
func (c *Collection) isExpired(loc IndexEntry) bool {
	return expired(loc.ExpiresAt, time.Now().UnixNano())
}

// reapExpired tombstones every record whose TTL has passed and drops it from the
// primary and secondary indexes, reclaiming it on the next compaction. It is
// driven off the compactor cadence. Records are hidden from reads the instant
// they expire (see isExpired); this pass makes that reclamation durable.
func (c *Collection) reapExpired() error {
	now := time.Now().UnixNano()

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
	var events []WatchEvent
	for _, id := range expiredIDs {
		loc, ok := c.index.Get(id)
		if !ok || !expired(loc.ExpiresAt, now) {
			continue // resurrected or refreshed since we snapshotted — leave it be
		}
		e := store.NewDelete(id)
		if _, err := c.active.Append(e); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("collection: reap id %d: %w", id, err)
		}
		c.index.Delete(id)
		c.sidxRemoveEntry(id)
		events = append(events, WatchEvent{Op: store.OpDelete, ID: id, Ts: e.Ts})
	}
	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("collection: reap sync: %w", err)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return fmt.Errorf("collection: rotate after reap: %w", err)
		}
	}

	for _, ev := range events {
		c.emit(ev)
	}
	return nil
}
