package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/srjn45/filedbv2/store"
)

// DefaultReplicationRingSize is the number of recent committed entries the
// leader keeps in memory so a briefly-disconnected follower can resume without
// re-fetching a full snapshot. A follower that falls further behind than this is
// told to re-bootstrap from a Snapshot.
const DefaultReplicationRingSize = 8192

// replStateFilename holds the DB-level replication watermarks (the leader's
// assigned LSN and a follower's durably-applied LSN). It lives at the data-dir
// root, alongside the per-collection sub-directories.
const replStateFilename = "replication.json"

// ReplicationEntry is one committed write tagged with the leader's monotonic
// global sequence number (LSN). It is the unit shipped over the Replicate feed:
// the server maps it to a proto ReplicationRecord, and a follower reproduces it
// verbatim via DB.ApplyReplication so its primary and secondary indexes match
// the leader's exactly.
type ReplicationEntry struct {
	LSN        uint64
	Collection string
	Entry      store.Entry
}

// replSub is a single Replicate subscriber: a buffered channel of committed
// entries plus a dead channel closed when the subscriber overflowed (fell too
// far behind the live feed) and must re-bootstrap. It doubles as the follower
// bookkeeping surfaced by ReplicationStatus.
type replSub struct {
	id          uint64
	followerID  string
	ch          chan ReplicationEntry
	dead        chan struct{}
	once        sync.Once
	connectedAt time.Time
	sent        atomic.Uint64 // last LSN shipped to this subscriber
}

func (s *replSub) kill() { s.once.Do(func() { close(s.dead) }) }

// replicationBroker sequences committed entries into a global LSN order, keeps a
// bounded in-memory ring of recent entries for resume, and fans them out to
// connected followers. It is created only when replication is enabled (a
// non-zero ring size), so the embedded/default write path pays nothing.
type replicationBroker struct {
	mu   sync.Mutex
	lsn  uint64             // last assigned LSN
	ring []ReplicationEntry // circular buffer of recent entries, ascending by LSN
	cap  int
	head int // index of the oldest retained entry
	size int // number of valid entries in the ring

	subBuf int
	subs   map[uint64]*replSub
	subSeq uint64
}

func newReplicationBroker(ringSize, subBuf int) *replicationBroker {
	if ringSize <= 0 {
		ringSize = DefaultReplicationRingSize
	}
	if subBuf <= 0 {
		subBuf = ringSize
	}
	return &replicationBroker{
		ring:   make([]ReplicationEntry, ringSize),
		cap:    ringSize,
		subBuf: subBuf,
		subs:   make(map[uint64]*replSub),
	}
}

// oldest returns the lowest LSN the ring can still serve. When the ring is empty
// it returns lsn+1 — the next entry to be assigned — so a follower requesting
// anything below that (history the leader no longer holds) is told to resync.
// The caller must hold b.mu.
func (b *replicationBroker) oldest() uint64 {
	if b.size == 0 {
		return b.lsn + 1
	}
	return b.ring[b.head].LSN
}

// publish assigns the next LSN to a committed entry, records it in the ring, and
// fans it out to every live subscriber (non-blocking: a subscriber whose buffer
// is full is killed so it re-bootstraps rather than receiving a gapped feed).
// It is called under the committing collection's write lock, so per-collection
// order — and therefore a consistent global LSN order — is preserved.
func (b *replicationBroker) publish(collection string, e store.Entry) uint64 {
	b.mu.Lock()
	b.lsn++
	lsn := b.lsn
	re := ReplicationEntry{LSN: lsn, Collection: collection, Entry: e}

	if b.size < b.cap {
		b.ring[(b.head+b.size)%b.cap] = re
		b.size++
	} else {
		// Ring full: overwrite the oldest entry.
		b.ring[b.head] = re
		b.head = (b.head + 1) % b.cap
	}

	for _, s := range b.subs {
		select {
		case <-s.dead:
			continue // already overflowed; leave it for the server to reap
		default:
		}
		select {
		case s.ch <- re:
		default:
			s.kill() // buffer full: signal resync rather than drop silently
		}
	}
	b.mu.Unlock()
	return lsn
}

// subscribe registers a follower resuming from fromLSN. It returns the buffered
// backlog (entries still in the ring with LSN > fromLSN, ascending) plus a live
// subscription for entries committed afterwards. ok is false when fromLSN is so
// far behind that the needed entries have aged out of the ring — the follower
// must re-bootstrap from a Snapshot.
func (b *replicationBroker) subscribe(fromLSN uint64, followerID string) (*replSub, []ReplicationEntry, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if fromLSN+1 < b.oldest() {
		return nil, nil, false
	}

	var backlog []ReplicationEntry
	for i := 0; i < b.size; i++ {
		re := b.ring[(b.head+i)%b.cap]
		if re.LSN > fromLSN {
			backlog = append(backlog, re)
		}
	}

	b.subSeq++
	sub := &replSub{
		id:          b.subSeq,
		followerID:  followerID,
		ch:          make(chan ReplicationEntry, b.subBuf),
		dead:        make(chan struct{}),
		connectedAt: time.Now().UTC(),
	}
	sub.sent.Store(fromLSN)
	b.subs[sub.id] = sub
	return sub, backlog, true
}

func (b *replicationBroker) unsubscribe(id uint64) {
	b.mu.Lock()
	delete(b.subs, id)
	b.mu.Unlock()
}

func (b *replicationBroker) currentLSN() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lsn
}

// setLSN restores the last assigned LSN on startup so LSNs stay monotonic across
// a leader restart (preventing a follower from mistaking replayed numbers for
// already-applied ones). Only meaningful before any write.
func (b *replicationBroker) setLSN(lsn uint64) {
	b.mu.Lock()
	if lsn > b.lsn {
		b.lsn = lsn
	}
	b.mu.Unlock()
}

func (b *replicationBroker) status() ReplicationStatusData {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := ReplicationStatusData{LeaderLSN: b.lsn}
	for _, s := range b.subs {
		sent := s.sent.Load()
		lag := uint64(0)
		if b.lsn > sent {
			lag = b.lsn - sent
		}
		out.Followers = append(out.Followers, FollowerStatusData{
			FollowerID:  s.followerID,
			AckedLSN:    sent,
			Lag:         lag,
			ConnectedAt: s.connectedAt,
		})
	}
	return out
}

// ReplicationStream is a live Replicate subscription handed to the server. The
// server drains Entries() (after first shipping the backlog returned alongside
// it), watches Dead() for an overflow that forces the follower to re-bootstrap,
// records each shipped LSN via NoteSent for status reporting, and Close()s it
// when the follower disconnects.
type ReplicationStream struct {
	broker *replicationBroker
	sub    *replSub
}

// Entries is the live feed of entries committed after the subscription began.
func (r *ReplicationStream) Entries() <-chan ReplicationEntry { return r.sub.ch }

// Dead is closed when the subscriber overflowed and must re-bootstrap.
func (r *ReplicationStream) Dead() <-chan struct{} { return r.sub.dead }

// NoteSent records the highest LSN shipped to this follower for ReplicationStatus.
func (r *ReplicationStream) NoteSent(lsn uint64) { r.sub.sent.Store(lsn) }

// Close unregisters the subscription.
func (r *ReplicationStream) Close() { r.broker.unsubscribe(r.sub.id) }

// FollowerStatusData is one connected follower's replication progress.
type FollowerStatusData struct {
	FollowerID  string
	AckedLSN    uint64 // last LSN shipped to the follower (async: sent, not yet acked)
	Lag         uint64 // LeaderLSN - AckedLSN
	ConnectedAt time.Time
}

// ReplicationStatusData is the leader's replication view: its current LSN plus
// one entry per connected follower.
type ReplicationStatusData struct {
	LeaderLSN uint64
	Followers []FollowerStatusData
}

// ---- DB-level replication wiring -------------------------------------------

// replState is the durable DB-level replication watermark file.
type replState struct {
	// LeaderLSN is the last LSN this node assigned as a leader; restored on
	// startup so LSNs never repeat across a restart.
	LeaderLSN uint64 `json:"leader_lsn"`
	// AppliedLSN is the highest LSN this node has durably applied as a follower.
	AppliedLSN uint64 `json:"applied_lsn"`
}

// initReplication builds the LSN broker (when enabled by a non-zero ring size)
// and restores the persisted LSN watermarks. Called from Open.
func (db *DB) initReplication(ringSize int) {
	st, _ := loadReplState(db.replStatePath())
	db.appliedLSN = st.AppliedLSN
	if ringSize > 0 {
		db.broker = newReplicationBroker(ringSize, 0)
		db.broker.setLSN(st.LeaderLSN)
	}
}

func (db *DB) replStatePath() string { return filepath.Join(db.dataDir, replStateFilename) }

func loadReplState(path string) (replState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return replState{}, err
	}
	var st replState
	if err := json.Unmarshal(data, &st); err != nil {
		return replState{}, err
	}
	return st, nil
}

// persistReplState writes the current LSN watermarks durably. Best-effort: a
// lost write only costs the follower a harmless re-apply (apply is idempotent by
// record revision) or, on the leader, a wider resync window after a crash.
func (db *DB) persistReplState() error {
	db.replMu.Lock()
	st := replState{AppliedLSN: db.appliedLSN}
	db.replMu.Unlock()
	if db.broker != nil {
		st.LeaderLSN = db.broker.currentLSN()
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writeFileAtomic(db.replStatePath(), data, 0o644)
}

// CurrentLSN returns the leader's last-assigned global sequence number, or 0 when
// replication is disabled.
func (db *DB) CurrentLSN() uint64 {
	if db.broker == nil {
		return 0
	}
	return db.broker.currentLSN()
}

// ReplicationStatus reports the leader LSN and per-follower progress. It returns
// a zero value when replication is disabled.
func (db *DB) ReplicationStatus() ReplicationStatusData {
	if db.broker == nil {
		return ReplicationStatusData{}
	}
	return db.broker.status()
}

// SubscribeReplication opens a Replicate feed resuming from fromLSN. ok is false
// when replication is disabled or the follower is too far behind the ring and
// must re-bootstrap from a Snapshot. The returned backlog must be shipped before
// draining the stream's Entries().
func (db *DB) SubscribeReplication(fromLSN uint64, followerID string) (stream *ReplicationStream, backlog []ReplicationEntry, ok bool) {
	if db.broker == nil {
		return nil, nil, false
	}
	sub, backlog, ok := db.broker.subscribe(fromLSN, followerID)
	if !ok {
		return nil, nil, false
	}
	return &ReplicationStream{broker: db.broker, sub: sub}, backlog, true
}

// AppliedLSN returns the highest LSN this follower has durably applied.
func (db *DB) AppliedLSN() uint64 {
	db.replMu.Lock()
	defer db.replMu.Unlock()
	return db.appliedLSN
}

// SetAppliedLSN advances and persists the follower's applied-LSN watermark. It
// never moves the watermark backwards. Persistence is best-effort — apply is
// idempotent, so a lost update only widens the harmless re-apply window on resume.
func (db *DB) SetAppliedLSN(lsn uint64) error {
	db.replMu.Lock()
	if lsn <= db.appliedLSN {
		db.replMu.Unlock()
		return nil
	}
	db.appliedLSN = lsn
	db.replMu.Unlock()
	return db.persistReplState()
}

// ApplyReplication applies a replicated entry to the named collection, creating
// the collection first if this follower has not seen it yet (so collections
// created on the leader replicate automatically). The entry is written verbatim,
// preserving its id, revision, timestamp, and expiry.
func (db *DB) ApplyReplication(collection string, e store.Entry) error {
	col, err := db.Collection(collection)
	if err != nil {
		// Not seen yet — create it (tolerating a concurrent creation).
		col, err = db.CreateCollection(collection)
		if err != nil {
			if col2, err2 := db.Collection(collection); err2 == nil {
				col = col2
			} else {
				return fmt.Errorf("apply replication: %w", err)
			}
		}
	}
	return col.applyEntry(e)
}

// applyEntry writes a replicated entry verbatim and maintains the primary and
// secondary indexes exactly as the originating local write did — but without
// assigning a new id/revision and without re-enforcing unique constraints (the
// leader already validated them). It is idempotent: an entry whose effect is
// already present (same id at an equal-or-newer revision, or a delete of an
// already-absent id) is skipped, so re-applying the snapshot/stream overlap or
// resuming after a disconnect never duplicates or corrupts state.
func (c *Collection) applyEntry(e store.Entry) error {
	// A keyed record needs the mandatory unique _key index to exist before it is
	// applied, mirroring the keyed write path. ensureKeyIndex takes c.mu (read),
	// so it must run before we take the write lock.
	if e.Data != nil {
		if _, keyed := e.Data[KeyField]; keyed {
			if err := c.ensureKeyIndex(); err != nil {
				return err
			}
		}
	}

	c.mu.Lock()
	cur, exists := c.index.Get(e.ID)

	switch e.Op {
	case store.OpDelete:
		if !exists {
			c.mu.Unlock()
			return nil // already absent — idempotent
		}
		if _, err := c.active.Append(e); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("collection: apply delete: %w", err)
		}
		c.index.Delete(e.ID)
		c.sidxRemoveEntry(e.ID)

	case store.OpInsert, store.OpUpdate:
		if exists && cur.Rev >= e.Rev {
			c.mu.Unlock()
			return nil // already applied at an equal-or-newer revision — idempotent
		}
		offset, err := c.active.Append(e)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("collection: apply write: %w", err)
		}
		c.index.Set(e.ID, IndexEntry{SegmentPath: c.active.Path(), Offset: offset, Rev: e.Rev, ExpiresAt: e.ExpiresAt})
		if exists {
			c.sidxUpdateEntry(e.ID, e.Data)
		} else {
			c.sidxIndexEntry(e.ID, e.Data)
		}

	default:
		c.mu.Unlock()
		return fmt.Errorf("collection: apply: unknown op %q", e.Op)
	}

	// Keep the id counter ahead of every applied id so a future local write (after
	// a promotion) never reuses an id the leader already assigned.
	for {
		old := c.idSeq.Load()
		if e.ID <= old || c.idSeq.CompareAndSwap(old, e.ID) {
			break
		}
	}

	if err := c.syncActiveLocked(); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("collection: apply: sync: %w", err)
	}
	needRotate := c.active.Size() >= c.cfg.SegmentMaxSize
	c.mu.Unlock()

	if needRotate {
		if err := c.rotateSegment(); err != nil {
			return fmt.Errorf("collection: rotate after apply: %w", err)
		}
	}

	// Surface applied changes to this follower's own Watch subscribers.
	switch e.Op {
	case store.OpInsert:
		c.emit(WatchEvent{Op: store.OpInsert, ID: e.ID, Data: e.Data, Ts: e.Ts})
	case store.OpUpdate:
		c.emit(WatchEvent{Op: store.OpUpdate, ID: e.ID, Data: e.Data, Ts: e.Ts})
	case store.OpDelete:
		c.emit(WatchEvent{Op: store.OpDelete, ID: e.ID, Ts: e.Ts})
	}
	return nil
}
