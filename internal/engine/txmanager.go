package engine

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// txOpKind identifies the operation staged inside a transaction.
type txOpKind int8

const (
	txOpInsert txOpKind = iota
	txOpUpdate
	txOpDelete
)

// txOp is one pending write staged inside a transaction.
type txOp struct {
	kind txOpKind
	id   uint64
	data map[string]any
	ts   time.Time
}

// Tx is an open, uncommitted transaction for a single collection.
type Tx struct {
	ID         string
	Collection string
	mu         sync.Mutex
	ops        []txOp
	createdAt  time.Time // when BeginTx allocated this transaction
	lastUsed   time.Time // bumped on every staged op; drives idle expiry
}

// touch records activity on the transaction. Caller must hold t.mu.
func (t *Tx) touch() {
	t.lastUsed = time.Now().UTC()
}

// StageInsert appends an insert op to the transaction buffer.
func (t *Tx) StageInsert(id uint64, data map[string]any) {
	t.mu.Lock()
	t.ops = append(t.ops, txOp{kind: txOpInsert, id: id, data: data, ts: time.Now().UTC()})
	t.touch()
	t.mu.Unlock()
}

// StageUpdate appends an update op to the transaction buffer.
func (t *Tx) StageUpdate(id uint64, data map[string]any) {
	t.mu.Lock()
	t.ops = append(t.ops, txOp{kind: txOpUpdate, id: id, data: data, ts: time.Now().UTC()})
	t.touch()
	t.mu.Unlock()
}

// StageDelete appends a delete op to the transaction buffer.
func (t *Tx) StageDelete(id uint64) {
	t.mu.Lock()
	t.ops = append(t.ops, txOp{kind: txOpDelete, id: id, ts: time.Now().UTC()})
	t.touch()
	t.mu.Unlock()
}

// Snapshot returns a copy of the staged ops (safe to call from any package).
func (t *Tx) Snapshot() []txOp {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := make([]txOp, len(t.ops))
	copy(cp, t.ops)
	return cp
}

// maxSweepInterval caps how long the sweeper sleeps between idle-tx scans, so a
// large --tx-timeout still gets reaped on a reasonable cadence.
const maxSweepInterval = time.Minute

// TxManager owns all open transactions. It is safe for concurrent use.
//
// When constructed with a positive ttl, a background sweeper rolls back and
// removes transactions that have been idle (no staged op) for longer than the
// ttl. This bounds the memory and reserved ids leaked by clients that call
// BeginTx and disconnect without committing or rolling back.
type TxManager struct {
	mu  sync.RWMutex
	txs map[string]*Tx

	ttl      time.Duration
	sweeping bool // true when a background sweeper goroutine is running
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// Close stops the background sweeper, if one is running. It is safe to call
// multiple times.
func (m *TxManager) Close() {
	m.stopOnce.Do(func() {
		close(m.stop)
		if m.sweeping {
			<-m.done // wait for the sweeper goroutine to exit
		}
	})
}

// NewTxManager returns a ready TxManager. If ttl > 0 it starts a background
// sweeper that reaps idle transactions; ttl <= 0 disables expiry entirely.
func NewTxManager(ttl time.Duration) *TxManager {
	m := &TxManager{
		txs:  make(map[string]*Tx),
		ttl:  ttl,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if ttl > 0 {
		m.sweeping = true
		go m.sweepLoop()
	}
	return m
}

// sweepLoop reaps idle transactions until Close is called.
func (m *TxManager) sweepLoop() {
	defer close(m.done)
	interval := m.ttl
	if interval > maxSweepInterval {
		interval = maxSweepInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.reapExpired(time.Now().UTC())
		}
	}
}

// reapExpired removes every transaction idle since before now-ttl and returns
// their ids. Reaping an abandoned transaction simply discards its staging
// buffer — identical to a rollback, since nothing was ever written to disk.
func (m *TxManager) reapExpired(now time.Time) []string {
	cutoff := now.Add(-m.ttl)
	var reaped []string
	m.mu.Lock()
	for id, tx := range m.txs {
		tx.mu.Lock()
		idle := tx.lastUsed.Before(cutoff)
		tx.mu.Unlock()
		if idle {
			delete(m.txs, id)
			reaped = append(reaped, id)
		}
	}
	m.mu.Unlock()
	return reaped
}

// Begin creates a new transaction for the given collection and returns its ID.
func (m *TxManager) Begin(collection string) string {
	id := newTxID()
	now := time.Now().UTC()
	m.mu.Lock()
	m.txs[id] = &Tx{ID: id, Collection: collection, createdAt: now, lastUsed: now}
	m.mu.Unlock()
	return id
}

// Get returns the transaction with the given ID, or (nil, false) if not found.
func (m *TxManager) Get(txID string) (*Tx, bool) {
	m.mu.RLock()
	tx, ok := m.txs[txID]
	m.mu.RUnlock()
	return tx, ok
}

// Remove deletes a transaction from the manager (used on commit or rollback).
func (m *TxManager) Remove(txID string) {
	m.mu.Lock()
	delete(m.txs, txID)
	m.mu.Unlock()
}

// newTxID generates a random UUID-shaped identifier using crypto/rand.
func newTxID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
