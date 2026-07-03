//nolint:errcheck
package engine

import (
	"testing"
	"time"
)

// setLastUsed overrides a transaction's idle clock for deterministic reaping
// tests, bypassing the wall-clock touch().
func setLastUsed(m *TxManager, id string, t time.Time) {
	tx, _ := m.Get(id)
	tx.mu.Lock()
	tx.lastUsed = t
	tx.mu.Unlock()
}

// TestReapExpiredRemovesIdleTx verifies an idle transaction is reaped past the
// ttl while a recently-used one is left alone.
func TestReapExpiredRemovesIdleTx(t *testing.T) {
	m := NewTxManager(0) // no background sweeper; drive reapExpired directly
	defer m.Close()
	m.ttl = 5 * time.Minute

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	idle := m.Begin("c")
	active := m.Begin("c")
	setLastUsed(m, idle, base)                      // last used long ago
	setLastUsed(m, active, base.Add(9*time.Minute)) // used recently

	reaped := m.reapExpired(base.Add(10 * time.Minute)) // cutoff = base+5m

	if len(reaped) != 1 || reaped[0] != idle {
		t.Fatalf("expected only idle tx reaped, got %v", reaped)
	}
	if _, ok := m.Get(idle); ok {
		t.Errorf("idle tx should be gone after reap")
	}
	if _, ok := m.Get(active); !ok {
		t.Errorf("active tx should survive reap")
	}
}

// TestReapedTxCommitFailsCleanly verifies that after a tx is reaped, Get reports
// it missing (so the server's CommitTx returns NotFound rather than committing
// stale staged ops).
func TestReapedTxCommitFailsCleanly(t *testing.T) {
	m := NewTxManager(0)
	defer m.Close()
	m.ttl = time.Minute

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	id := m.Begin("c")
	tx, _ := m.Get(id)
	tx.StageInsert(1, map[string]any{"a": float64(1)}) // stage some work
	setLastUsed(m, id, base)

	m.reapExpired(base.Add(2 * time.Minute))

	if _, ok := m.Get(id); ok {
		t.Fatalf("reaped tx must not be retrievable for commit")
	}
}

// TestSweeperReapsInBackground exercises the live sweeper goroutine end to end.
func TestSweeperReapsInBackground(t *testing.T) {
	m := NewTxManager(20 * time.Millisecond)
	defer m.Close()

	id := m.Begin("c")
	// Backdate so the very first sweep tick considers it idle.
	setLastUsed(m, id, time.Now().UTC().Add(-time.Second))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.Get(id); !ok {
			return // reaped as expected
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("background sweeper did not reap idle tx within deadline")
}

// TestTTLDisabledNoSweeper verifies ttl<=0 keeps transactions indefinitely and
// Close is still safe.
func TestTTLDisabledNoSweeper(t *testing.T) {
	m := NewTxManager(0)
	id := m.Begin("c")
	if _, ok := m.Get(id); !ok {
		t.Fatal("tx should exist")
	}
	m.Close()
	m.Close() // idempotent
}
