//nolint:errcheck
package engine

import (
	"testing"
	"time"

	"github.com/srjn45/filedbv2/store"
)

// recvEvent reads one event from ch, failing the test if none arrives promptly.
func recvEvent(t *testing.T, ch <-chan WatchEvent) WatchEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return WatchEvent{}
	}
}

// TestWatchOverflowSignal verifies that a subscriber whose buffer fills up is
// told about the gap with exactly one OpOverflow sentinel once its channel
// drains, rather than silently missing events.
func TestWatchOverflowSignal(t *testing.T) {
	cfg := testCfg()
	cfg.WatchBufferSize = 2 // tiny buffer so a few inserts overflow it

	col, err := OpenCollection("w", t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	_, ch, cancel := col.Subscribe()
	defer cancel()

	// Fill the buffer (2) and drop the rest — the watcher never drains, so the
	// 3rd+ events have nowhere to go and mark it overflowed.
	for i := 0; i < 5; i++ {
		if _, _, err := col.Insert(map[string]any{"n": float64(i)}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Drain the two buffered (pre-overflow) events.
	for i := 0; i < 2; i++ {
		ev := recvEvent(t, ch)
		if ev.Op != store.OpInsert {
			t.Fatalf("buffered event %d: op = %q, want insert", i, ev.Op)
		}
	}

	// Next write: now that the channel has room, the very next emit must deliver
	// the overflow sentinel before the real event.
	lastID, _, err := col.Insert(map[string]any{"n": float64(99)})
	if err != nil {
		t.Fatalf("insert after drain: %v", err)
	}

	if ev := recvEvent(t, ch); ev.Op != OpOverflow {
		t.Fatalf("expected overflow sentinel first, got op %q (id %d)", ev.Op, ev.ID)
	}
	ev := recvEvent(t, ch)
	if ev.Op != store.OpInsert || ev.ID != lastID {
		t.Fatalf("expected real insert id %d after sentinel, got op %q id %d", lastID, ev.Op, ev.ID)
	}

	// Exactly one sentinel per overflow episode: a further write delivers
	// normally, with no second OpOverflow.
	nextID, _, err := col.Insert(map[string]any{"n": float64(100)})
	if err != nil {
		t.Fatalf("insert after recovery: %v", err)
	}
	if ev := recvEvent(t, ch); ev.Op != store.OpInsert || ev.ID != nextID {
		t.Fatalf("expected normal delivery after recovery, got op %q id %d", ev.Op, ev.ID)
	}
}

// TestWatchNoOverflowWhenDrained verifies a subscriber that keeps up never sees
// an overflow sentinel.
func TestWatchNoOverflowWhenDrained(t *testing.T) {
	cfg := testCfg()
	cfg.WatchBufferSize = 4

	col, err := OpenCollection("w", t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()

	_, ch, cancel := col.Subscribe()
	defer cancel()

	for i := 0; i < 10; i++ {
		if _, _, err := col.Insert(map[string]any{"n": float64(i)}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		ev := recvEvent(t, ch) // drain immediately so the buffer never fills
		if ev.Op == OpOverflow {
			t.Fatalf("unexpected overflow at event %d", i)
		}
	}
}
