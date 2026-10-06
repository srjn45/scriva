package engine

import (
	"testing"
	"time"

)

func TestCompactor_RotationRace(t *testing.T) {
	dir := t.TempDir()
	
	hookCh := make(chan struct{})
	cfg := defaultConfig()
	cfg.CompactInterval = time.Hour // disable background
	cfg.preSwapHook = func() {
		// Signal we reached the hook
		hookCh <- struct{}{}
		// Wait for rotation to finish
		<-hookCh
	}

	c, err := OpenCollection("race", dir, cfg)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	// Insert enough to create at least one sealed segment.
	// SegmentMaxSize is 4MB by default, so we'll just force rotation.
	c.mu.Lock()
	c.cfg.SegmentMaxSize = 100 // small enough to rotate easily
	c.mu.Unlock()

	var insertedIDs []uint64
	for i := 0; i < 5; i++ {
		id, _, err := c.Insert(map[string]any{"val": i})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		insertedIDs = append(insertedIDs, id)
	}

	// Make sure we have some sealed segments
	c.mu.Lock()
	numSealed := len(c.sealed)
	c.mu.Unlock()
	if numSealed == 0 {
		t.Fatalf("expected sealed segments")
	}

	// Start compaction in background
	errCh := make(chan error)
	go func() {
		errCh <- c.CompactNow()
	}()

	// Wait for compaction to reach the hook
	<-hookCh

	// Now force rotation by inserting more records that push it over the edge
	for i := 5; i < 10; i++ {
		id, _, err := c.Insert(map[string]any{"val": i})
		if err != nil {
			t.Fatalf("Insert during hook: %v", err)
		}
		insertedIDs = append(insertedIDs, id)
	}

	// Let compaction finish
	hookCh <- struct{}{}
	if err := <-errCh; err != nil {
		t.Fatalf("CompactNow: %v", err)
	}

	// Check if all IDs are Get-able live
	for _, id := range insertedIDs {
		if _, err := c.Get(id); err != nil {
			t.Errorf("live Get(%d) failed: %v", id, err)
		}
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and check again
	cfg.preSwapHook = nil
	c2, err := OpenCollection("race", dir, cfg)
	if err != nil {
		t.Fatalf("OpenCollection 2: %v", err)
	}
	defer c2.Close()

	for _, id := range insertedIDs {
		if _, err := c2.Get(id); err != nil {
			t.Errorf("reopen Get(%d) failed: %v", id, err)
		}
	}
}
