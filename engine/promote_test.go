package engine

import (
	"errors"
	"testing"
	"time"
)

// followerCfg is a follower-role engine config (read-only until promoted), with
// leader-side replication enabled so a promoted node can continue assigning LSNs.
func followerCfg() CollectionConfig {
	return CollectionConfig{
		SegmentMaxSize:      4 * 1024 * 1024,
		CompactInterval:     time.Hour,
		CompactDirtyPct:     0.30,
		ReplicationRingSize: 1024,
		Follower:            true,
	}
}

// TestPromote_CaughtUpFollower: a follower whose applied LSN has caught up to the
// last-known leader LSN promotes cleanly to leader and can then accept writes.
func TestPromote_CaughtUpFollower(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), followerCfg())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if !db.IsFollower() {
		t.Fatalf("freshly opened follower reports role %s, want follower", db.CurrentRole())
	}

	// Simulate a follower that has applied everything the leader is known to hold.
	db.NoteLeaderLSN(10)
	if err := db.SetAppliedLSN(10); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}
	if lag := db.ReplicationLag(); lag != 0 {
		t.Fatalf("lag = %d, want 0 (caught up)", lag)
	}

	res, err := db.Promote(DefaultPromoteMaxLag, false)
	if err != nil {
		t.Fatalf("promote caught-up follower: %v", err)
	}
	if res.Role != RoleLeader {
		t.Fatalf("result role = %s, want leader", res.Role)
	}
	if res.Lag != 0 {
		t.Fatalf("result lag = %d, want 0", res.Lag)
	}
	// The new leader continues assigning LSNs strictly above the replicated tail.
	if res.LSN < 10 {
		t.Fatalf("result lsn = %d, want >= 10 (above replicated history)", res.LSN)
	}
	if db.IsFollower() {
		t.Fatalf("node still reports follower after promotion")
	}

	// The promoted leader accepts writes and assigns LSNs above the old history.
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatalf("create collection on promoted leader: %v", err)
	}
	if _, _, err := col.Insert(map[string]any{"x": float64(1)}); err != nil {
		t.Fatalf("insert on promoted leader: %v", err)
	}
	if got := db.CurrentLSN(); got <= 10 {
		t.Fatalf("leader LSN after write = %d, want > 10 (no LSN reuse)", got)
	}
}

// TestPromote_LaggingFollowerRefused: a follower behind the leader beyond the
// threshold is refused with ErrReplicaLagExceeded, but --force overrides it.
func TestPromote_LaggingFollowerRefused(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), followerCfg())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// The leader is known to be at LSN 100 but this follower only applied 40.
	db.NoteLeaderLSN(100)
	if err := db.SetAppliedLSN(40); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}
	if lag := db.ReplicationLag(); lag != 60 {
		t.Fatalf("lag = %d, want 60", lag)
	}

	// Default threshold (0) refuses the lagging follower.
	if _, err := db.Promote(DefaultPromoteMaxLag, false); !errors.Is(err, ErrReplicaLagExceeded) {
		t.Fatalf("promote lagging follower err = %v, want ErrReplicaLagExceeded", err)
	}
	if db.IsFollower() != true {
		t.Fatalf("refused promotion still flipped the role to leader")
	}

	// A threshold at or above the lag permits it.
	if _, err := db.Promote(60, false); err != nil {
		t.Fatalf("promote with sufficient threshold: %v", err)
	}
	if db.IsFollower() {
		t.Fatalf("node still follower after threshold-permitted promotion")
	}
}

// TestPromote_ForceOverridesLag: --force promotes a lagging follower the lag
// guard would otherwise refuse.
func TestPromote_ForceOverridesLag(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), followerCfg())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	db.NoteLeaderLSN(100)
	if err := db.SetAppliedLSN(40); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}

	res, err := db.Promote(DefaultPromoteMaxLag, true)
	if err != nil {
		t.Fatalf("forced promote: %v", err)
	}
	if res.Role != RoleLeader || db.IsFollower() {
		t.Fatalf("forced promote did not flip role: role=%s isFollower=%v", res.Role, db.IsFollower())
	}
	if res.Lag != 60 {
		t.Fatalf("forced promote reported lag = %d, want 60", res.Lag)
	}
}

// TestPromote_NotFollowerRefused: promoting a node that is already a leader is a
// no-op error (promotion is a one-way transition).
func TestPromote_NotFollowerRefused(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), replCfg()) // leader role (Follower unset)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if db.IsFollower() {
		t.Fatalf("leader-config DB reports follower role")
	}
	if _, err := db.Promote(DefaultPromoteMaxLag, false); !errors.Is(err, ErrNotFollower) {
		t.Fatalf("promote leader err = %v, want ErrNotFollower", err)
	}
	// Forcing does not change that a leader has nothing to promote.
	if _, err := db.Promote(DefaultPromoteMaxLag, true); !errors.Is(err, ErrNotFollower) {
		t.Fatalf("forced promote leader err = %v, want ErrNotFollower", err)
	}
}

// TestPromote_StopsHookAndIsIdempotentlyLeader: the promotion hook fires exactly
// once, and a second promotion attempt is refused because the node is now a leader.
func TestPromote_StopsHookAndIsIdempotentlyLeader(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), followerCfg())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	var hookCalls int
	db.SetPromoteHook(func() { hookCalls++ })

	db.NoteLeaderLSN(5)
	if err := db.SetAppliedLSN(5); err != nil {
		t.Fatalf("set applied lsn: %v", err)
	}

	if _, err := db.Promote(DefaultPromoteMaxLag, false); err != nil {
		t.Fatalf("first promote: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("promote hook called %d times, want 1", hookCalls)
	}
	// A second promotion is refused (already a leader) and does not re-fire the hook.
	if _, err := db.Promote(DefaultPromoteMaxLag, false); !errors.Is(err, ErrNotFollower) {
		t.Fatalf("second promote err = %v, want ErrNotFollower", err)
	}
	if hookCalls != 1 {
		t.Fatalf("promote hook re-fired: %d calls, want 1", hookCalls)
	}
}
