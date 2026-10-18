package engine

import (
	"syscall"
	"testing"
)

// Baseline of single-record write outcomes under injected I/O faults
// (design-database-integrity-hardening §2.3, §3.2 failure-state table).
//
// Each case asserts TODAY's behavior and logs where it differs from the
// ratified contract. The follow-up tasks implement the contract and replace
// `today` with `want`:
//
//	TODO(core-mutation-outcomes): Insert, InsertWithKey, Update, Delete,
//	  Upsert, UpdateIfRev, UpdateIfMatch — fence on sync / truncate failure.
//	TODO(background-and-replication-writes): UpdateByKey, DeleteByKey,
//	  reapExpired, applyEntry and the SyncModeInterval syncLoop.

// writeOutcomeCase is one fault class with its current and ratified outcome.
type writeOutcomeCase struct {
	fault       writeFault
	today, want func(op writeOutcomeOp) outcomeExpect
}

var writeOutcomeCases = []writeOutcomeCase{
	{
		// Control: no fault. Durable, published once, survives power loss.
		fault: faultNone,
		today: durableExpect,
		want:  durableExpect,
	},
	{
		// Append fails, truncate-to-previous-size succeeds: NotApplied, not
		// fenced, retry safe. Matches the contract today.
		fault: faultShortWrite,
		today: notAppliedExpect,
		want:  notAppliedExpect,
	},
	{
		// Append fails and truncate fails: Unknown, collection fenced, torn
		// tail recovered on reopen.
		fault: faultWriteTruncate,
		// TODO: today the segment is poisoned (writes refused) but reads are
		// still served and the error is not ErrWriteOutcomeUnknown.
		today: func(writeOutcomeOp) outcomeExpect {
			return outcomeExpect{outcome: "unknown", errIs: []error{syscall.ENOSPC, ErrSegmentPoisoned},
				immediate: "old", probe: "poisoned", reopen: []string{"old"}, powerLoss: []string{"old"}}
		},
		want: func(writeOutcomeOp) outcomeExpect {
			return outcomeExpect{outcome: "unknown", errIs: []error{syscall.ENOSPC},
				immediate: "fenced", probe: "fenced", reopen: []string{"old"}, powerLoss: []string{"old"}}
		},
	},
	{
		// Append succeeds, fsync fails: Unknown, collection fenced, record
		// unpublished; reopen shows it exactly once (page cache) or absent
		// (power loss).
		fault: faultSync,
		// TODO (H5): today the record is visible in memory and the collection
		// keeps accepting writes, yet nothing is published or emitted.
		today: func(writeOutcomeOp) outcomeExpect {
			return outcomeExpect{outcome: "unknown", errIs: []error{syscall.EIO},
				immediate: "new", probe: "ok", reopen: []string{"new"}, powerLoss: []string{"old"}}
		},
		want: func(writeOutcomeOp) outcomeExpect {
			return outcomeExpect{outcome: "unknown", errIs: []error{syscall.EIO},
				immediate: "fenced", probe: "fenced", reopen: []string{"new"}, powerLoss: []string{"old"}}
		},
	},
}

func durableExpect(op writeOutcomeOp) outcomeExpect {
	return outcomeExpect{outcome: "durable", immediate: "new", events: op.events, lsn: op.lsn,
		probe: "ok", reopen: []string{"new"}, powerLoss: []string{"new"}}
}

func notAppliedExpect(writeOutcomeOp) outcomeExpect {
	return outcomeExpect{outcome: "not-applied", errIs: []error{syscall.ENOSPC},
		immediate: "old", probe: "ok", reopen: []string{"old"}, powerLoss: []string{"old"}}
}

func TestWriteOutcomeBaseline_SyncAlways(t *testing.T) {
	t.Parallel()
	for _, op := range writeOutcomeOps() {
		for _, tc := range writeOutcomeCases {
			t.Run(op.name+"/"+tc.fault.name, func(t *testing.T) {
				t.Parallel()
				obs := observeWriteOutcome(t, op, tc.fault, CollectionConfig{SyncMode: SyncModeAlways})
				today, want := tc.today(op), tc.want(op)
				assertWriteOutcome(t, op, obs, today)
				reportOutcomeGap(t, op.followUp, today, want)
			})
		}
	}
}

// A syncLoop fsync failure under SyncModeInterval must fence the collection
// (§2.3: Unknown covers foreground or syncLoop). The op itself succeeded
// unsynced, as the interval contract allows.
func TestWriteOutcomeBaseline_SyncLoopFailure(t *testing.T) {
	t.Parallel()
	op := writeOutcomeOps()[0] // Insert; syncLoop is op-agnostic
	// TODO(background-and-replication-writes): today the failure is dropped
	// (`_ = active.Sync()`), the loop retries fsync on its next tick, and the
	// collection keeps serving; the retried fsync makes the record survive a
	// power loss. Ratified: fence, never retry fsync.
	today := outcomeExpect{outcome: "unknown", immediate: "new", events: op.events, lsn: op.lsn,
		probe: "ok", reopen: []string{"new"}, powerLoss: []string{"new"}}
	want := outcomeExpect{outcome: "unknown", immediate: "fenced", events: op.events, lsn: op.lsn,
		probe: "fenced", reopen: []string{"new"}, powerLoss: []string{"old", "new"}}

	cfg := CollectionConfig{SyncMode: SyncModeInterval, SyncInterval: 1e6} // 1ms
	obs := observeWriteOutcome(t, op, faultSyncLoop, cfg)
	assertWriteOutcome(t, op, obs, today)
	reportOutcomeGap(t, "background-and-replication-writes", today, want)
}
