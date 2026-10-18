package engine

import (
	"errors"
	"fmt"
	"time"
)

// Fail-stop fencing for durable write outcomes
// (docs/design-database-integrity-hardening.md §2.3, §3.2, §3.6).
//
// A write whose outcome the engine cannot prove — an fsync of the active
// segment failed, a failed append could not be truncated away (the segment is
// poisoned), or a Seal/rotation sync failed — leaves the collection's
// in-memory state untrusted until reopen. Such a collection is *fenced*: the
// first cause is recorded and every later read and write is refused with
// ErrCollectionFenced. Close, Verify and Stats remain allowed; Snapshot is
// refused. Recovery is reopening the collection (process restart): a complete
// valid line recovers as committed even though the caller saw an error, a torn
// tail is trimmed.
//
// Helper API (shared by every mutation path, including TTL reap, keyed
// mutations, replication apply and syncLoop):
//
//   - c.checkFenced() error — call lock-free at the top of every read and write
//     entry point, and again under c.mu before appending (appendLocked does).
//     Returns nil or an error wrapping ErrCollectionFenced.
//   - c.fence(op, cause) — record cause as the fence reason. First cause wins;
//     safe with or without c.mu (foreground writers hold c.mu, syncLoop does
//     not). Counts OnWriteOutcomeUnknown once per fenced collection.
//   - c.unknownOutcome(op, cause) error — fence, then return the caller-facing
//     error wrapping both ErrWriteOutcomeUnknown and cause. Use it when a
//     foreground write cannot prove its outcome; the record must not be
//     published (no index publish, publishCommit or emit) and fsync must not
//     be retried.
//
// The single-record write sequence built on these is (see insert):
//
//	c.mu.Lock(); validate; off, err := c.appendLocked(op, e)  // NotApplied or Unknown
//	err = c.syncCommitLocked(op)                               // Unknown on fsync error
//	index/sidx apply; publishCommit; c.mu.Unlock()
//	err = c.afterCommit(op, event, needRotate)                // emit, then rotate; *PostCommitError

// ErrWriteOutcomeUnknown is returned (wrapping the I/O cause) when a write
// reached the segment but the engine cannot prove whether it is durable: the
// fsync failed, or a failed append could not be rolled back. The write may or
// may not be visible after reopen; the collection is fenced. Retrying is not
// safe unless the operation is idempotent by construction (see §2.3 item 6).
var ErrWriteOutcomeUnknown = errors.New("engine: write outcome unknown")

// ErrCollectionFenced is returned by reads and writes on a collection fenced by
// an earlier outcome-unknown failure. Reopen the collection to recover.
var ErrCollectionFenced = errors.New("engine: collection fenced")

// ErrPostCommit marks a *PostCommitError: the write is committed (durable per
// the configured SyncMode) but a later ancillary step failed.
var ErrPostCommit = errors.New("engine: committed; post-commit step failed")

// PostCommitError reports a write that committed and was published, followed
// by a failed ancillary step (segment rotation, meta persist). Callers must
// treat the write as successful; errors.Is(err, ErrPostCommit) matches it, and
// errors.Is/As also reach the underlying cause.
type PostCommitError struct {
	Op    string // the committed operation, e.g. "insert"
	Stage string // the failed ancillary step, e.g. "rotate"
	Err   error
}

func (e *PostCommitError) Error() string {
	return fmt.Sprintf("engine: %s committed; post-commit %s failed: %v", e.Op, e.Stage, e.Err)
}

// Unwrap exposes both ErrPostCommit and the cause to errors.Is/As.
func (e *PostCommitError) Unwrap() []error { return []error{ErrPostCommit, e.Err} }

// fenceState is the recorded first cause of a fence.
type fenceState struct {
	op    string
	cause error
	at    time.Time
}

// fence records cause as the reason this collection is fenced. Only the first
// call takes effect. It never blocks and never calls back into the segment, so
// it is safe under c.mu and under a segment's lock (the poison hook).
func (c *Collection) fence(op string, cause error) {
	if !c.fenced.CompareAndSwap(nil, &fenceState{op: op, cause: cause, at: time.Now()}) {
		return
	}
	if l := c.cfg.Logger; l != nil {
		l.Error("collection fenced: write outcome unknown; reads and writes refused until reopen",
			"collection", c.name, "op", op, "cause", cause)
	}
	if h := c.cfg.OnWriteOutcomeUnknown; h != nil {
		h(c.name, op)
	}
}

// unknownOutcome fences the collection and returns the error for a foreground
// write whose outcome is unknown.
func (c *Collection) unknownOutcome(op string, cause error) error {
	c.fence(op, cause)
	return fmt.Errorf("collection: %s: %w: %w", op, ErrWriteOutcomeUnknown, cause)
}

// checkFenced returns nil, or an error wrapping ErrCollectionFenced and the first
// cause. It is lock-free.
func (c *Collection) checkFenced() error {
	f := c.fenced.Load()
	if f == nil {
		return nil
	}
	return fmt.Errorf("%w: collection %q since %s (%s: %w); reopen to recover",
		ErrCollectionFenced, c.name, f.at.UTC().Format(time.RFC3339), f.op, f.cause)
}

// Fenced reports whether the collection is fenced: nil, or an error wrapping
// ErrCollectionFenced that names the first cause.
func (c *Collection) Fenced() error { return c.checkFenced() }
