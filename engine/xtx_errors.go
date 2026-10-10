package engine

import (
	"errors"
	"fmt"
)

// Typed errors of the cross-collection transaction (XTx) protocol, see
// docs/design-cross-collection-transactions.md §8.1 and §11. They are
// sentinels for errors.Is; the concrete error usually wraps one of them with
// context (txid, offset, cause).

var (
	// ErrReservedName: a root-level directory or a collection name uses the
	// reserved "xtx." prefix (§2.1).
	ErrReservedName = errors.New("engine: collection name uses the reserved \"xtx.\" prefix")

	// ErrFormatTooNew: xtx.format demands a newer reader than this binary (§11.2).
	ErrFormatTooNew = errors.New("engine: xtx.format requires a newer ScrivaDB version")
	// ErrXTxUnsupported: the data directory holds XTx state this binary cannot
	// interpret or safely operate on (unknown feature, journal version, or an
	// operation that does not yet preserve XTx semantics). Distinct from
	// corruption: nothing is known to be damaged.
	ErrXTxUnsupported = errors.New("engine: cross-collection transaction state is not supported by this binary")
	// ErrXTxFormatCorrupt: xtx.format is unreadable or fails its checksum.
	ErrXTxFormatCorrupt = errors.New("engine: xtx.format is corrupt")

	// ErrXTxJournalCorrupt: a complete record of xtx.journal fails its checksum
	// or does not parse, or the header is invalid (§4.3). Open fails closed;
	// the decisions in the journal are never presumed aborted.
	ErrXTxJournalCorrupt = errors.New("engine: xtx.journal is corrupt")
	// ErrXTxJournalMissing: xtx.format says XTx is enabled and stamped entries
	// exist, but xtx.journal is absent (§7.2 row 12). Reserved for recovery.
	ErrXTxJournalMissing = errors.New("engine: xtx.journal is missing")
	// ErrXTxDecisionConflict: COMMIT and ABORT (or differing COMMIT contents)
	// for one txid (§4.3, §7.2 row 11).
	ErrXTxDecisionConflict = errors.New("engine: conflicting cross-collection transaction decisions")

	// Commit outcomes (§8.1).

	ErrXTxConflict       = errors.New("engine: cross-collection transaction conflict")
	ErrXTxTooLarge       = errors.New("engine: cross-collection transaction exceeds limits")
	ErrXTxDurability     = errors.New("engine: cross-collection transaction aborted: durability failure")
	ErrXTxOutcomeUnknown = errors.New("engine: cross-collection transaction outcome unknown")
	ErrXTxInProgress     = errors.New("engine: cross-collection transaction with this key is in progress")
	ErrXTxIncomplete     = errors.New("engine: committed cross-collection transaction is missing participant evidence")
	// ErrXTxRecoveryRequired: a standalone collection open in an XTx-enabled
	// root before the DB-level recovery phase produced a decision table (§7.1).
	ErrXTxRecoveryRequired = errors.New("engine: cross-collection transaction recovery required")
	ErrReadOnly            = errors.New("engine: cannot write to read-only follower")
	ErrCollectionNotFound  = errors.New("engine: collection not found")
	ErrXTxDuplicateOp      = errors.New("engine: duplicate operation on (collection, id) in transaction")

	// Parser errors for stamped entries (§3.2).
	ErrV1CRCOnStamp = errors.New("engine: stamped entry carries a v1 checksum")
	ErrMissingCRC   = errors.New("engine: stamped entry has no crc")
	ErrBadStamp     = errors.New("engine: invalid tx stamp")
)

// XTxError carries the txid (when known) and the underlying cause of a typed
// XTx failure. errors.Is matches both Err (one of the sentinels above) and
// Cause.
type XTxError struct {
	Tx    string // txid, empty when none was allocated
	Err   error  // sentinel
	Cause error  // underlying error, may be nil
}

func (e *XTxError) Error() string {
	s := e.Err.Error()
	if e.Tx != "" {
		s += " (tx " + e.Tx + ")"
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

// Unwrap lets errors.Is / errors.As reach both the sentinel and the cause.
func (e *XTxError) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.Cause}
}

func xtxErr(tx string, sentinel, cause error) error {
	return &XTxError{Tx: tx, Err: sentinel, Cause: cause}
}

// XTxStatus is the outcome a caller can ask for by txid or idempotency key
// (§8.2).
type XTxStatus string

const (
	XTxCommitted XTxStatus = "COMMITTED"
	XTxAborted   XTxStatus = "ABORTED"
	XTxPending   XTxStatus = "PENDING"
	XTxUnknown   XTxStatus = "UNKNOWN"
	XTxExpired   XTxStatus = "EXPIRED"
)

// ErrQuotaExceeded is the §8.1 alias for ErrResourceExhausted on quota breaches.
var ErrQuotaExceeded = ErrResourceExhausted

// XTxRetrySafe reports whether re-running a failed commit as a fresh XTx can
// never double-apply, per the "Retry safe?" column of §8.1. It is false for
// ErrXTxOutcomeUnknown (only status/idempotency key may resolve it) and for
// any error it does not recognise (fail closed).
func XTxRetrySafe(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrXTxOutcomeUnknown), errors.Is(err, ErrXTxInProgress),
		errors.Is(err, ErrXTxTooLarge):
		return false
	case errors.Is(err, ErrXTxConflict), errors.Is(err, ErrXTxDurability),
		errors.Is(err, ErrResourceExhausted), errors.Is(err, ErrQuotaExceeded):
		return true
	}
	return false
}

// journalCorruptError locates a journal parse failure.
type journalCorruptError struct {
	Offset int64
	Reason string
}

func (e *journalCorruptError) Error() string {
	return fmt.Sprintf("%s: offset %d: %s", ErrXTxJournalCorrupt, e.Offset, e.Reason)
}

// Is makes the error match ErrXTxJournalCorrupt and the generic ErrIntegrity,
// so callers that already map integrity failures keep working.
func (e *journalCorruptError) Is(target error) bool {
	return target == ErrXTxJournalCorrupt || target == ErrIntegrity
}
