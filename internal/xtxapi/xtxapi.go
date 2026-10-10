// Package xtxapi holds the wire-level constants of the cross-collection
// transaction API that the server and the CLI share: the machine-readable
// reasons carried in the google.rpc.ErrorInfo detail of every transaction
// error, the keys of its metadata, and the metadata header name.
package xtxapi

// Domain is the ErrorInfo domain of every cross-collection transaction error.
const Domain = "scriva.xtx"

// HeaderXTxID is the gRPC metadata key (and REST header) a client would use to
// tag an ordinary data RPC with a cross-collection transaction. The server
// never runs such an RPC: transactional work goes through StageXTx / GetXTx,
// and a tagged scan is rejected with ReasonScanUnsupported. It exists so that
// request is refused loudly instead of running outside the transaction.
const HeaderXTxID = "x-xtx-id"

// ErrorInfo reasons. Each maps to exactly one gRPC code (see server/xtx.go).
const (
	// ReasonConflict: validation found a document changed since the transaction
	// observed it. Nothing was applied. Retry the whole transaction. ABORTED.
	ReasonConflict = "XTX_CONFLICT"
	// ReasonCanceled: the commit was cancelled or timed out while waiting for
	// locks, before anything was written. The handle is still open.
	// CANCELLED or DEADLINE_EXCEEDED.
	ReasonCanceled = "XTX_CANCELED_BEFORE_PREPARE"
	// ReasonOutcomeUnknown: the commit may or may not have been applied. Do not
	// retry; resolve it with XTxStatus(metadata[MetaStatusRef]). UNKNOWN.
	ReasonOutcomeUnknown = "XTX_OUTCOME_UNKNOWN"
	// ReasonInProgress: a commit of this handle or idempotency key is still
	// executing. Look the outcome up with XTxStatus. UNAVAILABLE.
	ReasonInProgress = "XTX_IN_PROGRESS"
	// ReasonDurability: the commit was aborted by a storage failure before the
	// decision; nothing was applied. UNAVAILABLE.
	ReasonDurability = "XTX_DURABILITY"
	// ReasonNotParticipant: the collection was not declared at BeginXTx.
	// INVALID_ARGUMENT.
	ReasonNotParticipant = "XTX_NOT_PARTICIPANT"
	// ReasonInvalid: a malformed request. INVALID_ARGUMENT.
	ReasonInvalid = "XTX_INVALID"
	// ReasonTooLarge: the transaction, its key or a record exceeds a limit.
	// INVALID_ARGUMENT.
	ReasonTooLarge = "XTX_TOO_LARGE"
	// ReasonScanUnsupported: scans, range predicates, aggregations and index
	// lookups are not available inside a transaction. UNIMPLEMENTED.
	ReasonScanUnsupported = "XTX_SCAN_UNSUPPORTED"
	// ReasonNotFound: no open or recently finished transaction has this xtx_id
	// (never issued, long finished, or lost in a restart). NOT_FOUND.
	ReasonNotFound = "XTX_NOT_FOUND"
	// ReasonDocNotFound: the document is absent in the transaction's view.
	// NOT_FOUND.
	ReasonDocNotFound = "XTX_DOC_NOT_FOUND"
	// ReasonCollectionNotFound: a participant collection does not exist.
	// NOT_FOUND.
	ReasonCollectionNotFound = "XTX_COLLECTION_NOT_FOUND"
	// ReasonExpired: the handle was discarded after its idle timeout or
	// lifetime; nothing it staged was written. FAILED_PRECONDITION.
	ReasonExpired = "XTX_EXPIRED"
	// ReasonFinished: the transaction already committed, rolled back or failed.
	// FAILED_PRECONDITION.
	ReasonFinished = "XTX_FINISHED"
	// ReasonReadOnly: the node is a read-only follower. FAILED_PRECONDITION.
	ReasonReadOnly = "XTX_READ_ONLY"
	// ReasonUnsupported: the data directory holds transaction state this binary
	// cannot operate on. FAILED_PRECONDITION.
	ReasonUnsupported = "XTX_UNSUPPORTED"
	// ReasonResourceExhausted: a collection quota refused the transaction;
	// nothing was applied. RESOURCE_EXHAUSTED.
	ReasonResourceExhausted = "XTX_RESOURCE_EXHAUSTED"
	// ReasonDuplicateKey: a unique index refused the transaction; nothing was
	// applied. ALREADY_EXISTS.
	ReasonDuplicateKey = "XTX_DUPLICATE_KEY"
	// ReasonDataLoss: an integrity failure was detected. DATA_LOSS.
	ReasonDataLoss = "XTX_DATA_LOSS"
	// ReasonInternal: an unclassified server error. INTERNAL.
	ReasonInternal = "XTX_INTERNAL"
)

// ErrorInfo metadata keys.
const (
	// MetaRetrySafe is "true" when re-running the transaction as a new one can
	// never double-apply, "false" otherwise (including every unclassified error).
	MetaRetrySafe = "retry_safe"
	// MetaTxID is the coordinator tx_id, when one was allocated.
	MetaTxID = "tx_id"
	// MetaStatusRef is the reference to pass to XTxStatus to resolve the outcome
	// (set for ReasonOutcomeUnknown and ReasonInProgress).
	MetaStatusRef = "status_ref"

	// Conflict details (ReasonConflict).
	MetaConflictCollection  = "conflict_collection"
	MetaConflictID          = "conflict_id"
	MetaConflictKind        = "conflict_kind" // "read" or "write"
	MetaConflictExpectedRev = "conflict_expected_rev"
	MetaConflictActualRev   = "conflict_actual_rev"
	// A revision value of "absent" means the document did not exist.
	RevAbsent = "absent"
)
