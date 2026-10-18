// Package writeoutcome is the client side of ScrivaDB's write-outcome contract
// (docs/design-database-integrity-hardening.md §2.3) for callers that reach a
// server over gRPC. It holds the wire constants the server, the CLI and Go
// clients share, and decodes a gRPC error into errors.Is-able sentinels so a
// caller can tell the three non-success outcomes of a single-record write
// apart without parsing message text:
//
//	outcome unknown   UNKNOWN              WRITE_OUTCOME_UNKNOWN   do NOT blind-retry
//	collection fenced FAILED_PRECONDITION  COLLECTION_FENCED       retry only after the server reopens it
//	committed + warn  OK                   trailer scriva-post-commit-warning
//
// Every other error (validation, NOT_FOUND, ALREADY_EXISTS, quota, …) is an
// ordinary NotApplied outcome: nothing was written and the usual retry rules
// apply.
//
// Embedded callers (package engine or the scriva façade) get the engine's own
// sentinels instead: engine.ErrWriteOutcomeUnknown, engine.ErrCollectionFenced
// and engine.ErrPostCommit.
//
// The package depends only on gRPC so that light clients (scriva-cli) can use
// it without linking the storage engine.
package writeoutcome

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Domain is the google.rpc.ErrorInfo domain of a write-outcome error.
const Domain = "scriva.write"

// xtxDomain is the ErrorInfo domain of cross-collection transaction errors. A
// transaction whose participant is fenced reports ReasonCollectionFenced in
// that domain, and FromError decodes it like the single-record refusal.
const xtxDomain = "scriva.xtx"

// ErrorInfo reasons. Each maps to exactly one gRPC code (see server/outcome.go).
const (
	// ReasonOutcomeUnknown: the write reached the segment but the server could
	// not prove it durable (an fsync failed, or a failed append could not be
	// rolled back). It may or may not be visible after the collection is
	// reopened. The collection is now fenced. UNKNOWN.
	ReasonOutcomeUnknown = "WRITE_OUTCOME_UNKNOWN"
	// ReasonCollectionFenced: an earlier outcome-unknown write fenced the
	// collection; reads and writes are refused until the server reopens it
	// (restart). Nothing was applied by this request. FAILED_PRECONDITION.
	ReasonCollectionFenced = "COLLECTION_FENCED"
)

// ErrorInfo metadata keys.
const (
	// MetaCollection names the affected collection.
	MetaCollection = "collection"
	// MetaRetrySafe is always "false" for both reasons: an unknown outcome may
	// have been applied, and a fenced collection refuses every retry until it
	// is reopened.
	MetaRetrySafe = "retry_safe"
)

// TrailerPostCommit is the gRPC trailer set on an OK response whose write
// committed but whose ancillary step — segment rotation — failed. The server
// sends the same key as a response header too, and the REST gateway forwards
// it as the HTTP header HeaderPostCommit. The value is a human-readable
// description. The write succeeded; do not retry it.
const TrailerPostCommit = "scriva-post-commit-warning"

// HeaderPostCommit is the HTTP response header the REST gateway sets for
// TrailerPostCommit.
const HeaderPostCommit = "Scriva-Post-Commit-Warning"

// ErrOutcomeUnknown matches (errors.Is) an error decoded by FromError whose
// reason is ReasonOutcomeUnknown.
var ErrOutcomeUnknown = errors.New("scriva: write outcome unknown")

// ErrCollectionFenced matches (errors.Is) an error decoded by FromError whose
// reason is ReasonCollectionFenced.
var ErrCollectionFenced = errors.New("scriva: collection fenced")

// Error is a decoded write-outcome error. errors.Is matches its sentinel
// (ErrOutcomeUnknown or ErrCollectionFenced) and, through Unwrap, the original
// gRPC status error, so status.Code still works on it.
type Error struct {
	Reason     string // ReasonOutcomeUnknown or ReasonCollectionFenced
	Collection string // the affected collection, when the server named it
	err        error  // the original gRPC status error
}

func (e *Error) Error() string { return e.err.Error() }

// GRPCStatus keeps status.Code / status.Convert working on the decoded error.
func (e *Error) GRPCStatus() *status.Status { return status.Convert(e.err) }

// Unwrap exposes the sentinel for the reason and the original error.
func (e *Error) Unwrap() []error {
	switch e.Reason {
	case ReasonOutcomeUnknown:
		return []error{ErrOutcomeUnknown, e.err}
	case ReasonCollectionFenced:
		return []error{ErrCollectionFenced, e.err}
	}
	return []error{e.err}
}

// FromError decodes a gRPC error. It returns an *Error when err carries a
// write-outcome ErrorInfo detail, and err unchanged otherwise (including nil).
func FromError(err error) error {
	if err == nil {
		return nil
	}
	var we *Error
	if errors.As(err, &we) {
		return err
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok || !(info.Domain == Domain || info.Domain == xtxDomain && info.Reason == ReasonCollectionFenced) {
			continue
		}
		return &Error{Reason: info.Reason, Collection: info.Metadata[MetaCollection], err: err}
	}
	return err
}

// IsOutcomeUnknown reports whether err is (or decodes to) an unknown write
// outcome.
func IsOutcomeUnknown(err error) bool { return errors.Is(FromError(err), ErrOutcomeUnknown) }

// IsCollectionFenced reports whether err is (or decodes to) a refusal by a
// fenced collection.
func IsCollectionFenced(err error) bool { return errors.Is(FromError(err), ErrCollectionFenced) }

// PostCommitWarning returns the post-commit warning carried by response
// metadata (collect the trailer with grpc.Trailer(&md), or the header with
// grpc.Header(&md)), and whether there was one.
func PostCommitWarning(md metadata.MD) (string, bool) {
	if v := md.Get(TrailerPostCommit); len(v) > 0 {
		return v[0], true
	}
	return "", false
}
