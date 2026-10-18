package server

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/writeoutcome"
)

// This file maps the engine's fail-stop write outcomes
// (docs/design-database-integrity-hardening.md §2.3) onto gRPC. It is the one
// place that decides their codes; every handler funnels its engine error
// through outcomeStatus before any other mapping, so neither can ever surface
// as a retry-safe code (ABORTED, UNAVAILABLE, RESOURCE_EXHAUSTED):
//
//	engine.ErrWriteOutcomeUnknown  UNKNOWN              WRITE_OUTCOME_UNKNOWN  not retry-safe
//	engine.ErrCollectionFenced     FAILED_PRECONDITION  COLLECTION_FENCED      not retry-safe until reopen
//	*engine.PostCommitError        OK + header/trailer scriva-post-commit-warning
//
// UNKNOWN is deliberate, as for XTX_OUTCOME_UNKNOWN: it is also the code of a
// lost response, and both demand the same reaction — read the record back,
// never blind-retry.

// outcomeStatus returns the gRPC status for an outcome-unknown or fenced engine
// error, or nil when err is neither.
func outcomeStatus(collection string, err error) error {
	var code codes.Code
	var reason, hint string
	switch {
	case err == nil:
		return nil
	case errors.Is(err, engine.ErrWriteOutcomeUnknown):
		code, reason = codes.Unknown, writeoutcome.ReasonOutcomeUnknown
		hint = "; the write may or may not have been applied and the collection is now fenced — do not blind-retry, read the record back after the server reopens the collection"
	case errors.Is(err, engine.ErrCollectionFenced):
		code, reason = codes.FailedPrecondition, writeoutcome.ReasonCollectionFenced
		hint = "; nothing was applied — the collection refuses reads and writes until the server is restarted"
	default:
		return nil
	}
	return outcomeErrStatus(code, reason, collection, err.Error()+hint)
}

func outcomeErrStatus(code codes.Code, reason, collection, msg string) error {
	st := status.New(code, msg)
	withInfo, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: writeoutcome.Domain,
		Metadata: map[string]string{
			writeoutcome.MetaCollection: collection,
			writeoutcome.MetaRetrySafe:  "false",
		},
	})
	if err != nil {
		return st.Err()
	}
	return withInfo.Err()
}

// committed reports a committed write as success. A *engine.PostCommitError
// (the write is durable; a later rotation failed) returns nil after attaching
// the scriva-post-commit-warning header and trailer; any other error is
// returned unchanged for the caller to map. Use it on the result of every
// engine write before error mapping:
//
//	id, ts, err := col.Insert(data)
//	if err = committed(ctx, err); err != nil { return nil, s.writeErr(...) }
func committed(ctx context.Context, err error) error {
	if err == nil || !errors.Is(err, engine.ErrPostCommit) {
		return err
	}
	md := metadata.Pairs(writeoutcome.TrailerPostCommit, headerSafe(err.Error()))
	// Header for the REST gateway (trailers are only forwarded on request) and
	// for clients that read headers; trailer as the contract names it. Both
	// are best-effort: the write is committed whatever happens to them.
	_ = grpc.SetHeader(ctx, md)
	_ = grpc.SetTrailer(ctx, md)
	return nil
}

// headerSafe keeps a metadata value to printable ASCII, which is all an
// unprefixed (non -bin) gRPC metadata value may carry.
func headerSafe(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return '?'
		}
		return r
	}, s)
}
