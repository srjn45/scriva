package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/writeoutcome"
)

// TestWriteErrMapsRecordTooLarge verifies the engine.ErrRecordTooLarge → gRPC
// code mapping (issue #80). The gRPC transport caps received messages below the
// engine's 16 MiB record ceiling, so this mapping can't fire through the wire
// today; it's defense-in-depth for a raised message limit or a non-gRPC caller,
// exercised here directly against the helpers.
func TestWriteErrMapsRecordTooLarge(t *testing.T) {
	// Wrap it the way engine.Segment.Append does, to confirm errors.Is unwraps.
	err := fmt.Errorf("%w: record id=1 encodes to 99 bytes, limit is 42", engine.ErrRecordTooLarge)

	if got := status.Code((&GRPCServer{}).writeErr("c", "insert", err)); got != codes.InvalidArgument {
		t.Errorf("writeErr: got %v, want InvalidArgument", got)
	}
	if got := status.Code(keyedErr("c", err)); got != codes.InvalidArgument {
		t.Errorf("keyedErr: got %v, want InvalidArgument", got)
	}
}

func TestErrMapsIndexCorrupt(t *testing.T) {
	err := fmt.Errorf("lookup failed: %w", engine.ErrIndexCorrupt)

	if got := status.Code(keyedErr("c", err)); got != codes.DataLoss {
		t.Errorf("keyedErr: got %v, want DataLoss", got)
	}
}

// TestOutcomeErrMapping is the unit-level half of the status-mapping golden
// test (the integration half drives every RPC against a fenced collection):
// whatever helper a handler routes an engine error through, an unknown outcome
// is UNKNOWN/WRITE_OUTCOME_UNKNOWN and a fenced collection is
// FAILED_PRECONDITION/COLLECTION_FENCED, both marked not retry-safe — never
// ABORTED, UNAVAILABLE or RESOURCE_EXHAUSTED, even when the cause also wraps an
// error those helpers would otherwise map (a quota breach here).
func TestOutcomeErrMapping(t *testing.T) {
	unknown := fmt.Errorf("collection: insert: %w: %w", engine.ErrWriteOutcomeUnknown, engine.ErrResourceExhausted)
	fenced := fmt.Errorf("%w: collection \"c\" since now (insert: EIO); reopen to recover", engine.ErrCollectionFenced)
	s := &GRPCServer{}
	mappers := map[string]func(error) error{
		"writeErr":      func(err error) error { return s.writeErr("c", "insert", err) },
		"keyedErr":      func(err error) error { return keyedErr("c", err) },
		"keyedWriteErr": func(err error) error { return s.keyedWriteErr("c", err) },
		"opErr":         func(err error) error { return opErr("c", err) },
		"outcomeStatus": func(err error) error { return outcomeStatus("c", err) },
	}
	for name, mapErr := range mappers {
		for _, tc := range []struct {
			err    error
			code   codes.Code
			reason string
		}{
			{unknown, codes.Unknown, writeoutcome.ReasonOutcomeUnknown},
			{fenced, codes.FailedPrecondition, writeoutcome.ReasonCollectionFenced},
		} {
			got := mapErr(tc.err)
			if status.Code(got) != tc.code {
				t.Errorf("%s(%v): code %v, want %v", name, tc.err, status.Code(got), tc.code)
			}
			var info *errdetails.ErrorInfo
			for _, d := range status.Convert(got).Details() {
				info, _ = d.(*errdetails.ErrorInfo)
			}
			if info == nil || info.Domain != writeoutcome.Domain || info.Reason != tc.reason ||
				info.Metadata[writeoutcome.MetaCollection] != "c" || info.Metadata[writeoutcome.MetaRetrySafe] != "false" {
				t.Errorf("%s(%v): ErrorInfo %v, want %s/%s for c, not retry-safe", name, tc.err, info, writeoutcome.Domain, tc.reason)
			}
		}
	}
	if outcomeStatus("c", errors.New("boom")) != nil || outcomeStatus("c", nil) != nil {
		t.Error("outcomeStatus must leave other errors to the caller")
	}
}

// TestCommittedTreatsPostCommitAsSuccess: a *PostCommitError is a committed
// write — the handler sees nil — while any other error passes through.
func TestCommittedTreatsPostCommitAsSuccess(t *testing.T) {
	pce := &engine.PostCommitError{Op: "insert", Stage: "rotate", Err: errors.New("seal: EIO\n")}
	if err := committed(context.Background(), fmt.Errorf("wrapped: %w", pce)); err != nil {
		t.Errorf("committed(PostCommitError) = %v, want nil", err)
	}
	other := errors.New("boom")
	if err := committed(context.Background(), other); !errors.Is(err, other) {
		t.Errorf("committed(other) = %v, want it unchanged", err)
	}
	if got := headerSafe("a\nbé"); got != "a?b?" {
		t.Errorf("headerSafe = %q", got)
	}
}
