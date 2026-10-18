package server

import (
	"errors"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
)

// isNameError reports whether err is a collection-name policy or path
// containment refusal from the engine (docs/reviews/ratification D1/D2).
func isNameError(err error) bool {
	return errors.Is(err, engine.ErrInvalidName) || errors.Is(err, engine.ErrReservedName) ||
		errors.Is(err, engine.ErrUnsafePath)
}

// notFoundOrInvalid maps an engine lookup error to a gRPC status: a name the
// policy refuses is InvalidArgument (it can never exist), anything else keeps
// the NotFound the call sites always returned.
func notFoundOrInvalid(err error) error {
	if isNameError(err) {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return status.Errorf(codes.NotFound, "%v", err)
}

// createErr maps a CreateCollection failure: policy refusals are
// InvalidArgument; everything else (a name that is taken) stays AlreadyExists.
func createErr(err error) error {
	if isNameError(err) {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return status.Errorf(codes.AlreadyExists, "%v", err)
}

// ReasonParticipantRetained is the ErrorInfo reason of a DropCollection
// refused because the collection is still a participant of non-retired
// cross-collection transactions.
const ReasonParticipantRetained = "PARTICIPANT_RETAINED"

// participantRetainedStatus maps ErrParticipantRetained to FAILED_PRECONDITION
// with the offending txids in the ErrorInfo metadata (comma-separated).
func participantRetainedStatus(e *engine.ParticipantRetainedError) error {
	st := status.New(codes.FailedPrecondition, e.Error())
	withInfo, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: ReasonParticipantRetained,
		Domain: "scriva.dev/collection",
		Metadata: map[string]string{
			"collection": e.Collection,
			"txids":      strings.Join(e.TxIDs, ","),
		},
	})
	if err != nil {
		return st.Err()
	}
	return withInfo.Err()
}
