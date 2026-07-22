package server

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
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
	if got := status.Code(keyedErr(err)); got != codes.InvalidArgument {
		t.Errorf("keyedErr: got %v, want InvalidArgument", got)
	}
}
