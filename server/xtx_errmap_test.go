package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/xtxapi"
)

// xtxErrInfo extracts the ErrorInfo detail every transaction error carries.
func xtxErrInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	t.Fatalf("error %v carries no ErrorInfo", err)
	return nil
}

// TestXTxStatusErrMapping pins the engine error → (gRPC code, reason,
// retry_safe) table. Several of these failures (outcome unknown, durability,
// integrity) cannot be provoked through a healthy in-process server, so the
// mapping is exercised directly, like errmap_test.go does.
func TestXTxStatusErrMapping(t *testing.T) {
	wrap := func(sentinel, cause error) error {
		return &engine.XTxError{Tx: "tx-1", Err: sentinel, Cause: cause}
	}
	cases := []struct {
		name      string
		err       error
		code      codes.Code
		reason    string
		retrySafe string
	}{
		{"conflict", wrap(engine.ErrXTxConflict, nil), codes.Aborted, xtxapi.ReasonConflict, "true"},
		{"canceled", wrap(engine.ErrXTxCanceled, context.Canceled), codes.Canceled, xtxapi.ReasonCanceled, "true"},
		{"deadline", wrap(engine.ErrXTxCanceled, context.DeadlineExceeded), codes.DeadlineExceeded, xtxapi.ReasonCanceled, "true"},
		{"outcome unknown", wrap(engine.ErrXTxOutcomeUnknown, errors.New("fsync")), codes.Unknown, xtxapi.ReasonOutcomeUnknown, "false"},
		{"in progress", wrap(engine.ErrXTxInProgress, nil), codes.Unavailable, xtxapi.ReasonInProgress, "false"},
		{"durability", wrap(engine.ErrXTxDurability, errors.New("disk")), codes.Unavailable, xtxapi.ReasonDurability, "true"},
		{"not participant", wrap(engine.ErrXTxNotParticipant, nil), codes.InvalidArgument, xtxapi.ReasonNotParticipant, "false"},
		{"scan", wrap(engine.ErrXTxScanUnsupported, nil), codes.Unimplemented, xtxapi.ReasonScanUnsupported, "false"},
		{"too large", wrap(engine.ErrXTxTooLarge, nil), codes.InvalidArgument, xtxapi.ReasonTooLarge, "false"},
		{"record too large", fmt.Errorf("%w: 99 bytes", engine.ErrRecordTooLarge), codes.InvalidArgument, xtxapi.ReasonTooLarge, "false"},
		{"invalid", wrap(engine.ErrXTxInvalid, nil), codes.InvalidArgument, xtxapi.ReasonInvalid, "false"},
		{"reserved name", fmt.Errorf("%w", engine.ErrReservedName), codes.InvalidArgument, xtxapi.ReasonInvalid, "false"},
		{"duplicate op", wrap(engine.ErrXTxDuplicateOp, nil), codes.InvalidArgument, xtxapi.ReasonInvalid, "false"},
		{"handle not found", wrap(engine.ErrXTxHandleNotFound, nil), codes.NotFound, xtxapi.ReasonNotFound, "false"},
		{"doc not found", wrap(engine.ErrXTxDocNotFound, nil), codes.NotFound, xtxapi.ReasonDocNotFound, "false"},
		{"collection not found", fmt.Errorf("%w: x", engine.ErrCollectionNotFound), codes.NotFound, xtxapi.ReasonCollectionNotFound, "false"},
		{"expired", wrap(engine.ErrXTxExpired, nil), codes.FailedPrecondition, xtxapi.ReasonExpired, "true"},
		{"finished", wrap(engine.ErrXTxFinished, nil), codes.FailedPrecondition, xtxapi.ReasonFinished, "false"},
		// A handle finished by a failed commit wraps the original cause; the
		// state of the handle wins.
		{"finished wrapping conflict", wrap(engine.ErrXTxFinished, wrap(engine.ErrXTxConflict, nil)), codes.FailedPrecondition, xtxapi.ReasonFinished, "true"},
		{"read only", fmt.Errorf("%w", engine.ErrReadOnly), codes.FailedPrecondition, xtxapi.ReasonReadOnly, "false"},
		{"unsupported", fmt.Errorf("%w", engine.ErrXTxUnsupported), codes.FailedPrecondition, xtxapi.ReasonUnsupported, "false"},
		{"quota", fmt.Errorf("%w: docs", engine.ErrResourceExhausted), codes.ResourceExhausted, xtxapi.ReasonResourceExhausted, "true"},
		{"duplicate key", fmt.Errorf("%w", engine.ErrDuplicateKey), codes.AlreadyExists, xtxapi.ReasonDuplicateKey, "false"},
		{"integrity", fmt.Errorf("%w", engine.ErrIntegrity), codes.DataLoss, xtxapi.ReasonDataLoss, "false"},
		{"other", errors.New("boom"), codes.Internal, xtxapi.ReasonInternal, "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := xtxStatusErr(tc.err, "ref-1")
			if got := status.Code(err); got != tc.code {
				t.Errorf("code: got %v, want %v", got, tc.code)
			}
			info := xtxErrInfo(t, err)
			if info.Reason != tc.reason {
				t.Errorf("reason: got %q, want %q", info.Reason, tc.reason)
			}
			if info.Domain != xtxapi.Domain {
				t.Errorf("domain: got %q, want %q", info.Domain, xtxapi.Domain)
			}
			if got := info.Metadata[xtxapi.MetaRetrySafe]; got != tc.retrySafe {
				t.Errorf("retry_safe: got %q, want %q", got, tc.retrySafe)
			}
		})
	}
	if xtxStatusErr(nil, "ref") != nil {
		t.Error("nil error must map to nil")
	}
}

// TestXTxStatusErrOutcomeUnknownDirectsToStatus: an unknown outcome must be
// unmistakable and must point the client at the status lookup, never a retry.
func TestXTxStatusErrOutcomeUnknownDirectsToStatus(t *testing.T) {
	// With a txid the lookup reference is the txid.
	err := xtxStatusErr(&engine.XTxError{Tx: "tx-9", Err: engine.ErrXTxOutcomeUnknown}, "my-key")
	if status.Code(err) != codes.Unknown {
		t.Fatalf("code: got %v, want Unknown", status.Code(err))
	}
	info := xtxErrInfo(t, err)
	if info.Metadata[xtxapi.MetaStatusRef] != "tx-9" || info.Metadata[xtxapi.MetaTxID] != "tx-9" {
		t.Errorf("metadata: got %v, want status_ref and tx_id tx-9", info.Metadata)
	}
	msg := status.Convert(err).Message()
	if !strings.Contains(msg, "do not retry") || !strings.Contains(msg, "XTxStatus") {
		t.Errorf("message does not direct the client to XTxStatus: %q", msg)
	}

	// Without one, it falls back to the reference the handler supplied.
	err = xtxStatusErr(&engine.XTxError{Err: engine.ErrXTxOutcomeUnknown}, "my-key")
	if got := xtxErrInfo(t, err).Metadata[xtxapi.MetaStatusRef]; got != "my-key" {
		t.Errorf("status_ref: got %q, want my-key", got)
	}

	// A commit already in flight for the key also points at the lookup.
	err = xtxStatusErr(&engine.XTxError{Err: engine.ErrXTxInProgress}, "my-key")
	if got := xtxErrInfo(t, err).Metadata[xtxapi.MetaStatusRef]; got != "my-key" {
		t.Errorf("in-progress status_ref: got %q, want my-key", got)
	}
}

// TestXTxStatusErrConflictDetail: the conflicting document is machine-readable.
func TestXTxStatusErrConflictDetail(t *testing.T) {
	cause := &engine.XTxConflictError{Collection: "orders", ID: 7, Write: true, ExpectedPresent: true, ExpectedRev: 2}
	info := xtxErrInfo(t, xtxStatusErr(&engine.XTxError{Err: engine.ErrXTxConflict, Cause: cause}, "ref"))
	want := map[string]string{
		xtxapi.MetaConflictCollection:  "orders",
		xtxapi.MetaConflictID:          "7",
		xtxapi.MetaConflictKind:        "write",
		xtxapi.MetaConflictExpectedRev: "2",
		xtxapi.MetaConflictActualRev:   xtxapi.RevAbsent,
	}
	for k, v := range want {
		if info.Metadata[k] != v {
			t.Errorf("%s: got %q, want %q", k, info.Metadata[k], v)
		}
	}
}

// TestXTxIdleTimeoutFollowsTxTimeout: --tx-timeout governs handle reaping, and
// its "0 = disabled" must not become the engine's "0 = default".
func TestXTxIdleTimeoutFollowsTxTimeout(t *testing.T) {
	cfg := DefaultConfig()
	if got := cfg.EngineConfig().XTxIdleTimeout; got != cfg.TxTimeout {
		t.Errorf("default: got %v, want %v", got, cfg.TxTimeout)
	}
	cfg.TxTimeout = 0
	if got := cfg.EngineConfig().XTxIdleTimeout; got >= 0 {
		t.Errorf("disabled: got %v, want a negative value", got)
	}
}
