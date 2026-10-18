package writeoutcome

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/internal/xtxapi"
)

func statusErr(t *testing.T, code codes.Code, domain, reason string) error {
	t.Helper()
	st, err := status.New(code, "msg").WithDetails(&errdetails.ErrorInfo{
		Domain: domain, Reason: reason, Metadata: map[string]string{MetaCollection: "c"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func TestFromError(t *testing.T) {
	unknown := statusErr(t, codes.Unknown, Domain, ReasonOutcomeUnknown)
	fenced := statusErr(t, codes.FailedPrecondition, Domain, ReasonCollectionFenced)
	xtxFenced := statusErr(t, codes.FailedPrecondition, xtxapi.Domain, xtxapi.ReasonCollectionFenced)
	xtxUnknown := statusErr(t, codes.Unknown, xtxapi.Domain, xtxapi.ReasonOutcomeUnknown)
	plain := status.Error(codes.Unknown, "lost response")

	for _, tc := range []struct {
		name            string
		err             error
		unknown, fenced bool
	}{
		{"unknown", unknown, true, false},
		{"fenced", fenced, false, true},
		{"wrapped fenced", fmt.Errorf("cli: %w", fenced), false, true},
		{"xtx fenced participant", xtxFenced, false, true},
		{"xtx outcome unknown is not a single-record outcome", xtxUnknown, false, false},
		{"plain UNKNOWN", plain, false, false},
		{"not a status", errors.New("boom"), false, false},
		{"nil", nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsOutcomeUnknown(tc.err); got != tc.unknown {
				t.Errorf("IsOutcomeUnknown = %v, want %v", got, tc.unknown)
			}
			if got := IsCollectionFenced(tc.err); got != tc.fenced {
				t.Errorf("IsCollectionFenced = %v, want %v", got, tc.fenced)
			}
		})
	}

	d := FromError(unknown)
	var we *Error
	if !errors.As(d, &we) || we.Reason != ReasonOutcomeUnknown || we.Collection != "c" {
		t.Fatalf("FromError = %#v", d)
	}
	if status.Code(d) != codes.Unknown || !errors.Is(d, unknown) {
		t.Errorf("decoded error lost its status: code %v", status.Code(d))
	}
	if FromError(d) != d {
		t.Error("FromError is not idempotent")
	}
}

func TestPostCommitWarning(t *testing.T) {
	if _, ok := PostCommitWarning(metadata.MD{}); ok {
		t.Error("warning reported on empty metadata")
	}
	w, ok := PostCommitWarning(metadata.Pairs(TrailerPostCommit, "insert committed; post-commit rotate failed"))
	if !ok || w == "" {
		t.Errorf("PostCommitWarning = %q, %v", w, ok)
	}
}

// The private copy of the XTx domain must track the server's.
func TestXTxDomainInSync(t *testing.T) {
	if xtxDomain != xtxapi.Domain || ReasonCollectionFenced != xtxapi.ReasonCollectionFenced {
		t.Errorf("writeoutcome (%s, %s) out of sync with xtxapi (%s, %s)", xtxDomain, ReasonCollectionFenced, xtxapi.Domain, xtxapi.ReasonCollectionFenced)
	}
}
