package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/srjn45/scriva/writeoutcome"
)

// Exit codes. Scripts can tell an ordinary failure (nothing applied; the usual
// retry rules hold) from the two write outcomes that must not be blind-retried.
const (
	exitError          = 1 // any other failure
	exitOutcomeUnknown = 3 // WRITE_OUTCOME_UNKNOWN: the write may or may not have been applied
	exitFenced         = 4 // COLLECTION_FENCED: refused until the server restarts
)

// outcomeError is a write-outcome failure decoded for a human.
type outcomeError struct {
	err        error // as returned by the command (an *xtxError keeps its own text)
	reason     string
	collection string
}

func (e *outcomeError) Unwrap() error { return e.err }

func (e *outcomeError) Error() string {
	var b strings.Builder
	b.WriteString(e.err.Error())
	var xe *xtxError
	if !errors.As(e.err, &xe) { // an xtxError already prints its reason
		fmt.Fprintf(&b, "\n  reason:     %s", e.reason)
	}
	coll := "the collection"
	if e.collection != "" {
		coll = fmt.Sprintf("collection %q", e.collection)
	}
	switch e.reason {
	case writeoutcome.ReasonOutcomeUnknown:
		fmt.Fprintf(&b, "\n\nThe write may or may not have been applied, and %s is now fenced. Do NOT blind-retry it.\n"+
			"After the server restarts, read the record back (get / find-by-key) and retry only if it is\n"+
			"absent — or use upsert / update-if-rev, which are safe to repeat.", coll)
	case writeoutcome.ReasonCollectionFenced:
		fmt.Fprintf(&b, "\n\nNothing was applied: %s is fenced by an earlier write whose outcome is unknown.\n"+
			"It refuses reads and writes until the server is restarted.", coll)
	}
	return b.String()
}

// outcomeCLIError decodes a write-outcome error; any other error is returned
// unchanged.
func outcomeCLIError(err error) error {
	var we *writeoutcome.Error
	if !errors.As(writeoutcome.FromError(err), &we) {
		return err
	}
	return &outcomeError{err: err, reason: we.Reason, collection: we.Collection}
}

// exitCode maps a command error onto the process exit status.
func exitCode(err error) int {
	var oe *outcomeError
	if errors.As(err, &oe) {
		switch oe.reason {
		case writeoutcome.ReasonOutcomeUnknown:
			return exitOutcomeUnknown
		case writeoutcome.ReasonCollectionFenced:
			return exitFenced
		}
	}
	return exitError
}

// decodeOutcomes wraps the RunE of every command under root so a write-outcome
// error is decoded (distinct message, distinct exit code), and points the
// post-commit warning printer at the command's stderr.
func decodeOutcomes(root *cobra.Command, flags *cliFlags) {
	for _, c := range root.Commands() {
		decodeOutcomes(c, flags)
		if c.RunE == nil {
			continue
		}
		run := c.RunE
		c.RunE = func(cmd *cobra.Command, args []string) error {
			flags.warn = cmd.ErrOrStderr()
			return outcomeCLIError(run(cmd, args))
		}
	}
}

// postCommitWarnings is a client interceptor that prints the server's
// post-commit warning: the write committed, but a step after it (segment
// rotation) failed. The command still succeeds.
func postCommitWarnings(flags *cliFlags) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var header, trailer metadata.MD
		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Header(&header), grpc.Trailer(&trailer))...)
		w, ok := writeoutcome.PostCommitWarning(trailer)
		if !ok {
			w, ok = writeoutcome.PostCommitWarning(header)
		}
		if ok && err == nil {
			out := flags.warn
			if out == nil {
				out = os.Stderr
			}
			printPostCommitWarning(out, w)
		}
		return err
	}
}

func printPostCommitWarning(w io.Writer, msg string) {
	_, _ = fmt.Fprintf(w, "warning: the write committed, but a post-commit step failed: %s\n"+
		"The write is durable — do not retry it. Check the server log.\n", msg)
}
