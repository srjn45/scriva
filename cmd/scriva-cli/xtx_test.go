package main

import (
	"bytes"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/auth"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/internal/xtxapi"
	"github.com/srjn45/scriva/server"
)

// testCLI drives the real cobra command tree against a real in-process gRPC
// server backed by a real engine in a temp directory.
type testCLI struct {
	t      *testing.T
	host   string
	socket string
	apiKey string
}

func newTestCLI(t *testing.T, keys ...auth.Key) *testCLI {
	t.Helper()
	db, err := engine.Open(t.TempDir(), engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	authn, err := auth.New(keys)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	au, as := authn.Interceptors()
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
	pb.RegisterScrivaServer(srv, gs)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return &testCLI{
		t:    t,
		host: lis.Addr().String(),
		// A socket path that does not exist, so the CLI dials --host and never
		// a real server that happens to listen on the default socket.
		socket: filepath.Join(t.TempDir(), "absent.sock"),
	}
}

// run executes one CLI invocation and returns what it printed and its error
// (a non-nil error is what makes main exit non-zero).
func (c *testCLI) run(args ...string) (string, error) {
	c.t.Helper()
	root := rootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"--host", c.host, "--socket", c.socket, "--api-key", c.apiKey}, args...))
	err := root.Execute()
	return out.String(), err
}

func (c *testCLI) mustRun(args ...string) string {
	c.t.Helper()
	out, err := c.run(args...)
	if err != nil {
		c.t.Fatalf("scriva-cli %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(out)
}

// stagedID parses the "staged id:N" line.
func stagedID(t *testing.T, out string) string {
	t.Helper()
	id, ok := strings.CutPrefix(out, "staged id:")
	if !ok {
		t.Fatalf("unexpected stage output %q", out)
	}
	return id
}

// reasonOf returns the machine-readable reason of a failed xtx command.
func reasonOf(t *testing.T, err error) string {
	t.Helper()
	var xe *xtxError
	if !errors.As(err, &xe) {
		t.Fatalf("error is not a typed transaction error: %v", err)
	}
	return xe.reason
}

func TestCLI_XTx_CommitAcrossCollections(t *testing.T) {
	c := newTestCLI(t)
	c.mustRun("create-collection", "orders")
	c.mustRun("create-collection", "stock")

	xtx := c.mustRun("begin-xtx", "orders", "stock", "--key", "order-1")
	if xtx == "" {
		t.Fatal("begin-xtx printed no handle id")
	}
	orderID := stagedID(t, c.mustRun("xtx-insert", xtx, "orders", `{"item":"pen","qty":2}`))
	stockID := stagedID(t, c.mustRun("xtx-insert", xtx, "stock", `{"item":"pen","left":8}`))

	// Nothing is visible outside the transaction before commit...
	if out := c.mustRun("find", "orders"); strings.Contains(out, "pen") {
		t.Fatalf("staged insert visible before commit: %q", out)
	}
	// ...but the transaction reads its own writes.
	if out := c.mustRun("xtx-get", xtx, "orders", orderID); !strings.Contains(out, `"item":"pen"`) {
		t.Fatalf("xtx-get does not see the staged insert: %q", out)
	}
	if out := c.mustRun("xtx-status", "order-1"); out != "PENDING" {
		t.Errorf("status before commit: got %q, want PENDING", out)
	}

	out := c.mustRun("commit-xtx", xtx)
	first, _, _ := strings.Cut(out, "\n")
	txID, ok := strings.CutPrefix(first, "committed tx:")
	if !ok || txID == "" {
		t.Fatalf("unexpected commit output %q", out)
	}
	for _, want := range []string{"insert orders id:" + orderID, "insert stock id:" + stockID} {
		if !strings.Contains(out, want) {
			t.Errorf("commit output %q lacks %q", out, want)
		}
	}

	if out := c.mustRun("find", "orders"); !strings.Contains(out, "pen") {
		t.Errorf("orders after commit: %q", out)
	}
	if out := c.mustRun("find", "stock"); !strings.Contains(out, "pen") {
		t.Errorf("stock after commit: %q", out)
	}

	// The outcome is found by idempotency key and by tx id.
	for _, ref := range []string{"order-1", txID} {
		if out := c.mustRun("xtx-status", ref); out != "COMMITTED tx:"+txID {
			t.Errorf("xtx-status %s: got %q, want COMMITTED tx:%s", ref, out, txID)
		}
	}

	// Committing again under the same key replays the original outcome.
	again := c.mustRun("begin-xtx", "orders", "stock", "--key", "order-1")
	c.mustRun("xtx-insert", again, "orders", `{"item":"pen","qty":2}`)
	c.mustRun("xtx-insert", again, "stock", `{"item":"pen","left":8}`)
	replay := c.mustRun("commit-xtx", again)
	if !strings.HasPrefix(replay, "already committed tx:"+txID) {
		t.Errorf("replayed commit: got %q", replay)
	}
	if n := strings.Count(c.mustRun("find", "orders"), "pen"); n != 1 {
		t.Errorf("orders holds %d pen documents after a replayed commit, want 1", n)
	}
}

func TestCLI_XTx_RollbackLeavesNothing(t *testing.T) {
	c := newTestCLI(t)
	c.mustRun("create-collection", "a")
	c.mustRun("create-collection", "b")

	xtx := c.mustRun("begin-xtx", "a", "b")
	c.mustRun("xtx-insert", xtx, "a", `{"v":"ghost"}`)
	c.mustRun("xtx-insert", xtx, "b", `{"v":"ghost"}`)
	if out := c.mustRun("rollback-xtx", xtx); out != "rolled back" {
		t.Errorf("rollback output: %q", out)
	}
	for _, col := range []string{"a", "b"} {
		if out := c.mustRun("find", col); strings.Contains(out, "ghost") {
			t.Errorf("%s holds a rolled-back write: %q", col, out)
		}
	}
	if out := c.mustRun("xtx-status", xtx); out != "ABORTED" {
		t.Errorf("status after rollback: got %q, want ABORTED", out)
	}
	// The handle is finished: a later commit is a typed error and a non-zero exit.
	_, err := c.run("commit-xtx", xtx)
	if err == nil {
		t.Fatal("commit after rollback succeeded")
	}
	if got := reasonOf(t, err); got != xtxapi.ReasonFinished {
		t.Errorf("reason: got %s, want %s", got, xtxapi.ReasonFinished)
	}
}

func TestCLI_XTx_ConflictIsTypedAndAppliesNothing(t *testing.T) {
	c := newTestCLI(t)
	c.mustRun("create-collection", "a")
	c.mustRun("create-collection", "b")
	c.mustRun("insert", "a", `{"v":"old"}`)

	xtx := c.mustRun("begin-xtx", "a", "b")
	c.mustRun("xtx-get", xtx, "a", "1")
	c.mustRun("xtx-insert", xtx, "b", `{"v":"partial"}`)
	// Another client changes the document the transaction read.
	c.mustRun("update", "a", "1", `{"v":"new"}`)

	_, err := c.run("commit-xtx", xtx)
	if err == nil {
		t.Fatal("commit succeeded despite a changed read")
	}
	if got := reasonOf(t, err); got != xtxapi.ReasonConflict {
		t.Fatalf("reason: got %s, want %s", got, xtxapi.ReasonConflict)
	}
	if status.Code(err) != codes.Aborted {
		t.Errorf("code: got %v, want Aborted", status.Code(err))
	}
	for _, want := range []string{"reason:     " + xtxapi.ReasonConflict, "retry safe: true", "Nothing was applied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message lacks %q:\n%s", want, err)
		}
	}
	if out := c.mustRun("find", "b"); strings.Contains(out, "partial") {
		t.Errorf("a conflicted commit left partial state: %q", out)
	}
}

func TestCLI_XTx_UpdateDeleteAndTypedErrors(t *testing.T) {
	c := newTestCLI(t)
	c.mustRun("create-collection", "a")
	c.mustRun("create-collection", "b")
	c.mustRun("create-collection", "other")
	c.mustRun("insert", "a", `{"v":"one"}`)
	c.mustRun("insert", "b", `{"v":"two"}`)

	xtx := c.mustRun("begin-xtx", "a", "b")
	c.mustRun("xtx-update", xtx, "a", "1", `{"v":"changed"}`, "--expected-rev", "1")
	c.mustRun("xtx-delete", xtx, "b", "1")

	// A collection that was not declared at begin is rejected.
	_, err := c.run("xtx-insert", xtx, "other", `{"v":1}`)
	if err == nil || reasonOf(t, err) != xtxapi.ReasonNotParticipant {
		t.Errorf("undeclared collection: got %v, want %s", err, xtxapi.ReasonNotParticipant)
	}
	// A stale --expected-rev is rejected at once.
	_, err = c.run("xtx-delete", xtx, "a", "1", "--expected-rev", "9")
	if err == nil {
		t.Error("stale --expected-rev accepted")
	}

	out := c.mustRun("commit-xtx", xtx)
	if !strings.Contains(out, "update a id:1") || !strings.Contains(out, "delete b id:1") {
		t.Errorf("commit output: %q", out)
	}
	if out := c.mustRun("find", "a"); !strings.Contains(out, "changed") {
		t.Errorf("a after commit: %q", out)
	}
	if out := c.mustRun("find", "b"); strings.Contains(out, "two") {
		t.Errorf("b still holds the deleted document: %q", out)
	}

	// Unknown handles and bad arguments fail with a non-zero exit.
	_, err = c.run("commit-xtx", "no-such-handle")
	if err == nil || reasonOf(t, err) != xtxapi.ReasonNotFound {
		t.Errorf("unknown handle: got %v, want %s", err, xtxapi.ReasonNotFound)
	}
	if _, err := c.run("xtx-get", xtx, "a", "not-a-number"); err == nil {
		t.Error("non-numeric id accepted")
	}
	if _, err := c.run("begin-xtx"); err == nil {
		t.Error("begin-xtx with no collection accepted")
	}
	if out := c.mustRun("xtx-status", "never-seen"); out != "UNKNOWN" {
		t.Errorf("status of an unknown ref: got %q, want UNKNOWN", out)
	}
}

func TestCLI_XTx_AuthAndScope(t *testing.T) {
	c := newTestCLI(t,
		auth.Key{Key: "rw", Name: "writer", Scope: auth.ScopeReadWrite},
		auth.Key{Key: "ro", Name: "reader", Scope: auth.ScopeRead},
	)
	c.apiKey = "rw"
	c.mustRun("create-collection", "a")
	xtx := c.mustRun("begin-xtx", "a", "--key", "k")

	c.apiKey = ""
	for _, args := range [][]string{
		{"begin-xtx", "a"}, {"xtx-insert", xtx, "a", `{"v":1}`}, {"xtx-get", xtx, "a", "1"},
		{"commit-xtx", xtx}, {"rollback-xtx", xtx}, {"xtx-status", "k"},
	} {
		if _, err := c.run(args...); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s without a key: got %v, want Unauthenticated", args[0], err)
		}
	}

	c.apiKey = "ro"
	for _, args := range [][]string{
		{"begin-xtx", "a"}, {"xtx-insert", xtx, "a", `{"v":1}`}, {"xtx-get", xtx, "a", "1"},
		{"commit-xtx", xtx}, {"rollback-xtx", xtx},
	} {
		if _, err := c.run(args...); status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s with a read-only key: got %v, want PermissionDenied", args[0], err)
		}
	}
	// The status lookup is a read.
	if out := c.mustRun("xtx-status", "k"); out != "PENDING" {
		t.Errorf("xtx-status with a read-only key: got %q, want PENDING", out)
	}
}

// TestCLI_XTx_OutcomeUnknownGuidance: an unknown outcome cannot be provoked on
// a healthy server, so the rendering is checked against the status the server
// sends for it.
func TestCLI_XTx_OutcomeUnknownGuidance(t *testing.T) {
	st, err := status.New(codes.Unknown, "outcome unknown").WithDetails(&errdetails.ErrorInfo{
		Reason: xtxapi.ReasonOutcomeUnknown,
		Domain: xtxapi.Domain,
		Metadata: map[string]string{
			xtxapi.MetaRetrySafe: "false",
			xtxapi.MetaStatusRef: "order-7",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	msg := xtxCommitError(st.Err(), "handle-1").Error()
	for _, want := range []string{xtxapi.ReasonOutcomeUnknown, "retry safe: false", "Do NOT retry", "scriva-cli xtx-status order-7"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message lacks %q:\n%s", want, msg)
		}
	}

	// A commit whose answer never arrived gets the same guidance.
	lost := xtxCommitError(status.Error(codes.Unavailable, "connection reset"), "handle-1")
	if !strings.Contains(lost.Error(), "Do NOT retry") || !strings.Contains(lost.Error(), "xtx-status handle-1") {
		t.Errorf("lost-response message: %s", lost)
	}
	if status.Code(errors.Unwrap(lost)) != codes.Unavailable {
		t.Errorf("lost-response error does not wrap the status: %v", lost)
	}
	// A definite rejection is passed through untouched.
	denied := status.Error(codes.PermissionDenied, "no")
	if got := xtxCommitError(denied, "handle-1"); !errors.Is(got, denied) || got.Error() != denied.Error() {
		t.Errorf("definite rejection was rewritten: %v", got)
	}
}

func TestCLI_XTx_REPL(t *testing.T) {
	c := newTestCLI(t)
	c.mustRun("create-collection", "a")
	flags := &cliFlags{host: c.host, socket: c.socket}
	active := "a" // xtx commands name their collections; the active one is ignored

	for _, line := range []string{
		"begin-xtx a --key repl-1",
		"xtx-status repl-1",
	} {
		if err := handleREPLLine(line, &active, flags); err != nil {
			t.Errorf("REPL %q: %v", line, err)
		}
	}
	if out := c.mustRun("xtx-status", "repl-1"); out != "PENDING" {
		t.Errorf("transaction begun from the REPL: got %q, want PENDING", out)
	}
	for _, line := range []string{"begin-xtx", "xtx-insert h a", "commit-xtx", "xtx-status"} {
		if err := handleREPLLine(line, &active, flags); err == nil {
			t.Errorf("REPL %q: missing arguments accepted", line)
		}
	}
}

// TestCLI_XTx_HelpMakesNoSerializableClaim keeps the help text honest: the
// guarantee is optimistic point-read/write validation, without phantom
// protection.
func TestCLI_XTx_HelpMakesNoSerializableClaim(t *testing.T) {
	seen := 0
	for _, cmd := range rootCmd().Commands() {
		if !strings.Contains(cmd.Name(), "xtx") {
			continue
		}
		seen++
		help := strings.ToLower(cmd.Short + "\n" + cmd.Long)
		for _, banned := range []string{"serializab", "snapshot isolation"} {
			if strings.Contains(help, banned) {
				t.Errorf("%s help claims %q", cmd.Name(), banned)
			}
		}
	}
	if seen != 8 {
		t.Errorf("found %d xtx commands registered in rootCmd, want 8", seen)
	}
	if !strings.Contains(xtxIsolationHelp, "no phantom protection") {
		t.Error("the isolation help must state that there is no phantom protection")
	}
}
