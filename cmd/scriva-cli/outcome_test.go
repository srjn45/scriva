package main

import (
	"bytes"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
)

// newFaultCLI is newTestCLI over a SyncModeAlways engine whose active-segment
// fsync can be failed on demand.
func newFaultCLI(t *testing.T, segMax int64) (*testCLI, *engine.DB, *engine.FaultInjector) {
	t.Helper()
	fi := engine.NewFaultInjector()
	db, err := engine.Open(t.TempDir(), fi.Install(engine.CollectionConfig{
		SegmentMaxSize:  segMax,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.99,
		SyncMode:        engine.SyncModeAlways,
	}))
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	srv := grpc.NewServer()
	pb.RegisterScrivaServer(srv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return &testCLI{t: t, host: lis.Addr().String(), socket: filepath.Join(t.TempDir(), "absent.sock")}, db, fi
}

// runErr is run that also returns what the command printed to stderr.
func (c *testCLI) runErr(args ...string) (stdout, stderr string, err error) {
	c.t.Helper()
	root := rootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"--host", c.host, "--socket", c.socket, "--api-key", c.apiKey}, args...))
	err = root.Execute()
	return out.String(), errOut.String(), err
}

func collection(t *testing.T, db *engine.DB, name string) *engine.Collection {
	t.Helper()
	col, err := db.Collection(name)
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	return col
}

// TestCLI_WriteOutcomes: an unknown outcome, a fenced collection and an
// ordinary error each get their own message and exit code.
func TestCLI_WriteOutcomes(t *testing.T) {
	c, db, fi := newFaultCLI(t, 4<<20)
	c.mustRun("create-collection", "c")
	c.mustRun("insert", "c", `{"n":1}`)

	// Ordinary error: nothing applied, exit 1.
	_, err := c.run("get", "c", "999")
	if err == nil || exitCode(err) != exitError {
		t.Fatalf("missing record: err %v, exit %d, want %d", err, exitCode(err), exitError)
	}

	fi.FailSync(collection(t, db, "c"), 1, nil)
	_, err = c.run("insert", "c", `{"n":2}`)
	if err == nil || exitCode(err) != exitOutcomeUnknown {
		t.Fatalf("insert with failed fsync: err %v, exit %d, want %d", err, exitCode(err), exitOutcomeUnknown)
	}
	for _, want := range []string{"WRITE_OUTCOME_UNKNOWN", "Do NOT blind-retry", `collection "c" is now fenced`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("unknown-outcome message lacks %q:\n%s", want, err)
		}
	}

	for _, args := range [][]string{{"insert", "c", `{"n":3}`}, {"get", "c", "1"}, {"update-by-key", "c", "k", `{"n":4}`}} {
		_, err = c.run(args...)
		if err == nil || exitCode(err) != exitFenced {
			t.Fatalf("%v on a fenced collection: err %v, exit %d, want %d", args, err, exitCode(err), exitFenced)
		}
		for _, want := range []string{"COLLECTION_FENCED", "Nothing was applied", "until the server is restarted"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v: fenced message lacks %q:\n%s", args, want, err)
			}
		}
	}
}

// TestCLI_PostCommitWarning: a committed write whose rotation failed succeeds
// (exit 0, normal output) and prints a warning to stderr.
func TestCLI_PostCommitWarning(t *testing.T) {
	c, db, fi := newFaultCLI(t, 1)
	c.mustRun("create-collection", "c")

	fi.FailSync(collection(t, db, "c"), 2, nil) // commit sync ok, seal sync fails
	out, stderr, err := c.runErr("insert", "c", `{"n":1}`)
	if err != nil {
		t.Fatalf("insert: %v, want success", err)
	}
	if !strings.HasPrefix(out, "inserted id:") {
		t.Errorf("stdout = %q, want the normal insert line", out)
	}
	for _, want := range []string{"warning: the write committed", "rotate", "do not retry"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr lacks %q:\n%s", want, stderr)
		}
	}
}
