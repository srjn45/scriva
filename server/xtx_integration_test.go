//nolint:errcheck
package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/auth"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/internal/xtxapi"
	"github.com/srjn45/scriva/server"
)

// Cross-collection transaction (XTx) transport tests. Like the rest of the
// integration suite they run a real in-process gRPC server over a real engine.

// xtxInfo returns the ErrorInfo detail of a transaction error, failing the
// test when the status does not carry one.
func xtxInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			if info.Domain != xtxapi.Domain {
				t.Fatalf("ErrorInfo domain = %q, want %q", info.Domain, xtxapi.Domain)
			}
			return info
		}
	}
	t.Fatalf("error carries no ErrorInfo detail: %v", err)
	return nil
}

// wantXTxErr asserts the gRPC code and machine-readable reason of err and
// returns its ErrorInfo.
func wantXTxErr(t *testing.T, err error, code codes.Code, reason string) *errdetails.ErrorInfo {
	t.Helper()
	if err == nil {
		t.Fatalf("got no error, want %v / %s", code, reason)
	}
	if got := status.Code(err); got != code {
		t.Fatalf("code = %v, want %v (%v)", got, code, err)
	}
	info := xtxInfo(t, err)
	if info.Reason != reason {
		t.Fatalf("reason = %q, want %q (%v)", info.Reason, reason, err)
	}
	return info
}

// xtxSeed creates the two participant collections and one account document.
func xtxSeed(t *testing.T, c pb.ScrivaClient, cx context.Context) uint64 {
	t.Helper()
	for _, name := range []string{"accounts", "ledger"} {
		if _, err := c.CreateCollection(cx, &pb.CreateCollectionRequest{Name: name}); err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
	}
	ir, err := c.Insert(cx, &pb.InsertRequest{Collection: "accounts", Data: mustStruct(t, map[string]any{"balance": 100})})
	if err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	return ir.Id
}

func xtxBegin(t *testing.T, c pb.ScrivaClient, key string, collections ...string) string {
	t.Helper()
	b, err := c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: collections, IdempotencyKey: key})
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	if b.XtxId == "" {
		t.Fatal("BeginXTx returned an empty xtx_id")
	}
	return b.XtxId
}

func xtxStage(t *testing.T, c pb.ScrivaClient, xtx, collection string, op pb.XTxOpKind, id uint64, data map[string]any) uint64 {
	t.Helper()
	req := &pb.StageXTxRequest{XtxId: xtx, Collection: collection, Op: op, Id: id}
	if data != nil {
		req.Data = mustStruct(t, data)
	}
	r, err := c.StageXTx(ctx(), req)
	if err != nil {
		t.Fatalf("StageXTx(%s %s): %v", op, collection, err)
	}
	return r.Id
}

// countRecords returns how many live records a collection holds.
func countRecords(t *testing.T, c pb.ScrivaClient, collection string) int {
	t.Helper()
	stream, err := c.Find(ctx(), &pb.FindRequest{Collection: collection})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	return len(collectFind(t, stream))
}

func TestIntegration_XTx_CommitIsAtomicAndIdempotent(t *testing.T) {
	c := newTestServer(t)
	acct := xtxSeed(t, c, ctx())

	b, err := c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"ledger", "accounts"}, IdempotencyKey: "transfer-1"})
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	if len(b.Collections) != 2 || b.Collections[0] != "accounts" || b.Collections[1] != "ledger" || b.IdempotencyKey != "transfer-1" {
		t.Fatalf("BeginXTx response = %v", b)
	}
	xtx := b.XtxId

	xtxStage(t, c, xtx, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 60})
	entry := xtxStage(t, c, xtx, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -40})

	// Read-your-writes through the transaction.
	g, err := c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: xtx, Collection: "accounts", Id: acct})
	if err != nil || g.Record.Data.AsMap()["balance"] != 60.0 {
		t.Fatalf("GetXTx(accounts) = %v, %v", g, err)
	}
	g, err = c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: xtx, Collection: "ledger", Id: entry})
	if err != nil || g.Record.Data.AsMap()["delta"] != -40.0 || g.Record.Id != entry {
		t.Fatalf("GetXTx(ledger) = %v, %v", g, err)
	}

	// Nothing is visible outside the transaction before commit.
	out, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil || out.Record.Data.AsMap()["balance"] != 100.0 {
		t.Fatalf("staged update leaked before commit: %v, %v", out, err)
	}
	if n := countRecords(t, c, "ledger"); n != 0 {
		t.Fatalf("staged insert leaked before commit: %d ledger records", n)
	}
	st, err := c.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: "transfer-1"})
	if err != nil || st.Status != pb.XTxState_XTX_STATUS_PENDING {
		t.Fatalf("status before commit = %v, %v", st, err)
	}

	res, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtx})
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}
	if res.TxId == "" || len(res.Ops) != 2 || res.Replayed {
		t.Fatalf("CommitXTx response = %v", res)
	}
	for _, op := range res.Ops {
		if op.Rev == 0 || op.Op == pb.XTxOpKind_XTX_OP_UNSPECIFIED {
			t.Fatalf("op result = %v", op)
		}
	}

	// Both collections changed together.
	out, err = c.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil || out.Record.Data.AsMap()["balance"] != 60.0 {
		t.Fatalf("committed update: %v, %v", out, err)
	}
	out, err = c.FindById(ctx(), &pb.FindByIdRequest{Collection: "ledger", Id: entry})
	if err != nil || out.Record.Data.AsMap()["delta"] != -40.0 {
		t.Fatalf("committed insert: %v, %v", out, err)
	}

	// Status lookup by idempotency key, coordinator tx_id and handle id.
	for _, ref := range []string{"transfer-1", res.TxId, xtx} {
		st, err := c.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: ref})
		if err != nil || st.Status != pb.XTxState_XTX_STATUS_COMMITTED || st.TxId != res.TxId {
			t.Fatalf("XTxStatus(%q) = %v, %v", ref, st, err)
		}
	}

	// A retried CommitXTx on the same handle returns the original outcome.
	again, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtx})
	if err != nil || again.TxId != res.TxId || len(again.Ops) != 2 {
		t.Fatalf("repeated CommitXTx = %v, %v", again, err)
	}

	// A new transaction under the same idempotency key replays the original
	// outcome and applies none of its own writes.
	retry := xtxBegin(t, c, "transfer-1", "accounts", "ledger")
	xtxStage(t, c, retry, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 20})
	xtxStage(t, c, retry, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -40})
	replay, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: retry})
	if err != nil || !replay.Replayed || replay.TxId != res.TxId {
		t.Fatalf("replayed CommitXTx = %v, %v", replay, err)
	}
	out, _ = c.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if out.Record.Data.AsMap()["balance"] != 60.0 {
		t.Fatalf("replay applied its update: %v", out)
	}
	if n := countRecords(t, c, "ledger"); n != 1 {
		t.Fatalf("replay applied its insert: %d ledger records", n)
	}

	// A committed transaction cannot be rolled back.
	_, err = c.RollbackXTx(ctx(), &pb.RollbackXTxRequest{XtxId: xtx})
	wantXTxErr(t, err, codes.FailedPrecondition, xtxapi.ReasonFinished)
}

func TestIntegration_XTx_RollbackLeavesNothing(t *testing.T) {
	c := newTestServer(t)
	acct := xtxSeed(t, c, ctx())

	xtx := xtxBegin(t, c, "rb-1", "accounts", "ledger")
	xtxStage(t, c, xtx, "accounts", pb.XTxOpKind_XTX_OP_DELETE, acct, nil)
	xtxStage(t, c, xtx, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": 1})

	// The staged delete hides the document inside the transaction only.
	_, err := c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: xtx, Collection: "accounts", Id: acct})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonDocNotFound)

	if r, err := c.RollbackXTx(ctx(), &pb.RollbackXTxRequest{XtxId: xtx}); err != nil || !r.Ok {
		t.Fatalf("RollbackXTx = %v, %v", r, err)
	}
	// Rollback is idempotent.
	if _, err := c.RollbackXTx(ctx(), &pb.RollbackXTxRequest{XtxId: xtx}); err != nil {
		t.Fatalf("second RollbackXTx: %v", err)
	}

	if _, err := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct}); err != nil {
		t.Fatalf("rolled back delete was applied: %v", err)
	}
	if n := countRecords(t, c, "ledger"); n != 0 {
		t.Fatalf("rolled back insert was applied: %d ledger records", n)
	}
	st, err := c.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: "rb-1"})
	if err != nil || st.Status != pb.XTxState_XTX_STATUS_ABORTED {
		t.Fatalf("status after rollback = %v, %v", st, err)
	}

	// The handle is finished: no more staging, no commit.
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: xtx, Collection: "ledger", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: mustStruct(t, map[string]any{})})
	wantXTxErr(t, err, codes.FailedPrecondition, xtxapi.ReasonFinished)
	_, err = c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtx})
	wantXTxErr(t, err, codes.FailedPrecondition, xtxapi.ReasonFinished)
}

func TestIntegration_XTx_ConflictIsTypedAndAppliesNothing(t *testing.T) {
	c := newTestServer(t)
	acct := xtxSeed(t, c, ctx())

	// Read conflict: the transaction reads a document that then changes.
	xtx := xtxBegin(t, c, "", "accounts", "ledger")
	if _, err := c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: xtx, Collection: "accounts", Id: acct}); err != nil {
		t.Fatalf("GetXTx: %v", err)
	}
	xtxStage(t, c, xtx, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -1})
	if _, err := c.Update(ctx(), &pb.UpdateRequest{Collection: "accounts", Id: acct, Data: mustStruct(t, map[string]any{"balance": 5})}); err != nil {
		t.Fatalf("concurrent Update: %v", err)
	}

	_, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtx})
	info := wantXTxErr(t, err, codes.Aborted, xtxapi.ReasonConflict)
	if info.Metadata[xtxapi.MetaRetrySafe] != "true" {
		t.Fatalf("conflict retry_safe = %q, want true", info.Metadata[xtxapi.MetaRetrySafe])
	}
	if info.Metadata[xtxapi.MetaConflictCollection] != "accounts" ||
		info.Metadata[xtxapi.MetaConflictID] != strconv.FormatUint(acct, 10) ||
		info.Metadata[xtxapi.MetaConflictKind] != "read" ||
		info.Metadata[xtxapi.MetaConflictExpectedRev] != "1" ||
		info.Metadata[xtxapi.MetaConflictActualRev] != "2" {
		t.Fatalf("conflict metadata = %v", info.Metadata)
	}
	if n := countRecords(t, c, "ledger"); n != 0 {
		t.Fatalf("conflicting transaction left partial state: %d ledger records", n)
	}
	st, err := c.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: xtx})
	if err != nil || st.Status != pb.XTxState_XTX_STATUS_ABORTED {
		t.Fatalf("status after conflict = %v, %v", st, err)
	}

	// Write-write conflict: two transactions update the same document; the
	// second to commit loses and applies neither of its writes.
	first := xtxBegin(t, c, "", "accounts", "ledger")
	second := xtxBegin(t, c, "", "accounts", "ledger")
	xtxStage(t, c, first, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 1})
	xtxStage(t, c, first, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"by": "first"})
	xtxStage(t, c, second, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 2})
	xtxStage(t, c, second, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"by": "second"})
	if _, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: first}); err != nil {
		t.Fatalf("first CommitXTx: %v", err)
	}
	_, err = c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: second})
	info = wantXTxErr(t, err, codes.Aborted, xtxapi.ReasonConflict)
	if info.Metadata[xtxapi.MetaConflictKind] != "write" {
		t.Fatalf("conflict kind = %q, want write", info.Metadata[xtxapi.MetaConflictKind])
	}
	out, _ := c.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if out.Record.Data.AsMap()["balance"] != 1.0 {
		t.Fatalf("losing transaction changed the account: %v", out)
	}
	if n := countRecords(t, c, "ledger"); n != 1 {
		t.Fatalf("ledger holds %d records, want only the winner's", n)
	}

	// A stale expected_rev is rejected at stage time and leaves the handle open.
	third := xtxBegin(t, c, "", "accounts")
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{
		XtxId: third, Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_UPDATE, Id: acct,
		Data: mustStruct(t, map[string]any{"balance": 9}), ExpectedRev: 1,
	})
	wantXTxErr(t, err, codes.Aborted, xtxapi.ReasonConflict)
	xtxStage(t, c, third, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 9})
	if _, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: third}); err != nil {
		t.Fatalf("CommitXTx after a rejected stage: %v", err)
	}
}

func TestIntegration_XTx_ScansAreRejected(t *testing.T) {
	c := newTestServer(t)
	acct := xtxSeed(t, c, ctx())
	xtx := xtxBegin(t, c, "", "accounts")
	inXTx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(xtxapi.HeaderXTxID, xtx))

	// A scan, an aggregation and a change feed tagged with the transaction are
	// refused: nothing validates predicate reads at commit.
	find, err := c.Find(inXTx, &pb.FindRequest{Collection: "accounts"})
	if err == nil {
		_, err = find.Recv()
	}
	wantXTxErr(t, err, codes.Unimplemented, xtxapi.ReasonScanUnsupported)

	agg, err := c.Aggregate(inXTx, &pb.AggregateRequest{Collection: "accounts"})
	if err == nil {
		_, err = agg.Recv()
	}
	wantXTxErr(t, err, codes.Unimplemented, xtxapi.ReasonScanUnsupported)

	watch, err := c.Watch(inXTx, &pb.WatchRequest{Collection: "accounts"})
	if err == nil {
		_, err = watch.Recv()
	}
	wantXTxErr(t, err, codes.Unimplemented, xtxapi.ReasonScanUnsupported)

	// A scan of a collection the transaction did not declare.
	find, err = c.Find(inXTx, &pb.FindRequest{Collection: "ledger"})
	if err == nil {
		_, err = find.Recv()
	}
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonNotParticipant)

	// A scan tagged with an unknown transaction is refused too, never run.
	bogus := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(xtxapi.HeaderXTxID, "nope"))
	find, err = c.Find(bogus, &pb.FindRequest{Collection: "accounts"})
	if err == nil {
		_, err = find.Recv()
	}
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonNotFound)

	// Ordinary data RPCs tagged with the transaction never run outside it.
	_, err = c.Insert(inXTx, &pb.InsertRequest{Collection: "accounts", Data: mustStruct(t, map[string]any{"x": 1})})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.Update(inXTx, &pb.UpdateRequest{Collection: "accounts", Id: acct, Data: mustStruct(t, map[string]any{"x": 1})})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.Delete(inXTx, &pb.DeleteRequest{Collection: "accounts", Id: acct})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.FindById(inXTx, &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	if n := countRecords(t, c, "accounts"); n != 1 {
		t.Fatalf("a tagged write ran outside the transaction: %d accounts", n)
	}

	// The same RPCs without the header are untouched.
	if n := countRecords(t, c, "accounts"); n != 1 {
		t.Fatalf("plain Find = %d records", n)
	}
}

func TestIntegration_XTx_TypedErrors(t *testing.T) {
	c := newTestServer(t)
	acct := xtxSeed(t, c, ctx())
	xtx := xtxBegin(t, c, "", "accounts")
	data := mustStruct(t, map[string]any{"x": 1})

	// Begin.
	_, err := c.BeginXTx(ctx(), &pb.BeginXTxRequest{})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"accounts", "missing"}})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonCollectionNotFound)
	_, err = c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"xtx.journal"}})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"accounts"}, IdempotencyKey: string(make([]byte, 200))})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonTooLarge)
	_, err = c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"accounts"}, IdleTimeoutMs: -1})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"accounts"}, MaxLifetimeMs: -1})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)

	// Stage.
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: xtx, Collection: "ledger", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonNotParticipant)
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: xtx, Collection: "accounts", Data: data})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: xtx, Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_INSERT})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: xtx, Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_UPDATE, Id: 9999, Data: data})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonDocNotFound)
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: "nope", Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonNotFound)
	_, err = c.StageXTx(ctx(), &pb.StageXTxRequest{Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)

	// Get.
	_, err = c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: xtx, Collection: "ledger", Id: 1})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonNotParticipant)
	_, err = c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: xtx, Collection: "accounts", Id: 9999})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonDocNotFound)
	_, err = c.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: "nope", Collection: "accounts", Id: acct})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonNotFound)

	// Commit / rollback / status.
	_, err = c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: "nope"})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonNotFound)
	_, err = c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtx, LockTimeoutMs: -1})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	_, err = c.RollbackXTx(ctx(), &pb.RollbackXTxRequest{XtxId: "nope"})
	wantXTxErr(t, err, codes.NotFound, xtxapi.ReasonNotFound)
	_, err = c.XTxStatus(ctx(), &pb.XTxStatusRequest{})
	wantXTxErr(t, err, codes.InvalidArgument, xtxapi.ReasonInvalid)
	st, err := c.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: "never-seen"})
	if err != nil || st.Status != pb.XTxState_XTX_STATUS_UNKNOWN {
		t.Fatalf("XTxStatus(unknown ref) = %v, %v", st, err)
	}

	// None of the rejections above broke the handle.
	xtxStage(t, c, xtx, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 3})
	if _, err := c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtx, LockTimeoutMs: 5000}); err != nil {
		t.Fatalf("CommitXTx after rejected requests: %v", err)
	}

	// An idle handle expires; nothing it staged is written.
	b, err := c.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"accounts"}, IdleTimeoutMs: 20})
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	xtxStage(t, c, b.XtxId, "accounts", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"x": 1})
	time.Sleep(80 * time.Millisecond)
	_, err = c.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: b.XtxId})
	if status.Code(err) != codes.FailedPrecondition && status.Code(err) != codes.NotFound {
		t.Fatalf("commit of an expired handle = %v, want FAILED_PRECONDITION or NOT_FOUND", err)
	}
	if r := xtxInfo(t, err).Reason; r != xtxapi.ReasonExpired && r != xtxapi.ReasonNotFound {
		t.Fatalf("commit of an expired handle: reason %q", r)
	}
	if n := countRecords(t, c, "accounts"); n != 1 {
		t.Fatalf("expired transaction wrote: %d accounts", n)
	}
}

func TestIntegration_XTx_AuthAndScopes(t *testing.T) {
	keys := []auth.Key{
		{Key: "admin", Name: "admin", Scope: auth.ScopeReadWrite},
		{Key: "reader", Name: "reader", Scope: auth.ScopeRead},
		{Key: "scoped", Name: "app", Scope: auth.ScopeReadWrite, Collections: []string{"accounts"}},
	}
	c := pb.NewScrivaClient(newInstrumentedServer(t, nil, keys, nil))
	acct := xtxSeed(t, c, keyCtx("admin"))
	data := mustStruct(t, map[string]any{"x": 1})

	b, err := c.BeginXTx(keyCtx("admin"), &pb.BeginXTxRequest{Collections: []string{"accounts", "ledger"}, IdempotencyKey: "auth-1"})
	if err != nil {
		t.Fatalf("admin BeginXTx: %v", err)
	}
	xtx := b.XtxId

	// Every XTx RPC, as a function of the caller's context.
	calls := map[string]func(context.Context) error{
		"BeginXTx": func(cx context.Context) error {
			_, err := c.BeginXTx(cx, &pb.BeginXTxRequest{Collections: []string{"accounts"}})
			return err
		},
		"StageXTx": func(cx context.Context) error {
			_, err := c.StageXTx(cx, &pb.StageXTxRequest{XtxId: xtx, Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data})
			return err
		},
		"GetXTx": func(cx context.Context) error {
			_, err := c.GetXTx(cx, &pb.GetXTxRequest{XtxId: xtx, Collection: "accounts", Id: acct})
			return err
		},
		"CommitXTx": func(cx context.Context) error {
			_, err := c.CommitXTx(cx, &pb.CommitXTxRequest{XtxId: xtx})
			return err
		},
		"RollbackXTx": func(cx context.Context) error {
			_, err := c.RollbackXTx(cx, &pb.RollbackXTxRequest{XtxId: xtx})
			return err
		},
		"XTxStatus": func(cx context.Context) error {
			_, err := c.XTxStatus(cx, &pb.XTxStatusRequest{Ref: "auth-1"})
			return err
		},
	}

	// No RPC bypasses API-key auth.
	for name, call := range calls {
		if got := status.Code(call(context.Background())); got != codes.Unauthenticated {
			t.Errorf("%s without a key: code %v, want Unauthenticated", name, got)
		}
		if got := status.Code(call(keyCtx("wrong"))); got != codes.Unauthenticated {
			t.Errorf("%s with a wrong key: code %v, want Unauthenticated", name, got)
		}
	}

	// A read-only key may look an outcome up and nothing else — including the
	// transactional point read, which is classified as a write.
	for name, call := range calls {
		got := status.Code(call(keyCtx("reader")))
		want := codes.PermissionDenied
		if name == "XTxStatus" {
			want = codes.OK
		}
		if got != want {
			t.Errorf("%s with a read-only key: code %v, want %v", name, got, want)
		}
	}

	// A collection-scoped key cannot open a transaction that reaches outside
	// its allow-list, nor stage or read there through someone else's handle.
	if _, err := c.BeginXTx(keyCtx("scoped"), &pb.BeginXTxRequest{Collections: []string{"accounts", "ledger"}}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("scoped BeginXTx over a forbidden collection: %v, want PermissionDenied", err)
	}
	if _, err := c.StageXTx(keyCtx("scoped"), &pb.StageXTxRequest{XtxId: xtx, Collection: "ledger", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("scoped StageXTx on a forbidden collection: %v, want PermissionDenied", err)
	}
	if _, err := c.GetXTx(keyCtx("scoped"), &pb.GetXTxRequest{XtxId: xtx, Collection: "ledger", Id: 1}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("scoped GetXTx on a forbidden collection: %v, want PermissionDenied", err)
	}
	// Inside its allow-list the scoped key runs a whole transaction.
	sb, err := c.BeginXTx(keyCtx("scoped"), &pb.BeginXTxRequest{Collections: []string{"accounts"}})
	if err != nil {
		t.Fatalf("scoped BeginXTx: %v", err)
	}
	if _, err := c.StageXTx(keyCtx("scoped"), &pb.StageXTxRequest{XtxId: sb.XtxId, Collection: "accounts", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data}); err != nil {
		t.Fatalf("scoped StageXTx: %v", err)
	}
	if _, err := c.CommitXTx(keyCtx("scoped"), &pb.CommitXTxRequest{XtxId: sb.XtxId}); err != nil {
		t.Fatalf("scoped CommitXTx: %v", err)
	}

	// The admin's transaction was untouched by all the denied calls.
	if _, err := c.CommitXTx(keyCtx("admin"), &pb.CommitXTxRequest{XtxId: xtx}); err != nil {
		t.Fatalf("admin CommitXTx: %v", err)
	}
}

func TestIntegration_XTx_ReadOnlyFollower(t *testing.T) {
	t.Parallel()
	fdb, err := engine.Open(t.TempDir(), followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	t.Cleanup(func() { fdb.Close() })
	if _, err := fdb.CreateCollection("c"); err != nil {
		t.Fatalf("seed collection: %v", err)
	}
	ro := startReadOnlyReplica(t, fdb)
	data := mustStruct(t, map[string]any{"x": 1})

	// The whole handle lifecycle is a write on a follower.
	writes := map[string]error{
		"BeginXTx":    mustErr(ro.BeginXTx(ctx(), &pb.BeginXTxRequest{Collections: []string{"c"}})),
		"StageXTx":    mustErr(ro.StageXTx(ctx(), &pb.StageXTxRequest{XtxId: "x", Collection: "c", Op: pb.XTxOpKind_XTX_OP_INSERT, Data: data})),
		"GetXTx":      mustErr(ro.GetXTx(ctx(), &pb.GetXTxRequest{XtxId: "x", Collection: "c", Id: 1})),
		"CommitXTx":   mustErr(ro.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: "x"})),
		"RollbackXTx": mustErr(ro.RollbackXTx(ctx(), &pb.RollbackXTxRequest{XtxId: "x"})),
	}
	for name, err := range writes {
		st := status.Convert(err)
		if st.Code() != codes.FailedPrecondition || st.Message() != server.ReadOnlyReplicaMessage {
			t.Errorf("%s on a follower = %v, want FailedPrecondition %q", name, err, server.ReadOnlyReplicaMessage)
		}
	}

	// Outcome lookup is a read and stays available.
	st, err := ro.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: "anything"})
	if err != nil || st.Status != pb.XTxState_XTX_STATUS_UNKNOWN {
		t.Fatalf("XTxStatus on a follower = %v, %v", st, err)
	}
}

// ---- REST ------------------------------------------------------------------

// newXTxREST serves the REST gateway in front of a fresh in-process gRPC
// server and returns its base URL plus a gRPC client to the same server.
func newXTxREST(t *testing.T) (string, pb.ScrivaClient) {
	t.Helper()
	conn := newInstrumentedServer(t, nil, nil, nil)
	gwCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h, err := server.NewRESTGateway(gwCtx, conn.Target(), insecure.NewCredentials(), nil)
	if err != nil {
		t.Fatalf("NewRESTGateway: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL, pb.NewScrivaClient(conn)
}

// restCall issues one JSON request and decodes the JSON response body.
func restCall(t *testing.T, method, url string, body any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: non-JSON body %q", method, url, raw)
		}
	}
	return resp.StatusCode, out
}

// restReason extracts the ErrorInfo reason of a REST error body.
func restReason(t *testing.T, body map[string]any) (reason string, meta map[string]any) {
	t.Helper()
	// A server-streaming route (Find) wraps the status in {"error": ...}.
	if wrapped, ok := body["error"].(map[string]any); ok {
		body = wrapped
	}
	details, _ := body["details"].([]any)
	for _, d := range details {
		if m, ok := d.(map[string]any); ok && m["reason"] != nil {
			meta, _ = m["metadata"].(map[string]any)
			return m["reason"].(string), meta
		}
	}
	t.Fatalf("REST error body carries no ErrorInfo: %v", body)
	return "", nil
}

func TestIntegration_XTx_REST(t *testing.T) {
	base, c := newXTxREST(t)
	acct := xtxSeed(t, c, ctx())
	acctStr := strconv.FormatUint(acct, 10)

	// begin
	code, body := restCall(t, http.MethodPost, base+"/v1/xtx", map[string]any{
		"collections": []string{"accounts", "ledger"}, "idempotency_key": "rest-1",
	}, nil)
	if code != http.StatusOK {
		t.Fatalf("POST /v1/xtx = %d %v", code, body)
	}
	xtx, _ := body["xtxId"].(string)
	if xtx == "" {
		t.Fatalf("begin response = %v", body)
	}

	// stage an update and an insert
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx/"+xtx+"/ops", map[string]any{
		"collection": "accounts", "op": "XTX_OP_UPDATE", "id": acctStr, "data": map[string]any{"balance": 60},
	}, nil)
	if code != http.StatusOK {
		t.Fatalf("stage update = %d %v", code, body)
	}
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx/"+xtx+"/ops", map[string]any{
		"collection": "ledger", "op": "XTX_OP_INSERT", "data": map[string]any{"delta": -40},
	}, nil)
	if code != http.StatusOK {
		t.Fatalf("stage insert = %d %v", code, body)
	}
	entry, _ := body["id"].(string)

	// transactional point read sees the staged write
	code, body = restCall(t, http.MethodGet, base+"/v1/xtx/"+xtx+"/records/accounts/"+acctStr, nil, nil)
	if code != http.StatusOK {
		t.Fatalf("GET xtx record = %d %v", code, body)
	}
	if rec, _ := body["record"].(map[string]any); rec == nil || rec["data"].(map[string]any)["balance"] != 60.0 {
		t.Fatalf("xtx record = %v", body)
	}
	// ... while the plain REST read still sees the committed value
	code, body = restCall(t, http.MethodGet, base+"/v1/accounts/records/"+acctStr, nil, nil)
	if code != http.StatusOK || body["record"].(map[string]any)["data"].(map[string]any)["balance"] != 100.0 {
		t.Fatalf("plain read before commit = %d %v", code, body)
	}

	// a scan tagged with the transaction is rejected over REST as well
	code, body = restCall(t, http.MethodPost, base+"/v1/accounts/records/find", map[string]any{}, map[string]string{xtxapi.HeaderXTxID: xtx})
	if reason, _ := restReason(t, body); code != http.StatusNotImplemented || reason != xtxapi.ReasonScanUnsupported {
		t.Fatalf("tagged REST scan = %d %v", code, body)
	}

	// status before commit
	code, body = restCall(t, http.MethodGet, base+"/v1/xtx/status?ref=rest-1", nil, nil)
	if code != http.StatusOK || body["status"] != "XTX_STATUS_PENDING" {
		t.Fatalf("status before commit = %d %v", code, body)
	}

	// commit
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx/"+xtx+"/commit", map[string]any{}, nil)
	if code != http.StatusOK {
		t.Fatalf("commit = %d %v", code, body)
	}
	txID, _ := body["txId"].(string)
	if ops, _ := body["ops"].([]any); txID == "" || len(ops) != 2 {
		t.Fatalf("commit response = %v", body)
	}

	// status by key and by tx_id
	for _, ref := range []string{"rest-1", txID} {
		code, body = restCall(t, http.MethodGet, base+"/v1/xtx/status?ref="+ref, nil, nil)
		if code != http.StatusOK || body["status"] != "XTX_STATUS_COMMITTED" || body["txId"] != txID {
			t.Fatalf("status(%s) = %d %v", ref, code, body)
		}
	}

	// both writes are visible through the plain REST API
	code, body = restCall(t, http.MethodGet, base+"/v1/accounts/records/"+acctStr, nil, nil)
	if code != http.StatusOK || body["record"].(map[string]any)["data"].(map[string]any)["balance"] != 60.0 {
		t.Fatalf("account after commit = %d %v", code, body)
	}
	code, body = restCall(t, http.MethodGet, base+"/v1/ledger/records/"+entry, nil, nil)
	if code != http.StatusOK {
		t.Fatalf("ledger entry after commit = %d %v", code, body)
	}

	// rollback of a second transaction, and typed error bodies
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx", map[string]any{"collections": []string{"accounts"}}, nil)
	if code != http.StatusOK {
		t.Fatalf("second begin = %d %v", code, body)
	}
	second := body["xtxId"].(string)
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx/"+second+"/ops", map[string]any{
		"collection": "accounts", "op": "XTX_OP_UPDATE", "id": acctStr, "expected_rev": "1", "data": map[string]any{"balance": 0},
	}, nil)
	reason, meta := restReason(t, body)
	if code != http.StatusConflict || reason != xtxapi.ReasonConflict || meta[xtxapi.MetaRetrySafe] != "true" {
		t.Fatalf("stale expected_rev over REST = %d %v", code, body)
	}
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx/"+second+"/rollback", map[string]any{}, nil)
	if code != http.StatusOK || body["ok"] != true {
		t.Fatalf("rollback = %d %v", code, body)
	}
	code, body = restCall(t, http.MethodPost, base+"/v1/xtx/nope/commit", map[string]any{}, nil)
	if reason, _ := restReason(t, body); code != http.StatusNotFound || reason != xtxapi.ReasonNotFound {
		t.Fatalf("commit of an unknown handle over REST = %d %v", code, body)
	}
}
