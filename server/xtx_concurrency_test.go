package server_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/internal/xtxapi"
	"github.com/srjn45/scriva/server"
)

func newTestServerWithDB(t *testing.T, db *engine.DB) pb.ScrivaClient {
	t.Helper()

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	grpcSrv := grpc.NewServer()
	pb.RegisterScrivaServer(grpcSrv, gs)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewScrivaClient(conn)
}

// Scenario 4 (transport level): Client disconnects before locks are acquired.
// Scenario 4 (transport level): Client disconnects before locks are acquired.
// Retry succeeds and transaction is applied exactly once.
func TestIntegration_XTx_RetryDisconnectBeforeLocks(t *testing.T) {
	var hookHit sync.WaitGroup
	var hookOnce sync.Once
	var canceled atomic.Bool

	cfg := server.DefaultConfig().EngineConfig()
	engine.SetTestXTxHook(&cfg, func(point engine.XTxHookPoint, key, txid string) {
		if key == "disc-before-locks" && point == engine.XTxHookBeforeLocks {
			hookOnce.Do(func() {
				hookHit.Done()
				for !canceled.Load() {
					time.Sleep(2 * time.Millisecond)
				}
				time.Sleep(20 * time.Millisecond)
			})
		}
	})

	dir := t.TempDir()
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	client := newTestServerWithDB(t, db)
	acct := xtxSeed(t, client, ctx())

	xtxID := xtxBegin(t, client, "disc-before-locks", "accounts", "ledger")
	xtxStage(t, client, xtxID, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 50})
	xtxStage(t, client, xtxID, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -50})

	// Start CommitXTx with a context that will be canceled once hook is hit
	hookHit.Add(1)
	cancCtx, cancel := context.WithCancel(ctx())

	var commitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, commitErr = client.CommitXTx(cancCtx, &pb.CommitXTxRequest{XtxId: xtxID})
	}()

	hookHit.Wait()
	cancel()
	canceled.Store(true)
	wg.Wait()
	time.Sleep(30 * time.Millisecond) // ensure server handler finishes releasing in-flight key

	if commitErr == nil {
		t.Fatal("expected CommitXTx to fail due to context cancellation")
	}

	// Document not updated yet
	out, err := client.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil {
		t.Fatal(err)
	}
	if out.Record.Data.AsMap()["balance"] != 100.0 {
		t.Fatalf("uncommitted update leaked: %+v", out.Record.Data.AsMap())
	}

	// Retry CommitXTx with fresh context -> succeeds
	res, err := client.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtxID})
	if err != nil {
		t.Fatalf("retry CommitXTx failed: %v", err)
	}
	if res.TxId == "" {
		t.Fatal("expected valid txid")
	}

	// State applied exactly once
	outAfter, err := client.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil {
		t.Fatal(err)
	}
	if outAfter.Record.Data.AsMap()["balance"] != 50.0 {
		t.Fatalf("account balance = %v, want 50.0", outAfter.Record.Data.AsMap()["balance"])
	}
	if countRecords(t, client, "ledger") != 1 {
		t.Fatalf("ledger record count = %d, want 1", countRecords(t, client, "ledger"))
	}
}

// Scenario 4 (transport level): Client disconnects after commit is durable on disk.
// Status lookup and retry by idempotency key return the original outcome; applied once.
func TestIntegration_XTx_RetryDisconnectAfterDurableCommit(t *testing.T) {
	var hookHit sync.WaitGroup
	var hookOnce sync.Once
	var hookRelease = make(chan struct{})

	cfg := server.DefaultConfig().EngineConfig()
	engine.SetTestXTxHook(&cfg, func(point engine.XTxHookPoint, key, txid string) {
		if key == "disc-after-durable" && point == engine.XTxHookApplied {
			hookOnce.Do(func() {
				hookHit.Done()
				<-hookRelease
			})
		}
	})

	dir := t.TempDir()
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	client := newTestServerWithDB(t, db)
	acct := xtxSeed(t, client, ctx())

	xtxID := xtxBegin(t, client, "disc-after-durable", "accounts", "ledger")
	xtxStage(t, client, xtxID, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 25})
	xtxStage(t, client, xtxID, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -75})

	hookHit.Add(1)
	cancCtx, cancel := context.WithCancel(ctx())

	var commitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, commitErr = client.CommitXTx(cancCtx, &pb.CommitXTxRequest{XtxId: xtxID})
	}()

	hookHit.Wait()
	// Cancel caller context right when hook reaches applied
	cancel()
	close(hookRelease)
	wg.Wait()
	time.Sleep(20 * time.Millisecond)

	if commitErr == nil {
		t.Fatal("expected caller context to observe cancellation")
	}

	// Query status by idempotency key
	st, err := client.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: "disc-after-durable"})
	if err != nil {
		t.Fatalf("XTxStatus: %v", err)
	}
	if st.Status != pb.XTxState_XTX_STATUS_COMMITTED || st.TxId == "" {
		t.Fatalf("unexpected status: %+v", st)
	}

	// Calling CommitXTx on the same committed handle returns the original outcome
	replayRes, err := client.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtxID})
	if err != nil {
		t.Fatalf("CommitXTx on same handle failed: %v", err)
	}
	if replayRes.TxId != st.TxId {
		t.Fatalf("response mismatch: %+v, want TxId=%s", replayRes, st.TxId)
	}

	// Retrying via a new transaction with the same idempotency key returns Replayed: true
	xtxID2 := xtxBegin(t, client, "disc-after-durable", "accounts", "ledger")
	xtxStage(t, client, xtxID2, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 25})
	xtxStage(t, client, xtxID2, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -75})
	replayRes2, err := client.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtxID2})
	if err != nil {
		t.Fatalf("replay CommitXTx on new handle failed: %v", err)
	}
	if !replayRes2.Replayed || replayRes2.TxId != st.TxId {
		t.Fatalf("replay response mismatch: %+v, want TxId=%s, Replayed=true", replayRes2, st.TxId)
	}

	// Applied exactly once
	out, err := client.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil || out.Record.Data.AsMap()["balance"] != 25.0 {
		t.Fatalf("accounts balance = %v, want 25.0", out.Record.Data.AsMap()["balance"])
	}
	if countRecords(t, client, "ledger") != 1 {
		t.Fatalf("ledger records count = %d, want 1", countRecords(t, client, "ledger"))
	}
}

// Scenario 6 (transport level): No visibility before commit over gRPC.
// Readers over gRPC see none of the staged writes until commit finishes.
func TestIntegration_XTx_NoVisibilityBeforeCommit_GRPC(t *testing.T) {
	var hookHit atomic.Int64
	var hookProceed = make(chan struct{})

	cfg := server.DefaultConfig().EngineConfig()
	engine.SetTestXTxHook(&cfg, func(point engine.XTxHookPoint, key, txid string) {
		if key == "grpc-vis-tx" {
			hookHit.Store(int64(point))
			if point == engine.XTxHookDecided {
				<-hookProceed
			}
		}
	})

	dir := t.TempDir()
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	client := newTestServerWithDB(t, db)
	acct := xtxSeed(t, client, ctx())

	xtxID := xtxBegin(t, client, "grpc-vis-tx", "accounts", "ledger")
	xtxStage(t, client, xtxID, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 10})
	ledgerID := xtxStage(t, client, xtxID, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -90})

	// Pre-commit reader check over gRPC
	rBefore, err := client.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil || rBefore.Record.Data.AsMap()["balance"] != 100.0 {
		t.Fatalf("pre-commit leaked: %+v", rBefore)
	}
	if countRecords(t, client, "ledger") != 0 {
		t.Fatal("ledger record visible before commit")
	}

	var commitRes *pb.CommitXTxResponse
	var commitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		commitRes, commitErr = client.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtxID})
	}()

	for hookHit.Load() < int64(engine.XTxHookDecided) {
		time.Sleep(5 * time.Millisecond)
	}

	// Unblock commit
	close(hookProceed)
	wg.Wait()

	if commitErr != nil {
		t.Fatalf("commit failed: %v", commitErr)
	}
	if commitRes.TxId == "" {
		t.Fatal("expected non-empty TxId")
	}

	// Post-commit: both collections updated atomically
	rAfter, err := client.FindById(ctx(), &pb.FindByIdRequest{Collection: "accounts", Id: acct})
	if err != nil || rAfter.Record.Data.AsMap()["balance"] != 10.0 {
		t.Fatalf("accounts not updated: %+v", rAfter)
	}
	rLedger, err := client.FindById(ctx(), &pb.FindByIdRequest{Collection: "ledger", Id: ledgerID})
	if err != nil || rLedger.Record.Data.AsMap()["delta"] != -90.0 {
		t.Fatalf("ledger not visible: %+v", rLedger)
	}
}

// Scenario 7: ErrXTxOutcomeUnknown end-to-end over gRPC transport.
// Provoked by fault injection at coordinator journal sync; asserts XTX_OUTCOME_UNKNOWN
// and resolution via XTxStatus.
func TestIntegration_XTx_OutcomeUnknown_EndToEnd(t *testing.T) {
	ffs := engine.NewFaultFS()
	cfg := server.DefaultConfig().EngineConfig()
	ffs.Wrap(&cfg)

	dir := t.TempDir()
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	client := newTestServerWithDB(t, db)
	acct := xtxSeed(t, client, ctx())

	xtxID := xtxBegin(t, client, "tx-outcome-unknown-key", "accounts", "ledger")
	xtxStage(t, client, xtxID, "accounts", pb.XTxOpKind_XTX_OP_UPDATE, acct, map[string]any{"balance": 33})
	xtxStage(t, client, xtxID, "ledger", pb.XTxOpKind_XTX_OP_INSERT, 0, map[string]any{"delta": -67})

	// Pre-ensure coordinator journal exists before arming fault for S4 sync
	if _, err := engine.EnsureDBXTxJournal(db); err != nil {
		t.Fatal(err)
	}

	// Inject EIO on coordinator journal sync (sync #3: accounts sync #1, ledger sync #2, journal sync #3)
	ffs.FailSyncAt(ffs.Count("sync")+3, syscall.EIO)

	_, err = client.CommitXTx(ctx(), &pb.CommitXTxRequest{XtxId: xtxID})
	if err == nil {
		t.Fatal("expected CommitXTx to fail with outcome unknown")
	}

	info := wantXTxErr(t, err, codes.Unknown, xtxapi.ReasonOutcomeUnknown)
	if info.Metadata[xtxapi.MetaRetrySafe] != "false" {
		t.Errorf("outcome unknown must not be retry safe: %v", info.Metadata)
	}
	ref := info.Metadata[xtxapi.MetaStatusRef]
	if ref == "" {
		t.Fatal("expected non-empty status_ref in metadata")
	}

	// Query outcome via XTxStatus
	st, err := client.XTxStatus(ctx(), &pb.XTxStatusRequest{Ref: ref})
	if err != nil {
		t.Fatalf("XTxStatus failed: %v", err)
	}
	// Status should be resolved (unknown/aborted/committed per recovery)
	if st == nil {
		t.Fatal("expected non-nil status response")
	}
}
