//nolint:errcheck
package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/metrics"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
	"github.com/srjn45/scriva/store"
)

// ---- helpers ---------------------------------------------------------------

// serveDB serves db over an in-process gRPC server and returns a client plus a
// stop func that tears the server down (not the DB).
func serveDB(t *testing.T, db *engine.DB) (pb.ScrivaClient, func()) {
	t.Helper()
	gs := server.NewGRPCServer(db, 5*time.Minute)
	srv := grpc.NewServer()
	pb.RegisterScrivaServer(srv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			conn.Close()
			srv.Stop()
			gs.Close()
		})
	}
	t.Cleanup(stop)
	return pb.NewScrivaClient(conn), stop
}

func openDB(t *testing.T, dir string, cfg engine.CollectionConfig) *engine.DB {
	t.Helper()
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatalf("engine.Open(%s): %v", dir, err)
	}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { db.Close() }) })
	return db
}

// copyDataDir snapshots a live data directory: the crash image a power cut at
// this instant would leave behind.
func copyDataDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		if strings.HasSuffix(d.Name(), ".lock") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
}

func replFollowerCfg() engine.CollectionConfig {
	cfg := followerEngineConfig()
	// A broker on the follower lets a promoted node assign LSNs.
	cfg.ReplicationRingSize = 1024
	return cfg
}

// runFollower starts a follower apply loop. stop cancels it and returns Run's
// result; done is closed when Run has returned and result() then reports it.
func runFollower(fdb *engine.DB, leader pb.ScrivaClient) (stop func() error, done <-chan struct{}, result func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	fin := make(chan struct{})
	var res error
	go func() {
		res = server.NewFollower(fdb, leader, "f", "", discardLogger()).Run(ctx)
		close(fin)
	}()
	stop = func() error {
		cancel()
		<-fin
		return res
	}
	return stop, fin, func() error { <-fin; return res }
}

// seedPair creates xa/xb on the leader with one seed record in xa.
func seedPair(t *testing.T, leader pb.ScrivaClient) {
	t.Helper()
	mustCreate(t, leader, "xa")
	mustCreate(t, leader, "xb")
	mustInsert(t, leader, "xa", map[string]any{"seed": true})
}

// assertAtomic checks the all-or-nothing invariant for the commitCrossTx
// transactions: xa holds the seed plus one record per tx, xb one per tx.
func assertAtomic(t *testing.T, db *engine.DB, txs int) {
	t.Helper()
	na, nb := len(recState(t, db, "xa")), len(recState(t, db, "xb"))
	if na != 1+txs || nb != txs {
		t.Fatalf("atomicity: xa=%d xb=%d, want xa=%d xb=%d", na, nb, 1+txs, txs)
	}
}

// ---- leader crash during commit -------------------------------------------

// A transaction whose COMMIT is durable on the leader but which crashed before
// being published to the broker is committed by leader startup recovery. It is
// never republished, so a caught-up follower must be forced to re-bootstrap
// (FAILED_PRECONDITION) rather than silently resume past it.
func TestFailover_LeaderCrashAfterCommitForcesResync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A clean close writes the exact LSN, the worst case for the watermark.
	db0 := openDB(t, dir, leaderEngineConfig())
	for _, n := range []string{"xa", "xb"} {
		if _, err := db0.CreateCollection(n); err != nil {
			t.Fatal(err)
		}
	}
	xa, _ := db0.Collection("xa")
	if _, _, err := xa.Insert(map[string]any{"seed": true}); err != nil {
		t.Fatal(err)
	}
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	}

	image := filepath.Join(t.TempDir(), "image")
	var once sync.Once
	cfg := leaderEngineConfig()
	engine.SetTestXTxHook(&cfg, func(p engine.XTxHookPoint, key, _ string) {
		if p == engine.XTxHookDecided && key == "crash-after-commit" {
			once.Do(func() { copyDataDir(t, dir, image) })
		}
	})
	ldb := openDB(t, dir, cfg)
	lclient, _ := serveDB(t, ldb)

	// A caught-up follower at the leader's pre-transaction watermark.
	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), lclient, fdir)
	if err != nil {
		t.Fatal(err)
	}
	fdb := openDB(t, fdir, followerEngineConfig())
	fdb.SetAppliedLSN(wm)

	commitCrossTx(t, ldb, "crash-after-commit")

	// "Crash": the image is what the disk held between COMMIT and publish.
	rdb := openDB(t, image, leaderEngineConfig())
	assertAtomic(t, rdb, 1) // recovery committed it: both sides present
	rclient, _ := serveDB(t, rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = server.NewFollower(fdb, rclient, "f", "", discardLogger()).Run(ctx)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("caught-up follower must be forced to re-bootstrap, got %v", err)
	}
	if got := len(recState(t, fdb, "xb")); got != 0 {
		t.Fatalf("follower exposed %d records of the unpublished tx", got)
	}

	// Re-bootstrapping delivers the whole recovered transaction.
	f2dir := t.TempDir()
	wm2, err := server.Bootstrap(context.Background(), rclient, f2dir)
	if err != nil {
		t.Fatal(err)
	}
	f2 := openDB(t, f2dir, followerEngineConfig())
	f2.SetAppliedLSN(wm2)
	assertAtomic(t, f2, 1)
	if wm2 < wm {
		t.Fatalf("LSN went backwards across crash recovery: %d -> %d", wm, wm2)
	}
}

// A transaction interrupted before the journal decision is presumed aborted:
// no node ever shows any part of it.
func TestFailover_LeaderCrashBeforeCommitNeverVisible(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	image := filepath.Join(t.TempDir(), "image")
	var once sync.Once
	cfg := leaderEngineConfig()
	engine.SetTestXTxHook(&cfg, func(p engine.XTxHookPoint, key, _ string) {
		if p == engine.XTxHookPrepared && key == "crash-before-commit" {
			once.Do(func() { copyDataDir(t, dir, image) })
		}
	})
	ldb := openDB(t, dir, cfg)
	lclient, _ := serveDB(t, ldb)
	seedPair(t, lclient)

	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), lclient, fdir)
	if err != nil {
		t.Fatal(err)
	}
	fdb := openDB(t, fdir, followerEngineConfig())
	fdb.SetAppliedLSN(wm)

	commitCrossTx(t, ldb, "crash-before-commit")

	rdb := openDB(t, image, leaderEngineConfig())
	assertAtomic(t, rdb, 0)
	rclient, _ := serveDB(t, rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Resume or forced resync are both acceptable; exposing part of the tx is not.
	_ = server.NewFollower(fdb, rclient, "f", "", discardLogger()).Run(ctx)
	assertAtomic(t, fdb, 0)
	assertAtomic(t, rdb, 0)
}

// ---- follower crash during apply ------------------------------------------

func TestFailover_FollowerCrashMatrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		at   engine.XTxHookPoint
		// bound: the COMMIT was durable, so recovery must restore the tx and
		// its LSN.
		bound bool
	}{
		{"before_prepare", engine.XTxHookLocked, false},
		{"after_prepare", engine.XTxHookPrepared, false},
		{"after_commit_decision", engine.XTxHookDecided, true},
		{"after_writes_before_lsn_persist", engine.XTxHookReplApplied, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			leader, ldb := startLeader(t)
			seedPair(t, leader)
			dir := t.TempDir()
			wm, err := server.Bootstrap(context.Background(), leader, dir)
			if err != nil {
				t.Fatal(err)
			}
			image := filepath.Join(t.TempDir(), "image")
			var once sync.Once
			cfg := followerEngineConfig()
			engine.SetTestXTxHook(&cfg, func(p engine.XTxHookPoint, key, _ string) {
				if p == tc.at && key == "victim" {
					once.Do(func() { copyDataDir(t, dir, image) })
				}
			})
			fdb := openDB(t, dir, cfg)
			fdb.SetAppliedLSN(wm)
			stop, _, _ := runFollower(fdb, leader)
			commitCrossTx(t, ldb, "victim")
			waitAllConverged(t, ldb, fdb, "xa", "xb")
			stop()
			txLSN := ldb.CurrentLSN()

			// Restart from the crash image.
			idb := openDB(t, image, followerEngineConfig())
			if tc.bound {
				assertAtomic(t, idb, 1)
				if idb.AppliedLSN() != txLSN {
					t.Fatalf("recovery must restore applied LSN to the bound LSN %d, got %d", txLSN, idb.AppliedLSN())
				}
			} else {
				assertAtomic(t, idb, 0)
				if idb.AppliedLSN() >= txLSN {
					t.Fatalf("applied LSN %d covers an uncommitted tx (lsn %d)", idb.AppliedLSN(), txLSN)
				}
			}

			// Resume: the envelope is (re)delivered or skipped; either way the
			// result is the leader's state, once.
			stop2, _, _ := runFollower(idb, leader)
			defer stop2()
			waitAllConverged(t, ldb, idb, "xa", "xb")
			assertAtomic(t, idb, 1)
			if idb.AppliedLSN() != ldb.CurrentLSN() {
				t.Fatalf("applied %d != leader %d", idb.AppliedLSN(), ldb.CurrentLSN())
			}
		})
	}
}

// ---- disconnect / retry ----------------------------------------------------

// flakyProxy forwards TCP to a target and can sever every connection and refuse
// new ones, to simulate a network partition.
type flakyProxy struct {
	lis    net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
	down   bool
}

func newFlakyProxy(t *testing.T, target string) *flakyProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &flakyProxy{lis: lis, target: target}
	t.Cleanup(func() { lis.Close(); p.Partition(true) })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.down {
				p.mu.Unlock()
				c.Close()
				continue
			}
			u, err := net.Dial("tcp", p.target)
			if err != nil {
				p.mu.Unlock()
				c.Close()
				continue
			}
			p.conns = append(p.conns, c, u)
			p.mu.Unlock()
			go func() { io.Copy(u, c); u.Close() }()
			go func() { io.Copy(c, u); c.Close() }()
		}
	}()
	return p
}

// Partition(true) severs every connection and refuses new ones; false heals.
func (p *flakyProxy) Partition(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, c := range p.conns {
			c.Close()
		}
		p.conns = nil
	}
}

func TestFailover_DisconnectMidStreamCatchesUp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ldb := openDB(t, dir, leaderEngineConfig())
	gs := server.NewGRPCServer(ldb, 5*time.Minute)
	t.Cleanup(gs.Close)
	srv := grpc.NewServer()
	pb.RegisterScrivaServer(srv, gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	proxy := newFlakyProxy(t, lis.Addr().String())

	direct, _ := serveDB(t, ldb)
	seedPair(t, direct)

	conn, err := grpc.NewClient(proxy.lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	viaProxy := pb.NewScrivaClient(conn)

	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), viaProxy, fdir)
	if err != nil {
		t.Fatal(err)
	}
	fdb := openDB(t, fdir, followerEngineConfig())
	fdb.SetAppliedLSN(wm)
	stop, _, _ := runFollower(fdb, viaProxy)
	defer stop()

	for i := 0; i < 3; i++ {
		commitCrossTx(t, ldb, fmt.Sprintf("pre%d", i))
	}
	waitAllConverged(t, ldb, fdb, "xa", "xb")

	proxy.Partition(true)
	for i := 0; i < 4; i++ { // leader keeps committing while the follower is cut off
		commitCrossTx(t, ldb, fmt.Sprintf("during%d", i))
	}
	time.Sleep(100 * time.Millisecond)
	proxy.Partition(false)

	waitAllConverged(t, ldb, fdb, "xa", "xb")
	assertAtomic(t, fdb, 7)
	if fdb.AppliedLSN() != ldb.CurrentLSN() {
		t.Fatalf("applied %d != leader %d", fdb.AppliedLSN(), ldb.CurrentLSN())
	}
}

// captureEnvelope reads the transaction envelope the leader ships at lsn.
func captureEnvelope(t *testing.T, leader pb.ScrivaClient, lsn uint64) *pb.ReplicationRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := leader.Replicate(ctx, &pb.ReplicateRequest{
		FromLsn: lsn - 1, FollowerId: "capture",
		ProtocolVersions: []uint32{engine.ReplicationProtocolV1, engine.ReplicationProtocolV2},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := rs.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Lsn != lsn || rec.Tx == nil {
		t.Fatalf("want envelope at %d, got %+v", lsn, rec)
	}
	return rec
}

func envelopeOf(t *testing.T, rec *pb.ReplicationRecord, mutate func(*pb.ReplicationTx)) *engine.TxEnvelope {
	t.Helper()
	tx := rec.Tx
	if mutate != nil {
		mutate(tx)
	}
	var ops []engine.TxEnvelopeOp
	for _, o := range tx.Ops {
		e := store.Entry{ID: o.Id, Op: store.Op(strings.ToLower(strings.TrimPrefix(o.Op.String(), "REP_"))), Rev: o.Rev, ExpiresAt: o.ExpiresAt, Epoch: o.Epoch}
		if o.Ts != nil {
			e.Ts = o.Ts.AsTime()
		}
		if o.Data != nil {
			e.Data = o.Data.AsMap()
		}
		ops = append(ops, engine.TxEnvelopeOp{Collection: o.Collection, Entry: e})
	}
	env, err := engine.NewTxEnvelope(tx.TxId, tx.Key, ops)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// Redelivery of an applied transaction is a no-op that does not move or
// duplicate anything; the same identity with different content is the typed
// divergence error.
func TestFailover_RedeliveryIdempotentAndDivergenceTyped(t *testing.T) {
	t.Parallel()
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	fdb, stop := bootstrapFollower(t, leader)
	defer stop()
	commitCrossTx(t, ldb, "redeliver")
	waitAllConverged(t, ldb, fdb, "xa", "xb")
	lsn := ldb.CurrentLSN()

	rec := captureEnvelope(t, leader, lsn)
	env := envelopeOf(t, rec, nil)
	before := recState(t, fdb, "xb")
	applied, err := fdb.ApplyReplicatedTx(lsn, env)
	if err != nil || applied {
		t.Fatalf("redelivery: applied=%v err=%v, want no-op", applied, err)
	}
	if fdb.AppliedLSN() != lsn {
		t.Fatalf("redelivery moved the applied LSN: %d", fdb.AppliedLSN())
	}
	assertAtomic(t, fdb, 1)
	if fmt.Sprint(before) != fmt.Sprint(recState(t, fdb, "xb")) {
		t.Fatal("redelivery changed state")
	}

	rec2 := captureEnvelope(t, leader, lsn)
	diff := envelopeOf(t, rec2, func(tx *pb.ReplicationTx) {
		tx.Ops[0].Data.Fields["n"].Kind = tx.Ops[1].Data.Fields["n"].Kind
		tx.Ops[0].Data.Fields["extra"] = tx.Ops[1].Data.Fields["n"]
	})
	if _, err := fdb.ApplyReplicatedTx(lsn, diff); !errors.Is(err, engine.ErrReplicationDivergence) {
		t.Fatalf("want ErrReplicationDivergence, got %v", err)
	}
	assertAtomic(t, fdb, 1)
}

// fakeLeader serves a scripted Replicate feed.
type fakeLeader struct {
	pb.UnimplementedScrivaServer
	leaderLSN uint64
	records   []*pb.ReplicationRecord
}

func (f *fakeLeader) ReplicationStatus(context.Context, *pb.ReplicationStatusRequest) (*pb.ReplicationStatusResponse, error) {
	return &pb.ReplicationStatusResponse{LeaderLsn: f.leaderLSN}, nil
}

func (f *fakeLeader) Replicate(_ *pb.ReplicateRequest, s pb.Scriva_ReplicateServer) error {
	for _, r := range f.records {
		if err := s.Send(r); err != nil {
			return err
		}
	}
	<-s.Context().Done()
	return nil
}

// A stream that re-presents an applied transaction (same leader txid) under a
// new LSN with different content stops the follower with the typed error and
// leaves state and the applied LSN untouched.
func TestFailover_DivergentStreamStopsFollowerTyped(t *testing.T) {
	t.Parallel()
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	fdb, stop := bootstrapFollower(t, leader)
	defer stop()
	commitCrossTx(t, ldb, "orig")
	waitAllConverged(t, ldb, fdb, "xa", "xb")
	lsn := ldb.CurrentLSN()
	good := captureEnvelope(t, leader, lsn)

	bad := captureEnvelope(t, leader, lsn)
	bad.Lsn = lsn + 1
	bad.Tx.Ops[0].Data.Fields["n"].Kind = bad.Tx.Ops[1].Data.Fields["n"].Kind
	// Recompute a valid digest for the doctored body so only divergence trips.
	env := envelopeOf(t, bad, nil)
	bad.Tx.Digest = env.Digest[:]

	fl := &fakeLeader{leaderLSN: lsn + 1, records: []*pb.ReplicationRecord{bad}}
	_ = good
	client, _ := func() (pb.ScrivaClient, func()) {
		srv := grpc.NewServer()
		pb.RegisterScrivaServer(srv, fl)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return pb.NewScrivaClient(conn), nil
	}()

	stop() // halt the real follower loop; reuse its DB directly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fdb2 := fdb
	err := server.NewFollower(fdb2, client, "f", "", discardLogger()).Run(ctx)
	if !errors.Is(err, server.ErrEnvelopeApplyFailed) || !errors.Is(err, engine.ErrReplicationDivergence) {
		t.Fatalf("want ErrEnvelopeApplyFailed wrapping ErrReplicationDivergence, got %v", err)
	}
	if fdb.AppliedLSN() != lsn {
		t.Fatalf("applied LSN advanced past a diverged tx: %d", fdb.AppliedLSN())
	}
	assertAtomic(t, fdb, 1)
}

// ---- ring overflow / snapshot bootstrap -----------------------------------

func TestFailover_RingOverflowRebootstrapsAtomically(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := leaderEngineConfig()
	cfg.ReplicationRingSize = 8 // each tx weighs 2: the ring holds only a few
	ldb := openDB(t, dir, cfg)
	leader, _ := serveDB(t, ldb)
	seedPair(t, leader)

	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, fdir)
	if err != nil {
		t.Fatal(err)
	}
	fdb := openDB(t, fdir, followerEngineConfig())
	fdb.SetAppliedLSN(wm)

	for i := 0; i < 20; i++ { // follower is offline: the ring overflows
		commitCrossTx(t, ldb, fmt.Sprintf("o%d", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = server.NewFollower(fdb, leader, "f", "", discardLogger()).Run(ctx)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition (re-bootstrap), got %v", err)
	}
	assertAtomic(t, fdb, 0) // nothing partial from the failed catch-up

	f2dir := t.TempDir()
	wm2, err := server.Bootstrap(context.Background(), leader, f2dir)
	if err != nil {
		t.Fatal(err)
	}
	f2 := openDB(t, f2dir, followerEngineConfig())
	f2.SetAppliedLSN(wm2)
	assertAtomic(t, f2, 20)
	stop, _, _ := runFollower(f2, leader)
	defer stop()
	commitCrossTx(t, ldb, "after")
	waitAllConverged(t, ldb, f2, "xa", "xb")
	assertAtomic(t, f2, 21)
	if f2.AppliedLSN() != ldb.CurrentLSN() {
		t.Fatalf("applied %d != leader %d", f2.AppliedLSN(), ldb.CurrentLSN())
	}
}

// The bootstrap watermark is read before the snapshot, so envelopes already in
// the snapshot are redelivered; they must be no-ops, not duplicates.
func TestFailover_SnapshotOverlapRedeliveryIsNoop(t *testing.T) {
	t.Parallel()
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	for i := 0; i < 5; i++ {
		commitCrossTx(t, ldb, fmt.Sprintf("s%d", i))
	}
	fdir := t.TempDir()
	wm, err := server.Bootstrap(context.Background(), leader, fdir)
	if err != nil {
		t.Fatal(err)
	}
	fdb := openDB(t, fdir, followerEngineConfig())
	fdb.SetAppliedLSN(wm - 3) // the last 3 txs are in the snapshot AND in the stream
	stop, _, _ := runFollower(fdb, leader)
	defer stop()
	waitAllConverged(t, ldb, fdb, "xa", "xb")
	assertAtomic(t, fdb, 5)
	if fdb.AppliedLSN() != wm {
		t.Fatalf("applied %d, want %d", fdb.AppliedLSN(), wm)
	}
}

// ---- promotion -------------------------------------------------------------

func TestFailover_PromoteWhileCaughtUpContinuesLSNs(t *testing.T) {
	t.Parallel()
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	dir := t.TempDir()
	wm, _ := server.Bootstrap(context.Background(), leader, dir)
	fdb := openDB(t, dir, replFollowerCfg())
	fdb.SetAppliedLSN(wm)
	stop, _, _ := runFollower(fdb, leader)
	for i := 0; i < 3; i++ {
		commitCrossTx(t, ldb, fmt.Sprintf("p%d", i))
	}
	waitAllConverged(t, ldb, fdb, "xa", "xb")
	stop()
	want := ldb.CurrentLSN()

	res, err := fdb.Promote(0, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.LSN != want || fdb.CurrentLSN() != want {
		t.Fatalf("promoted at %d / current %d, want %d", res.LSN, fdb.CurrentLSN(), want)
	}
	commitCrossTx(t, fdb, "post-promote")
	if got := fdb.CurrentLSN(); got != want+1 {
		t.Fatalf("first promoted tx got LSN %d, want %d", got, want+1)
	}
	assertAtomic(t, fdb, 4)

	// The promoted leader's envelope is streamable to a new follower.
	pclient, _ := serveDB(t, fdb)
	f2dir := t.TempDir()
	f2wm, err := server.Bootstrap(context.Background(), pclient, f2dir)
	if err != nil {
		t.Fatal(err)
	}
	f2 := openDB(t, f2dir, followerEngineConfig())
	f2.SetAppliedLSN(f2wm - 1)
	stop2, _, _ := runFollower(f2, pclient)
	defer stop2()
	waitAllConverged(t, fdb, f2, "xa", "xb")
	assertAtomic(t, f2, 4)
}

// Promote waits for an in-flight transaction apply: the reported LSN covers the
// whole transaction, and while it is in flight neither the applied LSN nor the
// status accounting claims it. A later replicated apply is refused.
func TestFailover_PromoteWaitsForInflightApply(t *testing.T) {
	t.Parallel()
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	dir := t.TempDir()
	wm, _ := server.Bootstrap(context.Background(), leader, dir)

	inflight := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	cfg := replFollowerCfg()
	engine.SetTestXTxHook(&cfg, func(p engine.XTxHookPoint, key, _ string) {
		if p == engine.XTxHookPrepared && key == "inflight" {
			once.Do(func() { close(inflight); <-release })
		}
	})
	fdb := openDB(t, dir, cfg)
	fdb.SetAppliedLSN(wm)
	stop, done, runResult := runFollower(fdb, leader)
	defer stop()

	commitCrossTx(t, ldb, "inflight")
	txLSN := ldb.CurrentLSN()
	<-inflight

	// In flight: the leader LSN is known but the tx is not applied.
	deadline := time.Now().Add(5 * time.Second)
	for fdb.ReplicationLag() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fdb.AppliedLSN() != txLSN-1 || fdb.ReplicationLag() != 1 {
		t.Fatalf("in flight: applied=%d lag=%d, want %d / 1", fdb.AppliedLSN(), fdb.ReplicationLag(), txLSN-1)
	}

	type result struct {
		res engine.PromoteResult
		err error
	}
	rc := make(chan result, 1)
	go func() {
		r, err := fdb.Promote(5, false)
		rc <- result{r, err}
	}()
	select {
	case r := <-rc:
		t.Fatalf("Promote returned while an apply was in flight: %+v", r)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	r := <-rc
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.res.LSN != txLSN {
		t.Fatalf("promoted LSN %d, want %d (whole tx applied)", r.res.LSN, txLSN)
	}
	assertAtomic(t, fdb, 1)

	// After promotion further replicated transactions are refused.
	commitCrossTx(t, ldb, "late")
	select {
	case <-done:
		if err := runResult(); !errors.Is(err, engine.ErrReplicationPromoted) {
			t.Fatalf("want ErrReplicationPromoted, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("follower did not stop after promotion")
	}
	assertAtomic(t, fdb, 1)
	if fdb.AppliedLSN() != txLSN {
		t.Fatalf("applied %d after refused apply, want %d", fdb.AppliedLSN(), txLSN)
	}
}

// A follower that crashed after the COMMIT but before persisting its LSN, then
// is promoted straight after restart, owns the whole transaction and continues
// LSNs strictly above it.
func TestFailover_PromoteAfterCrashRecovery(t *testing.T) {
	t.Parallel()
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	dir := t.TempDir()
	wm, _ := server.Bootstrap(context.Background(), leader, dir)
	image := filepath.Join(t.TempDir(), "image")
	var once sync.Once
	cfg := replFollowerCfg()
	engine.SetTestXTxHook(&cfg, func(p engine.XTxHookPoint, key, _ string) {
		if p == engine.XTxHookReplApplied && key == "crashy" {
			once.Do(func() { copyDataDir(t, dir, image) })
		}
	})
	fdb := openDB(t, dir, cfg)
	fdb.SetAppliedLSN(wm)
	stop, _, _ := runFollower(fdb, leader)
	commitCrossTx(t, ldb, "crashy")
	waitAllConverged(t, ldb, fdb, "xa", "xb")
	stop()
	txLSN := ldb.CurrentLSN()

	idb := openDB(t, image, replFollowerCfg())
	res, err := idb.Promote(0, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.LSN != txLSN {
		t.Fatalf("promoted at %d, want bound LSN %d", res.LSN, txLSN)
	}
	assertAtomic(t, idb, 1)
	commitCrossTx(t, idb, "next")
	if got := idb.CurrentLSN(); got != txLSN+1 {
		t.Fatalf("next LSN %d, want %d", got, txLSN+1)
	}
	assertAtomic(t, idb, 2)
}

// ---- metrics ---------------------------------------------------------------

func TestFailover_ReplicatedTxMetrics(t *testing.T) {
	leader, ldb := startLeader(t)
	seedPair(t, leader)
	dir := t.TempDir()
	wm, _ := server.Bootstrap(context.Background(), leader, dir)
	fdb := openDB(t, dir, followerEngineConfig())
	fdb.SetAppliedLSN(wm)

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	m.RegisterReplicationGauges(fdb.AppliedLSN, fdb.ReplicationLag)

	ctx, cancel := context.WithCancel(context.Background())
	fol := server.NewFollower(fdb, leader, "f", "", discardLogger())
	fol.SetEnvelopeApplyObserver(m.ObserveReplTxApply)
	done := make(chan struct{})
	go func() { fol.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	for i := 0; i < 3; i++ {
		commitCrossTx(t, ldb, fmt.Sprintf("m%d", i))
	}
	waitAllConverged(t, ldb, fdb, "xa", "xb")

	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_replication_tx_apply_total{outcome="applied"} 3`,
		`scriva_replication_tx_apply_duration_seconds_count{outcome="applied"} 3`,
		`scriva_replication_tx_ops_total 6`,
		fmt.Sprintf(`scriva_replication_applied_lsn %d`, ldb.CurrentLSN()),
		`scriva_replication_lag_lsn 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_replication"))
		}
	}
}
