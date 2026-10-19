//nolint:errcheck
package server_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/auth"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/server"
)

const replKey = "repl-secret"

// secureLeader is a replication-enabled leader behind TLS (mTLS required) and
// API-key auth. restart() re-serves the same DB on the same address, modelling
// a leader bounce.
type secureLeader struct {
	t       *testing.T
	db      *engine.DB
	addr    string
	tlsCfg  *tls.Config
	authn   *auth.Authenticator
	srv     *grpc.Server
	gs      *server.GRPCServer
	ca      *testCA
	dir     string
	caPath  string
	clCert  string
	clKey   string
	clientC tls.Certificate
}

func startSecureLeader(t *testing.T) *secureLeader { return startSecureLeaderOpts(t, true) }

// startSecureLeaderOpts: mtls=false is TLS + API key only (a client cert is
// neither requested nor accepted as a credential).
func startSecureLeaderOpts(t *testing.T, mtls bool) *secureLeader {
	t.Helper()
	dir := t.TempDir()
	ca := newTestCA(t, "repl-ca")
	srvCert := ca.issue(t, "leader", x509.ExtKeyUsageServerAuth)
	clientCert := ca.issue(t, "follower", x509.ExtKeyUsageClientAuth)
	cert, key := writePEM(t, dir, "client", clientCert)
	db, err := engine.Open(filepath.Join(dir, "leader"), leaderEngineConfig())
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	authn, err := auth.New([]auth.Key{{Key: replKey, Name: "repl", Scope: auth.ScopeReadWrite}}, auth.WithCertAuth(mtls))
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	l := &secureLeader{
		t: t, db: db, ca: ca, dir: dir, authn: authn,
		caPath: writeCAPEM(t, dir, "ca", ca), clCert: cert, clKey: key, clientC: clientCert,
		tlsCfg: &tls.Config{
			Certificates: []tls.Certificate{srvCert},
			ClientCAs:    ca.pool(),
			ClientAuth:   tls.RequireAndVerifyClientCert,
		},
	}
	if !mtls {
		l.tlsCfg.ClientAuth = tls.NoClientCert
	}
	l.serve("127.0.0.1:0")
	t.Cleanup(func() { l.stop() })
	return l
}

func (l *secureLeader) serve(addr string) {
	l.t.Helper()
	au, as := l.authn.Interceptors()
	l.gs = server.NewGRPCServer(l.db, 5*time.Minute)
	l.srv = grpc.NewServer(grpc.Creds(credentials.NewTLS(l.tlsCfg)),
		grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
	pb.RegisterScrivaServer(l.srv, l.gs)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		l.t.Fatalf("listen: %v", err)
	}
	l.addr = lis.Addr().String()
	go l.srv.Serve(lis)
}

func (l *secureLeader) stop() {
	l.srv.Stop()
	l.gs.Close()
}

func (l *secureLeader) restart() {
	l.stop()
	l.serve(l.addr)
}

func (l *secureLeader) cfg() server.Config {
	c := server.DefaultConfig()
	c.ReplicateFrom = l.addr
	c.ReplicateCA = l.caPath
	c.ReplicateCert = l.clCert
	c.ReplicateKey = l.clKey
	c.ReplicateAPIKey = replKey
	return c
}

func dialRepl(t *testing.T, cfg server.Config) pb.ScrivaClient {
	t.Helper()
	rc, _, err := server.ReplicationTransport(cfg)
	if err != nil {
		t.Fatalf("ReplicationTransport: %v", err)
	}
	conn, err := grpc.NewClient(cfg.ReplicateFrom, grpc.WithTransportCredentials(rc.Creds))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewScrivaClient(conn)
}

func TestReplicationTransport_BootstrapAndTailOverMTLS(t *testing.T) {
	l := startSecureLeader(t)
	cfg := l.cfg()
	cl := dialRepl(t, cfg)
	ctx := server.ReplicationAuthContext(context.Background(), server.ReplicationAPIKey(cfg))
	if _, err := cl.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "c"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	mustInsertCtx(t, ctx, cl, "c", 1)

	dir := t.TempDir()
	wm, err := server.Bootstrap(ctx, cl, dir)
	if err != nil {
		t.Fatalf("bootstrap over mTLS: %v", err)
	}
	fdb, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer fdb.Close()
	fdb.SetAppliedLSN(wm)
	fctx, cancel := context.WithCancel(context.Background())
	fol := server.NewFollower(fdb, cl, "f1", server.ReplicationAPIKey(cfg), discardLogger())
	done := make(chan struct{})
	go func() { fol.Run(fctx); close(done) }()
	defer func() { cancel(); <-done }()

	mustInsertCtx(t, ctx, cl, "c", 2)
	waitConverged(t, l.db, fdb, "c")
	if got := len(scanState(t, fdb, "c")); got != 2 {
		t.Fatalf("follower has %d records, want 2", got)
	}
}

func TestReplicationTransport_ReconnectKeepsCredentials(t *testing.T) {
	l := startSecureLeader(t)
	cfg := l.cfg()
	cl := dialRepl(t, cfg)
	ctx := server.ReplicationAuthContext(context.Background(), replKey)
	cl.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "c"})
	mustInsertCtx(t, ctx, cl, "c", 1)

	dir := t.TempDir()
	wm, err := server.Bootstrap(ctx, cl, dir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	fdb, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer fdb.Close()
	fdb.SetAppliedLSN(wm)
	fctx, cancel := context.WithCancel(context.Background())
	fol := server.NewFollower(fdb, cl, "f1", replKey, discardLogger())
	fol.SetBackoff(20 * time.Millisecond)
	done := make(chan struct{})
	go func() { fol.Run(fctx); close(done) }()
	defer func() { cancel(); <-done }()

	mustInsertCtx(t, ctx, cl, "c", 2)
	waitConverged(t, l.db, fdb, "c")

	// Drop the leader, write while it is down is impossible, so bounce it and
	// keep writing: the follower must reconnect over mTLS + key and catch up.
	l.restart()
	var werr error
	for i := 0; i < 100; i++ {
		_, werr = cl.Insert(ctx, insertReq("c", 3))
		if werr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if werr != nil {
		t.Fatalf("insert after restart: %v", werr)
	}
	waitConverged(t, l.db, fdb, "c")
	if got := len(scanState(t, fdb, "c")); got != 3 {
		t.Fatalf("follower has %d records, want 3", got)
	}
}

func TestReplicationTransport_FailClosed(t *testing.T) {
	l := startSecureLeader(t)
	k := startSecureLeaderOpts(t, false)
	keyOnly := func() server.Config {
		c := k.cfg()
		c.ReplicateCert, c.ReplicateKey = "", ""
		return c
	}

	t.Run("missing API key", func(t *testing.T) {
		cl := dialRepl(t, keyOnly())
		dir := filepath.Join(t.TempDir(), "d")
		_, err := server.Bootstrap(context.Background(), cl, dir)
		if status.Code(errors.Unwrap(err)) != codes.Unauthenticated && status.Code(err) != codes.Unauthenticated {
			t.Fatalf("want Unauthenticated, got %v", err)
		}
		assertNoData(t, dir)
	})

	t.Run("wrong API key stops the follower", func(t *testing.T) {
		cl := dialRepl(t, keyOnly())
		fdb, err := engine.Open(t.TempDir(), followerEngineConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer fdb.Close()
		fol := server.NewFollower(fdb, cl, "f", "wrong", discardLogger())
		fol.SetBackoff(10 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = fol.Run(ctx)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("Run = %v, want Unauthenticated (fail closed, no retry loop)", err)
		}
	})

	t.Run("untrusted leader CA", func(t *testing.T) {
		other := newTestCA(t, "evil")
		cfg := l.cfg()
		cfg.ReplicateCA = writeCAPEM(t, t.TempDir(), "evil", other)
		cl := dialRepl(t, cfg)
		dir := filepath.Join(t.TempDir(), "d")
		ctx := server.ReplicationAuthContext(context.Background(), replKey)
		if _, err := server.Bootstrap(ctx, cl, dir); err == nil {
			t.Fatal("bootstrap succeeded against an untrusted CA")
		}
		assertNoData(t, dir)
	})

	t.Run("no client cert under mTLS", func(t *testing.T) {
		cfg := l.cfg()
		cfg.ReplicateCert, cfg.ReplicateKey = "", ""
		cl := dialRepl(t, cfg)
		dir := filepath.Join(t.TempDir(), "d")
		ctx := server.ReplicationAuthContext(context.Background(), replKey)
		if _, err := server.Bootstrap(ctx, cl, dir); err == nil {
			t.Fatal("bootstrap succeeded without a client certificate")
		}
		assertNoData(t, dir)
	})

	t.Run("plaintext client to TLS leader", func(t *testing.T) {
		cfg := server.DefaultConfig()
		cfg.ReplicateFrom = l.addr
		cfg.ReplicateInsecure = true // explicit override; the leader still refuses
		cl := dialRepl(t, cfg)
		dir := filepath.Join(t.TempDir(), "d")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := server.Bootstrap(server.ReplicationAuthContext(ctx, replKey), cl, dir); err == nil {
			t.Fatal("plaintext bootstrap succeeded against a TLS leader")
		}
		assertNoData(t, dir)
	})
}

func assertNoData(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err == nil && len(ents) > 0 {
		t.Fatalf("failed bootstrap left data in %s: %v", dir, ents)
	}
}

// protocolServer is a minimal leader whose advertised protocol set is fixed.
type protocolServer struct {
	pb.UnimplementedScrivaServer
	versions string
}

func (p *protocolServer) ReplicationStatus(ctx context.Context, _ *pb.ReplicationStatusRequest) (*pb.ReplicationStatusResponse, error) {
	grpc.SetHeader(ctx, metadata.Pairs(server.ReplicationProtocolsHeader, p.versions))
	return &pb.ReplicationStatusResponse{}, nil
}

func (p *protocolServer) Replicate(_ *pb.ReplicateRequest, s pb.Scriva_ReplicateServer) error {
	s.SetHeader(metadata.Pairs(server.ReplicationProtocolsHeader, p.versions))
	<-s.Context().Done()
	return nil
}

func TestReplicationProtocol_UnsupportedLeaderFailsClosed(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterScrivaServer(srv, &protocolServer{versions: "7,8"})
	go srv.Serve(lis)
	defer srv.Stop()
	cfg := server.DefaultConfig()
	cfg.ReplicateFrom = lis.Addr().String()
	cl := dialRepl(t, cfg)

	if _, err := server.Bootstrap(server.ReplicationAuthContext(context.Background(), ""), cl, filepath.Join(t.TempDir(), "d")); !server.IsReplicationProtocolError(err) {
		t.Fatalf("Bootstrap err = %v, want protocol error", err)
	}
	fdb, err := engine.Open(t.TempDir(), followerEngineConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer fdb.Close()
	fol := server.NewFollower(fdb, cl, "f", "", discardLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fol.Run(ctx); !server.IsReplicationProtocolError(err) {
		t.Fatalf("Run err = %v, want protocol error (no retry, no downgrade)", err)
	}
	if fdb.AppliedLSN() != 0 {
		t.Fatalf("applied lsn advanced to %d", fdb.AppliedLSN())
	}
}

func TestReplicationProtocol_UnsupportedFollowerRefusedByLeader(t *testing.T) {
	leader, _ := startLeader(t)
	// Via the request body.
	rs, err := leader.Replicate(context.Background(), &pb.ReplicateRequest{ProtocolVersions: []uint32{9}})
	if err == nil {
		_, err = rs.Recv()
	}
	if !server.IsReplicationProtocolError(err) {
		t.Fatalf("Replicate err = %v, want protocol error", err)
	}
	// Via the header on the other replication RPCs.
	ctx := metadata.AppendToOutgoingContext(context.Background(), server.ReplicationProtocolsHeader, "9")
	if _, err := leader.ReplicationStatus(ctx, &pb.ReplicationStatusRequest{}); !server.IsReplicationProtocolError(err) {
		t.Fatalf("ReplicationStatus err = %v, want protocol error", err)
	}
	sn, err := leader.Snapshot(ctx, &pb.SnapshotRequest{})
	if err == nil {
		_, err = sn.Recv()
	}
	if !server.IsReplicationProtocolError(err) {
		t.Fatalf("Snapshot err = %v, want protocol error", err)
	}
	// A compatible follower is still served.
	if _, err := leader.ReplicationStatus(server.ReplicationAuthContext(context.Background(), ""), &pb.ReplicationStatusRequest{}); err != nil {
		t.Fatalf("compatible ReplicationStatus: %v", err)
	}
}

func TestReplicationTransport_Validation(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "ca")
	caPath := writeCAPEM(t, dir, "ca", ca)
	certP, keyP := writePEM(t, dir, "cl", ca.issue(t, "cl", x509.ExtKeyUsageClientAuth))

	mk := func(mut func(*server.Config)) server.Config {
		cfg := server.DefaultConfig()
		cfg.ReplicateFrom = "10.0.0.5:5433"
		mut(&cfg)
		return cfg
	}
	bad := map[string]server.Config{
		"remote plaintext":        mk(func(*server.Config) {}),
		"insecure + TLS settings": mk(func(c *server.Config) { c.ReplicateInsecure = true; c.ReplicateCA = caPath }),
		"cert without key":        mk(func(c *server.Config) { c.ReplicateCA = caPath; c.ReplicateCert = certP }),
		"TLS without credentials": mk(func(c *server.Config) { c.ReplicateCA = caPath }),
		"missing CA file":         mk(func(c *server.Config) { c.ReplicateCA = filepath.Join(dir, "nope"); c.ReplicateAPIKey = "k" }),
	}
	for name, cfg := range bad {
		if _, _, err := server.ReplicationTransport(cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	good := map[string]server.Config{
		"loopback plaintext":  {ReplicateFrom: "127.0.0.1:5433"},
		"remote API key+TLS":  mk(func(c *server.Config) { c.ReplicateTLS = true; c.APIKey = "k" }),
		"remote mTLS only":    mk(func(c *server.Config) { c.ReplicateCA = caPath; c.ReplicateCert = certP; c.ReplicateKey = keyP }),
		"remote insecure ack": mk(func(c *server.Config) { c.ReplicateInsecure = true }),
	}
	for name, cfg := range good {
		rc, warns, err := server.ReplicationTransport(cfg)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if name == "remote insecure ack" && len(warns) == 0 {
			t.Errorf("%s: expected an INSECURE warning", name)
		}
		if rc.Secure != (name == "remote API key+TLS" || name == "remote mTLS only") {
			t.Errorf("%s: Secure=%v", name, rc.Secure)
		}
	}
	if server.ReplicationAPIKey(server.Config{APIKey: "a"}) != "a" || server.ReplicationAPIKey(server.Config{APIKey: "a", ReplicateAPIKey: "r"}) != "r" {
		t.Error("ReplicationAPIKey fallback wrong")
	}
}

func TestReplicationTransport_NoDowngradeMarker(t *testing.T) {
	dir := t.TempDir()
	if err := server.CheckReplicationTransportMarker(dir, false); err != nil {
		t.Fatalf("fresh dir: %v", err)
	}
	if err := server.WriteReplicationTransportMarker(dir, false); err != nil {
		t.Fatal(err)
	}
	if err := server.WriteReplicationTransportMarker(dir, true); err != nil {
		t.Fatal(err)
	}
	if err := server.CheckReplicationTransportMarker(dir, true); err != nil {
		t.Fatalf("secure restart: %v", err)
	}
	if err := server.CheckReplicationTransportMarker(dir, false); !errors.Is(err, server.ErrReplicationDowngrade) {
		t.Fatalf("plaintext restart = %v, want ErrReplicationDowngrade", err)
	}
	// An engine opens fine with the marker file present.
	db, err := engine.Open(dir, followerEngineConfig())
	if err != nil {
		t.Fatalf("open with marker: %v", err)
	}
	db.Close()
}

func insertReq(coll string, n int) *pb.InsertRequest {
	st, _ := structpb.NewStruct(map[string]any{"n": n})
	return &pb.InsertRequest{Collection: coll, Data: st}
}

func mustInsertCtx(t *testing.T, ctx context.Context, cl pb.ScrivaClient, coll string, n int) {
	t.Helper()
	if _, err := cl.Insert(ctx, insertReq(coll, n)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}
