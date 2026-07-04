//nolint:errcheck
package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/srjn45/filedbv2/engine"
	"github.com/srjn45/filedbv2/internal/auth"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
	"github.com/srjn45/filedbv2/server"
)

// ---- Certificate authority test helpers -----------------------------------

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// newTestCA mints a throwaway self-signed CA usable for signing both server and
// client leaf certificates in-test.
func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	return &testCA{cert: cert, key: key}
}

// issue signs a leaf certificate (server or client) under the CA and returns it
// as a tls.Certificate ready to load into a tls.Config.
func (ca *testCA) issue(t *testing.T, cn string, eku x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{eku},
	}
	if eku == x509.ExtKeyUsageServerAuth {
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// pool returns an x509.CertPool trusting this CA.
func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// writePEM writes a cert/key pair to files under dir and returns the two paths.
func writePEM(t *testing.T, dir, name string, cert tls.Certificate) (certPath, keyPath string) {
	t.Helper()
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	certOut := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certOut, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyOut, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

func writeCAPEM(t *testing.T, dir, name string, ca *testCA) string {
	t.Helper()
	p := filepath.Join(dir, name+".pem")
	out := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	return p
}

// newMTLSServer starts an in-process gRPC server with the given server TLS
// config and cert-auth setting, backed by a real engine.DB, and returns its
// listen address.
func newMTLSServer(t *testing.T, tlsCfg *tls.Config, certAuth bool) string {
	t.Helper()

	db, err := engine.Open(t.TempDir(), engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// mTLS-only: no API keys configured, so a verified client cert is the sole
	// credential (exercises the cert-auth path end to end).
	authn, err := auth.New(nil, auth.WithCertAuth(certAuth))
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	authUnary, authStream := authn.Interceptors()

	gs := server.NewGRPCServer(db, 5*time.Minute)
	t.Cleanup(gs.Close)
	grpcSrv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.ChainUnaryInterceptor(authUnary),
		grpc.ChainStreamInterceptor(authStream),
	)
	pb.RegisterFileDBServer(grpcSrv, gs)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)
	return lis.Addr().String()
}

// dial builds a client for addr using the given client-side TLS config.
func dial(t *testing.T, addr string, tlsCfg *tls.Config) pb.FileDBClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewFileDBClient(conn)
}

// ---- Tests ----------------------------------------------------------------

// A client presenting a certificate signed by the trusted CA authenticates and
// is granted read-write access (an mTLS principal).
func TestMTLS_ValidClientCertAuthenticates(t *testing.T) {
	ca := newTestCA(t, "test-ca")
	serverCert := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	clientCert := ca.issue(t, "svc-alice", x509.ExtKeyUsageClientAuth)

	addr := newMTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    ca.pool(),
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, true)

	c := dial(t, addr, &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      ca.pool(),
		ServerName:   "localhost",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// CreateCollection is a write RPC: it succeeds only if the cert principal
	// resolved with read-write scope.
	if _, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "users"}); err != nil {
		t.Fatalf("valid client cert should authenticate a write: %v", err)
	}
}

// A client whose certificate is signed by an untrusted CA is rejected at the
// TLS handshake and cannot reach any RPC.
func TestMTLS_UntrustedClientCertRejected(t *testing.T) {
	ca := newTestCA(t, "test-ca")
	rogue := newTestCA(t, "rogue-ca")
	serverCert := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	rogueClient := rogue.issue(t, "svc-mallory", x509.ExtKeyUsageClientAuth)

	addr := newMTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    ca.pool(), // trusts only ca, not rogue
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, true)

	c := dial(t, addr, &tls.Config{
		Certificates: []tls.Certificate{rogueClient},
		RootCAs:      ca.pool(),
		ServerName:   "localhost",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "users"}); err == nil {
		t.Fatal("untrusted client cert must be rejected")
	}
}

// Regression: server-only TLS (no client CA, no cert auth) still lets a client
// with no certificate connect and call unchanged.
func TestMTLS_ServerOnlyTLSStillWorks(t *testing.T) {
	ca := newTestCA(t, "test-ca")
	serverCert := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)

	addr := newMTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		// No ClientCAs, ClientAuth defaults to NoClientCert.
	}, false)

	c := dial(t, addr, &tls.Config{
		RootCAs:    ca.pool(),
		ServerName: "localhost",
		// No client certificate presented.
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{Name: "users"}); err != nil {
		t.Fatalf("server-only TLS should still work without a client cert: %v", err)
	}
}

// ServerTLSConfig builds the right *tls.Config for each client-auth mode and
// rejects misconfigurations.
func TestServerTLSConfig(t *testing.T) {
	ca := newTestCA(t, "test-ca")
	serverCert := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)

	dir := t.TempDir()
	certPath, keyPath := writePEM(t, dir, "server", serverCert)
	caPath := writeCAPEM(t, dir, "client-ca", ca)

	t.Run("tls disabled", func(t *testing.T) {
		cfg, certAuth, err := server.ServerTLSConfig(server.Config{})
		if err != nil || cfg != nil || certAuth {
			t.Fatalf("disabled TLS: got (%v, %v, %v), want (nil, false, nil)", cfg, certAuth, err)
		}
	})

	t.Run("server-only tls", func(t *testing.T) {
		cfg, certAuth, err := server.ServerTLSConfig(server.Config{TLSCert: certPath, TLSKey: keyPath})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg == nil || certAuth {
			t.Fatalf("server-only: got (cfg=%v, certAuth=%v), want a config with certAuth=false", cfg != nil, certAuth)
		}
		if cfg.ClientAuth != tls.NoClientCert {
			t.Errorf("ClientAuth = %v, want NoClientCert", cfg.ClientAuth)
		}
	})

	t.Run("require mode", func(t *testing.T) {
		cfg, certAuth, err := server.ServerTLSConfig(server.Config{
			TLSCert: certPath, TLSKey: keyPath, TLSClientCA: caPath, TLSClientAuth: "require",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !certAuth || cfg.ClientAuth != tls.RequireAndVerifyClientCert || cfg.ClientCAs == nil {
			t.Errorf("require: certAuth=%v ClientAuth=%v ClientCAs=%v", certAuth, cfg.ClientAuth, cfg.ClientCAs != nil)
		}
	})

	t.Run("verify-if-given mode", func(t *testing.T) {
		cfg, certAuth, err := server.ServerTLSConfig(server.Config{
			TLSCert: certPath, TLSKey: keyPath, TLSClientCA: caPath, TLSClientAuth: "verify-if-given",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !certAuth || cfg.ClientAuth != tls.VerifyClientCertIfGiven {
			t.Errorf("verify-if-given: certAuth=%v ClientAuth=%v", certAuth, cfg.ClientAuth)
		}
	})

	t.Run("client-ca without server tls is an error", func(t *testing.T) {
		if _, _, err := server.ServerTLSConfig(server.Config{TLSClientCA: caPath}); err == nil {
			t.Fatal("expected error: client CA without server TLS")
		}
	})

	t.Run("require without client-ca is an error", func(t *testing.T) {
		if _, _, err := server.ServerTLSConfig(server.Config{
			TLSCert: certPath, TLSKey: keyPath, TLSClientAuth: "require",
		}); err == nil {
			t.Fatal("expected error: require mode without client CA")
		}
	})

	t.Run("unknown mode is an error", func(t *testing.T) {
		if _, _, err := server.ServerTLSConfig(server.Config{
			TLSCert: certPath, TLSKey: keyPath, TLSClientCA: caPath, TLSClientAuth: "bogus",
		}); err == nil {
			t.Fatal("expected error for unknown client-auth mode")
		}
	})
}
