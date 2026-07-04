package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ctxWithVerifiedCert returns a context whose gRPC peer carries a verified TLS
// chain leafed by cert — the shape the transport produces once it has chained a
// client cert up to a configured client-CA pool.
func ctxWithVerifiedCert(cert *x509.Certificate) context.Context {
	info := credentials.TLSInfo{State: tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}},
	}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
}

func mustNewCertAuth(t *testing.T, keys ...Key) *Authenticator {
	t.Helper()
	a, err := New(keys, WithCertAuth(true))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func TestCertAuth_VerifiedCertAuthenticatesReadWrite(t *testing.T) {
	a := mustNewCertAuth(t) // cert-auth only, no api keys
	ctx := ctxWithVerifiedCert(&x509.Certificate{Subject: pkix.Name{CommonName: "svc-a"}})
	// A verified cert is trusted with read-write scope, so both reads and writes pass.
	if code := call(t, a, ctx, readMethod); code != codes.OK {
		t.Errorf("verified cert on read: got %v, want OK", code)
	}
	if code := call(t, a, ctx, writeMethod); code != codes.OK {
		t.Errorf("verified cert on write: got %v, want OK", code)
	}
}

func TestCertAuth_NoCredentialRejected(t *testing.T) {
	a := mustNewCertAuth(t)
	// No api key and no client certificate on the context.
	if code := call(t, a, context.Background(), readMethod); code != codes.Unauthenticated {
		t.Errorf("no credential: got %v, want Unauthenticated", code)
	}
}

func TestCertAuth_DisabledIgnoresCert(t *testing.T) {
	// Without WithCertAuth, a verified cert is not accepted; with no keys either,
	// auth is disabled entirely and everything passes.
	a := mustNew(t)
	ctx := ctxWithVerifiedCert(&x509.Certificate{Subject: pkix.Name{CommonName: "svc-a"}})
	if code := call(t, a, ctx, writeMethod); code != codes.OK {
		t.Errorf("auth disabled: got %v, want OK", code)
	}
}

func TestCertAuth_ComposesWithAPIKey(t *testing.T) {
	a := mustNewCertAuth(t, Key{Key: "ro", Name: "reader", Scope: ScopeRead})
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: "svc-a"}}

	// A valid API key wins and its (read-only) scope is enforced even though a
	// read-write cert is also present.
	if code := call(t, a, ctxWithKey("ro"), writeMethod); code != codes.PermissionDenied {
		t.Errorf("api key wins, read-only on write: got %v, want PermissionDenied", code)
	}

	// A presented-but-invalid API key is rejected outright — no silent fallback.
	badKeyCtx := peer.NewContext(ctxWithKey("wrong"), &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}},
	}})
	if code := call(t, a, badKeyCtx, readMethod); code != codes.Unauthenticated {
		t.Errorf("invalid key with cert present: got %v, want Unauthenticated", code)
	}

	// No API key at all → the verified cert authenticates as read-write.
	if code := call(t, a, ctxWithVerifiedCert(cert), writeMethod); code != codes.OK {
		t.Errorf("cert fallback on write: got %v, want OK", code)
	}
}

func TestCertPrincipalName_Fallbacks(t *testing.T) {
	uri, _ := url.Parse("spiffe://example/svc")
	cases := []struct {
		name string
		cert *x509.Certificate
		want string
	}{
		{"common-name", &x509.Certificate{Subject: pkix.Name{CommonName: "cn-id"}}, "cn-id"},
		{"dns-san", &x509.Certificate{DNSNames: []string{"svc.example"}}, "svc.example"},
		{"email-san", &x509.Certificate{EmailAddresses: []string{"svc@example"}}, "svc@example"},
		{"uri-san", &x509.Certificate{URIs: []*url.URL{uri}}, "spiffe://example/svc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := certPrincipalName(tc.cert); got != tc.want {
				t.Errorf("certPrincipalName = %q, want %q", got, tc.want)
			}
		})
	}
}
