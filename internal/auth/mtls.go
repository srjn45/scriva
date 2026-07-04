package auth

import (
	"context"
	"crypto/x509"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// certAuthPrincipal is the scope granted to a client authenticated purely by a
// mutual-TLS certificate. A CA-signed client certificate is an operator-issued,
// cryptographically strong identity, so it is trusted with read-write access —
// mirroring how the legacy --api-key becomes a read-write "default" principal.
// Finer, per-certificate scoping (mapping a cert onto a narrower scope or a
// per-collection ACL) is deferred to S3; until then the client CA is the trust
// boundary and issuing a cert grants full access.
const certAuthScope = ScopeReadWrite

// principalFromPeerCert derives a Principal from the verified client certificate
// on ctx's gRPC peer, if any. It returns ok=false when the transport is not TLS,
// when the client presented no certificate, or when the certificate was not
// verified against the server's configured client-CA pool.
//
// The TLS stack only populates VerifiedChains after it has chained the leaf up
// to a configured ClientCA (RequireAndVerifyClientCert or
// VerifyClientCertIfGiven), so a non-empty verified chain here *is* proof the
// certificate is trusted — an untrusted or unsigned cert fails the handshake and
// never reaches this code.
func principalFromPeerCert(ctx context.Context) (Principal, bool) {
	pr, ok := peer.FromContext(ctx)
	if !ok || pr.AuthInfo == nil {
		return Principal{}, false
	}
	tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return Principal{}, false
	}
	chains := tlsInfo.State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		return Principal{}, false
	}
	return Principal{Name: certPrincipalName(chains[0][0]), Scope: certAuthScope}, true
}

// certPrincipalName maps a client certificate to a stable principal name. It
// prefers the subject Common Name, then falls back to the first Subject
// Alternative Name (DNS, email, or URI) so certs issued without a CN still yield
// a meaningful identity. As a last resort it uses the raw subject DN.
func certPrincipalName(cert *x509.Certificate) string {
	if cn := cert.Subject.CommonName; cn != "" {
		return cn
	}
	if len(cert.DNSNames) > 0 {
		return cert.DNSNames[0]
	}
	if len(cert.EmailAddresses) > 0 {
		return cert.EmailAddresses[0]
	}
	if len(cert.URIs) > 0 {
		return cert.URIs[0].String()
	}
	return cert.Subject.String()
}
