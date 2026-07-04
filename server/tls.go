package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

// ParseClientAuth converts the --tls-client-auth config string into a
// tls.ClientAuthType and reports whether client-certificate verification (mTLS)
// is enabled. Accepted spellings:
//
//	off | ""       → no client certs requested (mTLS disabled)
//	require        → RequireAndVerifyClientCert (a valid client cert is mandatory)
//	verify-if-given → VerifyClientCertIfGiven (a cert, if presented, must verify)
func ParseClientAuth(mode string) (tls.ClientAuthType, bool, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "off", "none", "disable", "disabled":
		return tls.NoClientCert, false, nil
	case "require", "required", "require-and-verify":
		return tls.RequireAndVerifyClientCert, true, nil
	case "verify-if-given", "verify_if_given", "verify-if-provided", "optional":
		return tls.VerifyClientCertIfGiven, true, nil
	default:
		return tls.NoClientCert, false, fmt.Errorf(
			"unknown tls client auth mode %q (want off|require|verify-if-given)", mode)
	}
}

// ServerTLSConfig builds the server-side *tls.Config from cfg. It returns
// (nil, false, nil) when TLS is disabled (no --tls-cert/--tls-key). The boolean
// result reports whether mutual-TLS client-certificate verification is enabled,
// which the caller feeds to auth.WithCertAuth so a verified cert authenticates a
// request.
//
// mTLS composes on top of server TLS: --tls-client-ca and a non-off
// --tls-client-auth require --tls-cert/--tls-key to also be set (you cannot
// verify client certs without a TLS handshake).
func ServerTLSConfig(cfg Config) (*tls.Config, bool, error) {
	if cfg.TLSCert == "" || cfg.TLSKey == "" {
		// TLS is off. Requesting client-cert auth without server TLS is a
		// misconfiguration worth failing loudly rather than silently ignoring.
		if cfg.TLSClientCA != "" {
			return nil, false, fmt.Errorf("--tls-client-ca requires --tls-cert and --tls-key (mTLS needs TLS)")
		}
		if _, want, err := ParseClientAuth(cfg.TLSClientAuth); err != nil {
			return nil, false, err
		} else if want {
			return nil, false, fmt.Errorf("--tls-client-auth %q requires --tls-cert and --tls-key (mTLS needs TLS)", cfg.TLSClientAuth)
		}
		return nil, false, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, false, fmt.Errorf("load TLS key pair: %w", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	mode, wantClientCerts, err := ParseClientAuth(cfg.TLSClientAuth)
	if err != nil {
		return nil, false, err
	}
	if !wantClientCerts {
		return tlsCfg, false, nil
	}
	if cfg.TLSClientCA == "" {
		return nil, false, fmt.Errorf("--tls-client-auth %q requires --tls-client-ca", cfg.TLSClientAuth)
	}
	pool, err := loadCertPool(cfg.TLSClientCA)
	if err != nil {
		return nil, false, err
	}
	tlsCfg.ClientCAs = pool
	tlsCfg.ClientAuth = mode
	return tlsCfg, true, nil
}

// loadCertPool reads a PEM bundle of one or more CA certificates into a pool.
func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read client CA bundle %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("client CA bundle %q: no valid PEM certificates found", path)
	}
	return pool, nil
}
