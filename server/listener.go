package server

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// HTTP hardening defaults shared by the REST and metrics servers.
const (
	// DefaultMaxRequestBodyBytes caps a single REST request body.
	DefaultMaxRequestBodyBytes int64 = 32 << 20

	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpWriteTimeout      = 60 * time.Second
	httpIdleTimeout       = 120 * time.Second

	// unixSocketMode restricts the gRPC unix socket to the owning user.
	unixSocketMode os.FileMode = 0o600
)

// IsLoopbackAddr reports whether a host:port listen address binds only to a
// loopback interface. Wildcard (":5433", "0.0.0.0", "[::]") and any non-loopback
// IP or hostname other than "localhost" are not loopback.
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// hasAuthOrTLS reports whether the config provides API keys or TLS (which
// includes mTLS, as client-cert auth requires server TLS).
func (c Config) hasAuthOrTLS() bool {
	return c.APIKey != "" || len(c.Keys) > 0 || (c.TLSCert != "" && c.TLSKey != "")
}

// ValidateListeners enforces the secure-exposure rule: a TCP listener (gRPC,
// REST or metrics) bound to a non-loopback address requires API-key auth or
// TLS, unless InsecureAllowRemote is set. It returns a warning per exposed
// listener when the override is used, and an error (fail closed) otherwise.
func ValidateListeners(cfg Config) (warnings []string, err error) {
	listeners := []struct{ name, addr string }{
		{"gRPC (grpc_addr)", cfg.GRPCAddr},
		{"REST (rest_addr)", cfg.RESTAddr},
		{"metrics (metrics_addr)", cfg.MetricsAddr},
	}
	var exposed []string
	for _, l := range listeners {
		if l.addr == "" {
			continue
		}
		if _, _, splitErr := net.SplitHostPort(l.addr); splitErr != nil {
			return nil, fmt.Errorf("invalid %s %q: %w", l.name, l.addr, splitErr)
		}
		if !IsLoopbackAddr(l.addr) {
			exposed = append(exposed, fmt.Sprintf("%s=%s", l.name, l.addr))
		}
	}
	if len(exposed) == 0 || cfg.hasAuthOrTLS() {
		return nil, nil
	}
	list := strings.Join(exposed, ", ")
	if cfg.InsecureAllowRemote {
		return []string{fmt.Sprintf("INSECURE: listening on non-loopback address(es) [%s] with no API key and no TLS because insecure_allow_remote is set; anyone who can reach this host can read and write all data", list)}, nil
	}
	return nil, fmt.Errorf("refusing to start: non-loopback listener(s) [%s] without authentication or TLS. "+
		"The default bind is now 127.0.0.1. To expose the server, configure an API key (--api-key / keys:) or TLS (--tls-cert/--tls-key), "+
		"or bind to 127.0.0.1. To keep the old open behaviour explicitly, pass --insecure-allow-remote (insecure_allow_remote: true)", list)
}

// NewHTTPServer returns an http.Server with explicit timeouts. writeTimeout of
// 0 uses the default; long-lived streaming handlers clear their own deadline.
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}
}

// limitBody caps request bodies at max bytes (max <= 0 uses the default).
func limitBody(max int64, next http.Handler) http.Handler {
	if max <= 0 {
		max = DefaultMaxRequestBodyBytes
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > max {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, max)
		}
		next.ServeHTTP(w, r)
	})
}

// corsMiddleware reflects the request Origin only when it appears in allowed
// ("*" in the list opts in to any origin). With no allowed origins no CORS
// headers are emitted, so browsers enforce same-origin.
func corsMiddleware(allowed []string, next http.Handler) http.Handler {
	set := make(map[string]bool, len(allowed))
	any := false
	for _, o := range allowed {
		if o == "*" {
			any = true
		}
		set[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (any || set[origin]) {
			h := w.Header()
			h.Add("Vary", "Origin")
			if any {
				h.Set("Access-Control-Allow-Origin", "*")
			} else {
				h.Set("Access-Control-Allow-Origin", origin)
			}
			h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type, x-api-key, x-xtx-id")
			h.Set("Access-Control-Expose-Headers", "Scriva-Post-Commit-Warning")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ListenUnix creates a unix socket listener that is only accessible to the
// owner (0600). The umask is tightened around bind so the socket is never
// briefly world-accessible, and a stale socket file is replaced only if it
// is a socket.
func ListenUnix(path string) (net.Listener, error) {
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("unix socket path %q exists and is not a socket", path)
		}
		_ = os.Remove(path)
	}
	ln, err := listenUnixPrivate(path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, unixSocketMode); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("chmod unix socket: %w", err)
	}
	return ln, nil
}
