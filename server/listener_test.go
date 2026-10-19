package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:5433":     true,
		"127.1.2.3:80":       true,
		"[::1]:8080":         true,
		"localhost:9090":     true,
		":5433":              false,
		"0.0.0.0:5433":       false,
		"[::]:5433":          false,
		"192.168.1.10:5433":  false,
		"[2001:db8::1]:5433": false,
		"example.com:80":     false,
		"garbage":            false,
	}
	for addr, want := range cases {
		if got := IsLoopbackAddr(addr); got != want {
			t.Errorf("IsLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestDefaultConfigIsLoopback(t *testing.T) {
	cfg := DefaultConfig()
	for _, a := range []string{cfg.GRPCAddr, cfg.RESTAddr, cfg.MetricsAddr} {
		if !IsLoopbackAddr(a) {
			t.Errorf("default addr %q is not loopback", a)
		}
	}
	if w, err := ValidateListeners(cfg); err != nil || len(w) != 0 {
		t.Fatalf("defaults: warnings=%v err=%v", w, err)
	}
}

func TestValidateListeners(t *testing.T) {
	remote := []string{":5433", "0.0.0.0:5433", "[::]:5433", "10.0.0.5:5433", "[2001:db8::1]:5433"}
	for _, addr := range remote {
		for _, which := range []string{"grpc", "rest", "metrics"} {
			cfg := DefaultConfig()
			switch which {
			case "grpc":
				cfg.GRPCAddr = addr
			case "rest":
				cfg.RESTAddr = addr
			case "metrics":
				cfg.MetricsAddr = addr
			}
			name := which + "/" + addr

			if _, err := ValidateListeners(cfg); err == nil || !strings.Contains(err.Error(), "--insecure-allow-remote") {
				t.Errorf("%s: want fail-closed error with migration hint, got %v", name, err)
			}

			ov := cfg
			ov.InsecureAllowRemote = true
			if w, err := ValidateListeners(ov); err != nil || len(w) != 1 || !strings.Contains(w[0], "INSECURE") {
				t.Errorf("%s override: warnings=%v err=%v", name, w, err)
			}

			k := cfg
			k.APIKey = "k"
			if w, err := ValidateListeners(k); err != nil || len(w) != 0 {
				t.Errorf("%s api key: warnings=%v err=%v", name, w, err)
			}
			ks := cfg
			ks.Keys = []APIKeyConfig{{Name: "a", Key: "k"}}
			if _, err := ValidateListeners(ks); err != nil {
				t.Errorf("%s keys: %v", name, err)
			}
			tl := cfg
			tl.TLSCert, tl.TLSKey = "c.pem", "k.pem"
			if _, err := ValidateListeners(tl); err != nil {
				t.Errorf("%s tls: %v", name, err)
			}
		}
	}
}

func TestValidateListenersEdge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MetricsAddr = "" // disabled
	cfg.GRPCAddr = "[::1]:5433"
	if _, err := ValidateListeners(cfg); err != nil {
		t.Fatalf("ipv6 loopback + disabled metrics: %v", err)
	}
	cfg.GRPCAddr = "nonsense"
	if _, err := ValidateListeners(cfg); err == nil {
		t.Fatal("malformed addr should error")
	}
	// TLS cert without key is not "TLS enabled".
	cfg = DefaultConfig()
	cfg.GRPCAddr = "0.0.0.0:1"
	cfg.TLSCert = "c.pem"
	if _, err := ValidateListeners(cfg); err == nil {
		t.Fatal("cert without key must not satisfy the rule")
	}
}

func TestInsecureAllowRemoteFromYAML(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	body := "grpc_addr: \"0.0.0.0:5433\"\ninsecure_allow_remote: true\ncors_allowed_origins: [\"http://a.example\"]\nmax_request_body_bytes: 1024\n"
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.InsecureAllowRemote || len(cfg.CORSAllowedOrigins) != 1 || cfg.MaxRequestBodyBytes != 1024 {
		t.Fatalf("yaml not loaded: %+v", cfg)
	}
	if w, err := ValidateListeners(cfg); err != nil || len(w) != 1 {
		t.Fatalf("warnings=%v err=%v", w, err)
	}
}

func TestListenUnixPermissionsAndReplace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permissions")
	}
	dir := t.TempDir()
	sock := filepath.Join(dir, "s.sock")

	ln, err := ListenUnix(sock)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perm = %o, want 600", perm)
	}
	_ = ln.Close()

	// A stale socket file is replaced.
	ln2, err := ListenUnix(sock)
	if err != nil {
		t.Fatalf("replace stale socket: %v", err)
	}
	_ = ln2.Close()
	_ = os.Remove(sock)

	// A regular file is never clobbered.
	reg := filepath.Join(dir, "data")
	if err := os.WriteFile(reg, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenUnix(reg); err == nil {
		t.Fatal("must refuse to replace a non-socket file")
	}
	if b, _ := os.ReadFile(reg); string(b) != "x" {
		t.Fatal("regular file was modified")
	}
}

func TestNewHTTPServerTimeouts(t *testing.T) {
	s := NewHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if s.ReadHeaderTimeout <= 0 || s.ReadTimeout <= 0 || s.WriteTimeout <= 0 || s.IdleTimeout <= 0 {
		t.Fatalf("missing timeout: %+v", s)
	}
}

func TestLimitBody(t *testing.T) {
	h := limitBody(10, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64)
		n, err := r.Body.Read(buf)
		_ = n
		if err != nil && err.Error() != "EOF" {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	ok := httptest.NewRecorder()
	h.ServeHTTP(ok, httptest.NewRequest("POST", "/", strings.NewReader("small")))
	if ok.Code != http.StatusOK {
		t.Fatalf("small body: %d", ok.Code)
	}
	big := httptest.NewRecorder()
	h.ServeHTTP(big, httptest.NewRequest("POST", "/", strings.NewReader(strings.Repeat("x", 100))))
	if big.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("declared-length oversize: %d", big.Code)
	}
	// Unknown length (chunked): MaxBytesReader trips while reading.
	req := httptest.NewRequest("POST", "/", strings.NewReader(strings.Repeat("x", 100)))
	req.ContentLength = -1
	chunked := httptest.NewRecorder()
	h.ServeHTTP(chunked, req)
	if chunked.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked oversize: %d", chunked.Code)
	}
}

func TestCORS(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	do := func(h http.Handler, method, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/", nil)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	def := corsMiddleware(nil, next)
	if w := do(def, "GET", "http://evil.example"); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("default must not emit CORS headers")
	}
	if w := do(def, "OPTIONS", "http://evil.example"); w.Code != http.StatusTeapot {
		t.Fatalf("default preflight should fall through, got %d", w.Code)
	}

	al := corsMiddleware([]string{"http://ok.example"}, next)
	w := do(al, "GET", "http://ok.example")
	if w.Header().Get("Access-Control-Allow-Origin") != "http://ok.example" || w.Header().Get("Vary") != "Origin" {
		t.Fatalf("allowed origin headers: %v", w.Header())
	}
	if w := do(al, "GET", "http://evil.example"); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unlisted origin must not be allowed")
	}
	if w := do(al, "OPTIONS", "http://ok.example"); w.Code != http.StatusNoContent {
		t.Fatalf("preflight: %d", w.Code)
	}

	star := corsMiddleware([]string{"*"}, next)
	if w := do(star, "GET", "http://x.example"); w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("explicit * opt-in should work")
	}
}
