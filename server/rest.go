package server

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// headerMatcher forwards x-api-key and all default grpc-gateway headers.
func headerMatcher(key string) (string, bool) {
	if strings.ToLower(key) == "x-api-key" {
		return "x-api-key", true
	}
	return runtime.DefaultHeaderMatcher(key)
}

// corsMiddleware adds permissive CORS headers so browser clients (e.g. the
// web UI dev server at :5173) can call the REST gateway at :8080.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, x-api-key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// registerProbes wires GET /healthz (liveness) and GET /readyz (readiness) onto
// the gateway mux. ready reports whether the node can serve traffic (DB open,
// data dir writable); a nil ready means always ready.
func registerProbes(mux *runtime.ServeMux, ready func() error) error {
	liveness := LivenessHandler()
	readiness := ReadinessHandler(ready)
	if err := mux.HandlePath("GET", "/healthz", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		liveness(w, r)
	}); err != nil {
		return err
	}
	return mux.HandlePath("GET", "/readyz", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		readiness(w, r)
	})
}

// NewRESTGateway returns an http.Handler that proxies requests to the gRPC
// server listening on grpcAddr via the grpc-gateway. It also exposes the
// process probes GET /healthz and GET /readyz (the latter gated by ready).
// creds controls how the gateway dials gRPC (pass insecure.NewCredentials() when TLS is off).
func NewRESTGateway(ctx context.Context, grpcAddr string, creds credentials.TransportCredentials, ready func() error) (http.Handler, error) {
	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(headerMatcher))
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}

	if err := pb.RegisterScrivaHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}
	if err := registerProbes(mux, ready); err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	return corsMiddleware(watchInterceptor(mux, conn)), nil
}

// NewRESTGatewayUnix returns an http.Handler that dials the gRPC server via a
// Unix domain socket. Unix sockets are always local, so insecure credentials
// are used regardless of the server's TLS setting. ready gates GET /readyz (nil
// = always ready).
func NewRESTGatewayUnix(ctx context.Context, socketPath string, ready func() error) (http.Handler, error) {
	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(headerMatcher))
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}),
	}
	if err := pb.RegisterScrivaHandlerFromEndpoint(ctx, mux, "unix://"+socketPath, opts); err != nil {
		return nil, err
	}
	if err := registerProbes(mux, ready); err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient("unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}),
	)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	return corsMiddleware(watchInterceptor(mux, conn)), nil
}
