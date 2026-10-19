package server

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"github.com/srjn45/scriva/internal/xtxapi"
	"github.com/srjn45/scriva/writeoutcome"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// headerMatcher forwards x-api-key, x-xtx-id and all default grpc-gateway
// headers. x-xtx-id is forwarded so a REST request that tries to run a plain
// data RPC "inside" a cross-collection transaction is rejected exactly as it
// is over gRPC, instead of silently running outside the transaction.
func headerMatcher(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "x-api-key":
		return "x-api-key", true
	case xtxapi.HeaderXTxID:
		return xtxapi.HeaderXTxID, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

// outgoingHeaderMatcher forwards the post-commit warning as the plain HTTP
// header Scriva-Post-Commit-Warning, and every other response header under
// the gateway's default Grpc-Metadata- prefix.
func outgoingHeaderMatcher(key string) (string, bool) {
	if strings.EqualFold(key, writeoutcome.TrailerPostCommit) {
		return writeoutcome.HeaderPostCommit, true
	}
	return runtime.MetadataHeaderPrefix + key, true
}

// newGatewayMux is the gateway mux shared by the TCP and Unix gateways.
func newGatewayMux() *runtime.ServeMux {
	return runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(headerMatcher),
		runtime.WithOutgoingHeaderMatcher(outgoingHeaderMatcher),
	)
}

// RESTOptions tunes the REST gateway's HTTP hardening.
type RESTOptions struct {
	CORSAllowedOrigins []string // exact origins allowed cross-origin ("*" = any); empty = none
	MaxBodyBytes       int64    // request body cap; <= 0 uses DefaultMaxRequestBodyBytes
}

func wrapREST(h http.Handler, opts []RESTOptions) http.Handler {
	var o RESTOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	return limitBody(o.MaxBodyBytes, corsMiddleware(o.CORSAllowedOrigins, h))
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
func NewRESTGateway(ctx context.Context, grpcAddr string, creds credentials.TransportCredentials, ready func() error, ropts ...RESTOptions) (http.Handler, error) {
	mux := newGatewayMux()
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

	return wrapREST(watchInterceptor(mux, conn), ropts), nil
}

// NewRESTGatewayUnix returns an http.Handler that dials the gRPC server via a
// Unix domain socket. Unix sockets are always local, so insecure credentials
// are used regardless of the server's TLS setting. ready gates GET /readyz (nil
// = always ready).
func NewRESTGatewayUnix(ctx context.Context, socketPath string, ready func() error, ropts ...RESTOptions) (http.Handler, error) {
	mux := newGatewayMux()
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

	return wrapREST(watchInterceptor(mux, conn), ropts), nil
}
