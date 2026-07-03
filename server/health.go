package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthService wraps the standard grpc.health.v1.Health service so the caller
// can flip serving status as listeners come up and as graceful shutdown begins.
// The empty service name ("") is the conventional overall-server status a
// load balancer or Kubernetes gRPC probe watches.
type HealthService struct {
	srv *health.Server
}

// NewHealthService creates a health service. It starts in NOT_SERVING; call
// SetServing once the listeners are accepting connections.
func NewHealthService() *HealthService {
	h := &HealthService{srv: health.NewServer()}
	h.srv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return h
}

// Register attaches the health service to a gRPC server.
func (h *HealthService) Register(s *grpc.Server) {
	healthpb.RegisterHealthServer(s, h.srv)
}

// SetServing marks the overall server SERVING.
func (h *HealthService) SetServing() {
	h.srv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}

// SetNotServing marks the overall server NOT_SERVING so a load balancer drains
// traffic before the process stops accepting new work.
func (h *HealthService) SetNotServing() {
	h.srv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
}

// Shutdown sets every watched service to NOT_SERVING and terminates outstanding
// Watch streams. Call it as graceful shutdown begins.
func (h *HealthService) Shutdown() {
	h.srv.Shutdown()
}

// LivenessHandler reports 200 OK unconditionally: if the process can answer the
// request at all, it is alive.
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// ReadinessHandler reports 200 OK when ready() returns nil and 503 Service
// Unavailable (with the failure reason) otherwise. A nil ready func is treated
// as always ready.
func ReadinessHandler(ready func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if ready != nil {
			if err := ready(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintf(w, "not ready: %v\n", err)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	}
}

// CheckDataDirWritable reports whether dir exists and accepts writes by creating
// and removing a temporary probe file. It underpins the /readyz check: a full or
// read-only data directory makes the node unready even though the process is
// alive.
func CheckDataDirWritable(dir string) error {
	f, err := os.CreateTemp(dir, ".readyz-*")
	if err != nil {
		return fmt.Errorf("data dir %q not writable: %w", dir, err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	// Guard against a surprising CreateTemp success outside dir.
	if filepath.Dir(name) != filepath.Clean(dir) {
		return fmt.Errorf("data dir %q probe wrote to %q", dir, filepath.Dir(name))
	}
	return nil
}
