package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/srjn45/scriva/internal/pb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// watchInterceptor wraps next with a handler for POST /v1/{collection}/watch.
// All other requests are forwarded to next unchanged.
//
// The grpc-gateway generator does not emit a handler for the Watch RPC; this
// custom handler fills that gap by using the shared conn to the gRPC server and
// streaming the response as newline-delimited JSON in the grpc-gateway envelope
// format:
//
//	{"result":<WatchEvent JSON>}\n
func watchInterceptor(next http.Handler, conn *grpc.ClientConn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !isWatchPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		// Extract collection name from /v1/{collection}/watch.
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		collection := parts[1]

		// C1: Decode the JSON request body to extract an optional filter.
		req := &pb.WatchRequest{Collection: collection}
		if r.Body != nil {
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil {
				http.Error(w, "watch: read body: "+readErr.Error(), http.StatusBadRequest)
				return
			}
			if len(body) > 0 {
				if unmarshalErr := protojson.Unmarshal(body, req); unmarshalErr != nil {
					http.Error(w, "watch: decode body: "+unmarshalErr.Error(), http.StatusBadRequest)
					return
				}
				// Restore the collection from the URL path; the body may or may
				// not include it and the path is authoritative.
				req.Collection = collection
			}
		}

		// Propagate the API key as gRPC metadata.
		apiKey := r.Header.Get("x-api-key")
		ctx := metadata.NewOutgoingContext(r.Context(), metadata.Pairs("x-api-key", apiKey))

		stream, err := pb.NewScrivaClient(conn).Watch(ctx, req)
		if err != nil {
			http.Error(w, "watch: start: "+err.Error(), http.StatusBadGateway)
			return
		}

		// Receive on a single goroutine so the first outcome can be inspected
		// before the HTTP status line is committed: a stream refused at the
		// auth layer (or otherwise failing immediately) must surface as the
		// mapped HTTP status, exactly like every other REST route, rather than
		// as a 200 with an error envelope.
		type recvResult struct {
			ev  *pb.WatchEvent
			err error
		}
		results := make(chan recvResult, 1)
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				ev, recvErr := stream.Recv()
				select {
				case results <- recvResult{ev, recvErr}:
				case <-done:
					return
				}
				if recvErr != nil {
					return
				}
			}
		}()

		var first *recvResult
		grace := time.NewTimer(watchStartGrace)
		select {
		case res := <-results:
			grace.Stop()
			first = &res
			if res.err != nil && !errors.Is(res.err, io.EOF) {
				writeGatewayError(w, res.err)
				return
			}
		case <-grace.C:
		}

		flusher, canFlush := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering if present
		w.WriteHeader(http.StatusOK)
		if canFlush {
			flusher.Flush()
		}

		enc := json.NewEncoder(w)
		for {
			var res recvResult
			if first != nil {
				res, first = *first, nil
			} else {
				select {
				case res = <-results:
				case <-r.Context().Done():
					return
				}
			}
			if res.err != nil {
				if errors.Is(res.err, io.EOF) {
					// I4: Normal stream end — return silently.
					return
				}
				// I4: Non-EOF error — write an error envelope before closing.
				_ = enc.Encode(map[string]any{
					"error": map[string]any{
						"message": res.err.Error(),
						"code":    int(status.Code(res.err)),
					},
				})
				if canFlush {
					flusher.Flush()
				}
				return
			}
			// Wrap in the grpc-gateway envelope so the web client can use the
			// same parsing logic as all other streaming endpoints.
			if encErr := enc.Encode(map[string]any{"result": res.ev}); encErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
	})
}

// watchStartGrace is how long a Watch waits for the stream's first outcome
// before committing a 200: authorization and validation failures arrive
// immediately, a healthy idle watch simply produces nothing.
const watchStartGrace = 200 * time.Millisecond

// writeGatewayError writes err as the grpc-gateway would: the HTTP status
// mapped from its gRPC code and a {code, message} JSON body.
func writeGatewayError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(runtime.HTTPStatusFromCode(st.Code()))
	_ = json.NewEncoder(w).Encode(map[string]any{"code": int(st.Code()), "message": st.Message()})
}

// isWatchPath reports whether path matches /v1/<collection>/watch.
func isWatchPath(path string) bool {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) == 3 && parts[0] == "v1" && parts[2] == "watch"
}
