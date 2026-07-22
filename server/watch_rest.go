package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	pb "github.com/srjn45/scriva/internal/pb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

		flusher, canFlush := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering if present
		w.WriteHeader(http.StatusOK)

		enc := json.NewEncoder(w)
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					// I4: Normal stream end — return silently.
					return
				}
				// I4: Non-EOF error — write an error envelope before closing.
				_ = enc.Encode(map[string]any{
					"error": map[string]any{
						"message": recvErr.Error(),
						"code":    int(codes.Internal),
					},
				})
				if canFlush {
					flusher.Flush()
				}
				return
			}
			// Wrap in the grpc-gateway envelope so the web client can use the
			// same parsing logic as all other streaming endpoints.
			if encErr := enc.Encode(map[string]any{"result": event}); encErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
	})
}

// isWatchPath reports whether path matches /v1/<collection>/watch.
func isWatchPath(path string) bool {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) == 3 && parts[0] == "v1" && parts[2] == "watch"
}
