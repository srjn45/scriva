package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/internal/auth"
)

// NewLogger builds a *slog.Logger writing to w with the given minimum level
// (debug|info|warn|error) and handler format (json|text). Empty strings select
// the defaults (info, text). It returns an error for an unrecognised level or
// format so a typo fails loudly at startup.
func NewLogger(w io.Writer, level, format string) (*slog.Logger, error) {
	lvl, err := parseLogLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "text":
		h = slog.NewTextHandler(w, opts)
	case "json":
		h = slog.NewJSONHandler(w, opts)
	default:
		return nil, fmt.Errorf("invalid log format %q (want json|text)", format)
	}
	return slog.New(h), nil
}

// parseLogLevel maps a config string to a slog.Level.
func parseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q (want debug|info|warn|error)", level)
	}
}

// LoggingInterceptors returns unary and stream gRPC server interceptors that
// emit exactly one structured record per RPC once it returns, carrying the
// method, resolved principal, wall-clock duration, and gRPC status code. A
// failing RPC is logged at error level; a successful one at info. The principal
// is read from the context the auth interceptor populates, so these must be
// chained after the auth interceptors.
func LoggingInterceptors(logger *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logRPC(ctx, logger, info.FullMethod, start, err)
		return resp, err
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logRPC(ss.Context(), logger, info.FullMethod, start, err)
		return err
	}
	return unary, stream
}

// logRPC writes the per-RPC structured record.
func logRPC(ctx context.Context, logger *slog.Logger, method string, start time.Time, err error) {
	code := codes.OK
	if err != nil {
		code = status.Code(err)
	}
	principal := "anonymous"
	if p, ok := auth.PrincipalFromContext(ctx); ok {
		principal = p.Name
	}
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelError
	}
	logger.LogAttrs(ctx, level, "grpc request",
		slog.String("method", method),
		slog.String("principal", principal),
		slog.Duration("duration", time.Since(start)),
		slog.String("code", code.String()),
	)
}
