package server

import (
	"context"
	"io"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/internal/auth"
	pb "github.com/srjn45/scriva/internal/pb/proto"
)

// NewAuditLogger builds the dedicated audit *slog.Logger. It reuses the O1
// structured-logging plumbing (NewLogger) but always emits JSON at info level,
// so the audit stream is a well-formed append-only NDJSON record set regardless
// of the request log's configured format. w is typically the --audit-log file.
func NewAuditLogger(w io.Writer) (*slog.Logger, error) {
	return NewLogger(w, "info", "json")
}

// isAuditedMethod reports whether an RPC is recorded in the audit log on every
// call, regardless of outcome. The audited set is every state-mutating RPC —
// reusing R2's writeMethods so a newly added write RPC is audited automatically —
// plus the admin Promote, the one admin RPC R2 deliberately lets reach a follower.
// Read RPCs, the replication feed, and Snapshot are not audited here; an auth
// failure against any of them is still recorded via the audit interceptor's
// auth-failure branch.
func isAuditedMethod(fullMethod string) bool {
	if isWriteMethod(fullMethod) {
		return true
	}
	return fullMethod == pb.Scriva_Promote_FullMethodName
}

// isAuthFailure reports whether err is an authentication/authorization rejection.
// The auth interceptor (internal/auth) rejects an unauthenticated caller with
// Unauthenticated and a wrong-scope caller with PermissionDenied before any
// handler runs, so these codes mark a rejected-auth outcome that must be audited
// even for an otherwise-unaudited read RPC.
func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}

// auditLogger writes one audit record per audited event to its slog logger.
type auditLogger struct {
	logger *slog.Logger
}

// AuditInterceptors returns unary and stream gRPC interceptors that append one
// audit record per mutating/admin RPC and per auth failure. They MUST be chained
// *outside* the auth interceptor so a rejected-auth call is still observed; to
// recover the resolved principal (which auth stores on a context an outer
// interceptor never sees) they install a principal sink the auth interceptor
// fills in. A record carries the principal, RPC method, target (collection/key/
// id where applicable), and outcome (ok or the gRPC status code).
func AuditInterceptors(logger *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	al := &auditLogger{logger: logger}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, principal := auth.ContextWithPrincipalSink(ctx)
		resp, err := handler(ctx, req)
		al.maybeRecord(ctx, principal, info.FullMethod, req, err)
		return resp, err
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, principal := auth.ContextWithPrincipalSink(ss.Context())
		err := handler(srv, &auditServerStream{ServerStream: ss, ctx: ctx})
		// A streaming RPC has no single request message to derive a target from,
		// and none of the audited methods stream; the auth-failure branch below is
		// what records a rejected read stream.
		al.maybeRecord(ctx, principal, info.FullMethod, nil, err)
		return err
	}
	return unary, stream
}

// maybeRecord writes an audit record when the RPC is audited (mutating/admin) or
// when it was rejected by the auth layer. Everything else — a successful read, a
// read that failed in its handler — is left to the request log, keeping the audit
// stream focused on writes, admin actions, and access denials.
func (al *auditLogger) maybeRecord(ctx context.Context, principal func() (auth.Principal, bool), method string, req any, err error) {
	authFail := isAuthFailure(err)
	if !isAuditedMethod(method) && !authFail {
		return
	}

	name := "anonymous"
	if p, ok := principal(); ok {
		name = p.Name
	} else if authFail {
		name = "unauthenticated"
	}

	outcome := "ok"
	if err != nil {
		outcome = status.Code(err).String()
	}

	attrs := []slog.Attr{
		slog.String("method", method),
		slog.String("principal", name),
		slog.String("outcome", outcome),
	}
	if collection, key, id := auditTarget(req); collection != "" || key != "" || id != 0 {
		if collection != "" {
			attrs = append(attrs, slog.String("collection", collection))
		}
		if key != "" {
			attrs = append(attrs, slog.String("key", key))
		}
		if id != 0 {
			attrs = append(attrs, slog.Uint64("id", id))
		}
	}
	if authFail {
		attrs = append(attrs, slog.Bool("auth_failure", true))
	}
	al.logger.LogAttrs(ctx, slog.LevelInfo, "audit", attrs...)
}

// auditTarget extracts the acted-on target from a request message via the
// generated proto getters: the collection (or, for create/drop, its name), the
// caller-supplied key, and the numeric id — whichever the message carries. A nil
// request (a stream) yields all zero values.
func auditTarget(req any) (collection, key string, id uint64) {
	if r, ok := req.(interface{ GetCollection() string }); ok {
		collection = r.GetCollection()
	}
	if collection == "" {
		if r, ok := req.(interface{ GetName() string }); ok {
			collection = r.GetName()
		}
	}
	if r, ok := req.(interface{ GetKey() string }); ok {
		key = r.GetKey()
	}
	if r, ok := req.(interface{ GetId() uint64 }); ok {
		id = r.GetId()
	}
	return collection, key, id
}

// auditServerStream threads the principal-sink context down to the auth
// interceptor and the handler, mirroring auth's own wrappedServerStream.
type auditServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *auditServerStream) Context() context.Context { return w.ctx }
