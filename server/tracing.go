package server

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tracerName is the instrumentation scope reported on every span this package
// emits.
const tracerName = "github.com/srjn45/filedbv2/server"

// These are the OpenTelemetry RPC semantic-convention attribute keys. They are
// spelled out here (rather than pulled from a versioned semconv package) so the
// wire attributes stay stable regardless of which semconv release the OTel SDK
// happens to vendor.
const (
	attrRPCSystem      = "rpc.system"
	attrRPCMethod      = "rpc.method"
	attrRPCStatusCode  = "rpc.grpc.status_code"
	attrDBCollection   = "db.collection"
	rpcSystemGRPCValue = "grpc"
)

// NewTracerProvider builds an OTLP/gRPC-backed TracerProvider that batches spans
// to endpoint (a host:port collector address) and samples a sampleRatio fraction
// of traces (1.0 = every trace). The connection to the collector is insecure —
// tracing is an internal, opt-in diagnostic; front it with a local collector or
// a service-mesh sidecar for transport security.
//
// Tracing is opt-in: the server only calls this when --otlp-endpoint is set. The
// caller owns the returned provider and MUST call Shutdown on graceful stop to
// flush any spans still buffered in the batch processor.
func NewTracerProvider(ctx context.Context, endpoint string, sampleRatio float64) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		"",
		attribute.String("service.name", "filedb"),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		// ParentBased honours an upstream sampling decision (so a gateway → gRPC
		// hop stays in the same trace) and applies the ratio only at the root.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	)
	return tp, nil
}

// TracingInterceptors returns unary and stream server interceptors that start
// one span per RPC named after the full method, tagged with the RPC method and,
// once the call returns, its gRPC status code. Spans are children of any span
// already on the incoming context (e.g. one propagated from the REST gateway).
//
// They should be chained OUTERMOST — before auth/limiter/logging — so the span
// covers the whole handler (including a shed or unauthenticated request's status)
// and so the span-bearing context flows down to the engine scan hook, letting an
// engine.scan span nest under the RPC span.
func TracingInterceptors(tp trace.TracerProvider) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	tracer := tp.Tracer(tracerName)
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, span := tracer.Start(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String(attrRPCSystem, rpcSystemGRPCValue),
				attribute.String(attrRPCMethod, info.FullMethod),
			),
		)
		defer span.End()
		resp, err := handler(ctx, req)
		recordRPCStatus(span, err)
		return resp, err
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, span := tracer.Start(ss.Context(), info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String(attrRPCSystem, rpcSystemGRPCValue),
				attribute.String(attrRPCMethod, info.FullMethod),
			),
		)
		defer span.End()
		err := handler(srv, &tracedServerStream{ServerStream: ss, ctx: ctx})
		recordRPCStatus(span, err)
		return err
	}
	return unary, stream
}

// recordRPCStatus tags the span with the RPC's gRPC status code and marks the
// span errored on failure.
func recordRPCStatus(span trace.Span, err error) {
	code := grpccodes.OK
	if err != nil {
		code = status.Code(err)
	}
	span.SetAttributes(attribute.Int(attrRPCStatusCode, int(code)))
	if err != nil {
		span.SetStatus(otelcodes.Error, code.String())
		span.RecordError(err)
	}
}

// tracedServerStream overrides Context so the span started by the stream
// interceptor propagates to the handler (and thereby to the engine scan hook).
type tracedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (t *tracedServerStream) Context() context.Context { return t.ctx }

// ScanTraceHook returns an engine.CollectionConfig.OnScan hook that records each
// scan as an "engine.scan" span. The hook is handed the scan's context, so the
// span nests under the per-RPC span, revealing which segment scans dominate a
// slow Find. The span's start/end timestamps are reconstructed from the reported
// duration so its extent matches the actual scan.
func ScanTraceHook(tp trace.TracerProvider) func(ctx context.Context, collection string, dur time.Duration) {
	tracer := tp.Tracer(tracerName)
	return func(ctx context.Context, collection string, dur time.Duration) {
		end := time.Now()
		_, span := tracer.Start(ctx, "engine.scan",
			trace.WithTimestamp(end.Add(-dur)),
			trace.WithAttributes(attribute.String(attrDBCollection, collection)),
		)
		span.End(trace.WithTimestamp(end))
	}
}

// CompactionTraceHook returns an engine.CollectionConfig.OnCompaction hook that
// records each compaction run as a root "engine.compaction" span (compaction is
// a background operation with no request context to parent under). Compose it
// with any existing OnCompaction hook — the server chains it after the metrics
// hook so both fire.
func CompactionTraceHook(tp trace.TracerProvider) func(collection string, dur time.Duration) {
	tracer := tp.Tracer(tracerName)
	return func(collection string, dur time.Duration) {
		end := time.Now()
		_, span := tracer.Start(context.Background(), "engine.compaction",
			trace.WithTimestamp(end.Add(-dur)),
			trace.WithAttributes(attribute.String(attrDBCollection, collection)),
		)
		span.End(trace.WithTimestamp(end))
	}
}
