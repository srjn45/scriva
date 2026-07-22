package server

import (
	"context"
	"math"
	"sync"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/internal/auth"
)

// Limiter applies server-layer backpressure to incoming RPCs. It combines two
// independent, opt-in controls:
//
//   - a server-wide in-flight semaphore that sheds load with
//     codes.ResourceExhausted once a fixed number of RPCs are concurrently in
//     flight, and
//   - a per-principal token bucket that throttles a single API-key principal to
//     a requested requests-per-second rate without affecting other principals.
//
// Both are disabled at their zero value, so a Limiter built from an
// all-defaults config is a no-op. The type is safe for concurrent use.
type Limiter struct {
	// sem is a counting semaphore of in-flight RPCs. nil when the in-flight cap
	// is disabled (maxInflight <= 0).
	sem chan struct{}

	// rps and burst configure each per-principal token bucket. buckets is nil
	// when rate limiting is disabled (ratePerSec <= 0).
	rps   rate.Limit
	burst int

	mu      sync.Mutex
	buckets map[string]*rate.Limiter
}

// NewLimiter builds a Limiter. maxInflight caps the number of concurrently
// in-flight RPCs across the whole server (0 = unlimited). ratePerSec caps each
// individual principal to that many requests per second (0 = no rate limiting);
// the bucket burst is ratePerSec rounded up, so a short spike up to one second's
// worth of budget is admitted before throttling begins.
func NewLimiter(maxInflight int, ratePerSec float64) *Limiter {
	l := &Limiter{}
	if maxInflight > 0 {
		l.sem = make(chan struct{}, maxInflight)
	}
	if ratePerSec > 0 {
		l.rps = rate.Limit(ratePerSec)
		l.burst = int(math.Ceil(ratePerSec))
		if l.burst < 1 {
			l.burst = 1
		}
		l.buckets = make(map[string]*rate.Limiter)
	}
	return l
}

// Enabled reports whether either control is active. When false the Limiter is a
// no-op and callers may skip chaining its interceptors entirely.
func (l *Limiter) Enabled() bool { return l.sem != nil || l.buckets != nil }

// Interceptors returns unary and stream server interceptors that enforce the
// configured limits. They read the resolved principal the auth interceptor put
// on the context, so they must be chained after auth (and before logging, so a
// shed request is still logged with its ResourceExhausted status).
func (l *Limiter) Interceptors() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := l.allow(ctx); err != nil {
			return nil, err
		}
		release, err := l.acquire()
		if err != nil {
			return nil, err
		}
		defer release()
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := l.allow(ss.Context()); err != nil {
			return err
		}
		release, err := l.acquire()
		if err != nil {
			return err
		}
		defer release()
		return handler(srv, ss)
	}
	return unary, stream
}

// allow enforces the per-principal token bucket. An unauthenticated caller
// (auth disabled) shares a single "anonymous" bucket.
func (l *Limiter) allow(ctx context.Context) error {
	if l.buckets == nil {
		return nil
	}
	name := "anonymous"
	if p, ok := auth.PrincipalFromContext(ctx); ok {
		name = p.Name
	}
	if !l.limiterFor(name).Allow() {
		return status.Errorf(codes.ResourceExhausted, "rate limit exceeded for principal %q", name)
	}
	return nil
}

// limiterFor returns the token bucket for a principal, creating it on first use.
// Each principal gets an independent bucket, so throttling one never affects
// another.
func (l *Limiter) limiterFor(name string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.buckets[name]
	if !ok {
		lim = rate.NewLimiter(l.rps, l.burst)
		l.buckets[name] = lim
	}
	return lim
}

// acquire takes an in-flight slot and returns a release func. When the ceiling
// is saturated it returns codes.ResourceExhausted immediately rather than
// blocking, so the server sheds load instead of queueing it. The returned
// release func is always safe to call, including on the disabled path.
func (l *Limiter) acquire() (func(), error) {
	if l.sem == nil {
		return func() {}, nil
	}
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem }, nil
	default:
		return nil, status.Error(codes.ResourceExhausted, "server in-flight request limit reached")
	}
}
