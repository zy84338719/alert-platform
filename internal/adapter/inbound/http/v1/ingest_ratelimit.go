package v1

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	mw "github.com/oekit/middleware"
	ekiterrors "github.com/oekit/pkg/errors"

	"github.com/example/alert-platform/internal/application/ingest"
)

// Ingest rate limiting (AUD-016): a single noisy/compromised source must not
// be able to saturate the API and its auth path. The bucket key is the
// SERVER-DERIVED identity (tenant|source from the credential, or the edge
// identity), falling back to the client IP only for unauthenticated noise.
type ingestBucket struct {
	tokens float64
	last   time.Time
}

// IngestLimiter is a per-key token bucket with lazy eviction.
type IngestLimiter struct {
	mu        sync.Mutex
	rate      float64 // tokens per second
	burst     float64
	buckets   map[string]*ingestBucket
	now       func() time.Time
	lastSweep time.Time
}

// NewIngestLimiter builds a limiter. rate <= 0 disables limiting (nil is also
// accepted by SetIngestLimiter and by the middleware).
func NewIngestLimiter(rate, burst float64) *IngestLimiter {
	if burst < 1 {
		burst = 1
	}
	return &IngestLimiter{rate: rate, burst: burst, buckets: map[string]*ingestBucket{}, now: time.Now}
}

// Allow consumes one token for key. The second return is the suggested
// Retry-After (rounded up to whole seconds, minimum 1).
func (l *IngestLimiter) Allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if l.rate <= 0 {
		return true, 0
	}
	b := l.buckets[key]
	if b == nil {
		b = &ingestBucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	missing := 1 - b.tokens
	wait := time.Duration(missing / l.rate * float64(time.Second))
	if wait < time.Second {
		wait = time.Second
	}
	l.sweep(now)
	return false, wait
}

// sweep drops buckets that have been idle long enough to be full again. It
// runs at most once a minute and only while denying, so the hot path stays a
// map lookup.
func (l *IngestLimiter) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < time.Minute {
		return
	}
	l.lastSweep = now
	idle := time.Duration(l.burst/l.rate*float64(time.Second)) * 2
	if idle < time.Minute {
		idle = time.Minute
	}
	for k, b := range l.buckets {
		if now.Sub(b.last) > idle {
			delete(l.buckets, k)
		}
	}
}

var ingestLimiter *IngestLimiter

// SetIngestLimiter binds the process-wide ingest limiter (cmd wiring/tests).
func SetIngestLimiter(l *IngestLimiter) { ingestLimiter = l }

// IngestRateLimit rejects over-budget pushes with 429. It must run AFTER
// IngestAuthMiddleware so the bucket key is the authenticated identity.
func IngestRateLimit() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		l := ingestLimiter
		if l == nil {
			c.Next(ctx)
			return
		}
		key := ""
		if src := authenticatedSource(c); src != nil {
			key = src.TenantID + "|source:" + src.SourceID
		} else if ident := authenticatedEdge(c); ident != nil {
			key = ident.TenantID + "|edge:" + ident.EdgeID
		} else {
			key = c.ClientIP()
		}
		ok, wait := l.Allow(key)
		if ok {
			c.Next(ctx)
			return
		}
		secs := int(wait / time.Second)
		if secs < 1 {
			secs = 1
		}
		c.Header("Retry-After", strconv.Itoa(secs))
		ingest.RecordRejected("rate_limited")
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(42901, http.StatusTooManyRequests,
			"ingest rate limit exceeded; retry later"))
	}
}
