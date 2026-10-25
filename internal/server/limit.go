package server

import (
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// limiter holds each client of each service to a rate, as token buckets:
// a bucket holds up to twice the rate, a burst, and fills at the rate.
type limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	swept   time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter() *limiter { return &limiter{buckets: map[string]*bucket{}} }

// allow takes a token of key's bucket, at rate a second: false when it has
// none left.
func (l *limiter) allow(key string, rate int) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	// Buckets full again are the same as none: let them go, once a minute.
	if now.Sub(l.swept) > time.Minute {
		for k, b := range l.buckets {
			if now.Sub(b.last) > time.Minute {
				delete(l.buckets, k)
			}
		}
		l.swept = now
	}
	burst := float64(2 * rate)
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*float64(rate))
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// client is who sends a request, for its rate: the address the proxy in
// front saw (the last of X-Forwarded-For) when the request comes from a
// proxy of one's own, at a private or loopback address; the connection's
// otherwise, which a client can't make up.
func client(r *http.Request) string {
	ip := clientIP(r)
	if f := r.Header.Get("X-Forwarded-For"); f != "" {
		if a, err := netip.ParseAddr(ip); err == nil && (a.Unmap().IsPrivate() || a.Unmap().IsLoopback()) {
			parts := strings.Split(f, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	return ip
}

// maxHops is how many service bindings a request may go through, one
// service calling the next: past it, the services are in a cycle.
const maxHops = 16
