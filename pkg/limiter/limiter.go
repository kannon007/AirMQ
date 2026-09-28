package limiter

import (
	"net"
	"sync"
	"time"
)

// TokenBucket implements a thread-safe token bucket rate limiter.
type TokenBucket struct {
	rate       float64 // Tokens added per second
	burst      float64 // Maximum burst capacity
	tokens     float64 // Currently available tokens
	lastRefill time.Time
	mu         sync.Mutex
}

// NewTokenBucket creates a token bucket with rate (tokens/sec) and burst capacity.
func NewTokenBucket(rate float64, burst int64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		burst:      float64(burst),
		tokens:     float64(burst),
		lastRefill: time.Now(),
	}
}

// Allow consumes 1 token if available, returning true if allowed or false if rate limited.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(time.Now(), 1)
}

// AllowN consumes n tokens if available at time `now`.
func (tb *TokenBucket) AllowN(now time.Time, n float64) bool {
	if tb == nil || tb.rate <= 0 {
		return true // Unlimited
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Calculate refilled tokens based on elapsed duration
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.burst {
		tb.tokens = tb.burst
	}

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}

	return false
}

// ConnLimiterConfig configures connection rate limiting.
type ConnLimiterConfig struct {
	GlobalRate  float64 // Max new connections per second across all IPs (0 for unlimited)
	GlobalBurst int64   // Global burst capacity
	IPRate      float64 // Max new connections per second per IP (0 for unlimited)
	IPBurst     int64   // Per-IP burst capacity
}

// ConnLimiter throttles incoming connection handshake rates to prevent connection storms.
type ConnLimiter struct {
	cfg       ConnLimiterConfig
	global    *TokenBucket
	ipBuckets sync.Map // string (IP) -> *TokenBucket
}

// NewConnLimiter creates a new ConnLimiter.
func NewConnLimiter(cfg ConnLimiterConfig) *ConnLimiter {
	var global *TokenBucket
	if cfg.GlobalRate > 0 {
		global = NewTokenBucket(cfg.GlobalRate, cfg.GlobalBurst)
	}
	return &ConnLimiter{
		cfg:    cfg,
		global: global,
	}
}

// Allow checks whether a connection from remoteAddr is permitted under current rate limits.
func (cl *ConnLimiter) Allow(remoteAddr string) bool {
	if cl == nil {
		return true
	}

	// 1. Check global rate limit
	if cl.global != nil && !cl.global.Allow() {
		return false
	}

	// 2. Check per-IP rate limit
	if cl.cfg.IPRate > 0 {
		host, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			host = remoteAddr
		}
		bucketVal, ok := cl.ipBuckets.Load(host)
		if !ok {
			bucket := NewTokenBucket(cl.cfg.IPRate, cl.cfg.IPBurst)
			bucketVal, _ = cl.ipBuckets.LoadOrStore(host, bucket)
		}
		return bucketVal.(*TokenBucket).Allow()
	}

	return true
}

// PublishLimiterConfig configures per-client message publish rate limiting.
type PublishLimiterConfig struct {
	Rate  float64 // Max PUBLISH packets per second per client (0 for unlimited)
	Burst int64   // Burst capacity
}

// PublishLimiter controls message ingestion rates per client to prevent runaway devices or spammers.
type PublishLimiter struct {
	cfg     PublishLimiterConfig
	buckets sync.Map // clientID -> *TokenBucket
}

// NewPublishLimiter creates a new PublishLimiter.
func NewPublishLimiter(cfg PublishLimiterConfig) *PublishLimiter {
	return &PublishLimiter{
		cfg: cfg,
	}
}

// Allow checks if the given clientID is allowed to publish another message.
func (pl *PublishLimiter) Allow(clientID string) bool {
	if pl == nil || pl.cfg.Rate <= 0 {
		return true // Unlimited
	}

	bucketVal, ok := pl.buckets.Load(clientID)
	if !ok {
		bucket := NewTokenBucket(pl.cfg.Rate, pl.cfg.Burst)
		bucketVal, _ = pl.buckets.LoadOrStore(clientID, bucket)
	}

	return bucketVal.(*TokenBucket).Allow()
}

// Remove cleans up the limiter state when a client disconnects.
func (pl *PublishLimiter) Remove(clientID string) {
	if pl != nil {
		pl.buckets.Delete(clientID)
	}
}
