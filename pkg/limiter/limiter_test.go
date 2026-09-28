package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestTokenBucket_BurstAndRefill(t *testing.T) {
	// 10 tokens per second, burst capacity of 5
	tb := NewTokenBucket(10, 5)

	// Consume entire burst
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d to be allowed in burst", i+1)
		}
	}

	// 6th token must be rejected immediately
	if tb.Allow() {
		t.Fatalf("expected 6th token to be rejected when bucket is empty")
	}

	// Wait 250ms (refills ~2.5 tokens)
	time.Sleep(250 * time.Millisecond)

	// Should allow at least 2 tokens
	if !tb.Allow() {
		t.Fatalf("expected token 1 after refill to be allowed")
	}
	if !tb.Allow() {
		t.Fatalf("expected token 2 after refill to be allowed")
	}
}

func TestTokenBucket_Concurrency(t *testing.T) {
	tb := NewTokenBucket(1000, 100)

	var wg sync.WaitGroup
	workers := 10
	allowedCount := 0
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if tb.Allow() {
					mu.Lock()
					allowedCount++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if allowedCount > 105 {
		t.Fatalf("expected at most ~100 tokens granted under instant burst, got %d", allowedCount)
	}
}

func TestConnLimiter(t *testing.T) {
	cfg := ConnLimiterConfig{
		GlobalRate:  100,
		GlobalBurst: 10,
		IPRate:      10,
		IPBurst:     2,
	}
	limiter := NewConnLimiter(cfg)

	// IP 1: allow 2
	if !limiter.Allow("192.168.1.1:1001") {
		t.Fatalf("expected conn 1 from IP1 to be allowed")
	}
	if !limiter.Allow("192.168.1.1:1002") {
		t.Fatalf("expected conn 2 from IP1 to be allowed")
	}
	// IP 1: 3rd rejected
	if limiter.Allow("192.168.1.1:1003") {
		t.Fatalf("expected conn 3 from IP1 to be rejected (exceeded IP burst)")
	}

	// IP 2: distinct IP, should be allowed
	if !limiter.Allow("192.168.1.2:1001") {
		t.Fatalf("expected conn 1 from IP2 to be allowed")
	}
}

func TestPublishLimiter(t *testing.T) {
	cfg := PublishLimiterConfig{
		Rate:  50,
		Burst: 3,
	}
	limiter := NewPublishLimiter(cfg)

	clientA := "sensor-001"
	clientB := "sensor-002"

	// Client A consumes burst
	for i := 0; i < 3; i++ {
		if !limiter.Allow(clientA) {
			t.Fatalf("expected client A msg %d to be allowed", i+1)
		}
	}
	if limiter.Allow(clientA) {
		t.Fatalf("expected client A msg 4 to be rejected")
	}

	// Client B is unaffected
	if !limiter.Allow(clientB) {
		t.Fatalf("expected client B msg 1 to be allowed")
	}

	// Clean up client A
	limiter.Remove(clientA)
}

func BenchmarkLimiter_Allow(b *testing.B) {
	cfg := PublishLimiterConfig{
		Rate:  100000000,
		Burst: 100000000,
	}
	l := NewPublishLimiter(cfg)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = l.Allow("bench-client")
		}
	})
}
