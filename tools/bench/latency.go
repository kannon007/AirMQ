package main

import (
	"sort"
	"sync"
	"time"
)

// LatencyRecorder tracks and calculates latency percentiles.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
	maxSize int
}

func NewLatencyRecorder(maxSamples int) *LatencyRecorder {
	if maxSamples <= 0 {
		maxSamples = 100000
	}
	return &LatencyRecorder{
		samples: make([]time.Duration, 0, maxSamples),
		maxSize: maxSamples,
	}
}

func (lr *LatencyRecorder) Record(d time.Duration) {
	lr.mu.Lock()
	if len(lr.samples) < lr.maxSize {
		lr.samples = append(lr.samples, d)
	} else {
		// Reservoir sampling if capacity exceeded
		idx := int(time.Now().UnixNano() % int64(lr.maxSize))
		lr.samples[idx] = d
	}
	lr.mu.Unlock()
}

type LatencyStats struct {
	Count int
	Min   time.Duration
	P50   time.Duration
	P90   time.Duration
	P99   time.Duration
	Max   time.Duration
}

func (lr *LatencyRecorder) Snapshot() LatencyStats {
	lr.mu.Lock()
	n := len(lr.samples)
	if n == 0 {
		lr.mu.Unlock()
		return LatencyStats{}
	}

	cpy := make([]time.Duration, n)
	copy(cpy, lr.samples)
	lr.mu.Unlock()

	sort.Slice(cpy, func(i, j int) bool {
		return cpy[i] < cpy[j]
	})

	return LatencyStats{
		Count: n,
		Min:   cpy[0],
		P50:   cpy[n*50/100],
		P90:   cpy[n*90/100],
		P99:   cpy[n*99/100],
		Max:   cpy[n-1],
	}
}
