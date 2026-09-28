package pipeline

import (
	"sync"
	"time"
)

type CircuitState int

const (
	CircuitClosed   CircuitState = 1 // Normal: direct delivery
	CircuitOpen     CircuitState = 2 // Tripped: sink down, redirect to disk spooler
	CircuitHalfOpen CircuitState = 3 // Probing: testing sink recovery
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type CircuitBreakerConfig struct {
	FailureThreshold int           // Number of consecutive failures to trip open (default 5)
	RecoveryTimeout  time.Duration // Time to stay in Open state before attempting Half-Open (default 3s)
	SuccessThreshold int           // Consecutive successes in Half-Open to restore Closed (default 2)
}

// CircuitBreaker guards against cascading failures when an external sink is degraded.
type CircuitBreaker struct {
	mu             sync.RWMutex
	cfg            CircuitBreakerConfig
	state          CircuitState
	consecFailures int
	consecSuccess  int
	openedAt       time.Time
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 3 * time.Second
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}

	return &CircuitBreaker{
		cfg:   cfg,
		state: CircuitClosed,
	}
}

// Allow reports whether a direct send attempt to the sink should be permitted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		if now.Sub(cb.openedAt) >= cb.cfg.RecoveryTimeout {
			// Transition to Half-Open probe
			cb.state = CircuitHalfOpen
			cb.consecSuccess = 0
			return true
		}
		return false

	case CircuitHalfOpen:
		// In Half-Open, allow single probe attempt
		return true

	default:
		return false
	}
}

// RecordSuccess records a successful batch delivery to the sink.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecFailures = 0

	if cb.state == CircuitHalfOpen {
		cb.consecSuccess++
		if cb.consecSuccess >= cb.cfg.SuccessThreshold {
			cb.state = CircuitClosed
			cb.consecSuccess = 0
		}
	}
}

// RecordFailure records a delivery failure or timeout to the sink.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecFailures++
	cb.consecSuccess = 0

	if cb.state == CircuitHalfOpen || cb.consecFailures >= cb.cfg.FailureThreshold {
		cb.state = CircuitOpen
		cb.openedAt = time.Now()
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}
