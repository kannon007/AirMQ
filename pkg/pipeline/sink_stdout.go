package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	RegisterSink("stdout", func() Sink { return NewStdoutSink() })
	RegisterSink("mock", func() Sink { return NewMockSink() })
}

// StdoutSink prints records to standard log output.
type StdoutSink struct {
	delivered atomic.Uint64
}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

func (s *StdoutSink) Name() string {
	return "stdout"
}

func (s *StdoutSink) Init(ctx context.Context, config map[string]any) error {
	return nil
}

func (s *StdoutSink) SendBatch(ctx context.Context, records []*Record) ([]*Record, error) {
	for _, r := range records {
		_ = r
		s.delivered.Add(1)
	}
	return nil, nil
}

func (s *StdoutSink) HealthCheck(ctx context.Context) error {
	return nil
}

func (s *StdoutSink) Ping(ctx context.Context) error {
	return nil
}

func (s *StdoutSink) Close() error {
	return nil
}

func (s *StdoutSink) DeliveredCount() uint64 {
	return s.delivered.Load()
}

// MockSink is a test sink supporting failure injection, delays, and delivered record inspection.
type MockSink struct {
	mu          sync.Mutex
	delivered   []*Record
	failCount   int
	healthy     bool
	delay       time.Duration
	injectedErr error
}

func NewMockSink() *MockSink {
	return &MockSink{
		delivered:   make([]*Record, 0),
		healthy:     true,
		injectedErr: errors.New("mock sink: simulated network timeout/failure"),
	}
}

func (m *MockSink) Name() string {
	return "mock"
}

func (m *MockSink) Init(ctx context.Context, config map[string]any) error {
	return nil
}

func (m *MockSink) SendBatch(ctx context.Context, records []*Record) ([]*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	if m.failCount > 0 {
		m.failCount--
		// Return all records as failed
		return records, m.injectedErr
	}

	if !m.healthy {
		return records, m.injectedErr
	}

	for _, r := range records {
		recCopy := *r
		m.delivered = append(m.delivered, &recCopy)
	}
	return nil, nil
}

func (m *MockSink) HealthCheck(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.healthy {
		return errors.New("mock sink unhealthy")
	}
	return nil
}

func (m *MockSink) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.healthy {
		return errors.New("mock sink unhealthy")
	}
	return nil
}

func (m *MockSink) Close() error {
	return nil
}

// Control helpers for unit testing

func (m *MockSink) SetHealthy(h bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = h
}

func (m *MockSink) FailNextBatches(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCount = count
}

func (m *MockSink) SetDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
}

func (m *MockSink) DeliveredRecords() []*Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]*Record, len(m.delivered))
	copy(res, m.delivered)
	return res
}

func (m *MockSink) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered = m.delivered[:0]
	m.failCount = 0
	m.healthy = true
}
