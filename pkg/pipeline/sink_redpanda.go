package pipeline

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

func init() {
	RegisterSink("redpanda", func() Sink { return NewRedpandaSink() })
}

// RedpandaSink delivers records to a Redpanda cluster using Kafka wire protocol compatibility.
type RedpandaSink struct {
	mu           sync.RWMutex
	brokers      []string
	defaultTopic string
	writer       KafkaBatchWriter
	customWriter bool
}

// NewRedpandaSink creates a new RedpandaSink.
func NewRedpandaSink() *RedpandaSink {
	return &RedpandaSink{
		brokers:      []string{"127.0.0.1:9092"},
		defaultTopic: "mqtt_telemetry",
	}
}

func (r *RedpandaSink) Name() string {
	return "redpanda"
}

func (r *RedpandaSink) Init(ctx context.Context, config map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if config != nil {
		if bList, ok := config["brokers"].([]string); ok && len(bList) > 0 {
			r.brokers = bList
		} else if sList, ok := config["servers"].([]string); ok && len(sList) > 0 {
			r.brokers = sList
		}
		if topic, ok := config["topic"].(string); ok && topic != "" {
			r.defaultTopic = topic
		}
	}

	if !r.customWriter {
		r.writer = &tcpProbeKafkaWriter{brokers: r.brokers}
	}

	log.Printf("[RedpandaSink] Initialized for brokers=%v, defaultTopic=%s", r.brokers, r.defaultTopic)
	return nil
}

// SetWriter allows injecting a mock or concrete writer for testing/production.
func (r *RedpandaSink) SetWriter(w KafkaBatchWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writer = w
	r.customWriter = true
}

func (r *RedpandaSink) SendBatch(ctx context.Context, records []*Record) ([]*Record, error) {
	r.mu.RLock()
	w := r.writer
	r.mu.RUnlock()

	if w == nil {
		return records, fmt.Errorf("redpanda sink: writer not initialized")
	}

	if err := w.WriteMessages(ctx, records); err != nil {
		return records, err
	}

	return nil, nil
}

func (r *RedpandaSink) HealthCheck(ctx context.Context) error {
	return r.Ping(ctx)
}

func (r *RedpandaSink) Ping(ctx context.Context) error {
	r.mu.RLock()
	brokers := make([]string, len(r.brokers))
	copy(brokers, r.brokers)
	r.mu.RUnlock()

	if len(brokers) == 0 {
		return fmt.Errorf("no redpanda brokers configured")
	}

	var lastErr error
	var d net.Dialer
	d.Timeout = 3 * time.Second
	for _, b := range brokers {
		addr := strings.TrimPrefix(b, "tcp://")
		if !strings.Contains(addr, ":") {
			addr = addr + ":9092"
		}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("all redpanda brokers unreachable: %w", lastErr)
}

// GetBrokers returns configured broker endpoints.
func (r *RedpandaSink) GetBrokers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	res := make([]string, len(r.brokers))
	copy(res, r.brokers)
	return res
}

// UpdateBrokers hot-reloads broker endpoints.
func (r *RedpandaSink) UpdateBrokers(brokers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(brokers) > 0 {
		r.brokers = brokers
		if !r.customWriter {
			r.writer = &tcpProbeKafkaWriter{brokers: brokers}
		}
	}
}

func (r *RedpandaSink) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer != nil {
		return r.writer.Close()
	}
	return nil
}
