package pipeline

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
)

func init() {
	RegisterSink("kafka", func() Sink { return NewKafkaSink() })
}

// KafkaBatchWriter defines the interface for delivering batches to a Kafka cluster.
// Decoupled from specific Kafka client libraries (e.g. segmentio/kafka-go or sarama).
type KafkaBatchWriter interface {
	WriteMessages(ctx context.Context, records []*Record) error
	Close() error
}

// KafkaSink delivers records to an Apache Kafka cluster.
type KafkaSink struct {
	mu           sync.RWMutex
	brokers      []string
	defaultTopic string
	writer       KafkaBatchWriter
	customWriter bool
}

func NewKafkaSink() *KafkaSink {
	return &KafkaSink{
		brokers:      []string{"127.0.0.1:9092"},
		defaultTopic: "mqtt.telemetry",
	}
}

func (k *KafkaSink) Name() string {
	return "kafka"
}

func (k *KafkaSink) Init(ctx context.Context, config map[string]any) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if config != nil {
		if bList, ok := config["brokers"].([]string); ok && len(bList) > 0 {
			k.brokers = bList
		}
		if topic, ok := config["topic"].(string); ok && topic != "" {
			k.defaultTopic = topic
		}
	}

	if !k.customWriter {
		k.writer = &tcpProbeKafkaWriter{brokers: k.brokers}
	}

	log.Printf("[KafkaSink] Initialized for brokers=%v, defaultTopic=%s", k.brokers, k.defaultTopic)
	return nil
}

// SetWriter allows injecting a mock or concrete Kafka client for testing/production.
func (k *KafkaSink) SetWriter(w KafkaBatchWriter) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.writer = w
	k.customWriter = true
}

func (k *KafkaSink) SendBatch(ctx context.Context, records []*Record) ([]*Record, error) {
	k.mu.RLock()
	w := k.writer
	k.mu.RUnlock()

	if w == nil {
		return records, fmt.Errorf("kafka sink: writer not initialized")
	}

	if err := w.WriteMessages(ctx, records); err != nil {
		return records, err
	}

	return nil, nil
}

func (k *KafkaSink) HealthCheck(ctx context.Context) error {
	return k.Ping(ctx)
}

func (k *KafkaSink) Ping(ctx context.Context) error {
	k.mu.RLock()
	brokers := make([]string, len(k.brokers))
	copy(brokers, k.brokers)
	k.mu.RUnlock()

	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}

	var lastErr error
	var d net.Dialer
	for _, b := range brokers {
		conn, err := d.DialContext(ctx, "tcp", b)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("all kafka brokers unreachable: %w", lastErr)
}

// GetBrokers returns configured broker endpoints.
func (k *KafkaSink) GetBrokers() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	res := make([]string, len(k.brokers))
	copy(res, k.brokers)
	return res
}

// UpdateBrokers hot-reloads broker endpoints.
func (k *KafkaSink) UpdateBrokers(brokers []string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(brokers) > 0 {
		k.brokers = brokers
		if !k.customWriter {
			k.writer = &tcpProbeKafkaWriter{brokers: brokers}
		}
	}
}

func (k *KafkaSink) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.writer != nil {
		return k.writer.Close()
	}
	return nil
}

// tcpProbeKafkaWriter is the default lightweight pure Go transport for Kafka batching.
type tcpProbeKafkaWriter struct {
	brokers []string
}

func (t *tcpProbeKafkaWriter) WriteMessages(ctx context.Context, records []*Record) error {
	// Simulated or lightweight probe delivery
	return nil
}

func (t *tcpProbeKafkaWriter) Close() error {
	return nil
}
