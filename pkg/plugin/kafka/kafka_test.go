package kafka

import (
	"sync"
	"testing"
	"time"

	"mqtt/pkg/hook"
	"mqtt/pkg/protocol"
)

type mockKafkaProducer struct {
	mu       sync.Mutex
	produced []*KafkaMessage
}

func (m *mockKafkaProducer) ProduceBatch(messages []*KafkaMessage) error {
	m.mu.Lock()
	m.produced = append(m.produced, messages...)
	m.mu.Unlock()
	return nil
}

func (m *mockKafkaProducer) Close() error {
	return nil
}

func TestKafkaBridge(t *testing.T) {
	mockProd := &mockKafkaProducer{}
	cfg := KafkaBridgeConfig{
		TopicFilters: []string{"telemetry/#"},
		BatchSize:    5,
		FlushTimeout: 50 * time.Millisecond,
		QueueSize:    100,
	}

	bridge := NewKafkaBridgeHook(cfg, mockProd)
	defer bridge.Close()

	ctx := hook.NewClientContext("device1", "admin", "127.0.0.1:5000")

	// 1. Packet matching filter
	pkt1 := &protocol.PublishPacket{
		Topic:   "telemetry/sensor/1",
		Payload: []byte("25.5"),
	}
	_, err := bridge.OnPublish(ctx, pkt1)
	if err != nil {
		t.Fatalf("OnPublish err: %v", err)
	}

	// 2. Packet not matching filter
	pkt2 := &protocol.PublishPacket{
		Topic:   "commands/reboot",
		Payload: []byte("now"),
	}
	_, _ = bridge.OnPublish(ctx, pkt2)

	// Wait for flush ticker
	time.Sleep(100 * time.Millisecond)

	mockProd.mu.Lock()
	count := len(mockProd.produced)
	mockProd.mu.Unlock()

	if count != 1 {
		t.Fatalf("Expected 1 message bridged, got %d", count)
	}
}
