package pipeline

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestRedpandaSink_RegistrationAndInit(t *testing.T) {
	sink, err := CreateSink("redpanda")
	if err != nil {
		t.Fatalf("expected redpanda sink to be registered, got error: %v", err)
	}

	if sink.Name() != "redpanda" {
		t.Errorf("expected sink name 'redpanda', got %s", sink.Name())
	}

	cfg := map[string]any{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "iot.redpanda",
	}

	if err := sink.Init(context.Background(), cfg); err != nil {
		t.Fatalf("failed to init redpanda sink: %v", err)
	}

	rpSink, ok := sink.(*RedpandaSink)
	if !ok {
		t.Fatalf("expected *RedpandaSink type")
	}

	brokers := rpSink.GetBrokers()
	if len(brokers) != 1 || brokers[0] != "127.0.0.1:9092" {
		t.Errorf("unexpected brokers: %v", brokers)
	}
}

func TestRedpandaSink_MockTCPPing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock listener: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	sink := NewRedpandaSink()
	_ = sink.Init(context.Background(), map[string]any{
		"brokers": []string{listener.Addr().String()},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sink.Ping(ctx); err != nil {
		t.Errorf("expected Ping to succeed against mock Redpanda server, got: %v", err)
	}

	if err := sink.HealthCheck(ctx); err != nil {
		t.Errorf("expected HealthCheck to succeed, got: %v", err)
	}
}

func TestRedpandaSink_SendBatch(t *testing.T) {
	sink := NewRedpandaSink()
	_ = sink.Init(context.Background(), nil)

	rec := AcquireRecord()
	rec.ID = 202
	rec.Topic = "sensors/telemetry"
	rec.Target = "redpanda.events"
	rec.Value = []byte("hello redpanda")

	failed, err := sink.SendBatch(context.Background(), []*Record{rec})
	if err != nil {
		t.Errorf("expected SendBatch to succeed with default probe writer, got: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed records, got %d", len(failed))
	}
	ReleaseRecord(rec)
}
