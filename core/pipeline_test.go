package core_test

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"mqtt/core"
	"mqtt/pkg/protocol"
)

func TestCoreBroker_Pipeline_AuthAndValidation(t *testing.T) {
	addr := "tcp://127.0.0.1:28896"

	// 1. Compose an idiomatic Go Pipe with self-contained Processors
	pipe := core.NewPipe()

	// Processor 1: Auth (self-contained Match and Process)
	pipe.Add("auth_guard", core.NewAuthProcessor(func(clientID, username string) bool {
		return clientID == "trusted_sensor" || clientID == "$internal"
	}))

	// Processor 2: JSON Validator (self-contained Match and Process)
	pipe.Add("json_validator", core.NewValidateProcessor(
		core.NewJSONValidator("syntax_check"),
	))

	// Processor 3: Transform / Enrich (self-contained Match and Process)
	pipe.Add("enricher", core.NewTransformProcessor(func(msg *core.Message) error {
		enriched := fmt.Sprintf(`{"raw":%s,"gateway":"edge_01"}`, string(msg.Payload))
		msg.SetPayload([]byte(enriched))
		return nil
	}))

	// 2. Initialize standalone Broker with the pipeline bound to "telemetry/#"
	b, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithPipe("telemetry/#", pipe),
	)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	go func() {
		_ = b.Start()
	}()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	var receivedCount atomic.Int32
	var lastPayload atomic.Pointer[string]

	unsub, err := b.Subscribe("telemetry/#", 0, func(topic string, payload []byte) {
		receivedCount.Add(1)
		str := string(payload)
		lastPayload.Store(&str)
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer unsub()

	// 3. Connect TCP client
	conn, err := net.Dial("tcp", "127.0.0.1:28896")
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// Case A: Unauthorized Client -> Must be rejected by AuthProcessor
	connPktBad := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "unauthorized_attacker",
	}
	connData, _ := connPktBad.Encode()
	_, _ = conn.Write(connData)
	respBuf := make([]byte, 1024)
	_, _ = conn.Read(respBuf)

	pubPkt1 := &protocol.PublishPacket{
		Topic:   "telemetry/sensor1",
		Payload: []byte(`{"val":10}`),
		QoS:     0,
	}
	pubData1, _ := pubPkt1.Encode()
	_, _ = conn.Write(pubData1)

	time.Sleep(100 * time.Millisecond)
	if receivedCount.Load() != 0 {
		t.Fatalf("Expected 0 messages delivered (unauthorized client should be rejected), got %d", receivedCount.Load())
	}
	_ = conn.Close()

	// Case B: Authorized Client but INVALID JSON -> Must be dropped by ValidateProcessor
	conn2, err := net.Dial("tcp", "127.0.0.1:28896")
	if err != nil {
		t.Fatalf("Failed to redial: %v", err)
	}
	defer conn2.Close()

	connPktGood := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "trusted_sensor",
	}
	connData2, _ := connPktGood.Encode()
	_, _ = conn2.Write(connData2)
	_, _ = conn2.Read(respBuf)

	pubPktBadJSON := &protocol.PublishPacket{
		Topic:   "telemetry/sensor1",
		Payload: []byte(`NOT_JSON_DATA`),
		QoS:     0,
	}
	pubDataBad, _ := pubPktBadJSON.Encode()
	_, _ = conn2.Write(pubDataBad)

	time.Sleep(100 * time.Millisecond)
	if receivedCount.Load() != 0 {
		t.Fatalf("Expected 0 messages delivered (invalid JSON should be dropped), got %d", receivedCount.Load())
	}

	// Case C: Authorized Client and VALID JSON -> Must be enriched and delivered
	pubPktGood := &protocol.PublishPacket{
		Topic:   "telemetry/sensor1",
		Payload: []byte(`{"val":42}`),
		QoS:     0,
	}
	pubDataGood, _ := pubPktGood.Encode()
	_, _ = conn2.Write(pubDataGood)

	time.Sleep(100 * time.Millisecond)
	if receivedCount.Load() != 1 {
		t.Fatalf("Expected 1 message delivered, got %d", receivedCount.Load())
	}
	expected := `{"raw":{"val":42},"gateway":"edge_01"}`
	if lastPayload.Load() == nil || *lastPayload.Load() != expected {
		t.Errorf("Unexpected delivered payload: got %v, want %s", lastPayload.Load(), expected)
	}

	// Verify stats table generation
	stats := pipe.Stats()
	if len(stats) != 3 {
		t.Fatalf("Expected 3 processor stats, got %d", len(stats))
	}
}

func TestCoreBroker_Pipeline_DynamicAddAndRemove(t *testing.T) {
	addr := "tcp://127.0.0.1:28897"

	b, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	go func() {
		_ = b.Start()
	}()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	var deliveredCount atomic.Int32
	unsub, _ := b.Subscribe("devices/#", 0, func(topic string, payload []byte) {
		deliveredCount.Add(1)
	})
	defer unsub()

	// 1. Initial publish passes through (no pipeline registered)
	_ = b.Publish("devices/machine1", []byte("ok"), 0, false)
	time.Sleep(50 * time.Millisecond)
	if deliveredCount.Load() != 1 {
		t.Fatalf("Expected 1 message delivered, got %d", deliveredCount.Load())
	}

	// 2. Dynamically attach processor via b.AddProcessorFunc
	b.AddProcessorFunc("devices/#", "security_guard",
		func(c *core.Context) bool {
			// Pre-condition: only inspect blocked_payload
			return string(c.Message.Payload) == "blocked_payload"
		},
		func(c *core.Context) error {
			c.Drop("blocked")
			return nil
		},
	)

	// 3. Publish blocked payload -> should be dropped immediately
	_ = b.Publish("devices/machine1", []byte("blocked_payload"), 0, false)
	time.Sleep(50 * time.Millisecond)
	if deliveredCount.Load() != 1 {
		t.Fatalf("Expected count to remain 1 (blocked_payload should be dropped), got %d", deliveredCount.Load())
	}

	// 4. Dynamically remove pipeline
	removed := b.RemovePipeline("devices/#")
	if !removed {
		t.Fatal("Expected RemovePipeline to return true")
	}

	// 5. Publish again -> now it passes through
	_ = b.Publish("devices/machine1", []byte("blocked_payload"), 0, false)
	time.Sleep(50 * time.Millisecond)
	if deliveredCount.Load() != 2 {
		t.Fatalf("Expected count to be 2 after removing pipeline, got %d", deliveredCount.Load())
	}
}

func TestCoreBroker_WithProcessorOption(t *testing.T) {
	addr := "tcp://127.0.0.1:28895"

	var processedCount atomic.Int32

	b, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithProcessor("metrics/#", "metric_counter",
			core.NewFuncProcessor(
				func(c *core.Context) bool { return true },
				func(c *core.Context) error {
					processedCount.Add(1)
					return nil
				},
			),
		),
	)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	go func() {
		_ = b.Start()
	}()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	_ = b.Publish("metrics/cpu", []byte("80%"), 0, false)
	time.Sleep(50 * time.Millisecond)

	if processedCount.Load() != 1 {
		t.Fatalf("Expected processor to process 1 message, got %d", processedCount.Load())
	}
}
