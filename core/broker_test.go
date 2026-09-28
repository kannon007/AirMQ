package core_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mqtt/core"
	"mqtt/pkg/protocol"
)

func TestStandaloneBrokerLifecycle(t *testing.T) {
	addr := "tcp://127.0.0.1:18890"
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

	stats := b.GetOverview()
	if stats.NodeName == "" {
		t.Errorf("Expected non-empty NodeName, got empty")
	}

	listeners := b.GetListeners()
	if len(listeners) == 0 {
		t.Errorf("Expected at least 1 listener, got 0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Stop(ctx); err != nil {
		t.Fatalf("Broker stop error: %v", err)
	}
}

func TestInProcessPubSubAllTopics(t *testing.T) {
	addr := "tcp://127.0.0.1:18891"
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

	var receivedCount atomic.Int32
	var receivedTopic sync.Map

	// 1. Subscribe to ALL messages using "#"
	unsub, err := b.Subscribe("#", 0, func(topic string, payload []byte) {
		receivedCount.Add(1)
		receivedTopic.Store(topic, string(payload))
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer unsub()

	// 2. Publish several in-process messages
	msgs := map[string]string{
		"sensors/temp":     "23.5",
		"devices/status":   "running",
		"events/alarm/cpu": "high",
	}

	for top, pl := range msgs {
		if err := b.Publish(top, []byte(pl), 0, false); err != nil {
			t.Fatalf("Failed to publish to %s: %v", top, err)
		}
	}

	// Verify all received
	time.Sleep(100 * time.Millisecond)
	if count := receivedCount.Load(); count != 3 {
		t.Fatalf("Expected 3 messages received, got %d", count)
	}

	for top, pl := range msgs {
		val, ok := receivedTopic.Load(top)
		if !ok || val.(string) != pl {
			t.Errorf("Message for %s mismatch: expected %q, got %v", top, pl, val)
		}
	}

	// Test Unsubscribe
	unsub()
	_ = b.Publish("another/topic", []byte("val"), 0, false)
	time.Sleep(50 * time.Millisecond)
	if count := receivedCount.Load(); count != 3 {
		t.Errorf("Received message after unsubscribe: got count %d, expected 3", count)
	}
}

func TestInProcessAndTCPInteroperability(t *testing.T) {
	addr := "tcp://127.0.0.1:18892"
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

	// 1. In-process subscriber subscribes to "telemetry/#"
	inProcessReceived := make(chan string, 5)
	unsub, err := b.Subscribe("telemetry/#", 0, func(topic string, payload []byte) {
		inProcessReceived <- fmt.Sprintf("%s:%s", topic, string(payload))
	})
	if err != nil {
		t.Fatalf("Failed to subscribe in-process: %v", err)
	}
	defer unsub()

	// 2. Connect external TCP client
	conn, err := net.Dial("tcp", "127.0.0.1:18892")
	if err != nil {
		t.Fatalf("Failed to dial TCP listener: %v", err)
	}
	defer conn.Close()

	// Handshake: CONNECT
	connPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "tcp-test-publisher",
	}
	connRaw, _ := connPkt.Encode()
	if _, err := conn.Write(connRaw); err != nil {
		t.Fatalf("Failed to write CONNECT: %v", err)
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil || n < 4 {
		t.Fatalf("Failed to read CONNACK: %v", err)
	}

	// Publish via TCP socket: "telemetry/v1/voltage" -> "220V"
	pubPkt := &protocol.PublishPacket{
		ProtocolLevel: protocol.V311,
		Topic:         "telemetry/v1/voltage",
		Payload:       []byte("220V"),
		QoS:           protocol.QoS0,
	}
	pubRaw, _ := pubPkt.Encode()
	if _, err := conn.Write(pubRaw); err != nil {
		t.Fatalf("Failed to send PUBLISH over TCP: %v", err)
	}

	// Verify in-process subscriber received it
	select {
	case msg := <-inProcessReceived:
		if msg != "telemetry/v1/voltage:220V" {
			t.Errorf("Unexpected message: %s", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for in-process subscriber to receive TCP message")
	}

	// 3. Test reverse: TCP client subscribes to "commands/#", in-process publishes
	cmdTopic := "commands/#"
	subRaw := []byte{0x82, byte(2 + 2 + len(cmdTopic) + 1), 0x00, 0x01, 0x00, byte(len(cmdTopic))}
	subRaw = append(subRaw, []byte(cmdTopic)...)
	subRaw = append(subRaw, 0x00) // QoS 0
	if _, err := conn.Write(subRaw); err != nil {
		t.Fatalf("Failed to write SUBSCRIBE: %v", err)
	}

	// Read SUBACK
	n, err = conn.Read(buf)
	if err != nil || n < 5 || buf[0] != 0x90 {
		t.Fatalf("Failed to receive SUBACK: %v, buf=%x", err, buf[:n])
	}

	// In-process publish to "commands/restart"
	if err := b.Publish("commands/restart", []byte("now"), 0, false); err != nil {
		t.Fatalf("In-process publish failed: %v", err)
	}

	// Read PUBLISH on TCP socket
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, err = conn.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("TCP client failed to receive in-process publish: %v", err)
	}
	if buf[0]>>4 != protocol.PUBLISH {
		t.Errorf("Expected PUBLISH packet, got 0x%x", buf[0])
	}
}

func TestRetainedMessages(t *testing.T) {
	addr := "tcp://127.0.0.1:18893"
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

	// 1. Publish retained message
	err = b.Publish("device/001/state", []byte(`{"status":"online"}`), 0, true)
	if err != nil {
		t.Fatalf("Failed to publish retained: %v", err)
	}

	// 2. Query retained messages
	retained, err := b.GetRetainedMessages()
	if err != nil {
		t.Fatalf("Failed to get retained messages: %v", err)
	}
	if len(retained) != 1 {
		t.Fatalf("Expected 1 retained message, got %d", len(retained))
	}
	if retained[0].Topic != "device/001/state" {
		t.Errorf("Expected topic 'device/001/state', got %s", retained[0].Topic)
	}

	// 3. Delete retained message
	if err := b.DeleteRetainedMessage("device/001/state"); err != nil {
		t.Fatalf("Failed to delete retained message: %v", err)
	}

	retainedAfter, _ := b.GetRetainedMessages()
	if len(retainedAfter) != 0 {
		t.Errorf("Expected 0 retained messages after deletion, got %d", len(retainedAfter))
	}
}
