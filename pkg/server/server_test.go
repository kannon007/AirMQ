package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mqtt/pkg/pipeline"
	"mqtt/pkg/protocol"
)

func TestEndToEndBrokerPubSub(t *testing.T) {
	addr := "tcp://127.0.0.1:18883"
	srv := NewServer(Config{
		Addr:         addr,
		Multicore:    false,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)

	// Start server in background
	go func() {
		_ = srv.Start()
	}()
	defer srv.Stop(context.Background())

	// Allow server time to bind
	time.Sleep(200 * time.Millisecond)

	// --- 1. Client 1 (Subscriber) Connects ---
	conn1, err := net.Dial("tcp", "127.0.0.1:18883")
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn1.Close()

	// Send CONNECT
	connectPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		KeepAlive:     60,
		ClientID:      "subscriber-1",
	}
	// Raw connect packet: 10 bytes variable header + 14 bytes payload (ClientID "subscriber-1") = 24 (0x18)
	connectRaw := []byte{
		0x10, 0x18, // CONNECT, remaining length 24
		0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, // MQTT, level 4, clean session
		0x00, 0x3C, // KeepAlive 60s
		0x00, 0x0C, 's', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e', 'r', '-', '1',
	}
	_ = connectPkt
	_ = conn1.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = conn1.Write(connectRaw)
	if err != nil {
		t.Fatalf("conn1 write CONNECT failed: %v", err)
	}

	// Read CONNACK (4 bytes: 0x20, 0x02, 0x00, 0x00)
	respBuf := make([]byte, 64)
	n, err := conn1.Read(respBuf)
	if err != nil || n < 4 {
		t.Fatalf("Failed to read CONNACK: %v, n=%d", err, n)
	}
	if respBuf[0] != 0x20 || respBuf[3] != 0x00 {
		t.Fatalf("Invalid CONNACK: %x", respBuf[:n])
	}

	// Send SUBSCRIBE to "sensor/+/temperature" with PacketID 100
	subPkt := &protocol.SubscribePacket{
		PacketID: 100,
		Topics: []protocol.TopicSub{
			{Topic: "sensor/+/temperature", QoS: 0},
		},
	}
	// Raw subscribe packet
	topic := "sensor/+/temperature"
	remLen := 2 + 2 + len(topic) + 1
	subRaw := []byte{0x82, byte(remLen), 0x00, 0x64, 0x00, byte(len(topic))}
	subRaw = append(subRaw, []byte(topic)...)
	subRaw = append(subRaw, 0x00) // QoS 0
	_ = subPkt

	_, err = conn1.Write(subRaw)
	if err != nil {
		t.Fatalf("conn1 write SUBSCRIBE failed: %v", err)
	}

	// Read SUBACK
	n, err = conn1.Read(respBuf)
	if err != nil || n < 5 {
		t.Fatalf("Failed to read SUBACK: %v, n=%d", err, n)
	}
	if respBuf[0] != 0x90 {
		t.Fatalf("Expected SUBACK (0x90), got 0x%x", respBuf[0])
	}

	// --- 2. Client 2 (Publisher) Connects ---
	conn2, err := net.Dial("tcp", "127.0.0.1:18883")
	if err != nil {
		t.Fatalf("Failed to dial server for pub: %v", err)
	}
	defer conn2.Close()

	connectRawPub := []byte{
		0x10, 0x17, // CONNECT, length 23 (10 + 2 + 11)
		0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02,
		0x00, 0x3C,
		0x00, 0x0B, 'p', 'u', 'b', 'l', 'i', 's', 'h', 'e', 'r', '-', '2',
	}
	_ = conn2.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn2.Write(connectRawPub)
	n, _ = conn2.Read(respBuf)
	if respBuf[0] != 0x20 {
		t.Fatalf("conn2 failed to connect")
	}

	// Client 2 publishes to "sensor/living/temperature" with payload "26.8"
	pubPkt := &protocol.PublishPacket{
		Topic:   "sensor/living/temperature",
		Payload: []byte("26.8"),
		QoS:     0,
	}
	pubRaw, _ := pubPkt.Encode()
	_, err = conn2.Write(pubRaw)
	if err != nil {
		t.Fatalf("conn2 publish failed: %v", err)
	}

	// --- 3. Verify Client 1 receives the published message ---
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = conn1.Read(respBuf)
	if err != nil {
		t.Fatalf("Client 1 timed out or failed to receive forwarded PUBLISH: %v", err)
	}

	pkt, consumed, err := protocol.DecodePacket(respBuf[:n])
	if err != nil || consumed == 0 {
		t.Fatalf("Failed to decode forwarded packet: %v", err)
	}

	receivedPub, ok := pkt.(*protocol.PublishPacket)
	if !ok {
		t.Fatalf("Expected PUBLISH packet, got %T", pkt)
	}

	if receivedPub.Topic != "sensor/living/temperature" {
		t.Errorf("Topic mismatch: got %q, want 'sensor/living/temperature'", receivedPub.Topic)
	}
	if !bytes.Equal(receivedPub.Payload, []byte("26.8")) {
		t.Errorf("Payload mismatch: got %q, want '26.8'", string(receivedPub.Payload))
	}

	// --- 4. Verify PINGREQ / PINGRESP ---
	pingReq, _ := (&protocol.PingreqPacket{}).Encode()
	_, _ = conn1.Write(pingReq)
	n, err = conn1.Read(respBuf)
	if err != nil || respBuf[0] != 0xD0 {
		t.Fatalf("Expected PINGRESP (0xD0), got 0x%x, err: %v", respBuf[0], err)
	}
}

func TestServer_StreamingPipeline_EndToEnd(t *testing.T) {
	addr := "tcp://127.0.0.1:18894"
	spoolDir := filepath.Join(os.TempDir(), fmt.Sprintf("pipe_server_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(spoolDir)

	mockSink := pipeline.NewMockSink()
	pipe, err := pipeline.NewPipeline(pipeline.Config{
		BatchSize:      10,
		FlushTimeout:   10 * time.Millisecond,
		RingBufferSize: 1024,
		NumShards:      2,
		SpoolDir:       spoolDir,
	}, mockSink)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	srv := NewServer(Config{
		Addr:         addr,
		Multicore:    false,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)
	srv.SetPipeline(pipe)

	go func() {
		_ = srv.Start()
	}()
	defer srv.Stop(context.Background())

	time.Sleep(200 * time.Millisecond)

	// Connect publisher client
	conn, err := net.Dial("tcp", "127.0.0.1:18894")
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	// Send CONNECT
	connectPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "stream-publisher",
	}
	data, _ := connectPkt.Encode()
	_, _ = conn.Write(data)

	respBuf := make([]byte, 1024)
	n, err := conn.Read(respBuf)
	if err != nil || n < 4 || respBuf[0] != 0x20 || respBuf[3] != 0x00 {
		t.Fatalf("Failed to receive valid CONNACK: %v", err)
	}

	// Publish 25 messages
	msgCount := 25
	for i := 0; i < msgCount; i++ {
		pub := &protocol.PublishPacket{
			Topic:   fmt.Sprintf("factory/machine_%d/vibration", i%3),
			Payload: []byte(fmt.Sprintf(`{"val":%d}`, i)),
			QoS:     0,
		}
		data, _ := pub.Encode()
		_, err = conn.Write(data)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Wait for pipeline egress worker to flush batch
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(mockSink.DeliveredRecords()) >= msgCount {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	delivered := mockSink.DeliveredRecords()
	if len(delivered) != msgCount {
		t.Fatalf("Expected %d delivered records in mockSink, got %d", msgCount, len(delivered))
	}

	// Verify content of first delivered record
	if delivered[0].Topic != "factory/machine_0/vibration" {
		t.Errorf("Unexpected topic in sink record: %s", delivered[0].Topic)
	}
}
