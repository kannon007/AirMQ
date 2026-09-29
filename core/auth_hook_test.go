package core_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mqtt/core"
	"mqtt/pkg/protocol"
)

var authPortSeq int32 = 19300

func getNextAuthAddr() (string, string) {
	port := atomic.AddInt32(&authPortSeq, 1)
	return fmt.Sprintf("tcp://127.0.0.1:%d", port), fmt.Sprintf("127.0.0.1:%d", port)
}

// aclHook implements custom ACL rules:
// - devA can only subscribe and publish to "devices/devA/#"
// - devB can only subscribe and publish to "devices/devB/#"
type aclHook struct {
	core.BaseHook
}

func (h *aclHook) Name() string { return "DeviceACLHook" }

func (h *aclHook) OnAuthorize(ctx *core.ClientContext, action core.AuthAction, topic string) (bool, error) {
	prefix := fmt.Sprintf("devices/%s/", ctx.ClientID)
	// Allow only if topic starts with prefix or equals topic without trailing slash
	if strings.HasPrefix(topic, prefix) || topic == strings.TrimSuffix(prefix, "/") {
		return true, nil
	}
	return false, nil
}

// TestIssue1_SubscribeAuthorization_CrossDeviceLeak verifies Issue 1:
// OnAuthorize is called during SUBSCRIBE. Unauthorized subscriptions are rejected with 0x80 (MQTT 3.1.1)
// or 0x87 (MQTT 5.0), and cross-device telemetry is NEVER leaked.
func TestIssue1_SubscribeAuthorization_CrossDeviceLeak(t *testing.T) {
	gnetAddr, dialAddr := getNextAuthAddr()
	b, err := core.NewBroker(
		core.WithTCP(gnetAddr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithHook(&aclHook{}),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 1. Client A (devA) connects with MQTT 3.1.1
	connA, err := net.Dial("tcp", dialAddr)
	if err != nil {
		t.Fatalf("devA failed to connect: %v", err)
	}
	defer connA.Close()

	sendConnect(t, connA, "devA", protocol.V311)
	readConnack(t, connA)

	// 2. Client A attempts to subscribe to devB's private topic: "devices/devB/#"
	subPkt := &protocol.SubscribePacket{
		PacketID: 10,
		Topics: []protocol.TopicSub{
			{Topic: "devices/devB/#", QoS: 0},
		},
	}
	sendPacket(t, connA, subPkt)

	// In MQTT 3.1.1, unauthorized topic filter in SUBACK MUST return 0x80 (Failure)
	suback := readSuback(t, connA, protocol.V311)
	if len(suback.ReturnCodes) != 1 || suback.ReturnCodes[0] != 0x80 {
		t.Fatalf("Expected SUBACK return code 0x80 (failure), got: 0x%x", suback.ReturnCodes)
	}

	// 3. Client B (devB) connects with MQTT 5.0
	connB, err := net.Dial("tcp", dialAddr)
	if err != nil {
		t.Fatalf("devB failed to connect: %v", err)
	}
	defer connB.Close()

	sendConnect(t, connB, "devB", protocol.V50)
	readConnack(t, connB)

	// Client B attempts to subscribe to devA's private topic: "devices/devA/#"
	subPktB := &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      20,
		Topics: []protocol.TopicSub{
			{Topic: "devices/devA/#", QoS: 1},
		},
	}
	sendPacket(t, connB, subPktB)

	// In MQTT 5.0, SUBACK MUST return ReasonNotAuthorized (0x87)
	subackB := readSuback(t, connB, protocol.V50)
	if len(subackB.ReturnCodes) != 1 || subackB.ReturnCodes[0] != protocol.ReasonNotAuthorized {
		t.Fatalf("Expected SUBACK return code 0x87 (ReasonNotAuthorized), got: 0x%x", subackB.ReturnCodes)
	}

	// 4. Now devB publishes telemetry to "devices/devB/telemetry".
	// Even though devA tried to subscribe to it, devA must NEVER receive it.
	pubPkt := &protocol.PublishPacket{
		Topic:   "devices/devB/telemetry",
		Payload: []byte(`{"temperature":25}`),
		QoS:     0,
	}
	sendPacket(t, connB, pubPkt)

	// Verify devA receives nothing
	assertNoData(t, connA, 300*time.Millisecond)
}

// TestIssue1_PublishAuthorization verifies Issue 1:
// OnAuthorize is called during PUBLISH. Unauthorized publishes are blocked, and for QoS 1 in MQTT 5.0,
// broker returns PUBACK with ReasonNotAuthorized (0x87).
func TestIssue1_PublishAuthorization(t *testing.T) {
	gnetAddr, dialAddr := getNextAuthAddr()
	b, err := core.NewBroker(
		core.WithTCP(gnetAddr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithHook(&aclHook{}),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// devA connects with MQTT 5.0
	connA, err := net.Dial("tcp", dialAddr)
	if err != nil {
		t.Fatalf("devA failed to connect: %v", err)
	}
	defer connA.Close()

	sendConnect(t, connA, "devA", protocol.V50)
	readConnack(t, connA)

	// devA attempts to publish to devB's topic: "devices/devB/cmd" (QoS 1)
	pubPkt := &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		PacketID:      101,
		Topic:         "devices/devB/cmd",
		Payload:       []byte(`{"command":"reboot"}`),
		QoS:           1,
	}
	sendPacket(t, connA, pubPkt)

	// Broker should reject and return PUBACK with ReasonNotAuthorized (0x87)
	puback := readPuback(t, connA, protocol.V50)
	if puback.PacketID != 101 || puback.ReasonCode != protocol.ReasonNotAuthorized {
		t.Fatalf("Expected PUBACK with ReasonNotAuthorized (0x87), got code: 0x%x", puback.ReasonCode)
	}
}

// TestIssue2_RetainedMessageReplay_Authorization verifies Issue 2:
// Retained messages are subjected to OnAuthorize checks before delivery.
func TestIssue2_RetainedMessageReplay_Authorization(t *testing.T) {
	gnetAddr, dialAddr := getNextAuthAddr()
	b, err := core.NewBroker(
		core.WithTCP(gnetAddr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithHook(&aclHook{}),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 1. devB connects and publishes a retained message to "devices/devB/state"
	connB, err := net.Dial("tcp", dialAddr)
	if err != nil {
		t.Fatalf("devB dial failed: %v", err)
	}
	defer connB.Close()

	sendConnect(t, connB, "devB", protocol.V311)
	readConnack(t, connB)

	retainedMsg := &protocol.PublishPacket{
		Topic:   "devices/devB/state",
		Payload: []byte(`{"secret_state":"confidential"}`),
		QoS:     0,
		Retain:  true,
	}
	sendPacket(t, connB, retainedMsg)
	time.Sleep(100 * time.Millisecond)

	// 2. devA connects and subscribes to its own authorized topic "devices/devA/#"
	connA, err := net.Dial("tcp", dialAddr)
	if err != nil {
		t.Fatalf("devA dial failed: %v", err)
	}
	defer connA.Close()

	sendConnect(t, connA, "devA", protocol.V311)
	readConnack(t, connA)

	subA := &protocol.SubscribePacket{
		PacketID: 1,
		Topics: []protocol.TopicSub{
			{Topic: "devices/devA/#", QoS: 0},
		},
	}
	sendPacket(t, connA, subA)
	readSuback(t, connA, protocol.V311)

	// devA must NOT receive devB's retained message
	assertNoData(t, connA, 300*time.Millisecond)
}

// TestIssue3_StoreError_Propagation verifies Issue 3:
// WithPebbleStore and WithBadgerStore surface filesystem errors instead of silently swallowing them.
func TestIssue3_StoreError_Propagation(t *testing.T) {
	// Create a regular temporary file
	tmpFile, err := os.CreateTemp("", "airmq-store-file-*.tmp")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpFilePath)

	// Pebble requires a directory, passing a file must fail
	_, err = core.NewBroker(
		core.WithTCP(":0"),
		core.WithPebbleStore(tmpFilePath),
	)
	if err == nil {
		t.Fatal("Expected NewBroker with invalid PebbleStore path to return an error, got nil")
	}
	if !strings.Contains(err.Error(), "pebble store") {
		t.Fatalf("Expected error message mentioning pebble store, got: %v", err)
	}

	// BadgerDB on an invalid path (file instead of dir) must also fail
	_, err = core.NewBroker(
		core.WithTCP(":0"),
		core.WithBadgerStore(tmpFilePath),
	)
	if err == nil {
		t.Fatal("Expected NewBroker with invalid BadgerStore path to return an error, got nil")
	}
	if !strings.Contains(err.Error(), "badger store") {
		t.Fatalf("Expected error message mentioning badger store, got: %v", err)
	}
}

// TestIssue4_ConnectOnlyHook_PublishBypass verifies Issue 4:
// Registering a connect-only hook does not activate publish hooks (HasPublishHooks remains false).
func TestIssue4_ConnectOnlyHook_PublishBypass(t *testing.T) {
	type connectHook struct {
		core.BaseHook
		connected atomic.Bool
	}

	ch := &connectHook{}
	gnetAddr, dialAddr := getNextAuthAddr()
	b, err := core.NewBroker(
		core.WithTCP(gnetAddr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithHook(ch),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	// HasPublishHooks MUST be false
	if b.Server().HookManager().HasPublishHooks() {
		t.Fatal("Expected HasPublishHooks() to be false when only a connect hook is registered")
	}
	if b.Server().HookManager().HasAuthorizeHooks() {
		t.Fatal("Expected HasAuthorizeHooks() to be false when only a connect hook is registered")
	}

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	conn, err := net.Dial("tcp", dialAddr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	sendConnect(t, conn, "client-test", protocol.V311)
	readConnack(t, conn)
}

// TestPebbleStore_BrokerPersistence_AcrossRestart verifies data persistence across server restarts.
func TestPebbleStore_BrokerPersistence_AcrossRestart(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "airmq-pebble-restart-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pebbleDir := filepath.Join(tempDir, "pebble_data")

	// Phase 1: Start Broker 1 with PebbleStore, publish a retained message
	gnetAddr1, dialAddr1 := getNextAuthAddr()
	b1, err := core.NewBroker(
		core.WithTCP(gnetAddr1),
		core.WithPebbleStore(pebbleDir),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Broker 1 creation failed: %v", err)
	}

	go func() { _ = b1.Start() }()
	time.Sleep(150 * time.Millisecond)

	conn1, err := net.Dial("tcp", dialAddr1)
	if err != nil {
		t.Fatalf("Failed to connect to Broker 1: %v", err)
	}
	sendConnect(t, conn1, "publisher-1", protocol.V311)
	readConnack(t, conn1)

	retainedMsg := &protocol.PublishPacket{
		Topic:   "system/config/mode",
		Payload: []byte("industrial-mode-active"),
		QoS:     0,
		Retain:  true,
	}
	sendPacket(t, conn1, retainedMsg)
	time.Sleep(100 * time.Millisecond)
	conn1.Close()

	// Gracefully stop Broker 1
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	_ = b1.Stop(ctx1)
	cancel1()
	time.Sleep(100 * time.Millisecond)

	// Phase 2: Start Broker 2 on the SAME PebbleStore directory
	gnetAddr2, dialAddr2 := getNextAuthAddr()
	b2, err := core.NewBroker(
		core.WithTCP(gnetAddr2),
		core.WithPebbleStore(pebbleDir),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Broker 2 creation failed with persisted store: %v", err)
	}

	go func() { _ = b2.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel2()
		_ = b2.Stop(ctx2)
	}()

	// Connect a new subscriber to Broker 2 and subscribe to "system/config/+"
	conn2, err := net.Dial("tcp", dialAddr2)
	if err != nil {
		t.Fatalf("Failed to connect to Broker 2: %v", err)
	}
	defer conn2.Close()

	sendConnect(t, conn2, "subscriber-after-restart", protocol.V311)
	readConnack(t, conn2)

	sendPacket(t, conn2, &protocol.SubscribePacket{
		PacketID: 55,
		Topics:   []protocol.TopicSub{{Topic: "system/config/+", QoS: 0}},
	})
	readSuback(t, conn2, protocol.V311)

	// The subscriber must receive the persisted retained message across broker restart
	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 512)
	n, err := conn2.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read retained message after restart: %v", err)
	}
	pkt, _, err := protocol.DecodePacket(buf[:n], protocol.V311)
	if err != nil {
		t.Fatalf("Failed to decode packet: %v", err)
	}
	pubPkt, ok := pkt.(*protocol.PublishPacket)
	if !ok || pubPkt.Topic != "system/config/mode" || string(pubPkt.Payload) != "industrial-mode-active" {
		t.Fatalf("Persisted retained message mismatch: %v", pkt)
	}
}

// Helper test utilities

func sendPacket(t *testing.T, conn net.Conn, pkt protocol.Packet) {
	t.Helper()
	data, err := pkt.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func sendConnect(t *testing.T, conn net.Conn, clientID string, protoLevel byte) {
	t.Helper()
	pkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protoLevel,
		CleanSession:  true,
		ClientID:      clientID,
	}
	sendPacket(t, conn, pkt)
}

func readConnack(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil || n < 4 || buf[0] != 0x20 {
		t.Fatalf("Failed to read CONNACK: %v, n=%d", err, n)
	}
}

func readSuback(t *testing.T, conn net.Conn, protoLevel byte) *protocol.SubackPacket {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil || n < 3 {
		t.Fatalf("Failed to read SUBACK: %v, n=%d", err, n)
	}
	pkt, _, err := protocol.DecodePacket(buf[:n], protoLevel)
	if err != nil {
		t.Fatalf("Failed to decode SUBACK: %v", err)
	}
	suback, ok := pkt.(*protocol.SubackPacket)
	if !ok {
		t.Fatalf("Expected SubackPacket, got: %T", pkt)
	}
	return suback
}

func readPuback(t *testing.T, conn net.Conn, protoLevel byte) *protocol.PubackPacket {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil || n < 4 {
		t.Fatalf("Failed to read PUBACK: %v, n=%d", err, n)
	}
	pkt, _, err := protocol.DecodePacket(buf[:n], protoLevel)
	if err != nil {
		t.Fatalf("Failed to decode PUBACK: %v", err)
	}
	puback, ok := pkt.(*protocol.PubackPacket)
	if !ok {
		t.Fatalf("Expected PubackPacket, got: %T", pkt)
	}
	return puback
}

func assertNoData(t *testing.T, conn net.Conn, d time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if n > 0 {
		t.Fatalf("Expected no data, but received %d bytes: %x", n, buf[:n])
	}
	if err == nil {
		t.Fatalf("Expected timeout error, got nil")
	}
}
