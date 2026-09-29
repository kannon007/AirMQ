package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mqtt/pkg/protocol"
)

var portSeq int32 = 19100

func getNextAddr() (string, string) {
	port := atomic.AddInt32(&portSeq, 1)
	return fmt.Sprintf("tcp://127.0.0.1:%d", port), fmt.Sprintf("127.0.0.1:%d", port)
}

func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	gnetAddr, dialAddr := getNextAddr()
	srv := NewServer(Config{
		Addr:         gnetAddr,
		Multicore:    false,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)

	go func() {
		_ = srv.Start()
	}()

	time.Sleep(150 * time.Millisecond)

	cleanup := func() {
		_ = srv.Stop(context.Background())
		time.Sleep(50 * time.Millisecond)
	}
	return dialAddr, cleanup
}

func sendPkt(t *testing.T, conn net.Conn, pkt protocol.Packet) {
	t.Helper()
	data, err := pkt.Encode()
	if err != nil {
		t.Fatalf("Failed to encode packet: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("Failed to write packet: %v", err)
	}
}

var connBufMap sync.Map // net.Conn -> *bytes.Buffer

func recvPkt(t *testing.T, conn net.Conn, protoLevel byte) protocol.Packet {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	val, _ := connBufMap.LoadOrStore(conn, new(bytes.Buffer))
	buf := val.(*bytes.Buffer)

	for {
		if buf.Len() >= 2 {
			pkt, consumed, err := protocol.DecodePacket(buf.Bytes(), protoLevel)
			if err != nil {
				t.Fatalf("Failed to decode packet: %v", err)
			}
			if pkt != nil {
				buf.Next(consumed)
				return pkt
			}
		}

		tmp := make([]byte, 1024)
		n, err := conn.Read(tmp)
		if err != nil {
			t.Fatalf("Failed to read from conn: %v", err)
		}
		buf.Write(tmp[:n])

		pkt, consumed, err := protocol.DecodePacket(buf.Bytes(), protoLevel)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}
		if pkt != nil {
			buf.Next(consumed)
			return pkt
		}
	}
}

func assertNoIncoming(t *testing.T, conn net.Conn, waitDur time.Duration) {
	t.Helper()
	if val, ok := connBufMap.Load(conn); ok {
		buf := val.(*bytes.Buffer)
		if buf.Len() > 0 {
			t.Fatalf("Expected no incoming packet, but buffer contains %d bytes: %x", buf.Len(), buf.Bytes())
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(waitDur))
	tmp := make([]byte, 256)
	n, err := conn.Read(tmp)
	if err == nil && n > 0 {
		t.Fatalf("Expected no incoming packet, but received %d bytes: %x", n, tmp[:n])
	}
}

// ---------------------------------------------------------------------------------
// MQTT 3.1.1 Conformance Tests
// ---------------------------------------------------------------------------------

func TestMQTT311_ConnectConnack(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Send CONNECT
	sendPkt(t, conn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "c311-test-1",
		KeepAlive:     60,
	})

	// 2. Expect CONNACK
	resp := recvPkt(t, conn, protocol.V311)
	connack, ok := resp.(*protocol.ConnackPacket)
	if !ok {
		t.Fatalf("Expected ConnackPacket, got %T", resp)
	}
	if connack.ReturnCode != protocol.CodeAccepted {
		t.Fatalf("Expected ReturnCode 0, got %d", connack.ReturnCode)
	}

	// 3. Pingreq / Pingresp
	sendPkt(t, conn, &protocol.PingreqPacket{})
	pingResp := recvPkt(t, conn, protocol.V311)
	if _, ok := pingResp.(*protocol.PingrespPacket); !ok {
		t.Fatalf("Expected PingrespPacket, got %T", pingResp)
	}
}

func TestMQTT311_QoS0_PubSub(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-qos0",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 1,
		Topics:   []protocol.TopicSub{{Topic: "test/qos0", QoS: 0}},
	})
	suback := recvPkt(t, subConn, protocol.V311).(*protocol.SubackPacket)
	if len(suback.ReturnCodes) == 0 || suback.ReturnCodes[0] != 0 {
		t.Fatalf("Suback return code mismatch: %v", suback.ReturnCodes)
	}

	// Publisher
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pub-qos0",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	payload := []byte("hello-qos0")
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "test/qos0",
		Payload: payload,
		QoS:     0,
	})

	// Subscriber receives PUBLISH
	gotPub := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if gotPub.Topic != "test/qos0" || !bytes.Equal(gotPub.Payload, payload) {
		t.Fatalf("Pub message mismatch: topic=%s, payload=%s", gotPub.Topic, string(gotPub.Payload))
	}
}

func TestMQTT311_QoS1_PubSub(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-qos1",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 10,
		Topics:   []protocol.TopicSub{{Topic: "test/qos1", QoS: 1}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// Publisher
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pub-qos1",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	payload := []byte("hello-qos1-reliable")
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:    "test/qos1",
		Payload:  payload,
		QoS:      1,
		PacketID: 55,
	})

	// Broker sends PUBACK to publisher
	puback := recvPkt(t, pubConn, protocol.V311).(*protocol.PubackPacket)
	if puback.PacketID != 55 {
		t.Fatalf("Expected PUBACK PacketID 55, got %d", puback.PacketID)
	}

	// Subscriber receives message with QoS 1
	gotPub := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if gotPub.QoS != 1 || !bytes.Equal(gotPub.Payload, payload) {
		t.Fatalf("Received message mismatch: qos=%d, payload=%s", gotPub.QoS, string(gotPub.Payload))
	}
	// Subscriber replies PUBACK
	sendPkt(t, subConn, &protocol.PubackPacket{PacketID: gotPub.PacketID})
}

func TestMQTT311_QoS2_PubSub(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-qos2",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 20,
		Topics:   []protocol.TopicSub{{Topic: "test/qos2", QoS: 2}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// Publisher
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pub-qos2",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	payload := []byte("exactly-once-qos2")
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:    "test/qos2",
		Payload:  payload,
		QoS:      2,
		PacketID: 88,
	})

	// 1. Broker replies PUBREC
	pubrec := recvPkt(t, pubConn, protocol.V311).(*protocol.PubrecPacket)
	if pubrec.PacketID != 88 {
		t.Fatalf("Expected PUBREC PacketID 88, got %d", pubrec.PacketID)
	}

	// Message should NOT yet be delivered to subscriber before PUBREL!
	assertNoIncoming(t, subConn, 80*time.Millisecond)

	// 2. Publisher sends PUBREL
	sendPkt(t, pubConn, &protocol.PubrelPacket{PacketID: 88})

	// 3. Broker replies PUBCOMP
	pubcomp := recvPkt(t, pubConn, protocol.V311).(*protocol.PubcompPacket)
	if pubcomp.PacketID != 88 {
		t.Fatalf("Expected PUBCOMP PacketID 88, got %d", pubcomp.PacketID)
	}

	// 4. Subscriber now receives QoS 2 message
	gotPub := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if gotPub.QoS != 2 || !bytes.Equal(gotPub.Payload, payload) {
		t.Fatalf("Subscriber message mismatch: qos=%d, payload=%s", gotPub.QoS, string(gotPub.Payload))
	}
}

func TestMQTT311_RetainMessage(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// 1. Publisher publishes retained message
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pub-retain",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	retainedData := []byte("initial-retained-state")
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "device/status",
		Payload: retainedData,
		QoS:     0,
		Retain:  true,
	})

	time.Sleep(50 * time.Millisecond)

	// 2. New Subscriber connects afterwards and subscribes
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-retain",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 30,
		Topics:   []protocol.TopicSub{{Topic: "device/status", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311) // SUBACK

	// Subscriber should immediately receive the retained message
	gotPub := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if !bytes.Equal(gotPub.Payload, retainedData) || !gotPub.Retain {
		t.Fatalf("Retained message mismatch: payload=%s, retain=%v", string(gotPub.Payload), gotPub.Retain)
	}

	// 3. Clear retained message with empty payload
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "device/status",
		Payload: nil,
		QoS:     0,
		Retain:  true,
	})
	time.Sleep(50 * time.Millisecond)

	// 4. Another Subscriber connects, should NOT receive any retained message
	subConn2, _ := net.Dial("tcp", addr)
	defer subConn2.Close()
	sendPkt(t, subConn2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-retain-2",
	})
	_ = recvPkt(t, subConn2, protocol.V311)

	sendPkt(t, subConn2, &protocol.SubscribePacket{
		PacketID: 31,
		Topics:   []protocol.TopicSub{{Topic: "device/status", QoS: 0}},
	})
	_ = recvPkt(t, subConn2, protocol.V311) // SUBACK

	assertNoIncoming(t, subConn2, 80*time.Millisecond)
}

func TestMQTT311_LastWillAndTestament(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber listening for will topic
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-lwt",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 40,
		Topics:   []protocol.TopicSub{{Topic: "devices/c1/status", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// Client 1 with Will: abnormal disconnect (close socket directly without DISCONNECT)
	lwtConn, _ := net.Dial("tcp", addr)
	willMsg := []byte("c1-offline-abnormally")
	sendPkt(t, lwtConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "client-with-will",
		WillFlag:      true,
		WillTopic:     "devices/c1/status",
		WillMessage:   willMsg,
		WillQoS:       0,
	})
	_ = recvPkt(t, lwtConn, protocol.V311)

	// Abrupt close!
	lwtConn.Close()

	// Subscriber should receive the LWT message
	willPub := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if willPub.Topic != "devices/c1/status" || !bytes.Equal(willPub.Payload, willMsg) {
		t.Fatalf("LWT message mismatch: topic=%s, payload=%s", willPub.Topic, string(willPub.Payload))
	}
}

func TestMQTT311_WildcardTopics(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-wildcards",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// Subscribe to "building/+/floor/#"
	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 50,
		Topics:   []protocol.TopicSub{{Topic: "building/+/floor/#", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pub-wildcards",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	// 1. Should match "building/A/floor/2/room/101"
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "building/A/floor/2/room/101",
		Payload: []byte("match-1"),
		QoS:     0,
	})
	msg1 := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if string(msg1.Payload) != "match-1" {
		t.Fatalf("Expected match-1, got %s", string(msg1.Payload))
	}

	// 2. Should NOT match "building/A/room/101"
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "building/A/room/101",
		Payload: []byte("nomatch"),
		QoS:     0,
	})
	assertNoIncoming(t, subConn, 80*time.Millisecond)
}

// ---------------------------------------------------------------------------------
// MQTT 5.0 Conformance Tests
// ---------------------------------------------------------------------------------

func TestMQTT50_Connect_AssignedClientID(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Connect with empty ClientID
	sendPkt(t, conn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "", // Empty ClientID triggers server assignment
	})

	resp := recvPkt(t, conn, protocol.V50)
	connack, ok := resp.(*protocol.ConnackPacket)
	if !ok {
		t.Fatalf("Expected ConnackPacket, got %T", resp)
	}
	if connack.ReasonCode != protocol.ReasonSuccess {
		t.Fatalf("Expected ReasonSuccess, got 0x%02x", connack.ReasonCode)
	}
	if connack.Properties == nil || connack.Properties.AssignedClientIdentifier == "" {
		t.Fatalf("Expected AssignedClientIdentifier in CONNACK properties, got %+v", connack.Properties)
	}
	if connack.Properties.WildcardSubscriptionAvailable == nil || *connack.Properties.WildcardSubscriptionAvailable != 1 {
		t.Fatalf("Expected WildcardSubscriptionAvailable=1")
	}
}

func TestMQTT50_UserProperties_Propagation(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "sub-v5-props",
	})
	_ = recvPkt(t, subConn, protocol.V50)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      1,
		Topics:        []protocol.TopicSub{{Topic: "orders/new", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V50)

	// Publisher sends message with User Properties
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "pub-v5-props",
	})
	_ = recvPkt(t, pubConn, protocol.V50)

	pubProps := &protocol.Properties{
		ContentType: "application/json",
	}
	pubProps.AddUserProperty("trace-id", "req-xyz-987")
	pubProps.AddUserProperty("source", "order-service")

	sendPkt(t, pubConn, &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		Topic:         "orders/new",
		Payload:       []byte(`{"order_id":12345}`),
		QoS:           0,
		Properties:    pubProps,
	})

	// Subscriber receives message with identical User Properties
	gotMsg := recvPkt(t, subConn, protocol.V50).(*protocol.PublishPacket)
	if gotMsg.Properties == nil {
		t.Fatalf("Expected MQTT 5.0 properties on received message")
	}
	if gotMsg.Properties.ContentType != "application/json" {
		t.Fatalf("ContentType mismatch: %s", gotMsg.Properties.ContentType)
	}
	if len(gotMsg.Properties.UserProperties) != 2 {
		t.Fatalf("Expected 2 user properties, got %d", len(gotMsg.Properties.UserProperties))
	}
	if gotMsg.Properties.UserProperties[0].Key != "trace-id" || gotMsg.Properties.UserProperties[0].Value != "req-xyz-987" {
		t.Fatalf("User property 0 mismatch: %+v", gotMsg.Properties.UserProperties[0])
	}
}

func TestMQTT50_NoLocal_Option(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Client 1: Subscribes with NoLocal = true
	c1, _ := net.Dial("tcp", addr)
	defer c1.Close()
	sendPkt(t, c1, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "client-nolocal-test",
	})
	_ = recvPkt(t, c1, protocol.V50)

	sendPkt(t, c1, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      10,
		Topics: []protocol.TopicSub{
			{Topic: "chat/general", QoS: 0, NoLocal: true},
		},
	})
	_ = recvPkt(t, c1, protocol.V50)

	// Client 2: Subscribes with NoLocal = false (normal)
	c2, _ := net.Dial("tcp", addr)
	defer c2.Close()
	sendPkt(t, c2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "client-normal-sub",
	})
	_ = recvPkt(t, c2, protocol.V50)

	sendPkt(t, c2, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      11,
		Topics: []protocol.TopicSub{
			{Topic: "chat/general", QoS: 0, NoLocal: false},
		},
	})
	_ = recvPkt(t, c2, protocol.V50)

	// Now Client 1 publishes to "chat/general"
	sendPkt(t, c1, &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		Topic:         "chat/general",
		Payload:       []byte("from-c1"),
		QoS:           0,
	})

	// Client 2 MUST receive the message
	c2Msg := recvPkt(t, c2, protocol.V50).(*protocol.PublishPacket)
	if string(c2Msg.Payload) != "from-c1" {
		t.Fatalf("c2 message mismatch: %s", string(c2Msg.Payload))
	}

	// Client 1 MUST NOT receive its own message (NoLocal active!)
	assertNoIncoming(t, c1, 100*time.Millisecond)
}

func TestMQTT50_RetainHandling(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Publisher publishes a retained message
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "pub-rh",
	})
	_ = recvPkt(t, pubConn, protocol.V50)

	sendPkt(t, pubConn, &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		Topic:         "config/param",
		Payload:       []byte("v1.0.0"),
		QoS:           0,
		Retain:        true,
	})
	time.Sleep(50 * time.Millisecond)

	// Client with RetainHandling = 2 (Do NOT send retained message on subscribe)
	cRH2, _ := net.Dial("tcp", addr)
	defer cRH2.Close()
	sendPkt(t, cRH2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "client-rh-2",
	})
	_ = recvPkt(t, cRH2, protocol.V50)

	sendPkt(t, cRH2, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      1,
		Topics: []protocol.TopicSub{
			{Topic: "config/param", QoS: 0, RetainHandling: 2},
		},
	})
	_ = recvPkt(t, cRH2, protocol.V50) // SUBACK

	// MUST NOT receive retained message
	assertNoIncoming(t, cRH2, 80*time.Millisecond)

	// Client with RetainHandling = 0 (Always send retained message on subscribe)
	cRH0, _ := net.Dial("tcp", addr)
	defer cRH0.Close()
	sendPkt(t, cRH0, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "client-rh-0",
	})
	_ = recvPkt(t, cRH0, protocol.V50)

	sendPkt(t, cRH0, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      2,
		Topics: []protocol.TopicSub{
			{Topic: "config/param", QoS: 0, RetainHandling: 0},
		},
	})
	_ = recvPkt(t, cRH0, protocol.V50) // SUBACK

	// MUST receive retained message
	rh0Msg := recvPkt(t, cRH0, protocol.V50).(*protocol.PublishPacket)
	if string(rh0Msg.Payload) != "v1.0.0" {
		t.Fatalf("Expected v1.0.0, got %s", string(rh0Msg.Payload))
	}
}

func TestMQTT50_QoS2_ReasonCodes(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	sendPkt(t, conn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "v5-qos2-client",
	})
	_ = recvPkt(t, conn, protocol.V50)

	// Publish QoS 2
	sendPkt(t, conn, &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		Topic:         "v5/qos2/test",
		Payload:       []byte("v5-reliable"),
		QoS:           2,
		PacketID:      99,
	})

	// PUBREC should carry ReasonSuccess (0x00)
	pubrec := recvPkt(t, conn, protocol.V50).(*protocol.PubrecPacket)
	if pubrec.PacketID != 99 || pubrec.ReasonCode != protocol.ReasonSuccess {
		t.Fatalf("Expected PUBREC PID 99 ReasonSuccess, got pid=%d code=%d", pubrec.PacketID, pubrec.ReasonCode)
	}

	// Send PUBREL
	sendPkt(t, conn, &protocol.PubrelPacket{
		PacketID:   99,
		ReasonCode: protocol.ReasonSuccess,
	})

	// PUBCOMP should carry ReasonSuccess (0x00)
	pubcomp := recvPkt(t, conn, protocol.V50).(*protocol.PubcompPacket)
	if pubcomp.PacketID != 99 || pubcomp.ReasonCode != protocol.ReasonSuccess {
		t.Fatalf("Expected PUBCOMP PID 99 ReasonSuccess, got pid=%d code=%d", pubcomp.PacketID, pubcomp.ReasonCode)
	}
}

func TestMQTT50_DisconnectWithReasonCode(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "sub-v5-disc",
	})
	_ = recvPkt(t, subConn, protocol.V50)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      1,
		Topics:        []protocol.TopicSub{{Topic: "v5/node/status", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V50)

	// Publisher with Will sends DISCONNECT with ReasonDisconnectWithWill (0x04)
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "pub-v5-disc",
		WillFlag:      true,
		WillTopic:     "v5/node/status",
		WillMessage:   []byte("node-faulted"),
		WillQoS:       0,
	})
	_ = recvPkt(t, pubConn, protocol.V50)

	// Send DISCONNECT with 0x04 (DisconnectWithWill)
	sendPkt(t, pubConn, &protocol.DisconnectPacket{
		ReasonCode: protocol.ReasonDisconnectWithWill,
	})

	// Subscriber should receive the Will message!
	willMsg := recvPkt(t, subConn, protocol.V50).(*protocol.PublishPacket)
	if string(willMsg.Payload) != "node-faulted" {
		t.Fatalf("Expected LWT 'node-faulted', got %s", string(willMsg.Payload))
	}
}

func TestMQTT50_AuthPacket(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	sendPkt(t, conn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "auth-client",
	})
	_ = recvPkt(t, conn, protocol.V50)

	// Send AUTH packet
	sendPkt(t, conn, &protocol.AuthPacket{
		ReasonCode: protocol.ReasonSuccess,
	})

	// Expect AUTH response with ReasonSuccess
	resp := recvPkt(t, conn, protocol.V50)
	authResp, ok := resp.(*protocol.AuthPacket)
	if !ok {
		t.Fatalf("Expected AuthPacket response, got %T", resp)
	}
	if authResp.ReasonCode != protocol.ReasonSuccess {
		t.Fatalf("Expected ReasonSuccess, got 0x%02x", authResp.ReasonCode)
	}
}

// ---------------------------------------------------------------------------------
// Phase 1 Robustness & Production Edge Case Tests
// ---------------------------------------------------------------------------------

func TestMQTT_PersistentSession_OfflineReplay(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	clientID := "sub-persistent-client"

	// 1. Subscriber connects with CleanSession = false
	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	sendPkt(t, c1, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  false,
		ClientID:      clientID,
	})
	connack1 := recvPkt(t, c1, protocol.V311).(*protocol.ConnackPacket)
	if connack1.ReturnCode != protocol.CodeAccepted {
		t.Fatalf("Expected CodeAccepted, got %d", connack1.ReturnCode)
	}

	// 2. Subscribe to "alerts/critical" with QoS 1
	sendPkt(t, c1, &protocol.SubscribePacket{
		PacketID: 101,
		Topics:   []protocol.TopicSub{{Topic: "alerts/critical", QoS: 1}},
	})
	_ = recvPkt(t, c1, protocol.V311) // SUBACK

	// 3. Subscriber disconnects cleanly
	sendPkt(t, c1, &protocol.DisconnectPacket{})
	c1.Close()
	time.Sleep(50 * time.Millisecond)

	// 4. Publisher connects and publishes message while subscriber is offline!
	pubConn, _ := net.Dial("tcp", addr)
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "publisher-alerts",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	offlinePayload := []byte("battery-low-voltage-warning")
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:    "alerts/critical",
		Payload:  offlinePayload,
		QoS:      1,
		PacketID: 201,
	})
	_ = recvPkt(t, pubConn, protocol.V311) // PUBACK to publisher
	pubConn.Close()

	// 5. Subscriber reconnects with CleanSession = false
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}
	defer c2.Close()

	sendPkt(t, c2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  false,
		ClientID:      clientID,
	})

	// CONNACK must indicate SessionPresent = true
	connack2 := recvPkt(t, c2, protocol.V311).(*protocol.ConnackPacket)
	if !connack2.SessionPresent {
		t.Fatalf("Expected SessionPresent=true on reconnect")
	}

	// Subscriber MUST immediately receive the replayed offline message!
	replayedMsg := recvPkt(t, c2, protocol.V311).(*protocol.PublishPacket)
	if replayedMsg.Topic != "alerts/critical" || !bytes.Equal(replayedMsg.Payload, offlinePayload) {
		t.Fatalf("Replayed message mismatch: topic=%s, payload=%s", replayedMsg.Topic, string(replayedMsg.Payload))
	}
	if replayedMsg.QoS != 1 {
		t.Fatalf("Expected replayed QoS 1, got %d", replayedMsg.QoS)
	}

	// Send PUBACK
	sendPkt(t, c2, &protocol.PubackPacket{PacketID: replayedMsg.PacketID})
}

func TestMQTT_KeepAliveTimeout_Eviction(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// 1. Subscriber monitoring the will topic
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-heartbeat-monitor",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 1,
		Topics:   []protocol.TopicSub{{Topic: "status/zombie", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// 2. Client with KeepAlive = 1 second and Will configured
	willMsg := []byte("zombie-died-of-heartbeat-timeout")
	zombieConn, _ := net.Dial("tcp", addr)
	sendPkt(t, zombieConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "zombie-client",
		KeepAlive:     1, // 1 second keepalive!
		WillFlag:      true,
		WillTopic:     "status/zombie",
		WillMessage:   willMsg,
		WillQoS:       0,
	})
	_ = recvPkt(t, zombieConn, protocol.V311)

	// Client stays completely silent without PINGREQ.
	// After 1.5 * 1s = 1.5s (within ~2-3s), server should evict it and fire the Will message!
	willPkt := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if willPkt.Topic != "status/zombie" || !bytes.Equal(willPkt.Payload, willMsg) {
		t.Fatalf("Expected LWT from heartbeat eviction: %s", string(willPkt.Payload))
	}
}

func TestMQTT_Publish_TopicWildcard_Rejected(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	sendPkt(t, conn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "bad-pub-client",
	})
	_ = recvPkt(t, conn, protocol.V311)

	// Attempt to publish to topic with wildcard '+'
	sendPkt(t, conn, &protocol.PublishPacket{
		Topic:   "sensor/+/temperature",
		Payload: []byte("malicious"),
		QoS:     0,
	})

	// Server MUST close the socket
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 64)
	_, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("Expected connection to be closed after invalid topic publish")
	}
}

func TestMQTT311_LastWillAndTestament_CleanDisconnect(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-lwt-clean",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 41,
		Topics:   []protocol.TopicSub{{Topic: "devices/clean/status", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	lwtConn, _ := net.Dial("tcp", addr)
	defer lwtConn.Close()
	sendPkt(t, lwtConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "client-clean-disconnect",
		WillFlag:      true,
		WillTopic:     "devices/clean/status",
		WillMessage:   []byte("unexpected-offline"),
		WillQoS:       0,
	})
	_ = recvPkt(t, lwtConn, protocol.V311)

	// Send clean DISCONNECT
	sendPkt(t, lwtConn, &protocol.DisconnectPacket{})
	time.Sleep(100 * time.Millisecond)

	// Subscriber must NEVER receive the will message
	assertNoIncoming(t, subConn, 200*time.Millisecond)
}

func TestMQTT50_SharedSubscription_RoundRobin(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Worker 1
	connW1, _ := net.Dial("tcp", addr)
	defer connW1.Close()
	sendPkt(t, connW1, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanSession:  true,
		ClientID:      "worker-1",
	})
	_ = recvPkt(t, connW1, protocol.V50)
	sendPkt(t, connW1, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      1,
		Topics:        []protocol.TopicSub{{Topic: "$share/jobs/tasks/+", QoS: 0}},
	})
	_ = recvPkt(t, connW1, protocol.V50)

	// Worker 2
	connW2, _ := net.Dial("tcp", addr)
	defer connW2.Close()
	sendPkt(t, connW2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanSession:  true,
		ClientID:      "worker-2",
	})
	_ = recvPkt(t, connW2, protocol.V50)
	sendPkt(t, connW2, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      2,
		Topics:        []protocol.TopicSub{{Topic: "$share/jobs/tasks/+", QoS: 0}},
	})
	_ = recvPkt(t, connW2, protocol.V50)

	time.Sleep(50 * time.Millisecond)

	// Publisher
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanSession:  true,
		ClientID:      "producer",
	})
	_ = recvPkt(t, pubConn, protocol.V50)

	totalJobs := 10
	for i := 1; i <= totalJobs; i++ {
		sendPkt(t, pubConn, &protocol.PublishPacket{
			ProtocolLevel: protocol.V50,
			Topic:         fmt.Sprintf("tasks/%d", i),
			Payload:       []byte(fmt.Sprintf("job-%d", i)),
			QoS:           0,
		})
	}

	time.Sleep(100 * time.Millisecond)

	w1Count := 0
	w2Count := 0

	for {
		_ = connW1.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		val, _ := connBufMap.LoadOrStore(connW1, new(bytes.Buffer))
		buf := val.(*bytes.Buffer)
		if buf.Len() >= 2 {
			if pkt, consumed, err := protocol.DecodePacket(buf.Bytes(), protocol.V50); err == nil && pkt != nil {
				buf.Next(consumed)
				if _, ok := pkt.(*protocol.PublishPacket); ok {
					w1Count++
					continue
				}
			}
		}
		tmp := make([]byte, 1024)
		n, err := connW1.Read(tmp)
		if err != nil || n == 0 {
			break
		}
		buf.Write(tmp[:n])
		if pkt, consumed, err := protocol.DecodePacket(buf.Bytes(), protocol.V50); err == nil && pkt != nil {
			buf.Next(consumed)
			if _, ok := pkt.(*protocol.PublishPacket); ok {
				w1Count++
			}
		}
	}

	for {
		_ = connW2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		val, _ := connBufMap.LoadOrStore(connW2, new(bytes.Buffer))
		buf := val.(*bytes.Buffer)
		if buf.Len() >= 2 {
			if pkt, consumed, err := protocol.DecodePacket(buf.Bytes(), protocol.V50); err == nil && pkt != nil {
				buf.Next(consumed)
				if _, ok := pkt.(*protocol.PublishPacket); ok {
					w2Count++
					continue
				}
			}
		}
		tmp := make([]byte, 1024)
		n, err := connW2.Read(tmp)
		if err != nil || n == 0 {
			break
		}
		buf.Write(tmp[:n])
		if pkt, consumed, err := protocol.DecodePacket(buf.Bytes(), protocol.V50); err == nil && pkt != nil {
			buf.Next(consumed)
			if _, ok := pkt.(*protocol.PublishPacket); ok {
				w2Count++
			}
		}
	}

	if w1Count+w2Count != totalJobs {
		t.Fatalf("Expected total %d jobs processed across workers, got w1=%d, w2=%d (sum=%d)", totalJobs, w1Count, w2Count, w1Count+w2Count)
	}
	if w1Count == 0 || w2Count == 0 {
		t.Fatalf("Expected load balancing between workers, but w1=%d, w2=%d", w1Count, w2Count)
	}
}

func TestMQTT50_RetainAsPublished_Option(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Sub 1: RetainAsPublished = true
	conn1, _ := net.Dial("tcp", addr)
	defer conn1.Close()
	sendPkt(t, conn1, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanSession:  true,
		ClientID:      "sub-rap-true",
	})
	_ = recvPkt(t, conn1, protocol.V50)
	sendPkt(t, conn1, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      1,
		Topics: []protocol.TopicSub{
			{Topic: "telemetry/rap", QoS: 0, RetainAsPublished: true},
		},
	})
	_ = recvPkt(t, conn1, protocol.V50)

	// Sub 2: RetainAsPublished = false (default)
	conn2, _ := net.Dial("tcp", addr)
	defer conn2.Close()
	sendPkt(t, conn2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanSession:  true,
		ClientID:      "sub-rap-false",
	})
	_ = recvPkt(t, conn2, protocol.V50)
	sendPkt(t, conn2, &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      2,
		Topics: []protocol.TopicSub{
			{Topic: "telemetry/rap", QoS: 0, RetainAsPublished: false},
		},
	})
	_ = recvPkt(t, conn2, protocol.V50)

	time.Sleep(50 * time.Millisecond)

	// Publisher sends message with Retain = true
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanSession:  true,
		ClientID:      "rap-publisher",
	})
	_ = recvPkt(t, pubConn, protocol.V50)

	sendPkt(t, pubConn, &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		Topic:         "telemetry/rap",
		Payload:       []byte("state-v1"),
		QoS:           0,
		Retain:        true,
	})

	// Sub 1 should preserve Retain == true
	p1 := recvPkt(t, conn1, protocol.V50).(*protocol.PublishPacket)
	if !p1.Retain {
		t.Fatalf("Expected Sub1 to preserve Retain=true under RetainAsPublished, got retain=%v", p1.Retain)
	}

	// Sub 2 should receive Retain == false
	p2 := recvPkt(t, conn2, protocol.V50).(*protocol.PublishPacket)
	if p2.Retain {
		t.Fatalf("Expected Sub2 to receive Retain=false without RetainAsPublished, got retain=%v", p2.Retain)
	}
}

func TestMQTT_Unsubscribe_Flow(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "sub-unsub",
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// Subscribe
	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 10,
		Topics:   []protocol.TopicSub{{Topic: "test/unsub", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311) // SUBACK

	// Publish 1: sub should receive
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pub-unsub",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "test/unsub",
		Payload: []byte("msg1"),
		QoS:     0,
	})

	p1 := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if string(p1.Payload) != "msg1" {
		t.Fatalf("Expected msg1, got: %s", string(p1.Payload))
	}

	// Unsubscribe
	sendPkt(t, subConn, &protocol.UnsubscribePacket{
		PacketID: 11,
		Topics:   []string{"test/unsub"},
	})
	unsuback := recvPkt(t, subConn, protocol.V311).(*protocol.UnsubackPacket)
	if unsuback.PacketID != 11 {
		t.Fatalf("Expected UNSUBACK with PacketID 11, got: %d", unsuback.PacketID)
	}

	// Publish 2: sub should NOT receive
	sendPkt(t, pubConn, &protocol.PublishPacket{
		Topic:   "test/unsub",
		Payload: []byte("msg2"),
		QoS:     0,
	})

	assertNoIncoming(t, subConn, 200*time.Millisecond)
}

func TestMQTT311_EmptyClientID_CleanSessionFalse_Rejected(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// In MQTT 3.1.1, CleanSession MUST be true if ClientID is empty.
	// If CleanSession is false, broker MUST reject with IdentifierRejected (0x02) and close connection.
	sendPkt(t, conn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  false,
		ClientID:      "",
	})

	connack := recvPkt(t, conn, protocol.V311).(*protocol.ConnackPacket)
	if connack.ReturnCode != protocol.CodeIdentifierRejected {
		t.Fatalf("Expected ReturnCode=0x02 (CodeIdentifierRejected), got: %d", connack.ReturnCode)
	}

	// Server should close the connection
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err == nil && n > 0 {
		t.Fatalf("Expected connection closed after IdentifierRejected, but received %d bytes", n)
	}
}

func TestMQTT_DuplicateClientID_SessionTakeover(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// 1. Client 1 connects with ClientID "takeover-target"
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("conn1 dial failed: %v", err)
	}
	defer conn1.Close()

	sendPkt(t, conn1, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "takeover-target",
	})
	_ = recvPkt(t, conn1, protocol.V311)

	// 2. Client 2 connects with the SAME ClientID "takeover-target"
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("conn2 dial failed: %v", err)
	}
	defer conn2.Close()

	sendPkt(t, conn2, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "takeover-target",
	})
	connack2 := recvPkt(t, conn2, protocol.V311).(*protocol.ConnackPacket)
	if connack2.ReturnCode != 0 {
		t.Fatalf("Expected conn2 to be accepted, got code: %d", connack2.ReturnCode)
	}

	// 3. Client 1 must be evicted and disconnected by the broker per MQTT spec
	_ = conn1.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 64)
	n, err := conn1.Read(buf)
	if err == nil && n > 0 {
		t.Fatalf("Expected conn1 to be closed by broker takeover, but read %d bytes", n)
	}
}

func TestMQTT_TCP_StickyPackets_And_Fragmentation(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// Subscriber
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	sendPkt(t, subConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "stream-sub",
	})
	_ = recvPkt(t, subConn, protocol.V311)
	sendPkt(t, subConn, &protocol.SubscribePacket{
		PacketID: 1,
		Topics:   []protocol.TopicSub{{Topic: "stream/#", QoS: 0}},
	})
	_ = recvPkt(t, subConn, protocol.V311)

	// Publisher
	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	sendPkt(t, pubConn, &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "stream-pub",
	})
	_ = recvPkt(t, pubConn, protocol.V311)

	// --- 1. TCP Sticky Packets (Coalescing): write 2 PUBLISH packets in a single TCP frame ---
	pkt1 := &protocol.PublishPacket{Topic: "stream/sticky1", Payload: []byte("payload-1"), QoS: 0}
	pkt2 := &protocol.PublishPacket{Topic: "stream/sticky2", Payload: []byte("payload-2"), QoS: 0}
	raw1, _ := pkt1.Encode()
	raw2, _ := pkt2.Encode()

	stickyBytes := append(raw1, raw2...)
	_, err := pubConn.Write(stickyBytes)
	if err != nil {
		t.Fatalf("Failed to write sticky packets: %v", err)
	}

	rec1 := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	rec2 := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if rec1.Topic != "stream/sticky1" || rec2.Topic != "stream/sticky2" {
		t.Fatalf("Sticky packets routing mismatch: rec1=%s, rec2=%s", rec1.Topic, rec2.Topic)
	}

	// --- 2. TCP Fragmentation (Half-Packet): write 1 PUBLISH split into 2 chunks with pause ---
	pkt3 := &protocol.PublishPacket{Topic: "stream/fragmented", Payload: []byte("long-fragmented-payload-data"), QoS: 0}
	raw3, _ := pkt3.Encode()

	splitPoint := len(raw3) / 2
	_, _ = pubConn.Write(raw3[:splitPoint])
	time.Sleep(60 * time.Millisecond) // Simulate network transit delay
	_, _ = pubConn.Write(raw3[splitPoint:])

	rec3 := recvPkt(t, subConn, protocol.V311).(*protocol.PublishPacket)
	if rec3.Topic != "stream/fragmented" || !bytes.Equal(rec3.Payload, pkt3.Payload) {
		t.Fatalf("Fragmented packet reconstruction mismatch: got topic=%s, payload=%s", rec3.Topic, string(rec3.Payload))
	}
}

