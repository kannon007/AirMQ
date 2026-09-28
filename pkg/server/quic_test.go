package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/quic-go/quic-go"

	"mqtt/pkg/protocol"
)

func TestQUIC_Connect_Publish_Subscribe(t *testing.T) {
	quicAddr := "127.0.0.1:14590"

	srv := NewServer(Config{
		Addr:         "tcp://127.0.0.1:18895",
		Multicore:    false,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)

	// Start MQTT over QUIC on UDP port
	ql, err := srv.StartQUIC(QUICConfig{
		Addr: quicAddr,
	})
	if err != nil {
		t.Fatalf("StartQUIC failed: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ql.Stop(ctx)
		_ = srv.Stop(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Setup QUIC Client TLS config
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"mqtt", "hq-29"},
	}
	clientQUIC := &quic.Config{
		MaxIdleTimeout: 10 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, quicAddr, clientTLS, clientQUIC)
	if err != nil {
		t.Fatalf("quic.DialAddr failed: %v", err)
	}
	defer conn.CloseWithError(0, "test done")

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("OpenStreamSync failed: %v", err)
	}
	defer stream.Close()

	// 1. Send MQTT CONNECT over QUIC Stream
	connectPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "quic-test-device-1",
		KeepAlive:     30,
	}
	data, _ := connectPkt.Encode()
	if _, err := stream.Write(data); err != nil {
		t.Fatalf("Write CONNECT over QUIC stream failed: %v", err)
	}

	// 2. Read CONNACK
	respBuf := make([]byte, 1024)
	n, err := stream.Read(respBuf)
	if err != nil || n < 4 {
		t.Fatalf("Failed to receive CONNACK: %v (n=%d)", err, n)
	}
	if respBuf[0] != 0x20 || respBuf[3] != 0x00 {
		t.Fatalf("Invalid CONNACK packet: %x", respBuf[:n])
	}

	// 3. Subscribe to "sensors/telemetry"
	subPkt := &protocol.SubscribePacket{
		PacketID: 101,
		Topics: []protocol.TopicSub{
			{Topic: "sensors/telemetry", QoS: 0},
		},
	}
	subBytes, _ := subPkt.Encode()
	if _, err := stream.Write(subBytes); err != nil {
		t.Fatalf("Write SUBSCRIBE failed: %v", err)
	}

	// Read SUBACK
	n, err = stream.Read(respBuf)
	if err != nil || n < 3 || respBuf[0] != 0x90 {
		t.Fatalf("Failed to receive SUBACK: %v (n=%d)", err, n)
	}

	// 4. Publish message to "sensors/telemetry"
	payload := []byte(`{"temperature":26.5,"humidity":60.2}`)
	pubPkt := &protocol.PublishPacket{
		Topic:   "sensors/telemetry",
		Payload: payload,
		QoS:     0,
	}
	pubBytes, _ := pubPkt.Encode()
	if _, err := stream.Write(pubBytes); err != nil {
		t.Fatalf("Write PUBLISH failed: %v", err)
	}

	// 5. Verify delivery back over QUIC stream
	_ = stream.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = stream.Read(respBuf)
	if err != nil {
		t.Fatalf("Timeout waiting for published message: %v", err)
	}

	recvPkt, consumed, err := protocol.DecodePacket(respBuf[:n], protocol.V311)
	if err != nil || consumed == 0 {
		t.Fatalf("Failed to decode received packet: %v", err)
	}

	pubRecv, ok := recvPkt.(*protocol.PublishPacket)
	if !ok {
		t.Fatalf("Expected PublishPacket, got %T", recvPkt)
	}
	if pubRecv.Topic != "sensors/telemetry" {
		t.Errorf("Topic mismatch: got %q, want 'sensors/telemetry'", pubRecv.Topic)
	}
	if !bytes.Equal(pubRecv.Payload, payload) {
		t.Errorf("Payload mismatch: got %s, want %s", string(pubRecv.Payload), string(payload))
	}
}

func TestQUIC_CrossProtocol_TCP_TLS_WS_QUIC(t *testing.T) {
	quicAddr := "127.0.0.1:14591"
	tcpAddr := "127.0.0.1:18896"
	tlsAddr := "127.0.0.1:18897"
	wsAddr := "127.0.0.1:18898"

	srv := NewServer(Config{
		Addr:         fmt.Sprintf("tcp://%s", tcpAddr),
		Multicore:    false,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)

	go func() {
		_ = srv.Start()
	}()
	time.Sleep(100 * time.Millisecond)

	// Start QUIC
	ql, err := srv.StartQUIC(QUICConfig{Addr: quicAddr})
	if err != nil {
		t.Fatalf("StartQUIC failed: %v", err)
	}

	// Start TLS
	serverTLS, clientTLS, _, _ := generateTestPKI(t)
	tlsLn, err := tls.Listen("tcp", tlsAddr, serverTLS)
	if err != nil {
		t.Fatalf("tls.Listen failed: %v", err)
	}
	tlsSrv := &TLSServer{server: srv, listener: tlsLn, stopCh: make(chan struct{})}
	tlsSrv.wg.Add(1)
	go tlsSrv.serve()

	// Start WebSocket
	wsSrv, err := srv.StartWS(WSConfig{Addr: wsAddr, Path: "/mqtt"})
	if err != nil {
		t.Fatalf("StartWS failed: %v", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ql.Stop(ctx)
		_ = tlsSrv.Stop()
		_ = wsSrv.Stop(ctx)
		_ = srv.Stop(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// 1. TCP Client connects and subscribes to "broadcast/alerts"
	tcpConn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatalf("Dial TCP failed: %v", err)
	}
	defer tcpConn.Close()
	doConnectAndSubscribe(t, tcpConn, "tcp-sub-1", "broadcast/alerts")

	// 2. TLS Client connects and subscribes to "broadcast/alerts"
	tlsRaw, err := tls.Dial("tcp", tlsAddr, clientTLS)
	if err != nil {
		t.Fatalf("Dial TLS failed: %v", err)
	}
	defer tlsRaw.Close()
	doConnectAndSubscribe(t, tlsRaw, "tls-sub-1", "broadcast/alerts")

	// 3. WebSocket Client connects and subscribes to "broadcast/alerts"
	wsURL := url.URL{Scheme: "ws", Host: wsAddr, Path: "/mqtt"}
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		t.Fatalf("Dial WS failed: %v", err)
	}
	defer wsConn.Close()
	wsAdapter := &wsClientTestConn{wsConn}
	doConnectAndSubscribe(t, wsAdapter, "ws-sub-1", "broadcast/alerts")

	// 4. QUIC Client connects and PUBLISHES message to "broadcast/alerts"
	quicClientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"mqtt", "hq-29"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	qConn, err := quic.DialAddr(ctx, quicAddr, quicClientTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("Dial QUIC failed: %v", err)
	}
	defer qConn.CloseWithError(0, "done")

	qStream, err := qConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("OpenStreamSync failed: %v", err)
	}
	defer qStream.Close()

	// Connect QUIC publisher
	connPkt := &protocol.ConnectPacket{ProtocolName: "MQTT", ProtocolLevel: protocol.V311, CleanSession: true, ClientID: "quic-pub-1"}
	data, _ := connPkt.Encode()
	_, _ = qStream.Write(data)
	respBuf := make([]byte, 1024)
	_, _ = qStream.Read(respBuf)

	// Broadcast alert payload from QUIC
	alertMsg := []byte("CRITICAL_TEMPERATURE_ALERT_99C")
	pubPkt := &protocol.PublishPacket{
		Topic:   "broadcast/alerts",
		Payload: alertMsg,
		QoS:     0,
	}
	pubBytes, _ := pubPkt.Encode()
	if _, err := qStream.Write(pubBytes); err != nil {
		t.Fatalf("QUIC Publish failed: %v", err)
	}

	// 5. Verify all 3 clients (TCP, TLS, WebSocket) receive the QUIC-published alert
	verifyReceived(t, tcpConn, alertMsg, "TCP")
	verifyReceived(t, tlsRaw, alertMsg, "TLS")
	verifyReceived(t, wsAdapter, alertMsg, "WebSocket")
}

func doConnectAndSubscribe(t *testing.T, conn net.Conn, clientID, topic string) {
	connPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      clientID,
	}
	d, _ := connPkt.Encode()
	_, _ = conn.Write(d)
	buf := make([]byte, 1024)
	_, _ = conn.Read(buf)

	subPkt := &protocol.SubscribePacket{
		PacketID: 1,
		Topics: []protocol.TopicSub{
			{Topic: topic, QoS: 0},
		},
	}
	sd, _ := subPkt.Encode()
	_, _ = conn.Write(sd)
	_, _ = conn.Read(buf)
}

func verifyReceived(t *testing.T, conn net.Conn, wantPayload []byte, protoName string) {
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("[%s] Timed out waiting for cross-protocol message: %v", protoName, err)
	}
	pkt, _, err := protocol.DecodePacket(buf[:n], protocol.V311)
	if err != nil {
		t.Fatalf("[%s] Decode packet error: %v", protoName, err)
	}
	pub, ok := pkt.(*protocol.PublishPacket)
	if !ok {
		t.Fatalf("[%s] Expected PublishPacket, got %T", protoName, pkt)
	}
	if !bytes.Equal(pub.Payload, wantPayload) {
		t.Fatalf("[%s] Payload mismatch: got %s, want %s", protoName, string(pub.Payload), string(wantPayload))
	}
}

type wsClientTestConn struct {
	ws *websocket.Conn
}

func (w *wsClientTestConn) Read(b []byte) (n int, err error) {
	_, p, err := w.ws.ReadMessage()
	if err != nil {
		return 0, err
	}
	n = copy(b, p)
	return n, nil
}

func (w *wsClientTestConn) Write(b []byte) (n int, err error) {
	err = w.ws.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *wsClientTestConn) Close() error {
	return w.ws.Close()
}

func (w *wsClientTestConn) LocalAddr() net.Addr {
	return w.ws.LocalAddr()
}

func (w *wsClientTestConn) RemoteAddr() net.Addr {
	return w.ws.RemoteAddr()
}

func (w *wsClientTestConn) SetDeadline(t time.Time) error {
	_ = w.ws.SetReadDeadline(t)
	return w.ws.SetWriteDeadline(t)
}

func (w *wsClientTestConn) SetReadDeadline(t time.Time) error {
	return w.ws.SetReadDeadline(t)
}

func (w *wsClientTestConn) SetWriteDeadline(t time.Time) error {
	return w.ws.SetWriteDeadline(t)
}

func TestQUIC_ConnectionMigration(t *testing.T) {
	quicAddr := "127.0.0.1:14592"

	srv := NewServer(Config{
		Addr:         "tcp://127.0.0.1:18899",
		Multicore:    false,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)

	ql, err := srv.StartQUIC(QUICConfig{Addr: quicAddr})
	if err != nil {
		t.Fatalf("StartQUIC failed: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ql.Stop(ctx)
		_ = srv.Stop(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Create Path 1 (e.g. WiFi on ephemeral UDP port)
	udpConn1, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP 1 failed: %v", err)
	}
	tr1 := &quic.Transport{Conn: udpConn1}
	defer tr1.Close()

	rAddr, err := net.ResolveUDPAddr("udp", quicAddr)
	if err != nil {
		t.Fatalf("ResolveUDPAddr failed: %v", err)
	}

	clientTLS := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"mqtt", "hq-29"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr1.Dial(ctx, rAddr, clientTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("tr1.Dial failed: %v", err)
	}
	defer conn.CloseWithError(0, "done")

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("OpenStreamSync failed: %v", err)
	}

	// 1. Connect MQTT session on Path 1
	connPkt := &protocol.ConnectPacket{ProtocolName: "MQTT", ProtocolLevel: protocol.V311, CleanSession: true, ClientID: "migrating-vehicle"}
	d, _ := connPkt.Encode()
	_, _ = stream.Write(d)
	respBuf := make([]byte, 1024)
	n, err := stream.Read(respBuf)
	if err != nil || n < 4 || respBuf[0] != 0x20 || respBuf[3] != 0x00 {
		t.Fatalf("CONNACK failed on Path 1: %v", err)
	}

	// 2. Simulate Vehicle roaming: create Path 2 (e.g. 5G on another ephemeral UDP port)
	udpConn2, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP 2 failed: %v", err)
	}
	tr2 := &quic.Transport{Conn: udpConn2}
	defer tr2.Close()

	path2, err := conn.AddPath(tr2)
	if err != nil {
		t.Fatalf("AddPath failed: %v", err)
	}
	defer path2.Close()

	// Probe the new path (PATH_CHALLENGE / PATH_RESPONSE)
	if err := path2.Probe(ctx); err != nil {
		t.Fatalf("Probe path 2 failed: %v", err)
	}

	// 3. Switch to Path 2 (QUIC Connection Migration: IP/Port changed, CID constant)
	if err := path2.Switch(); err != nil {
		t.Fatalf("Switch to path 2 failed: %v", err)
	}

	// 4. Verify existing MQTT stream continues publishing seamlessly without reconnecting!
	pubPkt := &protocol.PublishPacket{
		Topic:   "vehicle/gps/coords",
		Payload: []byte("LAT:31.23,LON:121.47"),
		QoS:     0,
	}
	pd, _ := pubPkt.Encode()
	if _, err := stream.Write(pd); err != nil {
		t.Fatalf("Write after migration failed: %v", err)
	}
}
