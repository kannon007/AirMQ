package server

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mqtt/pkg/protocol"
)

// generateTestPKI creates in-memory CA, server, and client certificates for TLS and mTLS testing.
func generateTestPKI(t *testing.T) (serverTLS *tls.Config, clientTLS *tls.Config, mtlsServerTLS *tls.Config, mtlsClientTLS *tls.Config) {
	// 1. Generate CA
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "MQTT Test CA",
			Organization: []string{"MQTT Broker Org"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create CA cert: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)

	// 2. Generate Server Certificate
	srvKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}
	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvBytes, err := x509.CreateCertificate(rand.Reader, srvTemplate, caTemplate, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create server cert: %v", err)
	}
	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvBytes})
	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(srvKey)})
	srvTLSCert, err := tls.X509KeyPair(srvCertPEM, srvKeyPEM)
	if err != nil {
		t.Fatalf("failed to parse server keypair: %v", err)
	}

	// 3. Generate Client Certificate (with CommonName "device-sensor-alpha")
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "device-sensor-alpha",
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientBytes, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create client cert: %v", err)
	}
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientBytes})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})
	clientTLSCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("failed to parse client keypair: %v", err)
	}

	// Server config (TLS only)
	serverTLS = &tls.Config{
		Certificates: []tls.Certificate{srvTLSCert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"mqtt"},
	}

	// Client config (verifies server against CA)
	clientTLS = &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		NextProtos: []string{"mqtt"},
	}

	// mTLS Server config (requires client cert and verifies against CA)
	mtlsServerTLS = &tls.Config{
		Certificates: []tls.Certificate{srvTLSCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"mqtt"},
	}

	// mTLS Client config (provides client cert)
	mtlsClientTLS = &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientTLSCert},
		ServerName:   "localhost",
		NextProtos:   []string{"mqtt"},
	}

	return serverTLS, clientTLS, mtlsServerTLS, mtlsClientTLS
}

func readPkt(r *bufio.Reader) (protocol.Packet, error) {
	header, err := r.Peek(2)
	if err != nil {
		return nil, err
	}
	fixedByte := header[0]
	remLen, varByteLen, err := protocol.DecodeRemainingLength(header[1:])
	if err != nil {
		// need more bytes for remaining length
		for vLen := 2; vLen <= 5; vLen++ {
			buf, pErr := r.Peek(vLen)
			if pErr != nil {
				return nil, pErr
			}
			remLen, varByteLen, err = protocol.DecodeRemainingLength(buf[1:])
			if err == nil {
				break
			}
		}
	}
	if err != nil {
		return nil, err
	}

	totalLen := 1 + varByteLen + remLen
	pktBuf := make([]byte, totalLen)
	n, err := r.Read(pktBuf)
	if err != nil {
		return nil, err
	}
	for n < totalLen {
		nn, nErr := r.Read(pktBuf[n:])
		if nErr != nil {
			return nil, nErr
		}
		n += nn
	}

	pkt, _, err := protocol.DecodePacket(pktBuf, protocol.V311)
	if err != nil && (fixedByte>>4) == protocol.CONNACK {
		pkt, _, err = protocol.DecodePacket(pktBuf, protocol.V50)
	}
	return pkt, err
}

func TestMQTTS_Connect_Publish_Subscribe(t *testing.T) {
	srvTLS, cliTLS, _, _ := generateTestPKI(t)

	s := NewServer(Config{}, nil, nil, nil)
	ts, err := s.StartTLSWithConfig("127.0.0.1:0", srvTLS)
	if err != nil {
		t.Fatalf("failed to start TLS server: %v", err)
	}
	defer func() { _ = ts.Stop() }()

	addr := ts.Addr().String()

	// 1. Connect subscriber client via TLS
	subConn, err := tls.Dial("tcp", addr, cliTLS)
	if err != nil {
		t.Fatalf("subscriber failed to connect via TLS: %v", err)
	}
	defer subConn.Close()

	subReader := bufio.NewReader(subConn)

	// Send CONNECT
	connPkt := &protocol.ConnectPacket{
		ClientID:      "tls-subscriber",
		CleanSession:  true,
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		KeepAlive:     30,
	}
	data, _ := connPkt.Encode()
	_, _ = subConn.Write(data)

	resp, err := readPkt(subReader)
	if err != nil {
		t.Fatalf("failed to read CONNACK: %v", err)
	}
	connack, ok := resp.(*protocol.ConnackPacket)
	if !ok || connack.ReturnCode != 0 {
		t.Fatalf("expected ReturnCode 0, got %v", resp)
	}

	// SUBSCRIBE to "tls/secure"
	subPkt := &protocol.SubscribePacket{
		PacketID: 101,
		Topics: []protocol.TopicSub{
			{Topic: "tls/secure", QoS: protocol.QoS1},
		},
	}
	sData, _ := subPkt.Encode()
	_, _ = subConn.Write(sData)

	resp, err = readPkt(subReader)
	if err != nil {
		t.Fatalf("failed to read SUBACK: %v", err)
	}
	suback, ok := resp.(*protocol.SubackPacket)
	if !ok || len(suback.ReturnCodes) == 0 || suback.ReturnCodes[0] != protocol.QoS1 {
		t.Fatalf("unexpected SUBACK: %v", resp)
	}

	// 2. Connect publisher client via TLS
	pubConn, err := tls.Dial("tcp", addr, cliTLS)
	if err != nil {
		t.Fatalf("publisher failed to connect via TLS: %v", err)
	}
	defer pubConn.Close()

	pubReader := bufio.NewReader(pubConn)

	connPkt2 := &protocol.ConnectPacket{
		ClientID:      "tls-publisher",
		CleanSession:  true,
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		KeepAlive:     30,
	}
	d2, _ := connPkt2.Encode()
	_, _ = pubConn.Write(d2)

	resp, err = readPkt(pubReader)
	if err != nil {
		t.Fatalf("publisher failed to read CONNACK: %v", err)
	}

	// Publish QoS 1 to "tls/secure"
	pubMsg := &protocol.PublishPacket{
		Topic:    "tls/secure",
		Payload:  []byte("encrypted-mqtt-payload-123"),
		QoS:      protocol.QoS1,
		PacketID: 201,
	}
	pData, _ := pubMsg.Encode()
	_, _ = pubConn.Write(pData)

	// Read PUBACK on publisher
	resp, err = readPkt(pubReader)
	if err != nil {
		t.Fatalf("publisher failed to read PUBACK: %v", err)
	}
	puback, ok := resp.(*protocol.PubackPacket)
	if !ok || puback.PacketID != 201 {
		t.Fatalf("expected PUBACK 201, got %v", resp)
	}

	// Read delivered message on subscriber
	resp, err = readPkt(subReader)
	if err != nil {
		t.Fatalf("subscriber failed to read delivered message: %v", err)
	}
	delivered, ok := resp.(*protocol.PublishPacket)
	if !ok || delivered.Topic != "tls/secure" || string(delivered.Payload) != "encrypted-mqtt-payload-123" {
		t.Fatalf("delivered message mismatch: %+v", delivered)
	}
}

func TestMQTTS_mTLS_ClientCertAuth(t *testing.T) {
	_, cliTLSWithoutCert, mtlsServerTLS, mtlsClientTLS := generateTestPKI(t)

	s := NewServer(Config{}, nil, nil, nil)
	ts, err := s.StartTLSWithConfig("127.0.0.1:0", mtlsServerTLS)
	if err != nil {
		t.Fatalf("failed to start mTLS server: %v", err)
	}
	defer func() { _ = ts.Stop() }()

	addr := ts.Addr().String()

	// 1. Connection WITHOUT client certificate must fail handshake or be rejected
	noCertConn, err := tls.Dial("tcp", addr, cliTLSWithoutCert)
	if err == nil {
		defer noCertConn.Close()
		// If TCP connected, attempting to write or read MQTT packet must fail due to missing cert
		_, writeErr := noCertConn.Write([]byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00})
		if writeErr == nil {
			buf := make([]byte, 10)
			_ = noCertConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			_, readErr := noCertConn.Read(buf)
			if readErr == nil {
				t.Fatalf("expected failure when client certificate is omitted, but connection succeeded")
			}
		}
	}

	// 2. Connection WITH valid client certificate succeeds
	conn, err := tls.Dial("tcp", addr, mtlsClientTLS)
	if err != nil {
		t.Fatalf("expected mTLS handshake to succeed with valid cert: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connPkt := &protocol.ConnectPacket{
		ClientID:      "mtls-client-1",
		CleanSession:  true,
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		KeepAlive:     30,
	}
	data, _ := connPkt.Encode()
	_, _ = conn.Write(data)

	resp, err := readPkt(reader)
	if err != nil {
		t.Fatalf("failed to read CONNACK over mTLS: %v", err)
	}
	connack, ok := resp.(*protocol.ConnackPacket)
	if !ok || connack.ReturnCode != 0 {
		t.Fatalf("mTLS CONNACK unexpected: %v", resp)
	}

	// Verify that the server extracted ClientCertCN correctly
	val, ok := s.conns.Load("mtls-client-1")
	if !ok {
		t.Fatalf("expected client to be registered in s.conns")
	}
	entry := val.(*clientEntry)
	if entry.ctx.ClientCertCN != "device-sensor-alpha" {
		t.Fatalf("expected ClientCertCN 'device-sensor-alpha', got '%s'", entry.ctx.ClientCertCN)
	}
	if !entry.ctx.IsTLS {
		t.Fatalf("expected IsTLS to be true")
	}
}

func TestMQTT_WebSocket_Connect_Publish_Subscribe(t *testing.T) {
	s := NewServer(Config{}, nil, nil, nil)
	wsServer, err := s.StartWS(WSConfig{
		Addr: "127.0.0.1:0",
		Path: "/mqtt",
	})
	if err != nil {
		t.Fatalf("failed to start WebSocket server: %v", err)
	}
	defer func() { _ = wsServer.Stop(context.Background()) }()

	wsURL := fmt.Sprintf("ws://%s/mqtt", wsServer.Addr().String())

	dialer := websocket.Dialer{
		Subprotocols: []string{"mqtt"},
	}

	// 1. Connect subscriber client via WebSocket
	subWS, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial WebSocket subscriber: %v", err)
	}
	defer subWS.Close()

	// Send CONNECT
	connPkt := &protocol.ConnectPacket{
		ClientID:      "ws-subscriber",
		CleanSession:  true,
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		KeepAlive:     30,
	}
	cData, _ := connPkt.Encode()
	_ = subWS.WriteMessage(websocket.BinaryMessage, cData)

	msgType, payload, err := subWS.ReadMessage()
	if err != nil || msgType != websocket.BinaryMessage {
		t.Fatalf("failed to read WS CONNACK: %v", err)
	}
	pkt, _, err := protocol.DecodePacket(payload, protocol.V311)
	if err != nil {
		t.Fatalf("decode CONNACK err: %v", err)
	}
	if connack, ok := pkt.(*protocol.ConnackPacket); !ok || connack.ReturnCode != 0 {
		t.Fatalf("expected CONNACK ReturnCode 0, got %v", pkt)
	}

	// SUBSCRIBE to "ws/data"
	subPkt := &protocol.SubscribePacket{
		PacketID: 301,
		Topics: []protocol.TopicSub{
			{Topic: "ws/data", QoS: protocol.QoS0},
		},
	}
	sData, _ := subPkt.Encode()
	_ = subWS.WriteMessage(websocket.BinaryMessage, sData)

	_, payload, err = subWS.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read WS SUBACK: %v", err)
	}
	pkt, _, _ = protocol.DecodePacket(payload, protocol.V311)
	if suback, ok := pkt.(*protocol.SubackPacket); !ok || len(suback.ReturnCodes) == 0 || suback.ReturnCodes[0] != protocol.QoS0 {
		t.Fatalf("unexpected SUBACK: %v", pkt)
	}

	// 2. Connect publisher client via WebSocket
	pubWS, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial WebSocket publisher: %v", err)
	}
	defer pubWS.Close()

	connPkt2 := &protocol.ConnectPacket{
		ClientID:      "ws-publisher",
		CleanSession:  true,
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		KeepAlive:     30,
	}
	d2, _ := connPkt2.Encode()
	_ = pubWS.WriteMessage(websocket.BinaryMessage, d2)
	_, _, _ = pubWS.ReadMessage() // Read CONNACK

	// Publish to "ws/data"
	pubMsg := &protocol.PublishPacket{
		Topic:   "ws/data",
		Payload: []byte("hello-from-websocket-browser"),
		QoS:     protocol.QoS0,
	}
	pData, _ := pubMsg.Encode()
	_ = pubWS.WriteMessage(websocket.BinaryMessage, pData)

	// Subscriber receives message
	_, payload, err = subWS.ReadMessage()
	if err != nil {
		t.Fatalf("subscriber failed to read delivered message: %v", err)
	}
	pkt, _, _ = protocol.DecodePacket(payload, protocol.V311)
	delivered, ok := pkt.(*protocol.PublishPacket)
	if !ok || delivered.Topic != "ws/data" || string(delivered.Payload) != "hello-from-websocket-browser" {
		t.Fatalf("delivered message mismatch: %+v", delivered)
	}
}

func TestMQTT_CrossProtocol_TCP_TLS_WebSocket(t *testing.T) {
	srvTLS, cliTLS, _, _ := generateTestPKI(t)

	// Initialize broker engine
	s := NewServer(Config{Addr: "tcp://127.0.0.1:19201"}, nil, nil, nil)
	go func() {
		_ = s.Start()
	}()
	time.Sleep(100 * time.Millisecond)
	defer func() { _ = s.Stop(context.Background()) }()

	// Start TLS listener
	ts, err := s.StartTLSWithConfig("127.0.0.1:0", srvTLS)
	if err != nil {
		t.Fatalf("failed to start TLS: %v", err)
	}
	defer func() { _ = ts.Stop() }()

	// Start WebSocket listener
	wsServer, err := s.StartWS(WSConfig{
		Addr: "127.0.0.1:0",
		Path: "/mqtt",
	})
	if err != nil {
		t.Fatalf("failed to start WS: %v", err)
	}
	defer func() { _ = wsServer.Stop(context.Background()) }()

	// Client 1: TCP Client connects
	tcpConn, err := net.Dial("tcp", "127.0.0.1:19201")
	if err != nil {
		t.Fatalf("failed to connect TCP: %v", err)
	}
	defer tcpConn.Close()
	tcpReader := bufio.NewReader(tcpConn)

	c1 := &protocol.ConnectPacket{ClientID: "cross-tcp-sub", CleanSession: true, ProtocolName: "MQTT", ProtocolLevel: protocol.V311}
	d1, _ := c1.Encode()
	_, _ = tcpConn.Write(d1)
	_, _ = readPkt(tcpReader) // connack

	s1 := &protocol.SubscribePacket{PacketID: 1, Topics: []protocol.TopicSub{{Topic: "cross/demo", QoS: 0}}}
	sd1, _ := s1.Encode()
	_, _ = tcpConn.Write(sd1)
	_, _ = readPkt(tcpReader) // suback

	// Client 2: TLS Client connects
	tlsConn, err := tls.Dial("tcp", ts.Addr().String(), cliTLS)
	if err != nil {
		t.Fatalf("failed to connect TLS: %v", err)
	}
	defer tlsConn.Close()
	tlsReader := bufio.NewReader(tlsConn)

	c2 := &protocol.ConnectPacket{ClientID: "cross-tls-sub", CleanSession: true, ProtocolName: "MQTT", ProtocolLevel: protocol.V311}
	d2, _ := c2.Encode()
	_, _ = tlsConn.Write(d2)
	_, _ = readPkt(tlsReader) // connack

	s2 := &protocol.SubscribePacket{PacketID: 2, Topics: []protocol.TopicSub{{Topic: "cross/demo", QoS: 0}}}
	sd2, _ := s2.Encode()
	_, _ = tlsConn.Write(sd2)
	_, _ = readPkt(tlsReader) // suback

	// Client 3: WebSocket Client connects
	dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}
	u := url.URL{Scheme: "ws", Host: wsServer.Addr().String(), Path: "/mqtt"}
	wsConn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("failed to connect WS: %v", err)
	}
	defer wsConn.Close()

	c3 := &protocol.ConnectPacket{ClientID: "cross-ws-sub", CleanSession: true, ProtocolName: "MQTT", ProtocolLevel: protocol.V311}
	d3, _ := c3.Encode()
	_ = wsConn.WriteMessage(websocket.BinaryMessage, d3)
	_, _, _ = wsConn.ReadMessage() // connack

	s3 := &protocol.SubscribePacket{PacketID: 3, Topics: []protocol.TopicSub{{Topic: "cross/demo", QoS: 0}}}
	sd3, _ := s3.Encode()
	_ = wsConn.WriteMessage(websocket.BinaryMessage, sd3)
	_, _, _ = wsConn.ReadMessage() // suback

	// Action 1: WebSocket Client publishes -> TCP and TLS must receive!
	wsPub := &protocol.PublishPacket{Topic: "cross/demo", Payload: []byte("broadcast-from-websocket"), QoS: 0}
	wpData, _ := wsPub.Encode()
	_ = wsConn.WriteMessage(websocket.BinaryMessage, wpData)

	// Verify TCP received
	tcpMsg, err := readPkt(tcpReader)
	if err != nil {
		t.Fatalf("TCP failed to receive WS message: %v", err)
	}
	if p, ok := tcpMsg.(*protocol.PublishPacket); !ok || string(p.Payload) != "broadcast-from-websocket" {
		t.Fatalf("TCP got wrong payload: %v", tcpMsg)
	}

	// Verify TLS received
	tlsMsg, err := readPkt(tlsReader)
	if err != nil {
		t.Fatalf("TLS failed to receive WS message: %v", err)
	}
	if p, ok := tlsMsg.(*protocol.PublishPacket); !ok || string(p.Payload) != "broadcast-from-websocket" {
		t.Fatalf("TLS got wrong payload: %v", tlsMsg)
	}

	// Verify WebSocket also received its own broadcast (subscribed without NoLocal in MQTT 3.1.1)
	_, wsPayload1, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("WS failed to receive its own broadcast: %v", err)
	}
	pkt1, _, _ := protocol.DecodePacket(wsPayload1, protocol.V311)
	if p, ok := pkt1.(*protocol.PublishPacket); !ok || string(p.Payload) != "broadcast-from-websocket" {
		t.Fatalf("WS got wrong payload for Action 1: %v", pkt1)
	}

	// Action 2: TCP Client publishes -> TLS and WebSocket must receive!
	tcpPub := &protocol.PublishPacket{Topic: "cross/demo", Payload: []byte("broadcast-from-tcp"), QoS: 0}
	tpData, _ := tcpPub.Encode()
	_, _ = tcpConn.Write(tpData)

	// Verify TLS received
	tlsMsg, err = readPkt(tlsReader)
	if err != nil {
		t.Fatalf("TLS failed to receive TCP message: %v", err)
	}
	if p, ok := tlsMsg.(*protocol.PublishPacket); !ok || string(p.Payload) != "broadcast-from-tcp" {
		t.Fatalf("TLS got wrong payload: %v", tlsMsg)
	}

	// Verify WebSocket received
	_, wsPayload2, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("WS failed to receive TCP message: %v", err)
	}
	pkt2, _, _ := protocol.DecodePacket(wsPayload2, protocol.V311)
	if p, ok := pkt2.(*protocol.PublishPacket); !ok || string(p.Payload) != "broadcast-from-tcp" {
		t.Fatalf("WS got wrong payload: %v", pkt2)
	}
}
