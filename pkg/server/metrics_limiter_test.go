package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"mqtt/pkg/limiter"
	"mqtt/pkg/metrics"
	"mqtt/pkg/protocol"
)

func TestServer_PrometheusMetricsEndpoint(t *testing.T) {
	s := NewServer(Config{}, nil, nil, nil)
	wsSrv, err := s.StartWS(WSConfig{
		Addr: "127.0.0.1:0",
		Path: "/mqtt",
	})
	if err != nil {
		t.Fatalf("failed to start WS: %v", err)
	}
	defer func() { _ = wsSrv.Stop(context.Background()) }()

	// Start TCP server
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	tcpAddr := tcpLn.Addr().String()
	_ = tcpLn.Close()

	s.cfg.Addr = "tcp://" + tcpAddr
	go func() { _ = s.Start() }()
	time.Sleep(100 * time.Millisecond)
	defer func() { _ = s.Stop(context.Background()) }()

	// Connect TCP client
	conn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatalf("failed to dial TCP: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connPkt := &protocol.ConnectPacket{ClientID: "metrics-test-client", CleanSession: true, ProtocolName: "MQTT", ProtocolLevel: protocol.V311}
	d, _ := connPkt.Encode()
	_, _ = conn.Write(d)
	_, _ = readPkt(reader) // connack

	pubPkt := &protocol.PublishPacket{Topic: "metrics/test", Payload: []byte("prometheus-rocks"), QoS: protocol.QoS0}
	pd, _ := pubPkt.Encode()
	_, _ = conn.Write(pd)

	time.Sleep(50 * time.Millisecond)

	// Fetch /metrics from WebSocket server HTTP port
	metricsURL := fmt.Sprintf("http://%s/metrics", wsSrv.Addr().String())
	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatalf("failed to fetch metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /metrics, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "mqtt_connections_active") {
		t.Fatalf("metrics missing mqtt_connections_active: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `mqtt_connect_total{status="success"}`) {
		t.Fatalf("metrics missing mqtt_connect_total: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `mqtt_messages_received_total{qos="0"}`) {
		t.Fatalf("metrics missing mqtt_messages_received_total: %s", bodyStr)
	}
}

func TestServer_RateLimiter_ConnectionThrottling(t *testing.T) {
	// Restrict to max 2 burst connections
	reg := metrics.NewRegistry()
	s := NewServer(Config{
		ConnLimit: limiter.ConnLimiterConfig{
			GlobalRate:  1,
			GlobalBurst: 2,
		},
	}, nil, nil, nil)
	s.metrics = reg

	tcpLn, _ := net.Listen("tcp", "127.0.0.1:0")
	tcpAddr := tcpLn.Addr().String()
	_ = tcpLn.Close()

	s.cfg.Addr = "tcp://" + tcpAddr
	go func() { _ = s.Start() }()
	time.Sleep(100 * time.Millisecond)
	defer func() { _ = s.Stop(context.Background()) }()

	// Connect 3 times rapidly
	connectedCount := 0
	for i := 0; i < 4; i++ {
		c, err := net.Dial("tcp", tcpAddr)
		if err != nil {
			continue
		}
		r := bufio.NewReader(c)
		pkt := &protocol.ConnectPacket{ClientID: fmt.Sprintf("burst-conn-%d", i), CleanSession: true, ProtocolName: "MQTT", ProtocolLevel: protocol.V311}
		data, _ := pkt.Encode()
		_, _ = c.Write(data)

		resp, err := readPkt(r)
		if err == nil && resp != nil {
			if connack, ok := resp.(*protocol.ConnackPacket); ok && connack.ReturnCode == 0 {
				connectedCount++
			}
		}
		_ = c.Close()
	}

	if s.Metrics().RateLimitDroppedConn.Load() == 0 {
		t.Fatalf("expected RateLimitDroppedConn to be greater than 0, got %d", s.Metrics().RateLimitDroppedConn.Load())
	}
}

func TestServer_RateLimiter_PublishThrottling(t *testing.T) {
	reg := metrics.NewRegistry()
	s := NewServer(Config{
		PublishLimit: limiter.PublishLimiterConfig{
			Rate:  10,
			Burst: 2,
		},
	}, nil, nil, nil)
	s.metrics = reg

	tcpLn, _ := net.Listen("tcp", "127.0.0.1:0")
	tcpAddr := tcpLn.Addr().String()
	_ = tcpLn.Close()

	s.cfg.Addr = "tcp://" + tcpAddr
	go func() { _ = s.Start() }()
	time.Sleep(100 * time.Millisecond)
	defer func() { _ = s.Stop(context.Background()) }()

	conn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	c1 := &protocol.ConnectPacket{ClientID: "spam-client", CleanSession: true, ProtocolName: "MQTT", ProtocolLevel: protocol.V311}
	d1, _ := c1.Encode()
	_, _ = conn.Write(d1)
	_, _ = readPkt(reader)

	// Send 10 rapid QoS 0 messages (burst is 2)
	for i := 0; i < 10; i++ {
		pub := &protocol.PublishPacket{Topic: "flood/test", Payload: []byte("spam"), QoS: 0}
		pd, _ := pub.Encode()
		_, _ = conn.Write(pd)
	}

	time.Sleep(100 * time.Millisecond)

	dropped := s.Metrics().RateLimitDroppedPublish.Load()
	if dropped == 0 {
		t.Fatalf("expected RateLimitDroppedPublish > 0 when flooding, got %d", dropped)
	}
}
