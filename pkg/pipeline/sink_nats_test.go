package pipeline

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNATSSink_RegistrationAndInit(t *testing.T) {
	sink, err := CreateSink("nats")
	if err != nil {
		t.Fatalf("expected nats sink to be registered, got error: %v", err)
	}

	if sink.Name() != "nats" {
		t.Errorf("expected sink name 'nats', got %s", sink.Name())
	}

	cfg := map[string]any{
		"servers": []string{"127.0.0.1:4222"},
		"subject": "iot.telemetry",
		"stream":  "IOT_STREAM",
	}

	if err := sink.Init(context.Background(), cfg); err != nil {
		t.Fatalf("failed to init nats sink: %v", err)
	}

	natsSink, ok := sink.(*NATSSink)
	if !ok {
		t.Fatalf("expected *NATSSink type")
	}

	servers := natsSink.GetServers()
	if len(servers) != 1 || servers[0] != "127.0.0.1:4222" {
		t.Errorf("unexpected servers: %v", servers)
	}
}

func TestNATSSink_MockProtocolPing(t *testing.T) {
	// Start a mock NATS server listener
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
			go func(c net.Conn) {
				defer c.Close()
				// Send NATS INFO
				_, _ = c.Write([]byte("INFO {\"server_id\":\"mock-nats\"}\r\n"))

				reader := bufio.NewReader(c)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if strings.HasPrefix(line, "PING") {
						_, _ = c.Write([]byte("PONG\r\n"))
					}
				}
			}(conn)
		}
	}()

	sink := NewNATSSink()
	_ = sink.Init(context.Background(), map[string]any{
		"servers": []string{listener.Addr().String()},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sink.Ping(ctx); err != nil {
		t.Errorf("expected Ping to succeed against mock NATS server, got: %v", err)
	}

	if err := sink.HealthCheck(ctx); err != nil {
		t.Errorf("expected HealthCheck to succeed, got: %v", err)
	}
}

func TestNATSSink_SendBatch(t *testing.T) {
	sink := NewNATSSink()
	_ = sink.Init(context.Background(), nil)

	rec := AcquireRecord()
	rec.ID = 101
	rec.Topic = "test/topic"
	rec.Target = "test.target"
	rec.Value = []byte("hello nats")

	failed, err := sink.SendBatch(context.Background(), []*Record{rec})
	if err != nil {
		t.Errorf("expected SendBatch to succeed with default probe writer, got: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed records, got %d", len(failed))
	}
	ReleaseRecord(rec)
}
