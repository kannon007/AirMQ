package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestMetrics_AtomicOperations(t *testing.T) {
	reg := NewRegistry()

	// Concurrent increments
	var wg sync.WaitGroup
	workers := 10
	iterations := 1000

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				reg.IncConnActive("tcp")
				reg.IncMsgReceived(0)
				reg.IncMsgReceived(1)
				reg.IncMsgSent(0)
				reg.AddBytesReceived(100)
			}
		}()
	}
	wg.Wait()

	if expected := int64(workers * iterations); reg.ConnsActiveTCP.Load() != expected {
		t.Fatalf("expected ConnsActiveTCP %d, got %d", expected, reg.ConnsActiveTCP.Load())
	}
	if expected := uint64(workers * iterations); reg.MsgsReceivedQoS0.Load() != expected {
		t.Fatalf("expected MsgsReceivedQoS0 %d, got %d", expected, reg.MsgsReceivedQoS0.Load())
	}
	if expected := uint64(workers * iterations * 100); reg.BytesReceivedTotal.Load() != expected {
		t.Fatalf("expected BytesReceivedTotal %d, got %d", expected, reg.BytesReceivedTotal.Load())
	}
}

func TestMetrics_PrometheusExpositionFormat(t *testing.T) {
	reg := NewRegistry()
	reg.IncConnActive("tcp")
	reg.IncConnActive("tls")
	reg.IncConnActive("ws")
	reg.IncConnect("success")
	reg.IncConnect("rejected")
	reg.IncMsgReceived(0)
	reg.IncMsgReceived(1)
	reg.IncMsgReceived(2)
	reg.SetRetainedCount(42)
	reg.SetOfflineCount(99)
	reg.IncRateLimitDropped("publish")

	var buf bytes.Buffer
	err := reg.WritePrometheus(&buf)
	if err != nil {
		t.Fatalf("WritePrometheus error: %v", err)
	}

	out := buf.String()
	requiredSnippets := []string{
		`# HELP mqtt_connections_active`,
		`# TYPE mqtt_connections_active gauge`,
		`mqtt_connections_active{transport="tcp"} 1`,
		`mqtt_connections_active{transport="tls"} 1`,
		`mqtt_connections_active{transport="ws"} 1`,
		`mqtt_connect_total{status="success"} 1`,
		`mqtt_connect_total{status="rejected"} 1`,
		`mqtt_messages_received_total{qos="0"} 1`,
		`mqtt_messages_received_total{qos="1"} 1`,
		`mqtt_messages_received_total{qos="2"} 1`,
		`mqtt_retained_messages_count 42`,
		`mqtt_offline_messages_count 99`,
		`mqtt_rate_limit_dropped_total{type="publish"} 1`,
	}

	for _, snippet := range requiredSnippets {
		if !strings.Contains(out, snippet) {
			t.Errorf("expected output to contain %q, but not found", snippet)
		}
	}
}

func TestMetrics_HTTPHandler(t *testing.T) {
	reg := NewRegistry()
	reg.IncConnActive("tcp")

	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()

	handler := reg.Handler()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	ct := rr.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected Content-Type text/plain, got %s", ct)
	}

	body := rr.Body.String()
	if !strings.Contains(body, `mqtt_connections_active{transport="tcp"} 1`) {
		t.Fatalf("expected metrics body to contain connection count: %s", body)
	}
}

func BenchmarkMetrics_Inc(b *testing.B) {
	reg := NewRegistry()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reg.IncMsgReceived(0)
			reg.AddBytesReceived(64)
		}
	})
}
