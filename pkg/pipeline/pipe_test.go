package pipeline

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestPipe_ExecutionAndMetrics(t *testing.T) {
	p := NewPipe()

	// Processor 1: always matches
	p.Add("proc_1", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			c.Set("p1", "done")
			return nil
		},
	))

	// Processor 2: does NOT match (should be skipped)
	p.Add("proc_skipped", NewFuncProcessor(
		func(c *Context) bool { return false },
		func(c *Context) error {
			t.Fatal("Processor 2 should NOT have executed")
			return nil
		},
	))

	// Processor 3: matches if p1 is set
	p.Add("proc_3", NewFuncProcessor(
		func(c *Context) bool {
			v, ok := c.Get("p1")
			return ok && v.(string) == "done"
		},
		func(c *Context) error {
			c.Set("p3", "done")
			return nil
		},
	))

	msg := &Message{Topic: "sensors/temp", Payload: []byte("25.0")}
	err := p.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("Unexpected execute error: %v", err)
	}

	stats := p.Stats()
	if len(stats) != 3 {
		t.Fatalf("Expected 3 stats entries, got %d", len(stats))
	}

	// proc_1: 1 invocation, 1 matched, 0 skipped, 1 success
	if stats[0].MatchedCount != 1 || stats[0].SkippedCount != 0 || stats[0].SuccessCount != 1 {
		t.Errorf("Unexpected stats for proc_1: %+v", stats[0])
	}
	// proc_skipped: 1 invocation, 0 matched, 1 skipped, 0 success
	if stats[1].MatchedCount != 0 || stats[1].SkippedCount != 1 || stats[1].SuccessCount != 0 {
		t.Errorf("Unexpected stats for proc_skipped: %+v", stats[1])
	}
	// proc_3: 1 invocation, 1 matched, 0 skipped, 1 success
	if stats[2].MatchedCount != 1 || stats[2].SkippedCount != 0 || stats[2].SuccessCount != 1 {
		t.Errorf("Unexpected stats for proc_3: %+v", stats[2])
	}

	table := p.PrintStats()
	if !strings.Contains(table, "proc_1") || !strings.Contains(table, "proc_skipped") {
		t.Errorf("Formatted table missing processor names: %s", table)
	}
}

func TestPipe_Drop(t *testing.T) {
	p := NewPipe()

	p.Add("dropper", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			c.Drop("blacklisted message")
			return nil
		},
	))

	p.Add("never_run", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			t.Fatal("Downstream processor should NOT run after drop")
			return nil
		},
	))

	msg := &Message{Topic: "test/drop", Payload: []byte("bad")}
	err := p.Execute(context.Background(), msg)
	if !errors.Is(err, ErrMessageDropped) {
		t.Fatalf("Expected ErrMessageDropped, got %v", err)
	}

	stats := p.Stats()
	if stats[0].DroppedCount != 1 {
		t.Errorf("Expected DroppedCount=1, got %d", stats[0].DroppedCount)
	}
	if stats[1].Invocations != 0 {
		t.Errorf("Downstream processor should have 0 invocations, got %d", stats[1].Invocations)
	}
}

func TestPipe_Abort(t *testing.T) {
	p := NewPipe()
	customErr := errors.New("storage network timeout")

	p.Add("aborter", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			c.Abort(customErr)
			return customErr
		},
	))

	p.Add("never_run", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			t.Fatal("Downstream processor should NOT run after abort")
			return nil
		},
	))

	msg := &Message{Topic: "test/abort", Payload: []byte("val")}
	err := p.Execute(context.Background(), msg)
	if !errors.Is(err, customErr) {
		t.Fatalf("Expected customErr, got %v", err)
	}

	stats := p.Stats()
	if stats[0].ErrorCount != 1 {
		t.Errorf("Expected ErrorCount=1, got %d", stats[0].ErrorCount)
	}
	if stats[1].Invocations != 0 {
		t.Errorf("Downstream processor should have 0 invocations, got %d", stats[1].Invocations)
	}
}

func TestProcessors_Auth(t *testing.T) {
	authProc := NewAuthProcessor(func(clientID, username string) bool {
		return clientID == "admin_device" || username == "operator"
	})

	p := NewPipe()
	p.Add("auth_guard", authProc)

	// Case 1: authorized client
	msg1 := &Message{ClientID: "admin_device", Username: "guest"}
	if err := p.Execute(context.Background(), msg1); err != nil {
		t.Fatalf("admin_device should pass: %v", err)
	}

	// Case 2: internal client ($internal) -> automatically exempted
	msgInternal := &Message{ClientID: "$internal"}
	if err := p.Execute(context.Background(), msgInternal); err != nil {
		t.Fatalf("$internal should pass: %v", err)
	}
	// Check stats: msgInternal was skipped (Match returned false)
	stats := p.Stats()
	if stats[0].SkippedCount != 1 {
		t.Errorf("Expected 1 skipped for $internal, got %d", stats[0].SkippedCount)
	}

	// Case 3: unauthorized client
	msg3 := &Message{ClientID: "hacker", Username: "anon"}
	err := p.Execute(context.Background(), msg3)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Expected ErrUnauthorized, got %v", err)
	}
}

func TestProcessors_Validate(t *testing.T) {
	// JSON Validator targeting telemetry/#
	valProc := NewValidateProcessor(
		NewJSONValidator("json_check"),
		WithValidateTopic("telemetry/#"),
	)

	p := NewPipe()
	p.Add("json_validator", valProc)

	// 1. Topic doesn't match -> Match returns false, skipped
	msgNonTelemetry := &Message{Topic: "alerts/temp", Payload: []byte("NOT_JSON")}
	if err := p.Execute(context.Background(), msgNonTelemetry); err != nil {
		t.Fatalf("Non-telemetry should be skipped without error: %v", err)
	}
	if p.Stats()[0].SkippedCount != 1 {
		t.Fatalf("Expected skipped count 1, got %d", p.Stats()[0].SkippedCount)
	}

	// 2. Topic matches, invalid JSON -> Dropped
	msgBadJSON := &Message{Topic: "telemetry/meter1", Payload: []byte("INVALID_JSON")}
	err := p.Execute(context.Background(), msgBadJSON)
	if !errors.Is(err, ErrMessageDropped) {
		t.Fatalf("Invalid JSON should be dropped, got %v", err)
	}

	// 3. Topic matches, valid JSON -> Passes
	msgGoodJSON := &Message{Topic: "telemetry/meter1", Payload: []byte(`{"voltage":220}`)}
	if err := p.Execute(context.Background(), msgGoodJSON); err != nil {
		t.Fatalf("Valid JSON should pass: %v", err)
	}
}

func TestProcessors_BinaryValidator(t *testing.T) {
	binProc := NewValidateProcessor(
		NewBinaryValidator("bin_check", 8, 1024, WithMagic([]byte{0xDE, 0xAD, 0xBE, 0xEF})),
		WithValidateTopic("binary/#"),
	)
	p := NewPipe().Add("bin_val", binProc)

	// Valid packet: 4 bytes magic + 4 bytes length
	validBin := make([]byte, 8)
	copy(validBin[0:4], []byte{0xDE, 0xAD, 0xBE, 0xEF})
	binary.BigEndian.PutUint32(validBin[4:8], 0)

	msg := &Message{Topic: "binary/sensor1", Payload: validBin}
	if err := p.Execute(context.Background(), msg); err != nil {
		t.Fatalf("Valid binary packet should pass: %v", err)
	}

	// Invalid magic
	badBin := []byte{0x00, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00}
	msgBad := &Message{Topic: "binary/sensor1", Payload: badBin}
	err := p.Execute(context.Background(), msgBad)
	if !errors.Is(err, ErrMessageDropped) {
		t.Fatalf("Invalid magic header should be dropped: %v", err)
	}
}

func TestProcessors_ForwardAndTransform(t *testing.T) {
	var forwardedCount int

	p := NewPipe()
	p.Add("enricher", NewTransformProcessor(func(msg *Message) error {
		msg.SetPayload([]byte("enriched:" + string(msg.Payload)))
		return nil
	}, WithTransformTopic("telemetry/#")))

	p.Add("kafka_sink", NewForwardProcessor(func(msg *Message) error {
		forwardedCount++
		return nil
	}, true, WithForwardTopic("telemetry/#")))

	msg := &Message{Topic: "telemetry/v1", Payload: []byte("raw_data")}
	if err := p.Execute(context.Background(), msg); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if string(msg.Payload) != "enriched:raw_data" {
		t.Fatalf("Expected enriched:raw_data, got %s", string(msg.Payload))
	}
	if forwardedCount != 1 {
		t.Fatalf("Expected forwardedCount=1, got %d", forwardedCount)
	}
}

func TestRouter_Dispatch(t *testing.T) {
	r := NewRouter()
	if r.HasPipelines() {
		t.Fatal("Router should have no pipelines initially")
	}

	pTelemetry := NewPipe()
	pTelemetry.Add("transformer", NewTransformProcessor(func(msg *Message) error {
		msg.SetPayload([]byte("telemetry:" + string(msg.Payload)))
		return nil
	}))
	r.Handle("telemetry/#", pTelemetry)

	if !r.HasPipelines() {
		t.Fatal("Router should have pipelines registered")
	}

	// Message on telemetry/sensor1 -> matches
	msg1 := &Message{Topic: "telemetry/sensor1", Payload: []byte("100")}
	if err := r.Process(context.Background(), msg1); err != nil {
		t.Fatalf("Process telemetry error: %v", err)
	}
	if string(msg1.Payload) != "telemetry:100" {
		t.Fatalf("Payload was not transformed: %s", string(msg1.Payload))
	}

	// Message on alerts/fire -> no pipeline matches, passes through unaffected
	msg2 := &Message{Topic: "alerts/fire", Payload: []byte("warning")}
	if err := r.Process(context.Background(), msg2); err != nil {
		t.Fatalf("Process alerts error: %v", err)
	}
	if string(msg2.Payload) != "warning" {
		t.Fatalf("Payload should remain unchanged: %s", string(msg2.Payload))
	}

	// Remove pipeline
	removed := r.Remove("telemetry/#")
	if !removed {
		t.Fatal("Remove should return true")
	}
	if r.HasPipelines() {
		t.Fatal("Router should have no pipelines after remove")
	}
}

func BenchmarkRouter_FastPath_NoPipelines(b *testing.B) {
	r := NewRouter()
	ctx := context.Background()
	msg := &Message{Topic: "telemetry/sensor1", Payload: []byte("hello")}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.Process(ctx, msg)
	}
}

func BenchmarkPipe_Execution_HotPath(b *testing.B) {
	p := NewPipe()
	p.Add("proc1", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			c.Set("k", "v")
			return nil
		},
	))
	p.Add("proc2", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			return nil
		},
	))

	ctx := context.Background()
	msg := &Message{Topic: "telemetry/sensor1", Payload: []byte("hello")}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Execute(ctx, msg)
	}
}

func BenchmarkPipe_Execution_Sampled_16(b *testing.B) {
	p := NewPipe().SetSampleRate(16)
	p.Add("proc1", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			c.Set("k", "v")
			return nil
		},
	))
	p.Add("proc2", NewFuncProcessor(
		func(c *Context) bool { return true },
		func(c *Context) error {
			return nil
		},
	))

	ctx := context.Background()
	msg := &Message{Topic: "telemetry/sensor1", Payload: []byte("hello")}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Execute(ctx, msg)
	}
}
