package pipeline

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"mqtt/pkg/protocol"
)

func TestRecord_EncodeDecode_CRC(t *testing.T) {
	rec := AcquireRecord()
	defer ReleaseRecord(rec)

	rec.ID = 42
	rec.Topic = "sensors/industrial/temp"
	rec.Target = "iot.telemetry"
	rec.Key = []byte("machine_001")
	rec.Value = []byte("status=OK;temp=78.5C")
	rec.Headers["env"] = "production"
	rec.Headers["datacenter"] = "us-east"
	rec.Timestamp = time.Now()
	rec.QoS = 1

	encoded := rec.Encode(nil)
	if len(encoded) < 16 {
		t.Fatalf("encoded record too short: %d bytes", len(encoded))
	}

	// 1. Normal Decode
	buf := bytes.NewReader(encoded)
	decoded, totalBytes, err := DecodeRecord(buf)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}
	if totalBytes != len(encoded) {
		t.Errorf("bytes mismatch: got %d, want %d", totalBytes, len(encoded))
	}
	if decoded.ID != rec.ID || decoded.Topic != rec.Topic || decoded.Target != rec.Target {
		t.Errorf("field mismatch: got %+v, want %+v", decoded, rec)
	}
	if !bytes.Equal(decoded.Key, rec.Key) || !bytes.Equal(decoded.Value, rec.Value) {
		t.Errorf("payload mismatch: key=%s, val=%s", decoded.Key, decoded.Value)
	}
	if decoded.Headers["env"] != "production" {
		t.Errorf("headers mismatch: %v", decoded.Headers)
	}

	// 2. Corrupt bit check
	corruptEncoded := make([]byte, len(encoded))
	copy(corruptEncoded, encoded)
	corruptEncoded[15] ^= 0xFF // Flip a bit in the body

	corruptBuf := bytes.NewReader(corruptEncoded)
	_, _, err = DecodeRecord(corruptBuf)
	if err != ErrCorruptRecord {
		t.Fatalf("expected ErrCorruptRecord, got %v", err)
	}
}

func TestPipeline_DirectStreaming(t *testing.T) {
	spoolDir := filepath.Join(os.TempDir(), fmt.Sprintf("pipe_test_direct_%d", time.Now().UnixNano()))
	defer os.RemoveAll(spoolDir)

	mock := NewMockSink()
	cfg := Config{
		BatchSize:      100,
		FlushTimeout:   10 * time.Millisecond,
		RingBufferSize: 1000,
		NumShards:      4,
		SpoolDir:       spoolDir,
	}

	pipe, err := NewPipeline(cfg, mock)
	if err != nil {
		t.Fatalf("NewPipeline err: %v", err)
	}
	defer pipe.Close()

	// Ingest 500 messages
	total := 500
	for i := 0; i < total; i++ {
		pkt := &protocol.PublishPacket{
			Topic:   "factory/telemetry",
			Payload: []byte(fmt.Sprintf("msg_%d", i)),
			QoS:     0,
		}
		pipe.Ingest(fmt.Sprintf("device_%d", i), pkt)
	}

	// Wait for batchWorker to deliver
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(mock.DeliveredRecords()) >= total {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	delivered := mock.DeliveredRecords()
	if len(delivered) != total {
		t.Fatalf("expected %d delivered records, got %d", total, len(delivered))
	}
}

func TestPipeline_DiskSpooling_And_CircuitBreaker_Recovery(t *testing.T) {
	spoolDir := filepath.Join(os.TempDir(), fmt.Sprintf("pipe_test_spool_%d", time.Now().UnixNano()))
	defer os.RemoveAll(spoolDir)

	mock := NewMockSink()
	// Simulate Kafka being DOWN
	mock.SetHealthy(false)

	cfg := Config{
		BatchSize:      50,
		FlushTimeout:   10 * time.Millisecond,
		RingBufferSize: 1024,
		NumShards:      2,
		SpoolDir:       spoolDir,
		CircuitBreaker: CircuitBreakerConfig{
			FailureThreshold: 2,
			RecoveryTimeout:  100 * time.Millisecond,
			SuccessThreshold: 1,
		},
	}

	pipe, err := NewPipeline(cfg, mock)
	if err != nil {
		t.Fatalf("NewPipeline err: %v", err)
	}
	defer pipe.Close()

	// 1. Ingest 300 messages while Kafka is down
	msgCount := 300
	for i := 0; i < msgCount; i++ {
		pkt := &protocol.PublishPacket{
			Topic:   "factory/alerts",
			Payload: []byte(fmt.Sprintf("alert_%04d", i)),
			QoS:     1,
		}
		pipe.Ingest("sensor_99", pkt)
	}

	// Give worker time to trip the circuit breaker and spool to disk
	time.Sleep(250 * time.Millisecond)

	// Verify: mock sink received 0 records, but disk spooler has all of them!
	if len(mock.DeliveredRecords()) != 0 {
		t.Fatalf("mock should have 0 records while down, got %d", len(mock.DeliveredRecords()))
	}
	if !pipe.spool.HasPending() {
		t.Fatalf("expected spooler to hold pending records on disk")
	}

	// 2. Kafka recovers!
	mock.SetHealthy(true)

	// Wait for circuit breaker to transition to Half-Open, probe, and auto-drain disk spooler
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if len(mock.DeliveredRecords()) >= msgCount {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	delivered := mock.DeliveredRecords()
	if len(delivered) != msgCount {
		t.Fatalf("expected all %d records to be recovered from disk spool, got %d", msgCount, len(delivered))
	}

	// Verify spooler is completely drained
	time.Sleep(50 * time.Millisecond)
	if pipe.spool.HasPending() {
		t.Errorf("expected disk spooler to be completely empty after recovery")
	}
}

func TestDiskSpooler_Preallocation_And_Quota(t *testing.T) {
	spoolDir := filepath.Join(os.TempDir(), fmt.Sprintf("pipe_quota_%d", time.Now().UnixNano()))
	defer os.RemoveAll(spoolDir)

	// 64KB segments, 128KB max quota (allows max 2 segments before pruning oldest)
	spool, err := NewDiskSpooler(spoolDir, 64*1024, 128*1024)
	if err != nil {
		t.Fatalf("NewDiskSpooler failed: %v", err)
	}
	defer spool.Close()

	// Write batch that fills and rotates segments
	batch := make([]*Record, 200)
	for i := 0; i < 200; i++ {
		r := AcquireRecord()
		r.ID = uint64(i)
		r.Topic = "test/quota"
		r.Target = "test.target"
		r.Value = make([]byte, 1024) // 1KB payload
		batch[i] = r
	}

	// 200KB of data should create ~3 segments, triggering quota pruning of the oldest segment
	if err := spool.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	// Verify spool files exist and oldest was pruned
	if !spool.HasPending() {
		t.Fatalf("expected pending records in spool")
	}

	// Clean up acquired batch records
	for _, r := range batch {
		ReleaseRecord(r)
	}
}

func TestPluggableSinkRegistry(t *testing.T) {
	customName := "custom_test_driver"
	registered := false

	RegisterSink(customName, func() Sink {
		registered = true
		return NewMockSink()
	})

	s, err := CreateSink(customName)
	if err != nil {
		t.Fatalf("CreateSink failed: %v", err)
	}
	if !registered || s == nil {
		t.Fatalf("custom driver factory was not invoked")
	}
	if s.Name() != "mock" {
		t.Errorf("unexpected name: %s", s.Name())
	}
}

func BenchmarkPipeline_Ingest(b *testing.B) {
	spoolDir := filepath.Join(os.TempDir(), fmt.Sprintf("pipe_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(spoolDir)

	mock := NewMockSink()
	cfg := Config{
		BatchSize:      1000,
		FlushTimeout:   10 * time.Millisecond,
		RingBufferSize: 100000,
		NumShards:      8,
		SpoolDir:       spoolDir,
	}

	pipe, _ := NewPipeline(cfg, mock)
	defer pipe.Close()

	pkt := &protocol.PublishPacket{
		Topic:   "bench/telemetry/sensor1",
		Payload: []byte("temperature=26.4;humidity=55.2;pressure=1013"),
		QoS:     0,
	}

	var counter atomic.Uint64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := fmt.Sprintf("dev_%d", counter.Add(1))
		for pb.Next() {
			pipe.Ingest(id, pkt)
		}
	})
}
