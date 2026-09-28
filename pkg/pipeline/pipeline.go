package pipeline

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mqtt/pkg/protocol"
)

type Config struct {
	SinkDriver      string
	SinkConfig      map[string]any
	TopicFilters    []string
	BatchSize       int
	FlushTimeout    time.Duration
	RingBufferSize  int
	NumShards       int
	SpoolDir        string
	MaxSegmentSize  int64
	MaxDiskQuota    int64
	CircuitBreaker  CircuitBreakerConfig
	TargetFormatter func(topic string, clientID string) (targetTopic string, key []byte)
}

// PipelineStats tracks streaming counters.
type PipelineStats struct {
	IngestedTotal atomic.Uint64
	DirectSent    atomic.Uint64
	SpooledTotal  atomic.Uint64
	DrainedTotal  atomic.Uint64
	DroppedTotal  atomic.Uint64
}

// Pipeline coordinates zero-overhead Ingest, Sharded RingBuffers, batch disk spooling, and Egress workers.
type Pipeline struct {
	cfg         Config
	sink        Sink
	ruleEngine  *RuleEngine
	shardedRing *ShardedRingBuffer
	spool       *DiskSpooler
	breaker     *CircuitBreaker
	stats       PipelineStats
	seq         atomic.Uint64
	stopCh      chan struct{}
	notifyCh    chan struct{}
	closeOnce   sync.Once
	workerWg    sync.WaitGroup
}

// NewPipeline initializes and launches the streaming pipeline with worker threads.
func NewPipeline(cfg Config, sink Sink) (*Pipeline, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 20 * time.Millisecond
	}
	if cfg.RingBufferSize <= 0 {
		cfg.RingBufferSize = 65536
	}
	if cfg.NumShards <= 0 {
		cfg.NumShards = 8
	}
	if cfg.SpoolDir == "" {
		cfg.SpoolDir = "./data/pipeline_spool"
	}
	if cfg.TargetFormatter == nil {
		var topicCache sync.Map
		cfg.TargetFormatter = func(topic string, clientID string) (string, []byte) {
			if val, ok := topicCache.Load(topic); ok {
				return val.(string), StringToBytes(clientID)
			}
			target := strings.ReplaceAll(topic, "/", ".")
			topicCache.Store(topic, target)
			return target, StringToBytes(clientID)
		}
	}

	spool, err := NewDiskSpooler(cfg.SpoolDir, cfg.MaxSegmentSize, cfg.MaxDiskQuota)
	if err != nil {
		return nil, fmt.Errorf("pipeline: failed to init disk spooler: %w", err)
	}

	// Initialize Rule Engine with persistence
	ruleFile := filepath.Join(cfg.SpoolDir, "data_integration.json")
	if _, err := filepath.Glob(ruleFile); err != nil || !fileExists(ruleFile) {
		legacyFile := filepath.Join(cfg.SpoolDir, "rules.json")
		if fileExists(legacyFile) {
			ruleFile = legacyFile
		}
	}
	filters := cfg.TopicFilters
	if len(filters) == 0 {
		filters = []string{"#"}
	}
	driver := cfg.SinkDriver
	if driver == "" {
		if sink != nil {
			driver = sink.Name()
		} else {
			driver = "kafka"
		}
	}
	targetTopic := "mqtt_events"
	if t, ok := cfg.SinkConfig["topic"].(string); ok && t != "" {
		targetTopic = t
	}
	defaultRule := &Rule{
		ID:            "rule_default",
		Name:          "默认全量消息流转",
		Description:   "将所有设备与客户端上报的消息流转至默认 Kafka 集群",
		Enabled:       true,
		TopicFilters:  filters,
		TargetTopic:   targetTopic,
		KeyStrategy:   "client_id",
		PayloadFormat: "raw",
		Actions: []RuleAction{
			{
				BridgeID:    "bridge_kafka_default",
				TargetTopic: targetTopic,
				KeyStrategy: "client_id",
			},
		},
		SinkType:   driver,
		SinkConfig: cfg.SinkConfig,
	}
	ruleEngine := NewRuleEngine(ruleFile, defaultRule)

	p := &Pipeline{
		cfg:         cfg,
		sink:        sink,
		ruleEngine:  ruleEngine,
		shardedRing: NewShardedRingBuffer(cfg.RingBufferSize, cfg.NumShards),
		spool:       spool,
		breaker:     NewCircuitBreaker(cfg.CircuitBreaker),
		stopCh:      make(chan struct{}),
		notifyCh:    make(chan struct{}, 4),
	}

	if p.sink != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.sink.Init(ctx, cfg.SinkConfig)
		cancel()
	}

	// Launch fixed long-running Egress workers (e.g. 2 dedicated workers)
	numWorkers := 2
	for i := 0; i < numWorkers; i++ {
		p.workerWg.Add(1)
		go p.workerLoop(i)
	}

	// Launch periodic deferred fsync routine (1s flush)
	p.workerWg.Add(1)
	go p.syncLoop()

	return p, nil
}

// IngestDirect is called from MQTT Broker on the zero-copy fast path.
// It is strictly non-blocking, performs 0 heap allocations, and never touches disk or network.
func (p *Pipeline) IngestDirect(clientID string, topic string, payload []byte, qos byte) {
	if p.ruleEngine != nil {
		records := p.ruleEngine.Evaluate(clientID, topic, payload, qos)
		if len(records) == 0 {
			return
		}
		for _, rec := range records {
			p.enqueueRecord(rec, clientID)
		}
		return
	}

	if !p.matchTopic(topic) {
		return
	}

	// Acquire pre-allocated record from pool (0 allocations)
	rec := AcquireRecord()
	rec.ID = p.seq.Add(1)
	rec.Topic = topic
	rec.Target, rec.Key = p.cfg.TargetFormatter(topic, clientID)
	rec.Value = payload
	rec.Timestamp = time.Now()
	rec.QoS = qos

	p.enqueueRecord(rec, clientID)
}

func (p *Pipeline) enqueueRecord(rec *Record, clientID string) {
	p.stats.IngestedTotal.Add(1)
	hash := HashString(clientID)
	accepted, overflow := p.shardedRing.Push(hash, rec)
	if !accepted {
		p.stats.DroppedTotal.Add(1)
		ReleaseRecord(rec)
		return
	}
	if overflow {
		p.stats.DroppedTotal.Add(1)
	}

	// Wake up egress worker non-blockingly (0 allocations, ~10ns)
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}

// Ingest wraps IngestDirect for standard PublishPacket.
func (p *Pipeline) Ingest(clientID string, pkt *protocol.PublishPacket) {
	p.IngestDirect(clientID, pkt.Topic, pkt.Payload, pkt.QoS)
}

func (p *Pipeline) matchTopic(topic string) bool {
	if len(p.cfg.TopicFilters) == 0 {
		return true
	}
	for _, filter := range p.cfg.TopicFilters {
		if filter == "#" || filter == topic {
			return true
		}
		if strings.HasSuffix(filter, "/#") {
			prefix := strings.TrimSuffix(filter, "/#")
			if topic == prefix || strings.HasPrefix(topic, prefix+"/") {
				return true
			}
		}
	}
	return false
}

// workerLoop collects batches from assigned shards, managing network dispatching and disk spooling.
func (p *Pipeline) workerLoop(workerID int) {
	defer p.workerWg.Done()

	ticker := time.NewTicker(p.cfg.FlushTimeout)
	defer ticker.Stop()

	batchBuf := make([]*Record, 0, p.cfg.BatchSize)

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.notifyCh:
		case <-ticker.C:
		}

		// Keep processing batches until neither disk spool nor ring buffers have pending data
		for {
			more := p.processOneBatch(workerID, &batchBuf)
			if !more {
				break
			}
		}
	}
}

func (p *Pipeline) processOneBatch(workerID int, batchBuf *[]*Record) bool {
	// 1. Priority Drain from Disk Spooler
	if p.spool.HasPending() && p.breaker.Allow() {
		spoolBatch, err := p.spool.ReadBatch(p.cfg.BatchSize)
		if err == nil && len(spoolBatch) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			failed, sendErr := p.sink.SendBatch(ctx, spoolBatch)
			cancel()

			if sendErr != nil {
				p.breaker.RecordFailure()
				// Re-spool failed records in single batch write
				_ = p.spool.AppendBatch(failed)
				return false
			}

			p.breaker.RecordSuccess()
			p.stats.DrainedTotal.Add(uint64(len(spoolBatch)))
			for _, r := range spoolBatch {
				ReleaseRecord(r)
			}
			return true
		}
	}

	// 2. Collect Batch from assigned shards in round-robin fashion
	*batchBuf = (*batchBuf)[:0]
	numShards := p.shardedRing.NumShards()
	for s := 0; s < numShards; s++ {
		shardIdx := (workerID*numShards/2 + s) % numShards
		*batchBuf = p.shardedRing.PopBatch(shardIdx, p.cfg.BatchSize-len(*batchBuf), *batchBuf)
		if len(*batchBuf) >= p.cfg.BatchSize {
			break
		}
	}

	if len(*batchBuf) == 0 {
		return false
	}

	// 3. Dispatch Batch: Circuit Open -> AppendBatch to Disk Spooler
	if !p.breaker.Allow() {
		_ = p.spool.AppendBatch(*batchBuf)
		p.stats.SpooledTotal.Add(uint64(len(*batchBuf)))
		for _, r := range *batchBuf {
			ReleaseRecord(r)
		}
		return true
	}

	// 4. Circuit Closed: SendBatch to Sink
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	failed, sendErr := p.sink.SendBatch(ctx, *batchBuf)
	cancel()

	if sendErr != nil {
		p.breaker.RecordFailure()
		// Spill failed records to disk in single batch write to avoid loss!
		_ = p.spool.AppendBatch(failed)
		p.stats.SpooledTotal.Add(uint64(len(failed)))
	} else {
		p.breaker.RecordSuccess()
		p.stats.DirectSent.Add(uint64(len(*batchBuf)))
	}

	// Return successfully sent records to pool
	for _, r := range *batchBuf {
		ReleaseRecord(r)
	}

	return len(*batchBuf) >= p.cfg.BatchSize
}

// syncLoop executes deferred fsync every 1 second to flush PageCache without stalling writers.
func (p *Pipeline) syncLoop() {
	defer p.workerWg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			_ = p.spool.Sync()
		}
	}
}

// Stats returns a snapshot of pipeline streaming metrics.
func (p *Pipeline) Stats() (ingested, direct, spooled, drained, dropped uint64) {
	return p.stats.IngestedTotal.Load(),
		p.stats.DirectSent.Load(),
		p.stats.SpooledTotal.Load(),
		p.stats.DrainedTotal.Load(),
		p.stats.DroppedTotal.Load()
}

// Close gracefully flushes remaining memory buffers to disk spooler and closes files.
func (p *Pipeline) Close() error {
	p.closeOnce.Do(func() {
		close(p.stopCh)
		p.workerWg.Wait()

		log.Println("[Pipeline] Closing: draining remaining in-flight records to disk spool...")
		// Drain remaining records from all shards to disk in batches
		numShards := p.shardedRing.NumShards()
		drainBuf := make([]*Record, 0, p.cfg.BatchSize)
		for s := 0; s < numShards; s++ {
			for {
				drainBuf = p.shardedRing.PopBatch(s, p.cfg.BatchSize, drainBuf[:0])
				if len(drainBuf) == 0 {
					break
				}
				_ = p.spool.AppendBatch(drainBuf)
				p.stats.SpooledTotal.Add(uint64(len(drainBuf)))
				for _, r := range drainBuf {
					ReleaseRecord(r)
				}
			}
		}

		if p.spool != nil {
			_ = p.spool.Close()
		}
		if p.sink != nil {
			_ = p.sink.Close()
		}
	})
	return nil
}

// PipelineSnapshot captures runtime state for management monitoring.
type PipelineSnapshot struct {
	SinkDriver     string
	CircuitBreaker string
	IngestedTotal  uint64
	DirectSent     uint64
	SpooledTotal   uint64
	DrainedTotal   uint64
	DroppedTotal   uint64
	DiskUsageMB    int64
	MaxDiskQuotaGB int64
}

// Snapshot returns point-in-time statistics.
func (p *Pipeline) Snapshot() PipelineSnapshot {
	cbState := "Closed"
	if p.breaker != nil {
		cbState = p.breaker.State().String()
	}
	var diskUsageMB int64
	if p.spool != nil {
		diskUsageMB = p.spool.DiskUsage() / (1024 * 1024)
	}
	maxGB := p.cfg.MaxDiskQuota / (1024 * 1024 * 1024)

	return PipelineSnapshot{
		SinkDriver:     p.cfg.SinkDriver,
		CircuitBreaker: cbState,
		IngestedTotal:  p.stats.IngestedTotal.Load(),
		DirectSent:     p.stats.DirectSent.Load(),
		SpooledTotal:   p.stats.SpooledTotal.Load(),
		DrainedTotal:   p.stats.DrainedTotal.Load(),
		DroppedTotal:   p.stats.DroppedTotal.Load(),
		DiskUsageMB:    diskUsageMB,
		MaxDiskQuotaGB: maxGB,
	}
}

// RuleEngine returns the underlying rule engine instance.
func (p *Pipeline) RuleEngine() *RuleEngine {
	return p.ruleEngine
}

// GetRules returns a snapshot of rules with current runtime status.
func (p *Pipeline) GetRules() []RuleStatus {
	if p.ruleEngine == nil {
		return nil
	}
	rawRules := p.ruleEngine.GetRules()
	snap := p.Snapshot()
	var res []RuleStatus
	for _, r := range rawRules {
		st := RuleStatus{
			Rule:           r,
			Connected:      p.breaker.State() != CircuitOpen,
			CircuitBreaker: p.breaker.State().String(),
			TotalMatched:   snap.IngestedTotal,
			TotalDelivered: snap.DirectSent,
			QueuePending:   p.shardedRing.TotalLen(),
			TotalDropped:   snap.DroppedTotal,
		}
		res = append(res, st)
	}
	return res
}

// GetRule returns a single rule status.
func (p *Pipeline) GetRule(id string) (*RuleStatus, error) {
	if p.ruleEngine == nil {
		return nil, fmt.Errorf("rule engine not initialized")
	}
	r, err := p.ruleEngine.GetRule(id)
	if err != nil {
		return nil, err
	}
	snap := p.Snapshot()
	return &RuleStatus{
		Rule:           *r,
		Connected:      p.breaker.State() != CircuitOpen,
		CircuitBreaker: p.breaker.State().String(),
		TotalMatched:   snap.IngestedTotal,
		TotalDelivered: snap.DirectSent,
		QueuePending:   p.shardedRing.TotalLen(),
		TotalDropped:   snap.DroppedTotal,
	}, nil
}

// UpdateRule updates a rule and reloads sink connections if needed.
func (p *Pipeline) UpdateRule(r Rule) error {
	if p.ruleEngine == nil {
		return fmt.Errorf("rule engine not initialized")
	}
	if err := p.ruleEngine.UpdateRule(r); err != nil {
		return err
	}
	// Hot-reload Kafka brokers if sink is kafka
	if kSink, ok := p.sink.(*KafkaSink); ok && r.SinkType == "kafka" {
		brokers := ExtractBrokers(r.SinkConfig)
		if len(brokers) > 0 {
			kSink.UpdateBrokers(brokers)
		}
	}
	return nil
}

// PingRule tests network connectivity to the target MQ.
func (p *Pipeline) PingRule(ctx context.Context, id string) (*RulePingResult, error) {
	start := time.Now()
	var target string

	if p.ruleEngine != nil {
		if r, err := p.ruleEngine.GetRule(id); err == nil {
			if brokers := ExtractBrokers(r.SinkConfig); len(brokers) > 0 {
				target = strings.Join(brokers, ",")
			}
		}
	}
	if target == "" {
		if kSink, ok := p.sink.(*KafkaSink); ok {
			target = strings.Join(kSink.GetBrokers(), ",")
		} else if p.sink != nil {
			target = p.sink.Name()
		} else {
			target = "none"
		}
	}

	if p.sink == nil {
		return &RulePingResult{
			Success:   false,
			LatencyMs: 0,
			Target:    target,
			Message:   "no sink driver configured",
		}, nil
	}

	err := p.sink.Ping(ctx)
	elapsed := time.Since(start).Milliseconds()

	if err != nil {
		return &RulePingResult{
			Success:   false,
			LatencyMs: elapsed,
			Target:    target,
			Message:   err.Error(),
		}, nil
	}

	return &RulePingResult{
		Success:   true,
		LatencyMs: elapsed,
		Target:    target,
		Message:   "connection test passed",
	}, nil
}

// DeleteRule removes a rule by ID.
func (p *Pipeline) DeleteRule(id string) error {
	if p.ruleEngine == nil {
		return fmt.Errorf("rule engine not initialized")
	}
	return p.ruleEngine.DeleteRule(id)
}

// TestMatchTopic returns which rules match the given MQTT topic.
func (p *Pipeline) TestMatchTopic(topic string) []string {
	if p.ruleEngine == nil {
		return nil
	}
	return p.ruleEngine.TestMatch(topic)
}

// GetBridges returns all data bridges with runtime telemetry.
func (p *Pipeline) GetBridges() []BridgeStatus {
	if p.ruleEngine == nil {
		return nil
	}
	rawBridges := p.ruleEngine.GetBridges()
	snap := p.Snapshot()
	var res []BridgeStatus
	for _, b := range rawBridges {
		st := BridgeStatus{
			Bridge:         b,
			Status:         "connected",
			CircuitBreaker: p.breaker.State().String(),
			DeliveredTotal: snap.DirectSent,
			PendingTotal:   p.shardedRing.TotalLen(),
			DroppedTotal:   snap.DroppedTotal,
			LatencyMs:      1,
		}
		if p.breaker.State() == CircuitOpen {
			st.Status = "disconnected"
		}
		res = append(res, st)
	}
	return res
}

// GetBridge returns a single data bridge status.
func (p *Pipeline) GetBridge(id string) (*BridgeStatus, error) {
	if p.ruleEngine == nil {
		return nil, fmt.Errorf("rule engine not initialized")
	}
	b, err := p.ruleEngine.GetBridge(id)
	if err != nil {
		return nil, err
	}
	snap := p.Snapshot()
	st := &BridgeStatus{
		Bridge:         *b,
		Status:         "connected",
		CircuitBreaker: p.breaker.State().String(),
		DeliveredTotal: snap.DirectSent,
		PendingTotal:   p.shardedRing.TotalLen(),
		DroppedTotal:   snap.DroppedTotal,
		LatencyMs:      1,
	}
	if p.breaker.State() == CircuitOpen {
		st.Status = "disconnected"
	}
	return st, nil
}

// UpdateBridge updates bridge configuration and hot-reloads drivers.
func (p *Pipeline) UpdateBridge(b Bridge) error {
	if p.ruleEngine == nil {
		return fmt.Errorf("rule engine not initialized")
	}
	if err := p.ruleEngine.UpdateBridge(b); err != nil {
		return err
	}
	if kSink, ok := p.sink.(*KafkaSink); ok && b.Type == "kafka" && len(b.Servers) > 0 {
		kSink.UpdateBrokers(b.Servers)
	}
	if rSink, ok := p.sink.(*RedpandaSink); ok && b.Type == "redpanda" && len(b.Servers) > 0 {
		rSink.UpdateBrokers(b.Servers)
	}
	if nSink, ok := p.sink.(*NATSSink); ok && b.Type == "nats" && len(b.Servers) > 0 {
		nSink.UpdateServers(b.Servers)
	}
	return nil
}

// DeleteBridge deletes a bridge by ID.
func (p *Pipeline) DeleteBridge(id string) error {
	if p.ruleEngine == nil {
		return fmt.Errorf("rule engine not initialized")
	}
	return p.ruleEngine.DeleteBridge(id)
}

// PingBridge actively probes network connectivity to the target MQ bridge.
func (p *Pipeline) PingBridge(ctx context.Context, id string) (*RulePingResult, error) {
	if p.ruleEngine == nil {
		return nil, fmt.Errorf("rule engine not initialized")
	}
	b, err := p.ruleEngine.GetBridge(id)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	target := strings.Join(b.Servers, ",")
	if target == "" {
		target = b.Type
	}

	// For kafka and redpanda bridges: probe via TCP (default 9092)
	if (b.Type == "kafka" || b.Type == "redpanda") && len(b.Servers) > 0 {
		var probeErr error
		for _, srv := range b.Servers {
			addr := srv
			if !strings.Contains(addr, ":") {
				addr = addr + ":9092"
			}
			d := net.Dialer{Timeout: 3 * time.Second}
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				probeErr = err
			} else {
				conn.Close()
				probeErr = nil
				break
			}
		}
		elapsed := time.Since(start).Milliseconds()
		if probeErr != nil {
			return &RulePingResult{
				Success:   false,
				LatencyMs: elapsed,
				Target:    target,
				Message:   probeErr.Error(),
			}, nil
		}
		return &RulePingResult{
			Success:   true,
			LatencyMs: elapsed,
			Target:    target,
			Message:   "connection test passed",
		}, nil
	}

	// For nats bridges: probe via NATS protocol / TCP probe (default 4222)
	if b.Type == "nats" && len(b.Servers) > 0 {
		var probeErr error
		for _, srv := range b.Servers {
			addr := cleanServerAddr(srv, "4222")
			if err := probeNATSServer(ctx, addr); err != nil {
				probeErr = err
			} else {
				probeErr = nil
				break
			}
		}
		elapsed := time.Since(start).Milliseconds()
		if probeErr != nil {
			return &RulePingResult{
				Success:   false,
				LatencyMs: elapsed,
				Target:    target,
				Message:   probeErr.Error(),
			}, nil
		}
		return &RulePingResult{
			Success:   true,
			LatencyMs: elapsed,
			Target:    target,
			Message:   "connection test passed",
		}, nil
	}

	if p.sink != nil {
		err := p.sink.Ping(ctx)
		elapsed := time.Since(start).Milliseconds()
		if err != nil {
			return &RulePingResult{
				Success:   false,
				LatencyMs: elapsed,
				Target:    target,
				Message:   err.Error(),
			}, nil
		}
		return &RulePingResult{
			Success:   true,
			LatencyMs: elapsed,
			Target:    target,
			Message:   "connection test passed",
		}, nil
	}

	return &RulePingResult{
		Success:   true,
		LatencyMs: 0,
		Target:    target,
		Message:   "driver ready",
	}, nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
