package core

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"mqtt/pkg/cluster"
	"mqtt/pkg/hook"
	"mqtt/pkg/limiter"
	"mqtt/pkg/pipeline"
	"mqtt/pkg/server"
	"mqtt/pkg/store"
)

// Broker is the core MQTT Broker facade.
// It can be run standalone as an embedded Go dependency in external services,
// or coupled with the optional dashboard management server.
type Broker struct {
	cfg            *brokerConfig
	srv            *server.Server
	store          store.MessageStore
	hookMgr        *hook.Manager
	pipeline       *pipeline.Pipeline
	pipelineRouter *pipeline.Router
	clusterMesh    *cluster.ClusterMesh
}

// NewBroker instantiates an MQTT broker engine with the provided options.
func NewBroker(opts ...Option) (*Broker, error) {
	cfg := defaultBrokerConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	tcpAddr := cfg.tcpAddr
	if tcpAddr != "" && !strings.Contains(tcpAddr, "://") {
		tcpAddr = "tcp://" + tcpAddr
	}

	srvCfg := server.Config{
		Addr:         tcpAddr,
		Multicore:    cfg.multicore,
		ReusePort:    cfg.reusePort,
		TCPKeepAlive: cfg.tcpKeepAlive,
		MetricsAddr:  cfg.metricsAddr,
		ConnLimit: limiter.ConnLimiterConfig{
			GlobalRate:  cfg.rateLimitConn,
			GlobalBurst: cfg.rateLimitConnBst,
		},
		PublishLimit: limiter.PublishLimiterConfig{
			Rate:  cfg.rateLimitPub,
			Burst: cfg.rateLimitPubBst,
		},
	}

	srv := server.NewServer(srvCfg, cfg.store, cfg.hookMgr, nil)

	var pipe *pipeline.Pipeline
	if cfg.pipelineConfig != nil {
		driver, err := pipeline.CreateSink(cfg.pipelineConfig.driver)
		if err != nil {
			return nil, fmt.Errorf("failed to create pipeline sink '%s': %w", cfg.pipelineConfig.driver, err)
		}

		spoolDir := cfg.pipelineConfig.spoolDir
		if spoolDir == "" {
			spoolDir = "./data/pipeline_spool"
		}
		maxQuota := int64(cfg.pipelineConfig.maxQuotaGB) * 1024 * 1024 * 1024
		if maxQuota <= 0 {
			maxQuota = 5 * 1024 * 1024 * 1024
		}
		batchSize := cfg.pipelineConfig.batchSize
		if batchSize <= 0 {
			batchSize = 1000
		}
		flushTimeout := time.Duration(cfg.pipelineConfig.flushMs) * time.Millisecond
		if flushTimeout <= 0 {
			flushTimeout = 20 * time.Millisecond
		}

		p, err := pipeline.NewPipeline(pipeline.Config{
			BatchSize:      batchSize,
			FlushTimeout:   flushTimeout,
			RingBufferSize: 65536,
			NumShards:      runtime.NumCPU(),
			SpoolDir:       spoolDir,
			MaxSegmentSize: 16 * 1024 * 1024,
			MaxDiskQuota:   maxQuota,
			TopicFilters:   cfg.pipelineConfig.filters,
			SinkConfig:     cfg.pipelineConfig.driverConfig,
		}, driver)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize streaming pipeline: %w", err)
		}
		pipe = p
		srv.SetPipeline(pipe)
	}

	var mesh *cluster.ClusterMesh
	if cfg.clusterConfig != nil && cfg.clusterConfig.nodeID != "" && cfg.clusterConfig.listenAddr != "" {
		mesh = cluster.NewClusterMesh(cfg.clusterConfig.nodeID, cfg.clusterConfig.listenAddr, srv)
		srv.SetClusterMesh(mesh)
	}

	pr := srv.PipelineRouter()
	for _, pb := range cfg.pipelines {
		pr.Handle(pb.topicFilter, pb.pipeline)
	}

	return &Broker{
		cfg:            cfg,
		srv:            srv,
		store:          cfg.store,
		hookMgr:        cfg.hookMgr,
		pipeline:       pipe,
		pipelineRouter: pr,
		clusterMesh:    mesh,
	}, nil
}

// Start boots all configured listeners (Cluster Mesh, TLS, WebSocket, QUIC) and the core TCP engine.
// This call blocks until the broker is stopped or encounters an error.
func (b *Broker) Start() error {
	if b.clusterMesh != nil {
		if err := b.clusterMesh.Start(); err != nil {
			return fmt.Errorf("cluster mesh startup failed: %w", err)
		}
		if len(b.cfg.clusterConfig.seeds) > 0 {
			if err := b.clusterMesh.Join(b.cfg.clusterConfig.seeds); err != nil {
				log.Printf("[Broker] Notice: cluster seed join: %v", err)
			}
		}
		for _, p := range b.cfg.clusterConfig.peers {
			parts := strings.Split(p, "=")
			if len(parts) == 2 {
				peerID, peerAddr := parts[0], parts[1]
				if err := b.clusterMesh.ConnectPeer(peerID, peerAddr); err != nil {
					log.Printf("[Broker] Warning: failed to connect to peer %s: %v", peerID, err)
				}
			}
		}
	}

	if b.cfg.tlsConfig != nil {
		if _, err := b.srv.StartTLS(*b.cfg.tlsConfig); err != nil {
			return fmt.Errorf("failed to start TLS listener: %w", err)
		}
	}

	if b.cfg.wsConfig != nil {
		if _, err := b.srv.StartWS(*b.cfg.wsConfig); err != nil {
			return fmt.Errorf("failed to start WebSocket listener: %w", err)
		}
	}

	if b.cfg.quicConfig != nil {
		if _, err := b.srv.StartQUIC(*b.cfg.quicConfig); err != nil {
			return fmt.Errorf("failed to start QUIC listener: %w", err)
		}
	}

	return b.srv.Start()
}

// Stop gracefully shuts down the broker and all active transport listeners, cluster mesh, and storage.
func (b *Broker) Stop(ctx context.Context) error {
	if b.clusterMesh != nil {
		b.clusterMesh.BroadcastPeerLeave()
		_ = b.clusterMesh.Close()
	}
	err := b.srv.Stop(ctx)
	if b.store != nil {
		_ = b.store.Close()
	}
	return err
}

// In-Process Pub/Sub APIs

// Publish publishes a message into the broker engine directly.
// The message is delivered to matching local subscribers, persisted if retain=true,
// sent to data streaming pipeline, and routed across cluster mesh if clustered.
func (b *Broker) Publish(topic string, payload []byte, qos byte, retain bool) error {
	return b.srv.PublishMessage(topic, qos, retain, payload)
}

// PublishMessage sends a message into the broker directly (implements BrokerInterface).
func (b *Broker) PublishMessage(topic string, qos byte, retain bool, payload []byte) error {
	return b.srv.PublishMessage(topic, qos, retain, payload)
}

// Subscribe registers an in-process topic subscriber.
// Filter supports exact topics or MQTT wildcards (+, #).
// Returns an unsubscribe function and an error if subscription fails.
func (b *Broker) Subscribe(filter string, qos byte, handler MessageHandler) (func(), error) {
	_, unsub, err := b.srv.SubscribeInternal(filter, handler)
	return unsub, err
}

// SubscribeInternal registers an in-process topic subscriber returning the unique subscription ID.
func (b *Broker) SubscribeInternal(filter string, handler func(topic string, payload []byte)) (string, func(), error) {
	return b.srv.SubscribeInternal(filter, handler)
}

// UnsubscribeInternal unregisters an in-process topic subscriber by ID.
func (b *Broker) UnsubscribeInternal(id string) error {
	return b.srv.UnsubscribeInternal(id)
}

// Server returns the underlying Server instance.
func (b *Broker) Server() *server.Server {
	return b.srv
}

// Telemetry & Management APIs (delegated directly to Server)

func (b *Broker) GetOverview() OverviewStats {
	return b.srv.GetOverview()
}

func (b *Broker) GetClients(page, limit int, query string) ([]ClientSummary, int) {
	return b.srv.GetClients(page, limit, query)
}

func (b *Broker) GetClientDetail(clientID string) (*ClientDetail, bool) {
	return b.srv.GetClientDetail(clientID)
}

func (b *Broker) KickClient(clientID string) error {
	return b.srv.KickClient(clientID)
}

func (b *Broker) GetSubscriptions(page, limit int, query string) ([]SubscriptionSummary, int) {
	return b.srv.GetSubscriptions(page, limit, query)
}

func (b *Broker) UnsubscribeClient(clientID, topic string) error {
	return b.srv.UnsubscribeClient(clientID, topic)
}

func (b *Broker) GetRetainedMessages() ([]RetainedSummary, error) {
	return b.srv.GetRetainedMessages()
}

func (b *Broker) DeleteRetainedMessage(topic string) error {
	return b.srv.DeleteRetainedMessage(topic)
}

func (b *Broker) GetListeners() []ListenerSummary {
	return b.srv.GetListeners()
}

func (b *Broker) GetClusterNodes() ClusterSummary {
	return b.srv.GetClusterNodes()
}

func (b *Broker) GetPipelineStatus() *PipelineSummary {
	return b.srv.GetPipelineStatus()
}

func (b *Broker) GetRules() []RuleStatus {
	return b.srv.GetRules()
}

func (b *Broker) GetRule(id string) (*RuleStatus, error) {
	return b.srv.GetRule(id)
}

func (b *Broker) UpdateRule(r Rule) error {
	return b.srv.UpdateRule(r)
}

func (b *Broker) DeleteRule(id string) error {
	return b.srv.DeleteRule(id)
}

func (b *Broker) PingRule(ctx context.Context, id string) (*RulePingResult, error) {
	return b.srv.PingRule(ctx, id)
}

func (b *Broker) TestMatchTopic(topic string) []string {
	return b.srv.TestMatchTopic(topic)
}

func (b *Broker) GetBridges() []BridgeStatus {
	return b.srv.GetBridges()
}

func (b *Broker) GetBridge(id string) (*BridgeStatus, error) {
	return b.srv.GetBridge(id)
}

func (b *Broker) UpdateBridge(br Bridge) error {
	return b.srv.UpdateBridge(br)
}

func (b *Broker) DeleteBridge(id string) error {
	return b.srv.DeleteBridge(id)
}

func (b *Broker) PingBridge(ctx context.Context, id string) (*RulePingResult, error) {
	return b.srv.PingBridge(ctx, id)
}

func (b *Broker) GetRegisteredDrivers() []string {
	return b.srv.GetRegisteredDrivers()
}

// Idiomatic Go Pipeline & Middleware APIs

// AddProcessor attaches a named Processor to a topic pattern's pipeline.
func (b *Broker) AddProcessor(topicFilter, name string, proc Processor) {
	if b.pipelineRouter != nil {
		b.pipelineRouter.Add(topicFilter, name, proc)
	}
}

// AddProcessorFunc attaches a functional processor to a topic pattern's pipeline.
func (b *Broker) AddProcessorFunc(topicFilter, name string, matchFn func(c *Context) bool, processFn func(c *Context) error) {
	if b.pipelineRouter != nil {
		b.pipelineRouter.AddFunc(topicFilter, name, matchFn, processFn)
	}
}

// Handle registers a Pipeline for a topic pattern.
func (b *Broker) Handle(topicFilter string, p *Pipeline) {
	if b.pipelineRouter != nil {
		b.pipelineRouter.Handle(topicFilter, p)
	}
}

// RemovePipeline unregisters a pipeline for a topic pattern.
func (b *Broker) RemovePipeline(topicFilter string) bool {
	if b.pipelineRouter != nil {
		return b.pipelineRouter.Remove(topicFilter)
	}
	return false
}

// GetPipeline retrieves the pipeline for a topic pattern.
func (b *Broker) GetPipeline(topicFilter string) (*Pipeline, bool) {
	if b.pipelineRouter != nil {
		return b.pipelineRouter.Get(topicFilter)
	}
	return nil, false
}

// PipelineRouter returns the underlying pipeline Router.
func (b *Broker) PipelineRouter() *Router {
	return b.pipelineRouter
}
