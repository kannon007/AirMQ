package core

import (
	"fmt"
	"time"

	"mqtt/pkg/hook"
	"mqtt/pkg/server"
	"mqtt/pkg/store"
)

type brokerConfig struct {
	initErr          error
	tcpAddr          string
	multicore        bool
	reusePort        bool
	tcpKeepAlive     time.Duration
	store            store.MessageStore
	hookMgr          *hook.Manager
	metricsAddr      string
	rateLimitConn    float64
	rateLimitConnBst int64
	rateLimitPub     float64
	rateLimitPubBst  int64
	tlsConfig        *server.TLSConfig
	wsConfig         *server.WSConfig
	quicConfig       *server.QUICConfig
	pipelineConfig   *pipelineConfigWrapper
	clusterConfig    *clusterConfigWrapper
	pipelines        []pipelineBindingConfig
}

type pipelineBindingConfig struct {
	topicFilter string
	pipeline    *Pipeline
}

type pipelineConfigWrapper struct {
	driver       string
	driverConfig map[string]any
	filters      []string
	spoolDir     string
	batchSize    int
	flushMs      int
	maxQuotaGB   int
}

type clusterConfigWrapper struct {
	nodeID     string
	listenAddr string
	seeds      []string
	peers      []string
}

// Option configures the Broker during initialization.
type Option func(*brokerConfig)

func defaultBrokerConfig() *brokerConfig {
	return &brokerConfig{
		tcpAddr:          ":1883",
		multicore:        true,
		reusePort:        true,
		tcpKeepAlive:     60 * time.Second,
		store:            store.NewMemoryStore(),
		hookMgr:          hook.NewManager(),
		rateLimitConnBst: 1000,
		rateLimitPubBst:  500,
	}
}

// WithTCP sets the listening TCP address for standard MQTT (default: ":1883").
func WithTCP(addr string) Option {
	return func(c *brokerConfig) {
		c.tcpAddr = addr
	}
}

// WithMulticore configures whether gnet reactor uses multiple CPU event loops.
func WithMulticore(multicore bool) Option {
	return func(c *brokerConfig) {
		c.multicore = multicore
	}
}

// WithReusePort enables SO_REUSEPORT socket option.
func WithReusePort(reusePort bool) Option {
	return func(c *brokerConfig) {
		c.reusePort = reusePort
	}
}

// WithStore injects a custom storage engine implementing store.MessageStore.
func WithStore(s store.MessageStore) Option {
	return func(c *brokerConfig) {
		if s != nil {
			c.store = s
		}
	}
}

// WithMemoryStore uses ultra-fast in-memory storage (default).
func WithMemoryStore() Option {
	return func(c *brokerConfig) {
		c.store = store.NewMemoryStore()
	}
}

// WithPebbleStore initializes high-performance Pebble LSM-Tree storage.
func WithPebbleStore(dataDir string) Option {
	return func(c *brokerConfig) {
		if c.initErr != nil {
			return
		}
		ps, err := store.NewPebbleStore(dataDir)
		if err != nil {
			c.initErr = fmt.Errorf("failed to initialize pebble store at %q: %w", dataDir, err)
			return
		}
		c.store = ps
	}
}

// WithBadgerStore initializes BadgerDB key-value storage.
func WithBadgerStore(dataDir string) Option {
	return func(c *brokerConfig) {
		if c.initErr != nil {
			return
		}
		bs, err := store.NewBadgerStore(dataDir)
		if err != nil {
			c.initErr = fmt.Errorf("failed to initialize badger store at %q: %w", dataDir, err)
			return
		}
		c.store = bs
	}
}

// WithHookManager configures custom authentication and lifecycle hooks.
func WithHookManager(mgr *hook.Manager) Option {
	return func(c *brokerConfig) {
		if mgr != nil {
			c.hookMgr = mgr
		}
	}
}

// WithHook registers a lifecycle, authentication, or validator hook into the broker.
func WithHook(h Hook) Option {
	return func(c *brokerConfig) {
		if h == nil {
			return
		}
		if c.hookMgr == nil {
			c.hookMgr = hook.NewManager()
		}
		c.hookMgr.Register(h)
	}
}

// WithWebSocket enables MQTT over WebSocket listener (e.g. ":8083", "/mqtt").
func WithWebSocket(addr, path string) Option {
	return func(c *brokerConfig) {
		if addr != "" {
			c.wsConfig = &server.WSConfig{
				Addr: addr,
				Path: path,
			}
		}
	}
}

// WithTLS enables MQTT over TLS listener with optional mutual TLS (mTLS).
func WithTLS(cfg server.TLSConfig) Option {
	return func(c *brokerConfig) {
		c.tlsConfig = &cfg
	}
}

// WithQUIC enables MQTT over QUIC (UDP) listener.
func WithQUIC(cfg server.QUICConfig) Option {
	return func(c *brokerConfig) {
		c.quicConfig = &cfg
	}
}

// WithRateLimit configures connection and publish token-bucket rate limiters.
func WithRateLimit(connRate, connBurst, pubRate, pubBurst float64) Option {
	return func(c *brokerConfig) {
		c.rateLimitConn = connRate
		c.rateLimitConnBst = int64(connBurst)
		c.rateLimitPub = pubRate
		c.rateLimitPubBst = int64(pubBurst)
	}
}

// WithMetrics enables dedicated Prometheus HTTP metrics endpoint (e.g. ":8080").
func WithMetrics(addr string) Option {
	return func(c *brokerConfig) {
		c.metricsAddr = addr
	}
}

// WithPipeline enables the industrial streaming pipeline (Kafka, NATS, Redpanda, stdout, mock).
func WithPipeline(driver string, driverConfig map[string]any, topicFilters []string, spoolDir string) Option {
	return func(c *brokerConfig) {
		if driver == "" {
			driver = "stdout"
		}
		if spoolDir == "" {
			spoolDir = "./data/pipeline_spool"
		}
		c.pipelineConfig = &pipelineConfigWrapper{
			driver:       driver,
			driverConfig: driverConfig,
			filters:      topicFilters,
			spoolDir:     spoolDir,
			batchSize:    1000,
			flushMs:      20,
			maxQuotaGB:   5,
		}
	}
}

// WithPipelineSpool configures pipeline spool tuning (batch size, flush timeout ms, max quota GB).
func WithPipelineSpool(batchSize, flushMs, maxQuotaGB int) Option {
	return func(c *brokerConfig) {
		if c.pipelineConfig != nil {
			if batchSize > 0 {
				c.pipelineConfig.batchSize = batchSize
			}
			if flushMs > 0 {
				c.pipelineConfig.flushMs = flushMs
			}
			if maxQuotaGB > 0 {
				c.pipelineConfig.maxQuotaGB = maxQuotaGB
			}
		}
	}
}

// WithCluster enables dynamic decentralized Gossip mesh clustering.
func WithCluster(nodeID, listenAddr string, seeds []string) Option {
	return func(c *brokerConfig) {
		if c.clusterConfig == nil {
			c.clusterConfig = &clusterConfigWrapper{}
		}
		c.clusterConfig.nodeID = nodeID
		c.clusterConfig.listenAddr = listenAddr
		c.clusterConfig.seeds = seeds
	}
}

// WithClusterPeers specifies static cluster peers (e.g. ["node-2=127.0.0.1:19992"]).
func WithClusterPeers(peers []string) Option {
	return func(c *brokerConfig) {
		if c.clusterConfig == nil {
			c.clusterConfig = &clusterConfigWrapper{}
		}
		c.clusterConfig.peers = peers
	}
}

// WithPipe binds a topic filter to a composable Pipe.
func WithPipe(topicFilter string, p *Pipe) Option {
	return func(c *brokerConfig) {
		if p != nil && topicFilter != "" {
			c.pipelines = append(c.pipelines, pipelineBindingConfig{
				topicFilter: topicFilter,
				pipeline:    p,
			})
		}
	}
}

// WithProcessor binds a named processor directly to a topic filter.
func WithProcessor(topicFilter string, name string, proc Processor) Option {
	return func(c *brokerConfig) {
		if proc != nil && topicFilter != "" {
			p := NewPipe()
			p.Add(name, proc)
			c.pipelines = append(c.pipelines, pipelineBindingConfig{
				topicFilter: topicFilter,
				pipeline:    p,
			})
		}
	}
}
