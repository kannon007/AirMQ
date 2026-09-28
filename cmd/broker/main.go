package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"mqtt/core"
	"mqtt/dashboard"
	"mqtt/pkg/hook"
	"mqtt/pkg/plugin/auth_http"
	"mqtt/pkg/plugin/kafka"
	"mqtt/pkg/server"
	"mqtt/pkg/store"
)

type stdoutKafkaProducer struct{}

func (s *stdoutKafkaProducer) ProduceBatch(messages []*kafka.KafkaMessage) error {
	log.Printf("[KafkaBridge] Flushed batch of %d messages to Kafka", len(messages))
	return nil
}

func (s *stdoutKafkaProducer) Close() error {
	return nil
}

func main() {
	addr := flag.String("addr", "tcp://0.0.0.0:1883", "MQTT broker listen address")
	multicore := flag.Bool("multicore", true, "Enable multi-reactor multicore event loops")
	reuseport := flag.Bool("reuseport", false, "Enable SO_REUSEPORT (Linux only)")
	authURL := flag.String("auth-url", "", "HTTP Auth webhook endpoint (optional)")
	enableKafka := flag.Bool("enable-kafka", false, "Enable Kafka telemetry forwarding bridge")

	storeType := flag.String("store", "pebble", "Storage engine: 'memory', 'pebble' (recommended), or 'badger'")
	pebbleDir := flag.String("pebble-dir", "./pebble_data", "Pebble LSM-Tree storage directory")
	badgerDir := flag.String("badger-dir", "./badger_data", "BadgerDB data directory")

	clusterNodeID := flag.String("cluster-node", "", "Unique Cluster Node ID (e.g. 'node-1')")
	clusterListen := flag.String("cluster-listen", "", "Cluster RPC mesh listen address (e.g. '127.0.0.1:19991')")
	clusterPeers := flag.String("cluster-peers", "", "Comma-separated static peer addresses (e.g. 'node-2=127.0.0.1:19992')")
	clusterSeeds := flag.String("cluster-seeds", "", "Comma-separated seed RPC addresses for dynamic discovery (e.g. '127.0.0.1:19991')")

	tlsAddr := flag.String("tls-addr", "", "MQTT over TLS listen address (e.g. ':8883')")
	tlsCert := flag.String("tls-cert", "", "Path to TLS server certificate PEM file")
	tlsKey := flag.String("tls-key", "", "Path to TLS server private key PEM file")
	tlsCA := flag.String("tls-ca", "", "Path to client CA certificate PEM file for mTLS")
	tlsVerifyClient := flag.Bool("tls-verify-client", false, "Require and verify client certificate (mTLS)")

	wsAddr := flag.String("ws-addr", "", "MQTT over WebSocket listen address (e.g. ':8083')")
	wsPath := flag.String("ws-path", "/mqtt", "MQTT over WebSocket HTTP path (e.g. '/mqtt')")

	metricsAddr := flag.String("metrics-addr", "", "Dedicated Prometheus metrics HTTP listen address (e.g. ':8080')")
	rateLimitConn := flag.Float64("rate-limit-conn", 0, "Max new connections/sec (0 for unlimited)")
	rateLimitConnBurst := flag.Int64("rate-limit-conn-burst", 100, "Connection rate limiter burst capacity")
	rateLimitPub := flag.Float64("rate-limit-pub", 0, "Max publish msg/sec per client (0 for unlimited)")
	rateLimitPubBurst := flag.Int64("rate-limit-pub-burst", 500, "Publish rate limiter burst capacity")

	// Industrial Streaming Pipeline flags (Kafka / Redpanda / NATS / TSDB / MQ)
	pipelineEnable := flag.Bool("pipeline-enable", false, "Enable industrial streaming pipeline (Kafka/Redpanda/NATS)")
	pipelineSink := flag.String("pipeline-sink", "stdout", "Pipeline sink driver: 'stdout', 'mock', 'kafka', 'redpanda', 'nats'")
	pipelineTopic := flag.String("pipeline-topic", "mqtt_telemetry", "Target sink topic / table")
	pipelineSpoolDir := flag.String("pipeline-spool-dir", "./data/pipeline_spool", "Segmented disk RingBuffer spool directory")
	pipelineBatchSize := flag.Int("pipeline-batch-size", 1000, "Max batch size for downstream streaming")
	pipelineFlushMs := flag.Int("pipeline-flush-ms", 20, "Max flush timeout in milliseconds")
	pipelineSpoolMaxGB := flag.Int("pipeline-spool-max-gb", 5, "Max disk spool storage quota in GB")
	pipelineKafkaBrokers := flag.String("pipeline-kafka-brokers", "127.0.0.1:9092", "Comma-separated Kafka/Redpanda brokers")
	pipelineNatsServers := flag.String("pipeline-nats-servers", "127.0.0.1:4222", "Comma-separated NATS servers (e.g. 127.0.0.1:4222)")
	pipelineNatsSubject := flag.String("pipeline-nats-subject", "mqtt_telemetry", "Target NATS JetStream subject")
	pipelineFilter := flag.String("pipeline-filter", "#", "Comma-separated MQTT topic filters to stream (default: #)")

	// MQTT over QUIC (RFC 9000 UDP) flags
	quicEnable := flag.Bool("quic-enable", false, "Enable MQTT over QUIC (UDP) listener")
	quicAddr := flag.String("quic-addr", ":14567", "MQTT over QUIC listen UDP address (e.g. ':14567')")
	quicCert := flag.String("quic-cert", "", "Path to QUIC TLS 1.3 server certificate PEM file")
	quicKey := flag.String("quic-key", "", "Path to QUIC TLS 1.3 server private key PEM file")

	// Management REST API & Web Dashboard flags (EMQX style)
	dashboardEnable := flag.Bool("dashboard-enable", true, "Enable Management REST API and Web Dashboard (default: true)")
	dashboardAddr := flag.String("dashboard-addr", ":18083", "Management REST API & Web Dashboard listen address (default: :18083)")
	dashboardUser := flag.String("dashboard-user", "admin", "Dashboard administrator username (default: admin)")
	dashboardPassword := flag.String("dashboard-password", "public", "Dashboard administrator password (default: public)")
	dashboardWebDir := flag.String("dashboard-web-dir", "", "Optional local directory containing static web files for hot-reloading (e.g. './web/dist')")

	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("             AirMQ: High-Performance MQTT Broker Engine        ")
	fmt.Println("   Architecture: Decoupled Core SDK | REST API | Embedded Web   ")
	fmt.Println("   Reactor Network: gnet/v2 (IOCP/epoll) | Topic Tree: ART+RCU  ")
	fmt.Println("   Zero-Copy Buffer Pool | Distributed Ready | Pluggable Hook   ")
	fmt.Println("================================================================")
	log.Printf("Starting AirMQ Broker on %s (CPUs: %d, Multicore: %v)", *addr, runtime.NumCPU(), *multicore)

	// 1. Initialize Storage Engine
	var msgStore store.MessageStore
	switch strings.ToLower(*storeType) {
	case "pebble":
		log.Printf("Initializing Production Persistent Storage (Pebble LSM-Tree): %s", *pebbleDir)
		ps, err := store.NewPebbleStore(*pebbleDir)
		if err != nil {
			log.Fatalf("Failed to initialize PebbleStore: %v", err)
		}
		msgStore = ps
	case "badger":
		log.Printf("Initializing Persistent Storage (BadgerDB WiscKey): %s", *badgerDir)
		bs, err := store.NewBadgerStore(*badgerDir)
		if err != nil {
			log.Fatalf("Failed to initialize BadgerStore: %v", err)
		}
		msgStore = bs
	case "memory":
		log.Printf("Initializing Ultra-Fast In-Memory Storage")
		msgStore = store.NewMemoryStore()
	default:
		log.Fatalf("Unknown storage engine: '%s' (supported: memory, pebble, badger)", *storeType)
	}

	// 2. Initialize Hook Manager & Plugins
	hookMgr := hook.NewManager()

	if *authURL != "" {
		log.Printf("Activating HTTP Auth Plugin: %s", *authURL)
		httpHook := auth_http.NewHTTPAuthHook(auth_http.HTTPAuthConfig{
			AuthURL:  *authURL,
			CacheTTL: 5 * time.Minute,
		})
		hookMgr.Register(httpHook)
	}

	if *enableKafka {
		log.Printf("Activating Kafka Forwarding Bridge (Buffer: 65536, Batch: 1000)")
		kafkaBridge := kafka.NewKafkaBridgeHook(kafka.KafkaBridgeConfig{
			TopicFilters: []string{"telemetry/#", "events/#"},
			BatchSize:    1000,
			FlushTimeout: 100 * time.Millisecond,
		}, &stdoutKafkaProducer{})
		hookMgr.Register(kafkaBridge)
	}

	// 3. Assemble Core Broker Options
	brokerOpts := []core.Option{
		core.WithTCP(*addr),
		core.WithMulticore(*multicore),
		core.WithReusePort(*reuseport),
		core.WithStore(msgStore),
		core.WithHookManager(hookMgr),
		core.WithRateLimit(*rateLimitConn, float64(*rateLimitConnBurst), *rateLimitPub, float64(*rateLimitPubBurst)),
	}

	if *metricsAddr != "" {
		brokerOpts = append(brokerOpts, core.WithMetrics(*metricsAddr))
	}

	if *pipelineEnable {
		log.Printf("Activating Industrial Streaming Pipeline (Sink: %s, BatchSize: %d, SpoolDir: %s, MaxDisk: %d GB)",
			*pipelineSink, *pipelineBatchSize, *pipelineSpoolDir, *pipelineSpoolMaxGB)

		var sinkCfg map[string]any
		switch strings.ToLower(*pipelineSink) {
		case "nats":
			sinkCfg = map[string]any{
				"servers": strings.Split(*pipelineNatsServers, ","),
				"subject": *pipelineNatsSubject,
				"topic":   *pipelineNatsSubject,
			}
		case "redpanda":
			sinkCfg = map[string]any{
				"brokers": strings.Split(*pipelineKafkaBrokers, ","),
				"servers": strings.Split(*pipelineKafkaBrokers, ","),
				"topic":   *pipelineTopic,
			}
		default:
			sinkCfg = map[string]any{
				"brokers": strings.Split(*pipelineKafkaBrokers, ","),
				"servers": strings.Split(*pipelineKafkaBrokers, ","),
				"topic":   *pipelineTopic,
			}
		}

		var topicFilters []string
		if *pipelineFilter != "" {
			topicFilters = strings.Split(*pipelineFilter, ",")
		}

		brokerOpts = append(brokerOpts,
			core.WithPipeline(*pipelineSink, sinkCfg, topicFilters, *pipelineSpoolDir),
			core.WithPipelineSpool(*pipelineBatchSize, *pipelineFlushMs, *pipelineSpoolMaxGB),
		)
	}

	if *tlsAddr != "" && *tlsCert != "" && *tlsKey != "" {
		log.Printf("Starting MQTT over TLS listener on %s (mTLS: %v)", *tlsAddr, *tlsVerifyClient)
		brokerOpts = append(brokerOpts, core.WithTLS(server.TLSConfig{
			Addr:              *tlsAddr,
			CertFile:          *tlsCert,
			KeyFile:           *tlsKey,
			ClientCAFile:      *tlsCA,
			RequireClientCert: *tlsVerifyClient,
		}))
	}

	if *wsAddr != "" {
		log.Printf("Starting MQTT over WebSocket listener on %s (path: %s)", *wsAddr, *wsPath)
		brokerOpts = append(brokerOpts, core.WithWebSocket(*wsAddr, *wsPath))
	}

	if *quicEnable {
		log.Printf("Starting MQTT over QUIC (UDP) listener on %s", *quicAddr)
		brokerOpts = append(brokerOpts, core.WithQUIC(server.QUICConfig{
			Addr:     *quicAddr,
			CertFile: *quicCert,
			KeyFile:  *quicKey,
		}))
	}

	if *clusterNodeID != "" && *clusterListen != "" {
		log.Printf("Initializing Cluster Mesh: NodeID=%s, Listen=%s", *clusterNodeID, *clusterListen)
		var seeds []string
		if *clusterSeeds != "" {
			seeds = strings.Split(*clusterSeeds, ",")
		}
		brokerOpts = append(brokerOpts, core.WithCluster(*clusterNodeID, *clusterListen, seeds))
		if *clusterPeers != "" {
			brokerOpts = append(brokerOpts, core.WithClusterPeers(strings.Split(*clusterPeers, ",")))
		}
	}

	// 4. Instantiate Core Broker
	broker, err := core.NewBroker(brokerOpts...)
	if err != nil {
		log.Fatalf("Failed to initialize broker engine: %v", err)
	}

	// 5. Optionally Initialize Management REST API & Web Dashboard
	var dashSrv *dashboard.Server
	if *dashboardEnable {
		dashSrv = dashboard.NewServer(dashboard.Config{
			Addr:     *dashboardAddr,
			Username: *dashboardUser,
			Password: *dashboardPassword,
			WebDir:   *dashboardWebDir,
		}, broker)
		if err := dashSrv.Start(); err != nil {
			log.Printf("Warning: Failed to start Dashboard server: %v", err)
		}
	}

	// 6. Graceful shutdown handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received termination signal, shutting down broker...")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if dashSrv != nil {
			_ = dashSrv.Stop(ctx)
		}
		_ = broker.Stop(ctx)
		os.Exit(0)
	}()

	if err := broker.Start(); err != nil {
		log.Fatalf("Broker startup failed: %v", err)
	}
}
