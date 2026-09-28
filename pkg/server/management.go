package server

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	"mqtt/pkg/hook"
	"mqtt/pkg/pipeline"
	"mqtt/pkg/protocol"
)

// GetOverview returns real-time statistics and system telemetry.
func (s *Server) GetOverview() OverviewStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	nodeName := "standalone-node"
	if s.clusterMesh != nil {
		nodeName = s.clusterMesh.NodeID()
	}

	uptimeSec := int64(time.Since(s.startTime).Seconds())
	tcpConns := s.metrics.ConnsActiveTCP.Load()
	tlsConns := s.metrics.ConnsActiveTLS.Load()
	wsConns := s.metrics.ConnsActiveWS.Load()
	quicConns := s.metrics.ConnsActiveQUIC.Load()
	activeTotal := tcpConns + tlsConns + wsConns + quicConns

	subs := s.sessionMgr.GetAllSubscriptions()
	retained, _ := s.store.GetAllRetained()

	totalRecv := s.metrics.MsgsReceivedQoS0.Load() + s.metrics.MsgsReceivedQoS1.Load() + s.metrics.MsgsReceivedQoS2.Load()
	totalSent := s.metrics.MsgsSentQoS0.Load() + s.metrics.MsgsSentQoS1.Load() + s.metrics.MsgsSentQoS2.Load()

	clusterNodes := 1
	if s.clusterMesh != nil {
		clusterNodes = len(s.clusterMesh.Peers()) + 1
	}

	return OverviewStats{
		NodeName:          nodeName,
		Version:           "1.0.0",
		UptimeSeconds:     uptimeSec,
		ActiveConnections: activeTotal,
		TCPConnections:    tcpConns,
		TLSConnections:    tlsConns,
		WSConnections:     wsConns,
		QUICConnections:   quicConns,
		Subscriptions:     len(subs),
		RetainedCount:     len(retained),
		TotalMsgReceived:  totalRecv,
		TotalMsgSent:      totalSent,
		TotalBytesIn:      s.metrics.BytesReceivedTotal.Load(),
		TotalBytesOut:     s.metrics.BytesSentTotal.Load(),
		RateLimitDropped:  s.metrics.RateLimitDroppedConn.Load() + s.metrics.RateLimitDroppedPublish.Load(),
		ClusterNodesCount: clusterNodes,
		OS:                runtime.GOOS,
		Arch:              runtime.GOARCH,
		GoVersion:         runtime.Version(),
		Goroutines:        runtime.NumGoroutine(),
		MemoryAllocMB:     float64(m.Alloc) / (1024 * 1024),
		MemorySysMB:       float64(m.Sys) / (1024 * 1024),
		NumCPU:            runtime.NumCPU(),
	}
}

// GetClients returns a paginated and filtered list of active client connections.
func (s *Server) GetClients(page, limit int, query string) ([]ClientSummary, int) {
	var all []ClientSummary

	s.conns.Range(func(key, value any) bool {
		clientID, _ := key.(string)
		entry, ok := value.(*clientEntry)
		if !ok || entry == nil {
			return true
		}

		username := ""
		transport := "tcp"
		proto := "MQTT 3.1.1"
		var connectedAt time.Time
		keepAlive := 0
		cleanSession := true
		subsCount := 0

		if entry.ctx != nil {
			username = entry.ctx.Username
			transport = entry.ctx.Transport
			if entry.ctx.ProtocolLevel == protocol.V50 {
				proto = "MQTT 5.0"
			}
			keepAlive = int(entry.ctx.KeepAlive.Seconds())
			if entry.ctx.Session != nil {
				connectedAt = entry.ctx.Session.ConnectedAt
				cleanSession = entry.ctx.Session.CleanSession
				subsCount = len(entry.ctx.Session.GetSubscriptions())
			}
		}

		remoteAddr := ""
		if entry.conn != nil && entry.conn.RemoteAddr() != nil {
			remoteAddr = entry.conn.RemoteAddr().String()
		}

		// Filter
		if query != "" {
			q := strings.ToLower(query)
			if !strings.Contains(strings.ToLower(clientID), q) &&
				!strings.Contains(strings.ToLower(username), q) &&
				!strings.Contains(strings.ToLower(remoteAddr), q) &&
				!strings.Contains(strings.ToLower(transport), q) {
				return true
			}
		}

		all = append(all, ClientSummary{
			ClientID:      clientID,
			Username:      username,
			IPAddress:     remoteAddr,
			Protocol:      proto,
			Transport:     transport,
			ConnectedAt:   connectedAt,
			KeepAlive:     keepAlive,
			CleanSession:  cleanSession,
			Subscriptions: subsCount,
		})
		return true
	})

	// Stable sort by connected time desc
	sort.Slice(all, func(i, j int) bool {
		return all[i].ConnectedAt.After(all[j].ConnectedAt)
	})

	total := len(all)
	start := (page - 1) * limit
	if start >= total {
		return []ClientSummary{}, total
	}
	end := start + limit
	if end > total {
		end = total
	}

	return all[start:end], total
}

// GetClientDetail retrieves in-depth information about a specific client.
func (s *Server) GetClientDetail(clientID string) (*ClientDetail, bool) {
	entryVal, ok := s.conns.Load(clientID)
	if !ok {
		// Check if persistent session exists offline
		if sess, exists := s.sessionMgr.Get(clientID); exists && sess != nil {
			var subs []string
			for top := range sess.GetSubscriptions() {
				subs = append(subs, top)
			}
			willTopic := ""
			if sess.WillPacket != nil {
				willTopic = sess.WillPacket.Topic
			}
			return &ClientDetail{
				ClientSummary: ClientSummary{
					ClientID:      clientID,
					Username:      "",
					IPAddress:     "offline",
					Protocol:      "Offline Session",
					Transport:     "none",
					ConnectedAt:   sess.ConnectedAt,
					KeepAlive:     0,
					CleanSession:  sess.CleanSession,
					Subscriptions: len(subs),
				},
				Subscriptions: subs,
				WillTopic:     willTopic,
				IsTLS:         false,
			}, true
		}
		return nil, false
	}

	entry := entryVal.(*clientEntry)
	username := ""
	transport := "tcp"
	proto := "MQTT 3.1.1"
	var connectedAt time.Time
	keepAlive := 0
	cleanSession := true
	var subs []string
	willTopic := ""
	isTLS := false
	certCN := ""

	if entry.ctx != nil {
		username = entry.ctx.Username
		transport = entry.ctx.Transport
		if entry.ctx.ProtocolLevel == protocol.V50 {
			proto = "MQTT 5.0"
		}
		keepAlive = int(entry.ctx.KeepAlive.Seconds())
		if entry.ctx.Session != nil {
			connectedAt = entry.ctx.Session.ConnectedAt
			cleanSession = entry.ctx.Session.CleanSession
			for top := range entry.ctx.Session.GetSubscriptions() {
				subs = append(subs, top)
			}
			if entry.ctx.Session.WillPacket != nil {
				willTopic = entry.ctx.Session.WillPacket.Topic
			}
		}
		isTLS = entry.ctx.IsTLS
		certCN = entry.ctx.ClientCertCN
	}

	remoteAddr := ""
	if entry.conn != nil && entry.conn.RemoteAddr() != nil {
		remoteAddr = entry.conn.RemoteAddr().String()
	}

	return &ClientDetail{
		ClientSummary: ClientSummary{
			ClientID:      clientID,
			Username:      username,
			IPAddress:     remoteAddr,
			Protocol:      proto,
			Transport:     transport,
			ConnectedAt:   connectedAt,
			KeepAlive:     keepAlive,
			CleanSession:  cleanSession,
			Subscriptions: len(subs),
		},
		Subscriptions: subs,
		WillTopic:     willTopic,
		IsTLS:         isTLS,
		ClientCertCN:  certCN,
	}, true
}

// KickClient forces disconnect of an active client.
func (s *Server) KickClient(clientID string) error {
	entryVal, ok := s.conns.Load(clientID)
	if !ok {
		return fmt.Errorf("client %q not found or not connected", clientID)
	}

	entry := entryVal.(*clientEntry)
	if entry.conn != nil {
		_ = entry.conn.Close()
	}
	s.closeClient(entry.ctx, "administrative kick", nil)
	return nil
}

// GetSubscriptions returns all active subscriptions across the broker.
func (s *Server) GetSubscriptions(page, limit int, query string) ([]SubscriptionSummary, int) {
	allSubs := s.sessionMgr.GetAllSubscriptions()
	var all []SubscriptionSummary

	for _, subRec := range allSubs {
		if query != "" {
			q := strings.ToLower(query)
			if !strings.Contains(strings.ToLower(subRec.ClientID), q) && !strings.Contains(strings.ToLower(subRec.Topic), q) {
				continue
			}
		}
		all = append(all, SubscriptionSummary{
			ClientID: subRec.ClientID,
			Topic:    subRec.Topic,
			QoS:      subRec.QoS,
		})
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Topic == all[j].Topic {
			return all[i].ClientID < all[j].ClientID
		}
		return all[i].Topic < all[j].Topic
	})

	total := len(all)
	start := (page - 1) * limit
	if start >= total {
		return []SubscriptionSummary{}, total
	}
	end := start + limit
	if end > total {
		end = total
	}

	return all[start:end], total
}

// UnsubscribeClient removes a specific subscription for a client.
func (s *Server) UnsubscribeClient(clientID, topic string) error {
	sess, ok := s.sessionMgr.Get(clientID)
	if !ok {
		return fmt.Errorf("client %q session not found", clientID)
	}
	sess.RemoveSubscription(topic)
	s.router.Unsubscribe(topic, clientID)
	return nil
}

// GetRetainedMessages returns all currently stored retained messages.
func (s *Server) GetRetainedMessages() ([]RetainedSummary, error) {
	pkts, err := s.store.GetAllRetained()
	if err != nil {
		return nil, err
	}

	res := make([]RetainedSummary, 0, len(pkts))
	for _, p := range pkts {
		res = append(res, RetainedSummary{
			Topic:     p.Topic,
			QoS:       p.QoS,
			Size:      len(p.Payload),
			Payload:   string(p.Payload),
			UpdatedAt: time.Now(),
		})
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Topic < res[j].Topic
	})

	return res, nil
}

// DeleteRetainedMessage deletes a retained message for a given topic.
func (s *Server) DeleteRetainedMessage(topic string) error {
	return s.store.DeleteRetained(topic)
}

// PublishMessage sends a message into the broker directly.
func (s *Server) PublishMessage(topic string, qos byte, retain bool, payload []byte) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	pkt := &protocol.PublishPacket{
		ProtocolLevel: protocol.V311,
		Topic:         topic,
		QoS:           qos,
		Retain:        retain,
		Payload:       payload,
	}

	if s.hookMgr != nil && s.hookMgr.HasHooks() {
		hookCtx := hook.NewClientContext("$internal", "$internal", "127.0.0.1")
		drop, err := s.hookMgr.FirePublish(hookCtx, pkt)
		if err != nil || drop {
			if err != nil {
				return err
			}
			return fmt.Errorf("message dropped by hook")
		}
	}

	if s.pipelineRouter != nil && s.pipelineRouter.HasPipelines() {
		pipeMsg := pipeline.AcquireMessage()
		pipeMsg.ClientID = "$internal"
		pipeMsg.Topic = topic
		pipeMsg.Payload = payload
		pipeMsg.QoS = qos

		err := s.pipelineRouter.Process(context.Background(), pipeMsg)
		payload = pipeMsg.Payload
		pkt.Payload = payload
		pipeline.ReleaseMessage(pipeMsg)

		if err != nil {
			return err
		}
	}

	if retain {
		if len(payload) == 0 {
			_ = s.store.DeleteRetained(topic)
		} else {
			_ = s.store.SetRetained(topic, pkt)
		}
	}

	if s.pipeline != nil {
		s.pipeline.IngestDirect("$dashboard_admin", topic, payload, qos)
	}

	if s.clusterRouter != nil {
		_ = s.clusterRouter.RouteToCluster(topic, qos, payload)
	}

	s.dispatchPublish(pkt, "$dashboard_admin")
	s.metrics.MsgsReceivedQoS0.Add(1)
	s.metrics.BytesReceivedTotal.Add(uint64(len(payload)))
	return nil
}

// GetListeners returns all active network listener states.
func (s *Server) GetListeners() []ListenerSummary {
	var list []ListenerSummary

	if s.listeners != nil {
		for _, l := range s.listeners.Listeners() {
			var activeConns int64
			switch l.Name() {
			case "tcp":
				activeConns = s.metrics.ConnsActiveTCP.Load()
			case "tls":
				activeConns = s.metrics.ConnsActiveTLS.Load()
			case "ws":
				activeConns = s.metrics.ConnsActiveWS.Load()
			case "quic":
				activeConns = s.metrics.ConnsActiveQUIC.Load()
			}

			list = append(list, ListenerSummary{
				Name:        l.Name(),
				Protocol:    l.Protocol(),
				Address:     l.Addr().String(),
				Status:      "running",
				ActiveConns: activeConns,
			})
		}
	}

	if len(list) == 0 {
		list = append(list, ListenerSummary{
			Name:        "tcp",
			Protocol:    "tcp",
			Address:     s.cfg.Addr,
			Status:      "running",
			ActiveConns: s.metrics.ConnsActiveTCP.Load(),
		})
	}

	return list
}

// GetClusterNodes returns gossip cluster mesh status.
func (s *Server) GetClusterNodes() ClusterSummary {
	if s.clusterMesh == nil {
		return ClusterSummary{
			SelfNodeID: "standalone-node",
			SelfAddr:   s.cfg.Addr,
			Nodes:      []ClusterNodeSummary{},
		}
	}

	var nodes []ClusterNodeSummary
	for _, p := range s.clusterMesh.GetPeersDetails() {
		if p.NodeID != s.clusterMesh.NodeID() {
			nodes = append(nodes, ClusterNodeSummary{
				NodeID:   p.NodeID,
				Address:  p.Addr,
				Status:   p.State,
				LastSeen: p.LastSeen,
			})
		}
	}

	return ClusterSummary{
		SelfNodeID: s.clusterMesh.NodeID(),
		SelfAddr:   s.clusterMesh.ListenAddr(),
		Nodes:      nodes,
	}
}

// GetPipelineStatus returns streaming pipeline spooler stats.
func (s *Server) GetPipelineStatus() *PipelineSummary {
	if s.pipeline == nil {
		return &PipelineSummary{
			Enabled: false,
		}
	}

	snap := s.pipeline.Snapshot()
	return &PipelineSummary{
		Enabled:        true,
		SinkDriver:     snap.SinkDriver,
		CircuitBreaker: snap.CircuitBreaker,
		IngestedTotal:  snap.IngestedTotal,
		DirectSent:     snap.DirectSent,
		SpooledTotal:   snap.SpooledTotal,
		DrainedTotal:   snap.DrainedTotal,
		DroppedTotal:   snap.DroppedTotal,
		DiskUsageMB:    snap.DiskUsageMB,
		MaxDiskQuotaGB: snap.MaxDiskQuotaGB,
	}
}

// GetRules returns all configured forwarding rules.
func (s *Server) GetRules() []pipeline.RuleStatus {
	if s.pipeline == nil {
		return nil
	}
	return s.pipeline.GetRules()
}

// GetRule finds a rule by ID.
func (s *Server) GetRule(id string) (*pipeline.RuleStatus, error) {
	if s.pipeline == nil {
		return nil, fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.GetRule(id)
}

// UpdateRule saves or inserts a forwarding rule.
func (s *Server) UpdateRule(r pipeline.Rule) error {
	if s.pipeline == nil {
		return fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.UpdateRule(r)
}

// DeleteRule deletes a rule by ID.
func (s *Server) DeleteRule(id string) error {
	if s.pipeline == nil {
		return fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.DeleteRule(id)
}

// PingRule probes network connectivity for a rule.
func (s *Server) PingRule(ctx context.Context, id string) (*pipeline.RulePingResult, error) {
	if s.pipeline == nil {
		return nil, fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.PingRule(ctx, id)
}

// TestMatchTopic tests if an MQTT topic triggers any configured rules.
func (s *Server) TestMatchTopic(topic string) []string {
	if s.pipeline == nil {
		return []string{}
	}
	return s.pipeline.TestMatchTopic(topic)
}

// GetBridges returns all configured external MQ bridges.
func (s *Server) GetBridges() []pipeline.BridgeStatus {
	if s.pipeline == nil {
		return nil
	}
	return s.pipeline.GetBridges()
}

// GetBridge finds a bridge by ID.
func (s *Server) GetBridge(id string) (*pipeline.BridgeStatus, error) {
	if s.pipeline == nil {
		return nil, fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.GetBridge(id)
}

// UpdateBridge updates an external MQ bridge.
func (s *Server) UpdateBridge(b pipeline.Bridge) error {
	if s.pipeline == nil {
		return fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.UpdateBridge(b)
}

// DeleteBridge deletes a bridge by ID.
func (s *Server) DeleteBridge(id string) error {
	if s.pipeline == nil {
		return fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.DeleteBridge(id)
}

// PingBridge actively probes an external MQ bridge.
func (s *Server) PingBridge(ctx context.Context, id string) (*pipeline.RulePingResult, error) {
	if s.pipeline == nil {
		return nil, fmt.Errorf("pipeline not enabled")
	}
	return s.pipeline.PingBridge(ctx, id)
}

// GetRegisteredDrivers returns all supported MQ sink drivers.
func (s *Server) GetRegisteredDrivers() []string {
	return pipeline.ListSinks()
}
