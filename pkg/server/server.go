package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"

	"mqtt/pkg/cluster"
	"mqtt/pkg/hook"
	"mqtt/pkg/limiter"
	"mqtt/pkg/metrics"
	"mqtt/pkg/pipeline"
	"mqtt/pkg/protocol"
	"mqtt/pkg/session"
	"mqtt/pkg/store"
	"mqtt/pkg/trie"
)

// Config configures the MQTT server engine.
type Config struct {
	Addr             string // e.g. "tcp://0.0.0.0:1883"
	Multicore        bool
	ReusePort        bool
	TCPKeepAlive     time.Duration
	HandshakeTimeout time.Duration // Max duration to wait for CONNECT packet (defaults to 10s)
	ConnLimit        limiter.ConnLimiterConfig
	PublishLimit     limiter.PublishLimiterConfig
	MetricsAddr      string // Optional dedicated Prometheus metrics HTTP address (e.g. ":8080")
}

// Server is the high-performance MQTT Broker engine built atop gnet/v2.
type Server struct {
	gnet.BuiltinEventEngine
	eng             gnet.Engine
	cfg             Config
	router          *trie.Router
	sessionMgr      *session.SessionManager
	store           store.MessageStore
	hookMgr         *hook.Manager
	clusterMesh     *cluster.ClusterMesh
	clusterRouter   *cluster.ClusterRouter
	pipeline        *pipeline.Pipeline
	pipelineRouter  *pipeline.Router
	conns           sync.Map // clientID -> *clientEntry
	unauthedConns   sync.Map // gnet.Conn -> *ConnContext (Slowloris pre-auth timeout defense)
	stopHeartbeat   chan struct{}
	tlsServer       *TLSServer
	wsServer        *WSServer
	quicListener    *QUICListener
	listeners       *ListenerManager
	metrics         *metrics.Registry
	connLimiter     *limiter.ConnLimiter
	pubLimiter      *limiter.PublishLimiter
	metricsServer   *http.Server
	startTime       time.Time
	internalSubs    sync.Map // string (subID) -> *InternalSubscription
	hasInternalSubs atomic.Bool
	internalSubSeq  atomic.Uint64
}

// NewServer initializes the broker server.
func NewServer(cfg Config, store store.MessageStore, hookMgr *hook.Manager, cr *cluster.ClusterRouter) *Server {
	if store == nil {
		store = store_NewMemoryStore()
	}
	if hookMgr == nil {
		hookMgr = hook.NewManager()
	}

	reg := metrics.Default
	var connLimiter *limiter.ConnLimiter
	if cfg.ConnLimit.GlobalRate > 0 || cfg.ConnLimit.IPRate > 0 {
		connLimiter = limiter.NewConnLimiter(cfg.ConnLimit)
	}
	var pubLimiter *limiter.PublishLimiter
	if cfg.PublishLimit.Rate > 0 {
		pubLimiter = limiter.NewPublishLimiter(cfg.PublishLimit)
	}

	s := &Server{
		cfg:            cfg,
		router:         trie.NewRouter(nil),
		sessionMgr:     session.NewSessionManager(),
		store:          store,
		hookMgr:        hookMgr,
		clusterRouter:  cr,
		stopHeartbeat:  make(chan struct{}),
		listeners:      NewListenerManager(),
		metrics:        reg,
		connLimiter:    connLimiter,
		pubLimiter:     pubLimiter,
		pipelineRouter: pipeline.NewRouter(),
		startTime:      time.Now(),
	}

	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", reg.Handler())
		srv := &http.Server{
			Addr:    cfg.MetricsAddr,
			Handler: mux,
		}
		s.metricsServer = srv
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("Metrics HTTP server error: %v", err)
			}
		}()
	}

	return s
}

func store_NewMemoryStore() store.MessageStore {
	return store.NewMemoryStore()
}

// Metrics returns the Prometheus metrics registry.
func (s *Server) Metrics() *metrics.Registry {
	return s.metrics
}

// OnBoot captures the gnet engine instance.
func (s *Server) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	go s.startHeartbeatLoop()
	return gnet.None
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	select {
	case <-s.stopHeartbeat:
	default:
		close(s.stopHeartbeat)
	}
	if s.listeners != nil {
		_ = s.listeners.StopAll(ctx)
	}
	if s.tlsServer != nil {
		_ = s.tlsServer.Stop()
	}
	if s.wsServer != nil {
		_ = s.wsServer.Stop(ctx)
	}
	if s.quicListener != nil {
		_ = s.quicListener.Stop(ctx)
	}
	if s.metricsServer != nil {
		_ = s.metricsServer.Shutdown(ctx)
	}
	if s.pipeline != nil {
		_ = s.pipeline.Close()
	}
	return s.eng.Stop(ctx)
}

func (s *Server) startHeartbeatLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopHeartbeat:
			return
		case now := <-ticker.C:
			// 1. Check unauthenticated connections for handshake timeout (Slowloris defense)
			handshakeTimeout := s.cfg.HandshakeTimeout
			if handshakeTimeout <= 0 {
				handshakeTimeout = 10 * time.Second
			}
			s.unauthedConns.Range(func(key, value any) bool {
				conn, ok := key.(gnet.Conn)
				if !ok {
					return true
				}
				uctx, ok := value.(*ConnContext)
				if !ok || uctx == nil {
					return true
				}
				if !uctx.Authed && now.Sub(uctx.ConnectedAt) > handshakeTimeout {
					log.Printf("Unauthenticated connection %s handshake timed out (%v > %v), evicting",
						conn.RemoteAddr(), now.Sub(uctx.ConnectedAt), handshakeTimeout)
					_ = conn.Close()
					s.unauthedConns.Delete(key)
				}
				return true
			})

			// 2. KeepAlive eviction for authenticated active connections
			s.conns.Range(func(key, value any) bool {
				entry, ok := value.(*clientEntry)
				if !ok || entry.ctx == nil || entry.ctx.KeepAlive <= 0 {
					return true
				}
				// MQTT spec: 1.5 * KeepAlive
				timeout := (entry.ctx.KeepAlive * 3) / 2
				if now.Sub(entry.ctx.LastActive) > timeout {
					log.Printf("Client %s keepalive timeout (%v > %v), evicting", entry.ctx.ClientID, now.Sub(entry.ctx.LastActive), timeout)
					_ = entry.conn.Close()
				}
				return true
			})

			// 3. Garbage collect expired MQTT sessions
			expiredSessions := s.sessionMgr.CleanExpiredSessions(now)
			for _, sess := range expiredSessions {
				for topic := range sess.GetSubscriptions() {
					s.router.Unsubscribe(topic, sess.ClientID)
				}
			}
		}
	}
}

// Start boots up the gnet engine.
func (s *Server) Start() error {
	return gnet.Run(s, s.cfg.Addr,
		gnet.WithMulticore(s.cfg.Multicore),
		gnet.WithReusePort(s.cfg.ReusePort),
		gnet.WithTCPKeepAlive(s.cfg.TCPKeepAlive),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithSocketRecvBuffer(1024*1024),
		gnet.WithSocketSendBuffer(1024*1024),
		gnet.WithReadBufferCap(64*1024),
		gnet.WithWriteBufferCap(64*1024),
	)
}

// OnOpen is called when a new socket connects.
func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	s.metrics.IncConnActive("tcp")
	ctx := NewConnContext()
	c.SetContext(ctx)
	s.unauthedConns.Store(c, ctx)
	return nil, gnet.None
}

// OnClose is called when a socket is disconnected.
func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	s.unauthedConns.Delete(c)
	ctx, ok := c.Context().(*ConnContext)
	if ok && ctx != nil && ctx.ClientID != "" {
		s.closeClient(ctx, c.RemoteAddr().String(), err)
	}
	return gnet.None
}

func (s *Server) closeClient(ctx *ConnContext, remoteAddr string, err error) {
	if ctx == nil || ctx.ClientID == "" {
		return
	}
	s.metrics.DecConnActive(ctx.Transport)
	if ctx.CleanDisconnect {
		s.metrics.IncDisconnect("clean")
	} else if err != nil && strings.Contains(strings.ToLower(err.Error()), "timeout") {
		s.metrics.IncDisconnect("timeout")
	} else {
		s.metrics.IncDisconnect("error")
	}
	if s.pubLimiter != nil {
		s.pubLimiter.Remove(ctx.ClientID)
	}

	s.conns.Delete(ctx.ClientID)
	hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, remoteAddr)
	s.hookMgr.FireDisconnect(hookCtx, err)

	// LWT (Last Will and Testament) execution on abnormal disconnect
	if !ctx.CleanDisconnect && ctx.WillTopic != "" {
		willPub := &protocol.PublishPacket{
			Topic:      ctx.WillTopic,
			Payload:    ctx.WillMessage,
			QoS:        ctx.WillQoS,
			Retain:     ctx.WillRetain,
			Properties: ctx.WillProperties,
		}
		if willPub.Retain {
			if len(willPub.Payload) == 0 {
				_ = s.store.DeleteRetained(willPub.Topic)
			} else {
				_ = s.store.SetRetained(willPub.Topic, willPub)
			}
		}
		s.dispatchPublish(willPub, ctx.ClientID)
		ctx.WillTopic = "" // Prevent duplicate firing
	}

	// MQTT session lifecycle handling on disconnect
	if ctx.Session != nil {
		if ctx.ProtocolLevel == protocol.V50 {
			if ctx.SessionExpirySeconds == 0 {
				for topic := range ctx.Session.GetSubscriptions() {
					s.router.Unsubscribe(topic, ctx.ClientID)
				}
				s.sessionMgr.Delete(ctx.ClientID)
			} else {
				ctx.Session.SetDisconnected(time.Now(), ctx.SessionExpirySeconds)
			}
		} else {
			if ctx.Session.CleanSession {
				for topic := range ctx.Session.GetSubscriptions() {
					s.router.Unsubscribe(topic, ctx.ClientID)
				}
				s.sessionMgr.Delete(ctx.ClientID)
			} else {
				ctx.Session.SetDisconnected(time.Now(), 0xFFFFFFFF)
			}
		}
	}
}

// OnTraffic handles stream parsing and packet routing.
func (s *Server) OnTraffic(c gnet.Conn) (action gnet.Action) {
	ctx := c.Context().(*ConnContext)
	ctx.LastActive = time.Now()

	buffered := c.InboundBuffered()
	if buffered < 2 {
		return gnet.None
	}

	buf, err := c.Peek(buffered)
	if err != nil {
		return gnet.None
	}

	offset := 0
	for offset < len(buf) {
		// Fast-Path: Zero-alloc direct wire forwarding for QoS 0 PUBLISH (high-throughput telemetry bypass)
		if (buf[offset] >> 4) == protocol.PUBLISH {
			flags := buf[offset] & 0x0F
			qos := (flags >> 1) & 0x03
			retain := (flags & 0x01) != 0

			if qos == protocol.QoS0 && !retain && !s.hookMgr.HasPublishHooks() && !s.hookMgr.HasAuthorizeHooks() && s.clusterRouter == nil && ctx.ProtocolLevel != protocol.V50 {
				remLen, varByteLen, err := protocol.DecodeRemainingLength(buf[offset+1:])
				if err == nil && varByteLen > 0 {
					totalLen := 1 + varByteLen + remLen
					if offset+totalLen <= len(buf) {
						if remLen >= 2 {
							topicStart := offset + 1 + varByteLen
							topicLen := int(binary.BigEndian.Uint16(buf[topicStart:]))
							if 2+topicLen <= remLen {
								topicBytes := buf[topicStart+2 : topicStart+2+topicLen]
								if s.pubLimiter != nil && !s.pubLimiter.Allow(ctx.ClientID) {
									s.metrics.IncRateLimitDropped("publish")
									offset += totalLen
									continue
								}
								payloadStart := topicStart + 2 + topicLen
								payloadBytes := buf[payloadStart : offset+totalLen]

								// Idiomatic Go Pipeline (0-penalty atomic bypass, 0 allocs)
								if s.pipelineRouter != nil && s.pipelineRouter.HasPipelines() {
									pipeMsg := pipeline.AcquireMessage()
									pipeMsg.ClientID = ctx.ClientID
									pipeMsg.Username = ctx.Username
									pipeMsg.Topic = pipeline.BytesToString(topicBytes)
									pipeMsg.Payload = payloadBytes
									pipeMsg.QoS = 0

									err := s.pipelineRouter.Process(context.Background(), pipeMsg)
									payloadBytes = pipeMsg.Payload
									pipeline.ReleaseMessage(pipeMsg)

									if err != nil {
										offset += totalLen
										continue
									}
								}

								s.metrics.IncMsgReceived(0)
								s.metrics.AddBytesReceived(totalLen)
								if s.pipeline != nil {
									s.pipeline.IngestDirect(ctx.ClientID, pipeline.BytesToString(topicBytes), payloadBytes, 0)
								}
								if s.clusterRouter != nil {
									_ = s.clusterRouter.RouteToCluster(string(topicBytes), 0, payloadBytes)
								}
								if s.hasInternalSubs.Load() {
									s.notifyInternalSubscribers(pipeline.BytesToString(topicBytes), payloadBytes)
								}
								subscribers := s.router.MatchBytes(topicBytes)
								if len(subscribers) > 0 {
									rawPkt := buf[offset : offset+totalLen]
									for _, sub := range subscribers {
										if sub.NoLocal && sub.ClientID == ctx.ClientID {
											continue
										}
										if entryVal, ok := s.conns.Load(sub.ClientID); ok {
											if entry, ok := entryVal.(*clientEntry); ok {
												_, _ = entry.conn.Write(rawPkt)
												s.metrics.IncMsgSent(0)
												s.metrics.AddBytesSent(totalLen)
											}
										}
									}
								}
								offset += totalLen
								continue
							}
						}
					} else {
						// Incomplete packet in stream, wait for more data
						break
					}
				}
			}
		}

		pkt, consumed, err := protocol.DecodePacket(buf[offset:], ctx.ProtocolLevel)
		if err != nil {
			log.Printf("Packet decode error: %v, closing connection", err)
			if offset > 0 {
				_, _ = c.Discard(offset)
			}
			return gnet.Close
		}
		if consumed == 0 || pkt == nil {
			break
		}
		offset += consumed

		connWrapper := NewGnetClientConn(c)
		act := s.handlePacket(connWrapper, ctx, pkt)
		if act != gnet.None {
			_, _ = c.Discard(offset)
			return act
		}
	}

	if offset > 0 {
		_, _ = c.Discard(offset)
	}
	return gnet.None
}

func (s *Server) handlePacket(c ClientConn, ctx *ConnContext, pkt protocol.Packet) gnet.Action {
	switch p := pkt.(type) {
	case *protocol.ConnectPacket:
		return s.handleConnect(c, ctx, p)
	case *protocol.PublishPacket:
		return s.handlePublish(c, ctx, p)
	case *protocol.PubackPacket:
		return s.handlePuback(c, ctx, p)
	case *protocol.PubrecPacket:
		return s.handlePubrec(c, ctx, p)
	case *protocol.PubrelPacket:
		return s.handlePubrel(c, ctx, p)
	case *protocol.PubcompPacket:
		return s.handlePubcomp(c, ctx, p)
	case *protocol.SubscribePacket:
		return s.handleSubscribe(c, ctx, p)
	case *protocol.UnsubscribePacket:
		return s.handleUnsubscribe(c, ctx, p)
	case *protocol.PingreqPacket:
		resp := &protocol.PingrespPacket{}
		data, _ := resp.Encode()
		_, _ = c.Write(data)
	case *protocol.DisconnectPacket:
		return s.handleDisconnect(c, ctx, p)
	case *protocol.AuthPacket:
		return s.handleAuth(c, ctx, p)
	}
	return gnet.None
}

func (s *Server) handleConnect(c ClientConn, ctx *ConnContext, p *protocol.ConnectPacket) gnet.Action {
	ctx.ProtocolLevel = p.ProtocolLevel

	// Rate limiting on incoming connections
	if s.connLimiter != nil && !s.connLimiter.Allow(c.RemoteAddr().String()) {
		s.metrics.IncRateLimitDropped("conn")
		s.metrics.IncConnect("rejected")
		return gnet.Close
	}

	// Handle empty ClientID
	if p.ClientID == "" {
		if p.ProtocolLevel == protocol.V50 || (p.ProtocolLevel == protocol.V311 && p.CleanSession) {
			p.ClientID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
			ctx.AssignedClientID = true
		} else {
			s.metrics.IncConnect("rejected")
			var connack *protocol.ConnackPacket
			if p.ProtocolLevel == protocol.V50 {
				connack = &protocol.ConnackPacket{
					ProtocolLevel: protocol.V50,
					ReasonCode:    protocol.ReasonClientIdentifierNotValid,
				}
			} else {
				connack = &protocol.ConnackPacket{
					ReturnCode: protocol.CodeIdentifierRejected,
				}
			}
			data, _ := connack.Encode()
			_ = c.WriteAndClose(data)
			return gnet.None
		}
	}

	// Will configuration
	if p.WillFlag {
		ctx.WillTopic = p.WillTopic
		ctx.WillMessage = p.WillMessage
		ctx.WillQoS = p.WillQoS
		ctx.WillRetain = p.WillRetain
		ctx.WillProperties = p.WillProperties
	}

	// MQTT 5.0 Session Expiry Interval
	if p.Properties != nil && p.Properties.SessionExpiryInterval != nil {
		ctx.SessionExpirySeconds = *p.Properties.SessionExpiryInterval
	}

	// MQTT 5.0 Maximum Packet Size
	if p.Properties != nil && p.Properties.MaximumPacketSize != nil {
		ctx.MaxPacketSize = *p.Properties.MaximumPacketSize
	}

	hookCtx := hook.NewClientContext(p.ClientID, p.Username, c.RemoteAddr().String())
	allowed, code, err := s.hookMgr.FireConnect(hookCtx, p)
	if err != nil || !allowed {
		s.metrics.IncConnect("auth_failed")
		connack := &protocol.ConnackPacket{
			ProtocolLevel: p.ProtocolLevel,
			ReturnCode:    code,
			ReasonCode:    protocol.ReasonNotAuthorized,
		}
		data, _ := connack.Encode()
		_ = c.WriteAndClose(data)
		return gnet.None
	}

	s.metrics.IncConnect("success")

	ctx.ClientID = p.ClientID
	ctx.Username = p.Username
	ctx.Authed = true
	ctx.KeepAlive = time.Duration(p.KeepAlive) * time.Second
	if gc, ok := c.(*gnetClientConn); ok {
		s.unauthedConns.Delete(gc.RawConn())
	}

	if oldSess, ok := s.sessionMgr.Get(p.ClientID); ok {
		if p.CleanSession || oldSess.IsExpired(time.Now()) {
			for topic := range oldSess.GetSubscriptions() {
				s.router.Unsubscribe(topic, p.ClientID)
			}
		}
	}

	sess, sessionPresent := s.sessionMgr.GetOrSet(p.ClientID, p.CleanSession)
	ctx.Session = sess
	sess.SetConnected()

	// Evict existing connection with duplicate ClientID per MQTT spec
	if oldVal, ok := s.conns.Load(p.ClientID); ok {
		if oldEntry, ok := oldVal.(*clientEntry); ok {
			_ = oldEntry.conn.Close()
		}
	}
	s.conns.Store(p.ClientID, &clientEntry{conn: c, ctx: ctx})

	// Send CONNACK
	connack := &protocol.ConnackPacket{
		ProtocolLevel:  p.ProtocolLevel,
		SessionPresent: sessionPresent,
		ReturnCode:     0, // Connection Accepted
		ReasonCode:     protocol.ReasonSuccess,
	}
	if p.ProtocolLevel == protocol.V50 {
		var props protocol.Properties
		if ctx.AssignedClientID {
			props.AssignedClientIdentifier = p.ClientID
		}
		wAvail := byte(1)
		sAvail := byte(1)
		subIDAvail := byte(1)
		props.WildcardSubscriptionAvailable = &wAvail
		props.SharedSubscriptionAvailable = &sAvail
		props.SubscriptionIdentifierAvailable = &subIDAvail
		connack.Properties = &props
	}

	data, _ := connack.Encode()
	_, _ = c.Write(data)

	// Replay inflight and offline messages for persistent session
	if !p.CleanSession {
		// 1. Replay unacknowledged inflight messages with DUP=1 (per MQTT-4.3.2-1 & 4.4)
		if sess.Inflight != nil {
			for _, inflight := range sess.Inflight.GetPending() {
				if inflight != nil && inflight.Packet != nil {
					inflight.Packet.Dup = true
					inflight.Retries++
					if enc, err := inflight.Packet.Encode(); err == nil {
						_, _ = c.Write(enc)
					}
				}
			}
		}

		// 2. Replay offline messages that arrived while disconnected
		offlineMsgs, err := s.store.FetchOffline(p.ClientID)
		if err == nil && len(offlineMsgs) > 0 {
			for _, msg := range offlineMsgs {
				if pid, err := sess.PacketIDs.Allocate(); err == nil {
					msg.PacketID = pid
					_ = sess.Inflight.Push(&session.InflightMessage{
						PacketID:  pid,
						Packet:    msg,
						Timestamp: time.Now(),
					})
				}
				if enc, err := msg.Encode(); err == nil {
					_, _ = c.Write(enc)
				}
			}
		}
	} else {
		// CleanSession: discard any old offline messages
		_ = s.store.ClearOffline(p.ClientID)
	}

	return gnet.None
}

func (s *Server) handlePublish(c ClientConn, ctx *ConnContext, p *protocol.PublishPacket) gnet.Action {
	// Topic validation: PUBLISH topic MUST NOT contain wildcards (+ or #), null bytes, or be empty
	if !protocol.ValidatePublishTopic(p.Topic) {
		log.Printf("Client %s sent invalid PUBLISH topic: %s", ctx.ClientID, p.Topic)
		return gnet.Close
	}

	// Rate limiting on incoming publish
	if s.pubLimiter != nil && !s.pubLimiter.Allow(ctx.ClientID) {
		s.metrics.IncRateLimitDropped("publish")
		if ctx.ProtocolLevel == protocol.V50 {
			disc := &protocol.DisconnectPacket{
				ReasonCode: protocol.ReasonQuotaExceeded,
			}
			data, _ := disc.Encode()
			_, _ = c.Write(data)
			return gnet.Close
		}
		return gnet.None
	}

	s.metrics.IncMsgReceived(p.QoS)
	s.metrics.AddBytesReceived(len(p.Payload))

	if s.hookMgr != nil && s.hookMgr.HasAuthorizeHooks() {
		hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, c.RemoteAddr().String())
		allow, err := s.hookMgr.FireAuthorize(hookCtx, hook.AuthActionPublish, p.Topic)
		if err != nil || !allow {
			if p.QoS == protocol.QoS1 && ctx.ProtocolLevel == protocol.V50 {
				puback := &protocol.PubackPacket{
					PacketID:   p.PacketID,
					ReasonCode: protocol.ReasonNotAuthorized,
				}
				data, _ := puback.Encode()
				_, _ = c.Write(data)
			} else if p.QoS == protocol.QoS2 && ctx.ProtocolLevel == protocol.V50 {
				pubrec := &protocol.PubrecPacket{
					PacketID:   p.PacketID,
					ReasonCode: protocol.ReasonNotAuthorized,
				}
				data, _ := pubrec.Encode()
				_, _ = c.Write(data)
			}
			return gnet.None
		}
	}

	if s.hookMgr != nil && s.hookMgr.HasPublishHooks() {
		hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, c.RemoteAddr().String())
		drop, err := s.hookMgr.FirePublish(hookCtx, p)
		if err != nil || drop {
			return gnet.None
		}
	}

	// Idiomatic Go Pipeline
	if s.pipelineRouter != nil && s.pipelineRouter.HasPipelines() {
		pipeMsg := pipeline.AcquireMessage()
		pipeMsg.ClientID = ctx.ClientID
		pipeMsg.Username = ctx.Username
		pipeMsg.Topic = p.Topic
		pipeMsg.Payload = p.Payload
		pipeMsg.QoS = p.QoS

		err := s.pipelineRouter.Process(context.Background(), pipeMsg)
		p.Payload = pipeMsg.Payload
		pipeline.ReleaseMessage(pipeMsg)

		if err != nil {
			if errors.Is(err, pipeline.ErrUnauthorized) && p.QoS == protocol.QoS1 {
				puback := &protocol.PubackPacket{
					PacketID:   p.PacketID,
					ReasonCode: protocol.ReasonNotAuthorized,
				}
				data, _ := puback.Encode()
				_, _ = c.Write(data)
			}
			return gnet.None
		}
	}

	// QoS 1 response: Send PUBACK immediately
	if p.QoS == protocol.QoS1 {
		puback := &protocol.PubackPacket{
			PacketID:   p.PacketID,
			ReasonCode: protocol.ReasonSuccess,
		}
		data, _ := puback.Encode()
		_, _ = c.Write(data)
	}

	// QoS 2 response: Store incoming and reply PUBREC, wait for PUBREL before dispatching
	if p.QoS == protocol.QoS2 {
		if ctx.Session != nil {
			ctx.Session.AddQoS2Incoming(p.PacketID, p)
		}
		pubrec := &protocol.PubrecPacket{
			PacketID:   p.PacketID,
			ReasonCode: protocol.ReasonSuccess,
		}
		data, _ := pubrec.Encode()
		_, _ = c.Write(data)
		return gnet.None
	}

	// Handle Retained message
	if p.Retain {
		if len(p.Payload) == 0 {
			_ = s.store.DeleteRetained(p.Topic)
		} else {
			_ = s.store.SetRetained(p.Topic, p)
		}
	}

	// Forward to streaming pipeline (Kafka / TSDB)
	if s.pipeline != nil {
		s.pipeline.IngestDirect(ctx.ClientID, p.Topic, p.Payload, p.QoS)
	}

	// Forward to cluster mesh
	if s.clusterRouter != nil {
		_ = s.clusterRouter.RouteToCluster(p.Topic, p.QoS, p.Payload)
	}

	// Dispatch to local subscribers
	s.dispatchPublish(p, ctx.ClientID)

	return gnet.None
}

func (s *Server) dispatchPublish(p *protocol.PublishPacket, senderClientID string) {
	s.notifyInternalSubscribers(p.Topic, p.Payload)

	subscribers := s.router.Match(p.Topic)
	if len(subscribers) == 0 {
		return
	}

	var qos0BytesV3 [2][]byte
	var qos0V3Encoded [2]bool
	var qos0BytesV5 [2][]byte
	var qos0V5Encoded [2]bool

	for _, sub := range subscribers {
		// MQTT 5.0 NoLocal: sender should not receive own message
		if sub.NoLocal && sub.ClientID == senderClientID {
			continue
		}

		entryVal, ok := s.conns.Load(sub.ClientID)
		if !ok {
			// Persistent session offline message storage
			if sess, ok := s.sessionMgr.Get(sub.ClientID); ok && sess != nil && !sess.CleanSession {
				grantedQoS := p.QoS
				if sub.QoS < grantedQoS {
					grantedQoS = sub.QoS
				}
				// QoS 0 is fire-and-forget, only QoS 1 & 2 are persisted for offline delivery
				if grantedQoS > protocol.QoS0 {
					offlineMsg := &protocol.PublishPacket{
						ProtocolLevel: p.ProtocolLevel,
						Topic:         p.Topic,
						Payload:       p.Payload,
						QoS:           grantedQoS,
						Retain:        false,
						Properties:    p.Properties,
					}
					_ = s.store.StoreOffline(sub.ClientID, offlineMsg)
				}
			}
			continue
		}
		entry := entryVal.(*clientEntry)

		// Grant QoS is min(publishQoS, subscribeQoS)
		grantedQoS := p.QoS
		if sub.QoS < grantedQoS {
			grantedQoS = sub.QoS
		}

		retainFlag := false
		if sub.RetainAsPublished {
			retainFlag = p.Retain
		}

		clientProto := protocol.V311
		if entry.ctx != nil && entry.ctx.ProtocolLevel != 0 {
			clientProto = entry.ctx.ProtocolLevel
		}

		outPub := &protocol.PublishPacket{
			ProtocolLevel: clientProto,
			Topic:         p.Topic,
			Payload:       p.Payload,
			QoS:           grantedQoS,
			Retain:        retainFlag,
			Properties:    p.Properties,
		}

		if grantedQoS == protocol.QoS0 {
			rIdx := 0
			if retainFlag {
				rIdx = 1
			}
			var wireBytes []byte
			if clientProto == protocol.V50 {
				if !qos0V5Encoded[rIdx] {
					outPub.PacketID = 0
					qos0BytesV5[rIdx], _ = outPub.Encode()
					qos0V5Encoded[rIdx] = true
				}
				wireBytes = qos0BytesV5[rIdx]
			} else {
				if !qos0V3Encoded[rIdx] {
					outPub.PacketID = 0
					qos0BytesV3[rIdx], _ = outPub.Encode()
					qos0V3Encoded[rIdx] = true
				}
				wireBytes = qos0BytesV3[rIdx]
			}
			if entry.ctx != nil && entry.ctx.MaxPacketSize > 0 && uint32(len(wireBytes)) > entry.ctx.MaxPacketSize {
				continue
			}
			_, _ = entry.conn.Write(wireBytes)
			s.metrics.IncMsgSent(0)
			s.metrics.AddBytesSent(len(p.Payload))
			if s.hookMgr != nil {
				var remoteAddr string
				if entry.conn != nil && entry.conn.RemoteAddr() != nil {
					remoteAddr = entry.conn.RemoteAddr().String()
				}
				hookCtx := hook.NewClientContext(sub.ClientID, "", remoteAddr)
				s.hookMgr.FireDelivered(hookCtx, outPub)
			}
		} else {
			if sess, ok := s.sessionMgr.Get(sub.ClientID); ok && sess != nil {
				if pid, err := sess.PacketIDs.Allocate(); err == nil {
					outPub.PacketID = pid
					_ = sess.Inflight.Push(&session.InflightMessage{
						PacketID:  pid,
						Packet:    outPub,
						Timestamp: time.Now(),
					})
				}
			}
			encoded, err := outPub.Encode()
			if err == nil {
				if entry.ctx != nil && entry.ctx.MaxPacketSize > 0 && uint32(len(encoded)) > entry.ctx.MaxPacketSize {
					continue
				}
				_, _ = entry.conn.Write(encoded)
				s.metrics.IncMsgSent(grantedQoS)
				s.metrics.AddBytesSent(len(p.Payload))
				if s.hookMgr != nil {
					var remoteAddr string
					if entry.conn != nil && entry.conn.RemoteAddr() != nil {
						remoteAddr = entry.conn.RemoteAddr().String()
					}
					hookCtx := hook.NewClientContext(sub.ClientID, "", remoteAddr)
					s.hookMgr.FireDelivered(hookCtx, outPub)
				}
			}
		}
	}
}

func (s *Server) handlePubrec(c ClientConn, ctx *ConnContext, p *protocol.PubrecPacket) gnet.Action {
	pubrel := &protocol.PubrelPacket{
		PacketID:   p.PacketID,
		ReasonCode: protocol.ReasonSuccess,
	}
	data, _ := pubrel.Encode()
	_, _ = c.Write(data)
	return gnet.None
}

func (s *Server) handlePubrel(c ClientConn, ctx *ConnContext, p *protocol.PubrelPacket) gnet.Action {
	pubcomp := &protocol.PubcompPacket{
		PacketID:   p.PacketID,
		ReasonCode: protocol.ReasonSuccess,
	}
	data, _ := pubcomp.Encode()
	_, _ = c.Write(data)

	// Release QoS 2 message and dispatch to local subscribers and cluster
	if ctx.Session != nil {
		if storedPub, ok := ctx.Session.ReleaseQoS2Incoming(p.PacketID); ok && storedPub != nil {
			if storedPub.Retain {
				if len(storedPub.Payload) == 0 {
					_ = s.store.DeleteRetained(storedPub.Topic)
				} else {
					_ = s.store.SetRetained(storedPub.Topic, storedPub)
				}
			}
			if s.clusterRouter != nil {
				_ = s.clusterRouter.RouteToCluster(storedPub.Topic, storedPub.QoS, storedPub.Payload)
			}
			s.dispatchPublish(storedPub, ctx.ClientID)
		}
	}
	return gnet.None
}

func (s *Server) handlePubcomp(c ClientConn, ctx *ConnContext, p *protocol.PubcompPacket) gnet.Action {
	if ctx.Session != nil {
		ctx.Session.Inflight.Ack(p.PacketID)
		ctx.Session.PacketIDs.Release(p.PacketID)
	}
	return gnet.None
}

func (s *Server) handleAuth(c ClientConn, ctx *ConnContext, p *protocol.AuthPacket) gnet.Action {
	authResp := &protocol.AuthPacket{
		ReasonCode: protocol.ReasonSuccess,
	}
	data, _ := authResp.Encode()
	_, _ = c.Write(data)
	return gnet.None
}

func (s *Server) handleDisconnect(c ClientConn, ctx *ConnContext, p *protocol.DisconnectPacket) gnet.Action {
	if p.ReasonCode != protocol.ReasonDisconnectWithWill {
		ctx.CleanDisconnect = true
	}
	if p.Properties != nil && p.Properties.SessionExpiryInterval != nil {
		ctx.SessionExpirySeconds = *p.Properties.SessionExpiryInterval
	}
	return gnet.Close
}

func (s *Server) handleSubscribe(c ClientConn, ctx *ConnContext, p *protocol.SubscribePacket) gnet.Action {
	retCodes := make([]byte, len(p.Topics))
	allRetained, _ := s.store.GetAllRetained()
	var retainedToSend []*protocol.PublishPacket

	for i, sub := range p.Topics {
		if s.hookMgr != nil && s.hookMgr.HasAuthorizeHooks() {
			hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, c.RemoteAddr().String())
			allowed, err := s.hookMgr.FireAuthorize(hookCtx, hook.AuthActionSubscribe, sub.Topic)
			if err != nil || !allowed {
				if ctx.ProtocolLevel == protocol.V50 {
					retCodes[i] = protocol.ReasonNotAuthorized
				} else {
					retCodes[i] = 0x80
				}
				continue
			}
		}

		isNewSub := true
		if ctx.Session != nil {
			subs := ctx.Session.GetSubscriptions()
			if _, exists := subs[sub.Topic]; exists {
				isNewSub = false
			}
			ctx.Session.AddSubscription(sub.Topic, sub.QoS)
		}

		s.router.Subscribe(sub.Topic, ctx.ClientID, sub.QoS, trie.SubOption{
			NoLocal:           sub.NoLocal,
			RetainAsPublished: sub.RetainAsPublished,
			RetainHandling:    sub.RetainHandling,
		})

		if s.clusterMesh != nil {
			_ = s.clusterMesh.BroadcastRoute("", sub.Topic, 1)
		}

		if ctx.ProtocolLevel == protocol.V50 {
			switch sub.QoS {
			case protocol.QoS0:
				retCodes[i] = protocol.ReasonGrantedQoS0
			case protocol.QoS1:
				retCodes[i] = protocol.ReasonGrantedQoS1
			case protocol.QoS2:
				retCodes[i] = protocol.ReasonGrantedQoS2
			default:
				retCodes[i] = protocol.ReasonUnspecifiedError
			}
		} else {
			retCodes[i] = sub.QoS
		}

		// Retain Handling:
		// 0: Send retained messages at the time of the subscribe
		// 1: Send retained messages only if the subscription does not currently exist
		// 2: Do not send retained messages at the time of the subscribe
		shouldSendRetained := false
		if ctx.ProtocolLevel == protocol.V50 {
			if sub.RetainHandling == 0 || (sub.RetainHandling == 1 && isNewSub) {
				shouldSendRetained = true
			}
		} else {
			shouldSendRetained = true
		}

		if shouldSendRetained && len(allRetained) > 0 {
			for _, rMsg := range allRetained {
				if trie.TopicFilterMatches(sub.Topic, rMsg.Topic) {
					if s.hookMgr != nil && s.hookMgr.HasAuthorizeHooks() {
						hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, c.RemoteAddr().String())
						allowed, err := s.hookMgr.FireAuthorize(hookCtx, hook.AuthActionSubscribe, rMsg.Topic)
						if err != nil || !allowed {
							continue
						}
					}
					if s.hookMgr != nil && s.hookMgr.HasPublishHooks() {
						hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, c.RemoteAddr().String())
						rPub := &protocol.PublishPacket{
							ProtocolLevel: ctx.ProtocolLevel,
							Topic:         rMsg.Topic,
							Payload:       rMsg.Payload,
							QoS:           rMsg.QoS,
							Retain:        true,
							Properties:    rMsg.Properties,
						}
						drop, err := s.hookMgr.FirePublish(hookCtx, rPub)
						if err != nil || drop {
							continue
						}
					}
					grantedQoS := rMsg.QoS
					if sub.QoS < grantedQoS {
						grantedQoS = sub.QoS
					}
					outPub := &protocol.PublishPacket{
						ProtocolLevel: ctx.ProtocolLevel,
						Topic:         rMsg.Topic,
						Payload:       rMsg.Payload,
						QoS:           grantedQoS,
						Retain:        true,
						Properties:    rMsg.Properties,
					}
					if grantedQoS > protocol.QoS0 && ctx.Session != nil {
						if pid, err := ctx.Session.PacketIDs.Allocate(); err == nil {
							outPub.PacketID = pid
						}
					}
					retainedToSend = append(retainedToSend, outPub)
				}
			}
		}
	}

	suback := &protocol.SubackPacket{
		ProtocolLevel: ctx.ProtocolLevel,
		PacketID:      p.PacketID,
		ReturnCodes:   retCodes,
	}
	data, _ := suback.Encode()
	_, _ = c.Write(data)

	// Deliver retained messages after SUBACK
	for _, outPub := range retainedToSend {
		if enc, err := outPub.Encode(); err == nil {
			_, _ = c.Write(enc)
			if s.hookMgr != nil {
				hookCtx := hook.NewClientContext(ctx.ClientID, ctx.Username, c.RemoteAddr().String())
				s.hookMgr.FireDelivered(hookCtx, outPub)
			}
		}
	}

	return gnet.None
}

func (s *Server) handleUnsubscribe(c ClientConn, ctx *ConnContext, p *protocol.UnsubscribePacket) gnet.Action {
	for _, topic := range p.Topics {
		s.router.Unsubscribe(topic, ctx.ClientID)
		if ctx.Session != nil {
			ctx.Session.RemoveSubscription(topic)
		}
		if s.clusterMesh != nil {
			_ = s.clusterMesh.BroadcastRoute("", topic, 2)
		}
	}

	reasonCodes := make([]byte, len(p.Topics))
	for i := range reasonCodes {
		reasonCodes[i] = protocol.ReasonSuccess
	}

	unsuback := &protocol.UnsubackPacket{
		ProtocolLevel: ctx.ProtocolLevel,
		PacketID:      p.PacketID,
		ReasonCodes:   reasonCodes,
	}
	data, _ := unsuback.Encode()
	_, _ = c.Write(data)

	return gnet.None
}

// SetClusterMesh connects a cluster mesh transport to this broker.
func (s *Server) SetClusterMesh(mesh *cluster.ClusterMesh) {
	s.clusterMesh = mesh
	s.clusterRouter = mesh.Router()
}

// DeliverFromCluster dispatches an incoming cross-node cluster message to local subscribers.
func (s *Server) DeliverFromCluster(topic string, qos byte, payload []byte) {
	outPub := &protocol.PublishPacket{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  false,
	}
	s.dispatchPublish(outPub, "")
}

func (s *Server) handlePuback(c ClientConn, ctx *ConnContext, p *protocol.PubackPacket) gnet.Action {
	if ctx.Session != nil {
		ctx.Session.Inflight.Ack(p.PacketID)
		ctx.Session.PacketIDs.Release(p.PacketID)
	}
	return gnet.None
}

// Router returns the underlying topic router (useful for testing & stats).
func (s *Server) Router() *trie.Router {
	return s.router
}

// SessionManager returns the session manager.
func (s *Server) SessionManager() *session.SessionManager {
	return s.sessionMgr
}

// HookManager returns the hook manager.
func (s *Server) HookManager() *hook.Manager {
	return s.hookMgr
}

// SetPipeline attaches an industrial streaming pipeline (Kafka/MQ/TSDB).
func (s *Server) SetPipeline(p *pipeline.Pipeline) {
	s.pipeline = p
	// Pipeline legacy streaming spooler
}

// Pipeline returns the active streaming pipeline, if any.
func (s *Server) Pipeline() *pipeline.Pipeline {
	return s.pipeline
}

// SetPipelineRouter sets the pipeline Router on the server.
func (s *Server) SetPipelineRouter(r *pipeline.Router) {
	s.pipelineRouter = r
}

// PipelineRouter returns the active pipeline Router instance.
func (s *Server) PipelineRouter() *pipeline.Router {
	return s.pipelineRouter
}

// ProcessPacket implements PacketDispatcher, allowing any TransportListener to hand decoded packets into the broker engine.
func (s *Server) ProcessPacket(conn ClientConn, ctx *ConnContext, pkt protocol.Packet) gnet.Action {
	return s.handlePacket(conn, ctx, pkt)
}

// OnConnClosed implements PacketDispatcher, notifying the core engine to release client state.
func (s *Server) OnConnClosed(ctx *ConnContext, remoteAddr string, err error) {
	s.closeClient(ctx, remoteAddr, err)
}

// RegisterListener registers an external network transport listener into the manager.
func (s *Server) RegisterListener(l TransportListener) {
	s.listeners.Register(l)
}

// ListenerManager returns the transport listeners coordinator.
func (s *Server) ListenerManager() *ListenerManager {
	return s.listeners
}

// StartQUIC initializes and boots the MQTT over QUIC (RFC 9000 UDP) listener.
func (s *Server) StartQUIC(cfg QUICConfig) (*QUICListener, error) {
	ql, err := NewQUICListener(cfg)
	if err != nil {
		return nil, err
	}
	if err := ql.Start(s); err != nil {
		return nil, err
	}
	s.quicListener = ql
	s.listeners.Register(ql)
	return ql, nil
}

// QUICListener returns the active QUIC listener, if running.
func (s *Server) QUICListener() *QUICListener {
	return s.quicListener
}

// InternalSubscription represents an in-process topic subscriber.
type InternalSubscription struct {
	ID      string
	Filter  string
	Handler func(topic string, payload []byte)
}

// SubscribeInternal registers an in-process topic subscriber.
// Filter supports exact topics or MQTT wildcards (+, #).
func (s *Server) SubscribeInternal(filter string, handler func(topic string, payload []byte)) (string, func(), error) {
	if filter == "" {
		return "", nil, fmt.Errorf("filter cannot be empty")
	}
	if handler == nil {
		return "", nil, fmt.Errorf("handler cannot be nil")
	}

	id := fmt.Sprintf("sub_%d", s.internalSubSeq.Add(1))
	sub := &InternalSubscription{
		ID:      id,
		Filter:  filter,
		Handler: handler,
	}
	s.internalSubs.Store(id, sub)
	s.hasInternalSubs.Store(true)

	unsub := func() {
		_ = s.UnsubscribeInternal(id)
	}
	return id, unsub, nil
}

// UnsubscribeInternal unregisters an in-process topic subscriber by its ID.
func (s *Server) UnsubscribeInternal(id string) error {
	s.internalSubs.Delete(id)
	remaining := false
	s.internalSubs.Range(func(_, _ any) bool {
		remaining = true
		return false
	})
	s.hasInternalSubs.Store(remaining)
	return nil
}

func (s *Server) notifyInternalSubscribers(topic string, payload []byte) {
	if !s.hasInternalSubs.Load() {
		return
	}
	s.internalSubs.Range(func(key, value any) bool {
		sub, ok := value.(*InternalSubscription)
		if !ok {
			return true
		}
		if sub.Filter == "#" || sub.Filter == topic || pipeline.MatchTopicFilter(sub.Filter, topic) {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[InternalSub] handler %s panicked: %v", sub.ID, r)
					}
				}()
				sub.Handler(topic, payload)
			}()
		}
		return true
	})
}
