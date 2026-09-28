package session

import (
	"hash/fnv"
	"sync"
	"time"

	"mqtt/pkg/protocol"
)

// Session represents the MQTT client state across network disconnects.
type Session struct {
	mu            sync.RWMutex
	ClientID      string
	CleanSession  bool
	Subscriptions map[string]byte // TopicFilter -> Granted QoS
	Inflight      *InflightQueue
	PacketIDs     *PacketIDAllocator
	QoS2Inflight  map[uint16]*protocol.PublishPacket // Incoming QoS 2 packets awaiting PUBREL
	ConnectedAt   time.Time
	LastActiveAt  time.Time
	Conn          any // Network connection handle (e.g., gnet.Conn)
	WillPacket    *protocol.PublishPacket
}

// NewSession creates an active or clean session for a client.
func NewSession(clientID string, cleanSession bool) *Session {
	return &Session{
		ClientID:      clientID,
		CleanSession:  cleanSession,
		Subscriptions: make(map[string]byte),
		Inflight:      NewInflightQueue(1024),
		PacketIDs:     NewPacketIDAllocator(),
		QoS2Inflight:  make(map[uint16]*protocol.PublishPacket),
		ConnectedAt:   time.Now(),
		LastActiveAt:  time.Now(),
	}
}

// AddQoS2Incoming records an incoming QoS 2 PUBLISH packet awaiting PUBREL.
func (s *Session) AddQoS2Incoming(pid uint16, p *protocol.PublishPacket) {
	s.mu.Lock()
	if s.QoS2Inflight == nil {
		s.QoS2Inflight = make(map[uint16]*protocol.PublishPacket)
	}
	s.QoS2Inflight[pid] = p
	s.mu.Unlock()
}

// ReleaseQoS2Incoming releases and returns the stored QoS 2 message when PUBREL arrives.
func (s *Session) ReleaseQoS2Incoming(pid uint16) (*protocol.PublishPacket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.QoS2Inflight == nil {
		return nil, false
	}
	p, exists := s.QoS2Inflight[pid]
	if exists {
		delete(s.QoS2Inflight, pid)
	}
	return p, exists
}

// AddSubscription updates or records a topic subscription on the session.
func (s *Session) AddSubscription(topic string, qos byte) {
	s.mu.Lock()
	s.Subscriptions[topic] = qos
	s.mu.Unlock()
}

// RemoveSubscription deletes a topic subscription from the session.
func (s *Session) RemoveSubscription(topic string) {
	s.mu.Lock()
	delete(s.Subscriptions, topic)
	s.mu.Unlock()
}

// GetSubscriptions returns a snapshot of all active subscriptions.
func (s *Session) GetSubscriptions() map[string]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]byte, len(s.Subscriptions))
	for k, v := range s.Subscriptions {
		res[k] = v
	}
	return res
}

// SessionManager manages active client sessions using 64 mutex shards
// to eliminate global lock contention across thousands of concurrent clients.
type SessionManager struct {
	shards [64]*sessionShard
}

type sessionShard struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewSessionManager creates a 64-way sharded session manager.
func NewSessionManager() *SessionManager {
	sm := &SessionManager{}
	for i := 0; i < 64; i++ {
		sm.shards[i] = &sessionShard{
			sessions: make(map[string]*Session, 256),
		}
	}
	return sm
}

func (sm *SessionManager) getShard(clientID string) *sessionShard {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return sm.shards[h.Sum32()%64]
}

// GetOrSet retrieves an existing session or registers a new one.
// Returns (session, sessionPresent).
func (sm *SessionManager) GetOrSet(clientID string, cleanSession bool) (*Session, bool) {
	shard := sm.getShard(clientID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	existing, exists := shard.sessions[clientID]
	if exists && !cleanSession {
		existing.CleanSession = cleanSession
		existing.LastActiveAt = time.Now()
		return existing, true
	}

	newSess := NewSession(clientID, cleanSession)
	shard.sessions[clientID] = newSess
	return newSess, false
}

// Get retrieves an existing session if present.
func (sm *SessionManager) Get(clientID string) (*Session, bool) {
	shard := sm.getShard(clientID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	s, ok := shard.sessions[clientID]
	return s, ok
}

// Delete removes a session (e.g. upon CleanSession disconnect).
func (sm *SessionManager) Delete(clientID string) {
	shard := sm.getShard(clientID)
	shard.mu.Lock()
	delete(shard.sessions, clientID)
	shard.mu.Unlock()
}

// SubscriptionRecord represents an active subscription held by a client.
type SubscriptionRecord struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
}

// GetAllSubscriptions returns a snapshot of all active subscriptions across all shards.
func (sm *SessionManager) GetAllSubscriptions() []SubscriptionRecord {
	var records []SubscriptionRecord
	for _, shard := range sm.shards {
		shard.mu.RLock()
		for clientID, sess := range shard.sessions {
			sess.mu.RLock()
			for topic, qos := range sess.Subscriptions {
				records = append(records, SubscriptionRecord{
					ClientID: clientID,
					Topic:    topic,
					QoS:      qos,
				})
			}
			sess.mu.RUnlock()
		}
		shard.mu.RUnlock()
	}
	return records
}

// Count returns the total number of registered sessions across all shards.
func (sm *SessionManager) Count() int {
	count := 0
	for _, shard := range sm.shards {
		shard.mu.RLock()
		count += len(shard.sessions)
		shard.mu.RUnlock()
	}
	return count
}
