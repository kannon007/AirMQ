package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"mqtt/pkg/metrics"
)

var (
	ErrPeerNotFound = errors.New("cluster peer connection not found")
)

type LocalDispatcher interface {
	DeliverFromCluster(topic string, qos byte, payload []byte)
}

// ClusterMesh manages peer TCP connections, SWIM-inspired 3-state health,
// PEX decentralized discovery, route propagation, and message forwarding.
type ClusterMesh struct {
	mu           sync.RWMutex
	nodeID       string
	listenAddr   string
	listener     net.Listener
	peers        map[string]net.Conn  // peerNodeID -> TCP Conn
	peerAddrs    map[string]string    // peerNodeID -> cluster listen addr
	peerStates   map[string]PeerState // peerNodeID -> StateAlive / StateSuspect / StateDead
	peerLastSeen map[string]time.Time // peerNodeID -> last heartbeat timestamp
	knownAddrs   map[string]string    // peerNodeID -> known cluster listen addr (for auto-reconnect)
	connecting   map[string]bool      // peerNodeID or addr -> in-flight dial protection
	router       *ClusterRouter
	dispatcher   LocalDispatcher
	stopCh       chan struct{}
	closeOnce    sync.Once
}

func NewClusterMesh(nodeID, listenAddr string, dispatcher LocalDispatcher) *ClusterMesh {
	mesh := &ClusterMesh{
		nodeID:       nodeID,
		listenAddr:   listenAddr,
		peers:        make(map[string]net.Conn),
		peerAddrs:    make(map[string]string),
		peerStates:   make(map[string]PeerState),
		peerLastSeen: make(map[string]time.Time),
		knownAddrs:   make(map[string]string),
		connecting:   make(map[string]bool),
		dispatcher:   dispatcher,
		stopCh:       make(chan struct{}),
	}
	mesh.router = NewClusterRouter(nodeID, mesh)
	return mesh
}

func (m *ClusterMesh) Router() *ClusterRouter {
	return m.router
}

func (m *ClusterMesh) NodeID() string {
	return m.nodeID
}

func (m *ClusterMesh) ListenAddr() string {
	return m.listenAddr
}

// Peers returns a copy of currently connected peer IDs.
func (m *ClusterMesh) Peers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]string, 0, len(m.peers))
	for id := range m.peers {
		res = append(res, id)
	}
	return res
}

// PeerState returns the current health state of a peer.
func (m *ClusterMesh) PeerState(peerID string) PeerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.peerStates[peerID]
}

// PeerDetail provides snapshot info for cluster dashboard.
type PeerDetail struct {
	NodeID   string
	Addr     string
	State    string
	LastSeen time.Time
}

// GetPeersDetails returns snapshot details of all cluster peers.
func (m *ClusterMesh) GetPeersDetails() []PeerDetail {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]PeerDetail, 0, len(m.knownAddrs)+len(m.peers))
	seen := make(map[string]bool)

	for id, conn := range m.peers {
		seen[id] = true
		state := "alive"
		if s, ok := m.peerStates[id]; ok {
			switch s {
			case StateSuspect:
				state = "suspect"
			case StateDead:
				state = "dead"
			default:
				state = "alive"
			}
		}
		addr := ""
		if a, ok := m.peerAddrs[id]; ok {
			addr = a
		} else if conn != nil {
			addr = conn.RemoteAddr().String()
		}
		res = append(res, PeerDetail{
			NodeID:   id,
			Addr:     addr,
			State:    state,
			LastSeen: m.peerLastSeen[id],
		})
	}

	for id, addr := range m.knownAddrs {
		if !seen[id] {
			res = append(res, PeerDetail{
				NodeID:   id,
				Addr:     addr,
				State:    "dead",
				LastSeen: m.peerLastSeen[id],
			})
		}
	}

	return res
}

// Start begins listening for peer broker connections and starts the heartbeat loop.
func (m *ClusterMesh) Start() error {
	ln, err := net.Listen("tcp", m.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind cluster mesh on %s: %w", m.listenAddr, err)
	}
	m.listener = ln
	go m.acceptLoop()
	go m.heartbeatLoop()
	return nil
}

// Join connects to one or more seed nodes to discover the full cluster mesh.
func (m *ClusterMesh) Join(seeds []string) error {
	var errs []error
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s == "" || s == m.listenAddr {
			continue
		}
		if err := m.ConnectPeer("", s); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 && len(errs) == len(seeds) {
		return fmt.Errorf("failed to connect to any cluster seeds: %v", errs)
	}
	return nil
}

// heartbeatLoop executes periodic 1s heartbeat and SWIM 3-state failure detection.
func (m *ClusterMesh) heartbeatLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case now := <-ticker.C:
			m.mu.Lock()

			// 1. Send heartbeat carrying 8-byte local route generation epoch
			localEpoch := m.router.LocalEpoch()
			hbFrame := make([]byte, 5+8)
			hbFrame[0] = MsgHeartbeat
			binary.BigEndian.PutUint32(hbFrame[1:5], 8)
			binary.BigEndian.PutUint64(hbFrame[5:13], localEpoch)

			for peerID, conn := range m.peers {
				_, err := conn.Write(hbFrame)
				if err != nil {
					log.Printf("[ClusterMesh] Peer %s write heartbeat failed: %v", peerID, err)
				}
			}

			// 2. SWIM 3-State Health Evaluation
			for peerID, last := range m.peerLastSeen {
				duration := now.Sub(last)
				currState := m.peerStates[peerID]

				if duration > 4*time.Second {
					// Transition to StateDead: evict node and route
					log.Printf("[ClusterMesh] Peer %s confirmed DEAD (>4s timeout, %v), evicting from cluster", peerID, duration)
					if conn, ok := m.peers[peerID]; ok {
						_ = conn.Close()
						delete(m.peers, peerID)
					}
					delete(m.peerLastSeen, peerID)
					delete(m.peerStates, peerID)
					delete(m.peerAddrs, peerID)
					m.router.EvictNode(peerID)
				} else if duration > 2*time.Second {
					// Transition to StateSuspect: suspecting failure, but retain routes
					if currState == StateAlive {
						m.peerStates[peerID] = StateSuspect
						log.Printf("[ClusterMesh] Peer %s heartbeat delayed (%v > 2s), marking SUSPECT (routes retained)", peerID, duration)
					}
				}
			}

			// 3. Auto-reconnect: if known peer is missing and m.nodeID < peerID (tie-breaking)
			for peerID, addr := range m.knownAddrs {
				if peerID != m.nodeID && m.peers[peerID] == nil && !m.connecting[peerID] {
					if m.nodeID < peerID {
						go m.ConnectPeer(peerID, addr)
					}
				}
			}

			onlineCount := int64(len(m.peers))
			m.mu.Unlock()

			metrics.Default.SetClusterNodesOnline(onlineCount)
		}
	}
}

func (m *ClusterMesh) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.stopCh:
				return
			default:
				log.Printf("[ClusterMesh] Accept error: %v", err)
				continue
			}
		}
		go m.handlePeerConn(conn, false)
	}
}

// ConnectPeer dials a peer broker node with deduplication and tie-breaking.
func (m *ClusterMesh) ConnectPeer(peerID, peerAddr string) error {
	m.mu.Lock()
	if peerID != "" && m.peers[peerID] != nil {
		m.mu.Unlock()
		return nil
	}
	key := peerID
	if key == "" {
		key = peerAddr
	}
	if m.connecting[key] {
		m.mu.Unlock()
		return nil
	}
	m.connecting[key] = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.connecting, key)
		m.mu.Unlock()
	}()

	conn, err := net.DialTimeout("tcp", peerAddr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to dial peer %s at %s: %w", peerID, peerAddr, err)
	}

	// 1. Send our extended handshake:
	// [2-byte NodeID len][NodeID][2-byte Addr len][ClusterListenAddr]
	handshake := encodeHandshake(m.nodeID, m.listenAddr)
	if _, err := conn.Write(handshake); err != nil {
		conn.Close()
		return err
	}

	// 2. Read remote peer's handshake response
	remoteNodeID, remoteListenAddr, err := decodeHandshake(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("handshake with %s failed: %w", peerAddr, err)
	}

	m.mu.Lock()
	if existing, ok := m.peers[remoteNodeID]; ok && existing != nil {
		// Connection collision resolved: keep existing, discard new
		m.mu.Unlock()
		conn.Close()
		return nil
	}

	m.peers[remoteNodeID] = conn
	if remoteListenAddr != "" {
		m.peerAddrs[remoteNodeID] = remoteListenAddr
		m.knownAddrs[remoteNodeID] = remoteListenAddr
	} else if peerAddr != "" {
		m.peerAddrs[remoteNodeID] = peerAddr
		m.knownAddrs[remoteNodeID] = peerAddr
	}
	m.peerStates[remoteNodeID] = StateAlive
	m.peerLastSeen[remoteNodeID] = time.Now()
	metrics.Default.SetClusterNodesOnline(int64(len(m.peers)))
	m.mu.Unlock()

	// 3. Immediately exchange route snapshot with newly connected peer
	_ = m.SendRouteSnapshot(remoteNodeID, m.router.LocalTopics())

	go m.handlePeerConn(conn, true)
	return nil
}

func (m *ClusterMesh) handlePeerConn(conn net.Conn, isOutbound bool) {
	defer conn.Close()

	var remoteNodeID string
	var remoteListenAddr string

	if !isOutbound {
		// Inbound connection:
		// 1. Read remote handshake
		var err error
		remoteNodeID, remoteListenAddr, err = decodeHandshake(conn)
		if err != nil {
			return
		}

		// 2. Reply with our handshake
		respHandshake := encodeHandshake(m.nodeID, m.listenAddr)
		if _, err := conn.Write(respHandshake); err != nil {
			return
		}

		m.mu.Lock()
		if existing, ok := m.peers[remoteNodeID]; ok && existing != nil {
			// Duplicate connection: close incoming
			m.mu.Unlock()
			return
		}
		m.peers[remoteNodeID] = conn
		if remoteListenAddr != "" {
			m.peerAddrs[remoteNodeID] = remoteListenAddr
			m.knownAddrs[remoteNodeID] = remoteListenAddr
		}
		m.peerStates[remoteNodeID] = StateAlive
		m.peerLastSeen[remoteNodeID] = time.Now()
		metrics.Default.SetClusterNodesOnline(int64(len(m.peers)))

		// Collect current peer directory for PEX
		peerList := make([]NodeInfo, 0, len(m.peers)+1)
		// Add self
		peerList = append(peerList, NodeInfo{ID: m.nodeID, RPCAddr: m.listenAddr})
		for pID, pAddr := range m.peerAddrs {
			if pID != remoteNodeID {
				peerList = append(peerList, NodeInfo{ID: pID, RPCAddr: pAddr})
			}
		}
		m.mu.Unlock()

		// 3. Send PeerList to new node (PEX)
		_ = m.sendPeerList(conn, peerList)

		// 4. Broadcast PeerJoin to existing peers
		if remoteListenAddr != "" {
			m.broadcastPeerJoin(remoteNodeID, remoteListenAddr)
		}

		// 5. Send our local route snapshot to the new peer
		_ = m.SendRouteSnapshot(remoteNodeID, m.router.LocalTopics())
	} else {
		// Outbound: remoteNodeID already set in ConnectPeer
		m.mu.RLock()
		for id, c := range m.peers {
			if c == conn {
				remoteNodeID = id
				remoteListenAddr = m.peerAddrs[id]
				break
			}
		}
		m.mu.RUnlock()
	}

	defer func() {
		m.mu.Lock()
		if remoteNodeID != "" && m.peers[remoteNodeID] == conn {
			delete(m.peers, remoteNodeID)
			delete(m.peerLastSeen, remoteNodeID)
			delete(m.peerStates, remoteNodeID)
			delete(m.peerAddrs, remoteNodeID)
			m.router.EvictNode(remoteNodeID)
			metrics.Default.SetClusterNodesOnline(int64(len(m.peers)))
		}
		m.mu.Unlock()
	}()

	// Frame processing loop
	var header [5]byte // 1 byte Type + 4 bytes Length
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		msgType := header[0]
		payloadLen := binary.BigEndian.Uint32(header[1:5])

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
		}

		m.mu.Lock()
		if remoteNodeID != "" {
			m.peerLastSeen[remoteNodeID] = time.Now()
			if m.peerStates[remoteNodeID] == StateSuspect {
				m.peerStates[remoteNodeID] = StateAlive
				log.Printf("[ClusterMesh] Peer %s recovered from SUSPECT to ALIVE", remoteNodeID)
			}
		}
		m.mu.Unlock()

		m.handleFrame(remoteNodeID, msgType, payload)
	}
}

func (m *ClusterMesh) handleFrame(remoteNodeID string, msgType byte, payload []byte) {
	switch msgType {
	case MsgHeartbeat:
		// Heartbeat received: check remote epoch
		if len(payload) >= 8 {
			remoteEpoch := binary.BigEndian.Uint64(payload[:8])
			lastEpoch := m.router.RemoteEpoch(remoteNodeID)
			if remoteEpoch > lastEpoch {
				m.router.SetRemoteEpoch(remoteNodeID, remoteEpoch)
			}
		}
	case MsgRouteAdd:
		topic := string(payload)
		m.router.OnRemoteRouteSync(remoteNodeID, topic, 1)
	case MsgRouteDel:
		topic := string(payload)
		m.router.OnRemoteRouteSync(remoteNodeID, topic, 2)
	case MsgRouteSnapshot:
		// Decode topic list: 2 bytes count + (2 bytes len + string)*
		if len(payload) >= 2 {
			count := int(binary.BigEndian.Uint16(payload[:2]))
			offset := 2
			topics := make([]string, 0, count)
			for i := 0; i < count && offset+2 <= len(payload); i++ {
				tLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
				offset += 2
				if offset+tLen <= len(payload) {
					topics = append(topics, string(payload[offset:offset+tLen]))
					offset += tLen
				}
			}
			m.router.SyncRouteSnapshot(remoteNodeID, topics)
		}
	case MsgForwardPub:
		if len(payload) < 3 {
			return
		}
		qos := payload[0]
		topicLen := int(binary.BigEndian.Uint16(payload[1:3]))
		if len(payload) < 3+topicLen {
			return
		}
		topic := string(payload[3 : 3+topicLen])
		pubPayload := payload[3+topicLen:]

		if m.dispatcher != nil {
			m.dispatcher.DeliverFromCluster(topic, qos, pubPayload)
		}
	case MsgPeerJoin:
		// Gossip new peer discovery: [2-byte idLen][id][2-byte addrLen][addr]
		pID, pAddr, err := decodePeerTuple(payload)
		if err == nil && pID != m.nodeID && pAddr != "" {
			m.mu.Lock()
			m.knownAddrs[pID] = pAddr
			alreadyConnected := (m.peers[pID] != nil)
			m.mu.Unlock()

			// Deterministic Tie-Breaking: only dial if m.nodeID < pID
			if !alreadyConnected && m.nodeID < pID {
				log.Printf("[ClusterMesh] Discovered new peer %s at %s via Gossip, initiating link...", pID, pAddr)
				go m.ConnectPeer(pID, pAddr)
			}
		}
	case MsgPeerList:
		// PEX directory response: [2-byte count] + tuples
		if len(payload) >= 2 {
			count := int(binary.BigEndian.Uint16(payload[:2]))
			offset := 2
			for i := 0; i < count && offset < len(payload); i++ {
				pID, pAddr, n, err := decodePeerTupleWithOffset(payload[offset:])
				if err != nil {
					break
				}
				offset += n
				if pID == m.nodeID || pAddr == "" {
					continue
				}

				m.mu.Lock()
				m.knownAddrs[pID] = pAddr
				alreadyConnected := (m.peers[pID] != nil)
				m.mu.Unlock()

				// Deterministic Tie-Breaking: only dial if m.nodeID < pID
				if !alreadyConnected && m.nodeID < pID {
					log.Printf("[ClusterMesh] Discovered peer %s (%s) from peer list, initiating link...", pID, pAddr)
					go m.ConnectPeer(pID, pAddr)
				}
			}
		}
	case MsgPeerLeave:
		// Graceful departure notification
		leavingID := remoteNodeID
		if len(payload) >= 2 {
			idLen := int(binary.BigEndian.Uint16(payload[:2]))
			if len(payload) >= 2+idLen && idLen > 0 {
				leavingID = string(payload[2 : 2+idLen])
			}
		}
		log.Printf("[ClusterMesh] Peer %s gracefully left the cluster", leavingID)
		m.mu.Lock()
		if conn, ok := m.peers[leavingID]; ok {
			_ = conn.Close()
			delete(m.peers, leavingID)
		}
		delete(m.peerLastSeen, leavingID)
		delete(m.peerStates, leavingID)
		delete(m.peerAddrs, leavingID)
		m.router.EvictNode(leavingID)
		metrics.Default.SetClusterNodesOnline(int64(len(m.peers)))
		m.mu.Unlock()
	}
}

// BroadcastPeerLeave notifies all connected peers of graceful departure before shutting down.
func (m *ClusterMesh) BroadcastPeerLeave() {
	m.mu.RLock()
	peers := make([]net.Conn, 0, len(m.peers))
	for _, c := range m.peers {
		peers = append(peers, c)
	}
	m.mu.RUnlock()

	idBytes := []byte(m.nodeID)
	payloadLen := 2 + len(idBytes)
	frame := make([]byte, 5+payloadLen)
	frame[0] = MsgPeerLeave
	binary.BigEndian.PutUint32(frame[1:5], uint32(payloadLen))
	binary.BigEndian.PutUint16(frame[5:7], uint16(len(idBytes)))
	copy(frame[7:], idBytes)

	for _, conn := range peers {
		_, _ = conn.Write(frame)
	}
	time.Sleep(30 * time.Millisecond) // Allow TCP buffer to flush
}

// sendPeerList sends known cluster directory to a newly joined node.
func (m *ClusterMesh) sendPeerList(conn net.Conn, peers []NodeInfo) error {
	payloadLen := 2
	for _, p := range peers {
		payloadLen += 2 + len(p.ID) + 2 + len(p.RPCAddr)
	}

	frame := make([]byte, 5+payloadLen)
	frame[0] = MsgPeerList
	binary.BigEndian.PutUint32(frame[1:5], uint32(payloadLen))
	binary.BigEndian.PutUint16(frame[5:7], uint16(len(peers)))

	offset := 7
	for _, p := range peers {
		binary.BigEndian.PutUint16(frame[offset:offset+2], uint16(len(p.ID)))
		offset += 2
		copy(frame[offset:], p.ID)
		offset += len(p.ID)

		binary.BigEndian.PutUint16(frame[offset:offset+2], uint16(len(p.RPCAddr)))
		offset += 2
		copy(frame[offset:], p.RPCAddr)
		offset += len(p.RPCAddr)
	}

	_, err := conn.Write(frame)
	return err
}

// broadcastPeerJoin gossips a new node's existence to all existing peers.
func (m *ClusterMesh) broadcastPeerJoin(newNodeID, newAddr string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idBytes := []byte(newNodeID)
	addrBytes := []byte(newAddr)
	payloadLen := 2 + len(idBytes) + 2 + len(addrBytes)

	frame := make([]byte, 5+payloadLen)
	frame[0] = MsgPeerJoin
	binary.BigEndian.PutUint32(frame[1:5], uint32(payloadLen))
	binary.BigEndian.PutUint16(frame[5:7], uint16(len(idBytes)))
	copy(frame[7:7+len(idBytes)], idBytes)

	offset := 7 + len(idBytes)
	binary.BigEndian.PutUint16(frame[offset:offset+2], uint16(len(addrBytes)))
	copy(frame[offset+2:], addrBytes)

	for pID, conn := range m.peers {
		if pID != newNodeID {
			_, _ = conn.Write(frame)
		}
	}
}

// SendRouteSnapshot synchronizes all current local topics to a remote node.
func (m *ClusterMesh) SendRouteSnapshot(targetNodeID string, topics []string) error {
	m.mu.RLock()
	conn, ok := m.peers[targetNodeID]
	m.mu.RUnlock()
	if !ok {
		return ErrPeerNotFound
	}

	payloadLen := 2
	for _, t := range topics {
		payloadLen += 2 + len(t)
	}

	buf := make([]byte, 5+payloadLen)
	buf[0] = MsgRouteSnapshot
	binary.BigEndian.PutUint32(buf[1:5], uint32(payloadLen))
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(topics)))

	offset := 7
	for _, t := range topics {
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(t)))
		offset += 2
		copy(buf[offset:], t)
		offset += len(t)
	}

	_, err := conn.Write(buf)
	return err
}

// ForwardPublish satisfies cluster.ClusterRPC.
func (m *ClusterMesh) ForwardPublish(targetNodeID string, topic string, qos byte, payload []byte) error {
	m.mu.RLock()
	conn, ok := m.peers[targetNodeID]
	m.mu.RUnlock()
	if !ok {
		return ErrPeerNotFound
	}

	topicBytes := []byte(topic)
	payloadLen := 1 + 2 + len(topicBytes) + len(payload)
	frame := make([]byte, 5+payloadLen)
	frame[0] = MsgForwardPub
	binary.BigEndian.PutUint32(frame[1:5], uint32(payloadLen))

	frame[5] = qos
	binary.BigEndian.PutUint16(frame[6:8], uint16(len(topicBytes)))
	copy(frame[8:8+len(topicBytes)], topicBytes)
	copy(frame[8+len(topicBytes):], payload)

	_, err := conn.Write(frame)
	if err == nil {
		metrics.Default.IncClusterRoutedMsgs(1)
	}
	return err
}

// BroadcastRoute satisfies cluster.ClusterRPC.
func (m *ClusterMesh) BroadcastRoute(nodeID string, topic string, action byte) error {
	if action == 1 {
		m.router.AddLocalTopic(topic)
	} else if action == 2 {
		m.router.RemoveLocalTopic(topic)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var msgType byte
	if action == 1 {
		msgType = MsgRouteAdd
	} else {
		msgType = MsgRouteDel
	}

	topicBytes := []byte(topic)
	frame := make([]byte, 5+len(topicBytes))
	frame[0] = msgType
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(topicBytes)))
	copy(frame[5:], topicBytes)

	for _, conn := range m.peers {
		_, _ = conn.Write(frame)
	}
	return nil
}

func (m *ClusterMesh) Close() error {
	m.closeOnce.Do(func() {
		m.BroadcastPeerLeave()
		close(m.stopCh)
		if m.listener != nil {
			_ = m.listener.Close()
		}
		m.mu.Lock()
		for _, conn := range m.peers {
			_ = conn.Close()
		}
		m.mu.Unlock()
	})
	return nil
}

// Handshake encoding & decoding helpers
func encodeHandshake(nodeID, listenAddr string) []byte {
	idBytes := []byte(nodeID)
	addrBytes := []byte(listenAddr)
	buf := make([]byte, 2+len(idBytes)+2+len(addrBytes))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(idBytes)))
	copy(buf[2:2+len(idBytes)], idBytes)
	offset := 2 + len(idBytes)
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(addrBytes)))
	copy(buf[offset+2:], addrBytes)
	return buf
}

func decodeHandshake(r io.Reader) (nodeID, listenAddr string, err error) {
	var lenBuf [2]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return "", "", err
	}
	idLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	idBuf := make([]byte, idLen)
	if _, err = io.ReadFull(r, idBuf); err != nil {
		return "", "", err
	}
	nodeID = string(idBuf)

	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return nodeID, "", nil // Graceful fallback if address len not sent
	}
	addrLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	if addrLen > 0 {
		addrBuf := make([]byte, addrLen)
		if _, err = io.ReadFull(r, addrBuf); err != nil {
			return nodeID, "", err
		}
		listenAddr = string(addrBuf)
	}
	return nodeID, listenAddr, nil
}

func decodePeerTuple(payload []byte) (nodeID, addr string, err error) {
	pID, pAddr, _, err := decodePeerTupleWithOffset(payload)
	return pID, pAddr, err
}

func decodePeerTupleWithOffset(buf []byte) (nodeID, addr string, bytesRead int, err error) {
	if len(buf) < 4 {
		return "", "", 0, io.ErrUnexpectedEOF
	}
	idLen := int(binary.BigEndian.Uint16(buf[:2]))
	if len(buf) < 2+idLen+2 {
		return "", "", 0, io.ErrUnexpectedEOF
	}
	nodeID = string(buf[2 : 2+idLen])
	offset := 2 + idLen

	addrLen := int(binary.BigEndian.Uint16(buf[offset : offset+2]))
	offset += 2
	if len(buf) < offset+addrLen {
		return "", "", 0, io.ErrUnexpectedEOF
	}
	addr = string(buf[offset : offset+addrLen])
	offset += addrLen

	return nodeID, addr, offset, nil
}
