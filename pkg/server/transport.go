package server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/panjf2000/gnet/v2"

	"mqtt/pkg/protocol"
)

// PacketDispatcher defines the callback boundary from transport listeners into the broker core engine.
// Transports decode network frames and hand standard MQTT packets off to Dispatcher;
// the core engine executes routing, state machines, and streaming pipelines.
type PacketDispatcher interface {
	// ProcessPacket processes an incoming decoded MQTT packet from any transport layer.
	ProcessPacket(conn ClientConn, ctx *ConnContext, pkt protocol.Packet) gnet.Action
	// OnConnClosed notifies the core engine of connection termination.
	OnConnClosed(ctx *ConnContext, remoteAddr string, err error)
}

// TransportListener defines the lifecycle interface for all pluggable network transport drivers.
// Future network protocols (QUIC, CoAP, MQTT-SN, WebTransport, HTTP/3) only need to implement this interface.
type TransportListener interface {
	// Name returns the driver name (e.g. "tcp", "tls", "websocket", "quic").
	Name() string
	// Protocol returns the underlying transport protocol ("tcp" or "udp").
	Protocol() string
	// Addr returns the listening network address.
	Addr() net.Addr
	// Start launches the network listener in the background, delegating packets to dispatcher.
	Start(dispatcher PacketDispatcher) error
	// Stop gracefully shuts down the listener and disconnects active sessions.
	Stop(ctx context.Context) error
}

// ListenerManager coordinates the lifecycle of all registered transport listeners.
type ListenerManager struct {
	mu        sync.RWMutex
	listeners []TransportListener
}

// NewListenerManager creates an empty listener manager.
func NewListenerManager() *ListenerManager {
	return &ListenerManager{
		listeners: make([]TransportListener, 0, 4),
	}
}

// Register adds a transport listener to the manager.
func (m *ListenerManager) Register(l TransportListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, l)
}

// StartAll launches all registered transport listeners concurrently.
func (m *ListenerManager) StartAll(dispatcher PacketDispatcher) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, l := range m.listeners {
		if err := l.Start(dispatcher); err != nil {
			return fmt.Errorf("transport: failed to start listener %s (%s): %w", l.Name(), l.Addr(), err)
		}
	}
	return nil
}

// StopAll gracefully shuts down all registered transport listeners.
func (m *ListenerManager) StopAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, l := range m.listeners {
		if err := l.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Listeners returns a copy of all registered listeners.
func (m *ListenerManager) Listeners() []TransportListener {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]TransportListener, len(m.listeners))
	copy(res, m.listeners)
	return res
}
