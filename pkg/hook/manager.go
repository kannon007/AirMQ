package hook

import (
	"sync"
	"sync/atomic"

	"mqtt/pkg/protocol"
)

// Manager manages a list of registered hooks and executes them sequentially.
type Manager struct {
	mu         sync.RWMutex
	hooks      []Hook
	hasPublish atomic.Bool
}

func NewManager() *Manager {
	return &Manager{
		hooks: make([]Hook, 0, 8),
	}
}

// HasHooks returns true if any hook is registered.
func (m *Manager) HasHooks() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.hooks) > 0
}

// HasPublishHooks returns true if any publish hook is registered.
func (m *Manager) HasPublishHooks() bool {
	return m.hasPublish.Load()
}

// Register appends a hook plugin to the chain.
func (m *Manager) Register(h Hook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks = append(m.hooks, h)
	m.hasPublish.Store(true)
}

// FireConnect runs all OnConnect hooks. If any hook rejects, connection is denied.
func (m *Manager) FireConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.hooks {
		allow, code, err := h.OnConnect(ctx, pkt)
		if err != nil || !allow {
			return false, code, err
		}
	}
	return true, 0, nil
}

// FireAuthorize runs all OnAuthorize hooks.
func (m *Manager) FireAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.hooks {
		allow, err := h.OnAuthorize(ctx, action, topic)
		if err != nil || !allow {
			return false, err
		}
	}
	return true, nil
}

// FirePublish runs all OnPublish hooks. If any hook returns drop=true, message is discarded.
func (m *Manager) FirePublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.hooks {
		drop, err := h.OnPublish(ctx, pkt)
		if err != nil || drop {
			return true, err
		}
	}
	return false, nil
}

// FireDelivered notifies hooks that a message was dispatched to a subscriber.
func (m *Manager) FireDelivered(ctx *ClientContext, pkt *protocol.PublishPacket) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.hooks {
		h.OnDelivered(ctx, pkt)
	}
}

// FireDisconnect runs all OnDisconnect hooks.
func (m *Manager) FireDisconnect(ctx *ClientContext, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.hooks {
		h.OnDisconnect(ctx, err)
	}
}
