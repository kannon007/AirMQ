package hook

import (
	"sync"
	"sync/atomic"

	"mqtt/pkg/protocol"
)

// Manager manages a list of registered hooks and executes them sequentially.
type Manager struct {
	mu           sync.RWMutex
	hooks        []Hook
	connectHooks []ConnectHook
	authHooks    []AuthorizeHook
	publishHooks []PublishHook
	delivHooks   []DeliveredHook
	discHooks    []DisconnectHook
	hasPublish   atomic.Bool
	hasAuthorize atomic.Bool
}

func NewManager() *Manager {
	return &Manager{
		hooks:        make([]Hook, 0, 8),
		connectHooks: make([]ConnectHook, 0, 8),
		authHooks:    make([]AuthorizeHook, 0, 8),
		publishHooks: make([]PublishHook, 0, 8),
		delivHooks:   make([]DeliveredHook, 0, 8),
		discHooks:    make([]DisconnectHook, 0, 8),
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

// HasAuthorizeHooks returns true if any authorize hook is registered.
func (m *Manager) HasAuthorizeHooks() bool {
	return m.hasAuthorize.Load()
}

// Register appends a hook plugin to the chain, categorizing by implemented capabilities.
func (m *Manager) Register(h Hook) {
	if h == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks = append(m.hooks, h)

	if ch, ok := h.(ConnectHook); ok {
		m.connectHooks = append(m.connectHooks, ch)
	}
	if ah, ok := h.(AuthorizeHook); ok {
		m.authHooks = append(m.authHooks, ah)
		m.hasAuthorize.Store(true)
	}
	if ph, ok := h.(PublishHook); ok {
		m.publishHooks = append(m.publishHooks, ph)
		m.hasPublish.Store(true)
	}
	if dh, ok := h.(DeliveredHook); ok {
		m.delivHooks = append(m.delivHooks, dh)
	}
	if dch, ok := h.(DisconnectHook); ok {
		m.discHooks = append(m.discHooks, dch)
	}
}

// FireConnect runs all OnConnect hooks. If any hook rejects, connection is denied.
func (m *Manager) FireConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.connectHooks {
		allow, code, err := h.OnConnect(ctx, pkt)
		if err != nil || !allow {
			return false, code, err
		}
	}
	return true, 0, nil
}

// FireAuthorize runs all OnAuthorize hooks.
func (m *Manager) FireAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error) {
	if !m.hasAuthorize.Load() {
		return true, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.authHooks {
		allow, err := h.OnAuthorize(ctx, action, topic)
		if err != nil || !allow {
			return false, err
		}
	}
	return true, nil
}

// FirePublish runs all OnPublish hooks. If any hook returns drop=true, message is discarded.
func (m *Manager) FirePublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error) {
	if !m.hasPublish.Load() {
		return false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.publishHooks {
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

	for _, h := range m.delivHooks {
		h.OnDelivered(ctx, pkt)
	}
}

// FireDisconnect runs all OnDisconnect hooks.
func (m *Manager) FireDisconnect(ctx *ClientContext, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, h := range m.discHooks {
		h.OnDisconnect(ctx, err)
	}
}
