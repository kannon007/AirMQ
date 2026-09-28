package store

import (
	"sync"

	"mqtt/pkg/protocol"
)

// MemoryStore is an ultra-fast in-memory implementation of MessageStore.
type MemoryStore struct {
	retainedMu sync.RWMutex
	retained   map[string]*protocol.PublishPacket

	offlineMu sync.RWMutex
	offline   map[string][]*protocol.PublishPacket
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		retained: make(map[string]*protocol.PublishPacket),
		offline:  make(map[string][]*protocol.PublishPacket),
	}
}

func (m *MemoryStore) SetRetained(topic string, msg *protocol.PublishPacket) error {
	m.retainedMu.Lock()
	m.retained[topic] = msg
	m.retainedMu.Unlock()
	return nil
}

func (m *MemoryStore) GetRetained(topic string) (*protocol.PublishPacket, error) {
	m.retainedMu.RLock()
	msg, ok := m.retained[topic]
	m.retainedMu.RUnlock()
	if !ok {
		return nil, nil
	}
	return msg, nil
}

func (m *MemoryStore) DeleteRetained(topic string) error {
	m.retainedMu.Lock()
	delete(m.retained, topic)
	m.retainedMu.Unlock()
	return nil
}

func (m *MemoryStore) GetAllRetained() ([]*protocol.PublishPacket, error) {
	m.retainedMu.RLock()
	defer m.retainedMu.RUnlock()

	res := make([]*protocol.PublishPacket, 0, len(m.retained))
	for _, msg := range m.retained {
		res = append(res, msg)
	}
	return res, nil
}

func (m *MemoryStore) StoreOffline(clientID string, msg *protocol.PublishPacket) error {
	m.offlineMu.Lock()
	m.offline[clientID] = append(m.offline[clientID], msg)
	m.offlineMu.Unlock()
	return nil
}

func (m *MemoryStore) FetchOffline(clientID string) ([]*protocol.PublishPacket, error) {
	m.offlineMu.Lock()
	defer m.offlineMu.Unlock()
	msgs := m.offline[clientID]
	delete(m.offline, clientID)
	return msgs, nil
}

func (m *MemoryStore) ClearOffline(clientID string) error {
	m.offlineMu.Lock()
	delete(m.offline, clientID)
	m.offlineMu.Unlock()
	return nil
}

func (m *MemoryStore) Close() error {
	return nil
}
