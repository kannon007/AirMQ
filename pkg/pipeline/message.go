package pipeline

import (
	"sync"
	"time"
)

// Message is the data envelope traveling through the pipeline handlers.
// It encapsulates MQTT message attributes, wire payload, and custom handler attributes.
type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	QoS       byte
	Retain    bool
	Timestamp time.Time
	ClientID  string
	Username  string
	Headers   map[string]string
	Attrs     map[string]any
}

var messagePool = sync.Pool{
	New: func() any {
		return &Message{
			Headers: make(map[string]string, 4),
			Attrs:   make(map[string]any, 4),
		}
	},
}

// AcquireMessage fetches a clean pre-allocated Message from the pool.
func AcquireMessage() *Message {
	m := messagePool.Get().(*Message)
	m.ID = ""
	m.Topic = ""
	m.Payload = nil
	m.QoS = 0
	m.Retain = false
	m.Timestamp = time.Now()
	m.ClientID = ""
	m.Username = ""
	clear(m.Headers)
	clear(m.Attrs)
	return m
}

// ReleaseMessage returns a Message to the pool.
func ReleaseMessage(m *Message) {
	if m != nil {
		m.Payload = nil
		messagePool.Put(m)
	}
}

// SetPayload updates the active payload slice.
func (m *Message) SetPayload(p []byte) {
	m.Payload = p
}

// SetTopic updates the message destination topic.
func (m *Message) SetTopic(topic string) {
	m.Topic = topic
}

// SetHeader sets a metadata header.
func (m *Message) SetHeader(k, v string) {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[k] = v
}

// GetHeader retrieves a metadata header.
func (m *Message) GetHeader(k string) (string, bool) {
	if m.Headers == nil {
		return "", false
	}
	v, ok := m.Headers[k]
	return v, ok
}

// SetAttr attaches custom handler attributes to the message.
func (m *Message) SetAttr(k string, v any) {
	if m.Attrs == nil {
		m.Attrs = make(map[string]any)
	}
	m.Attrs[k] = v
}

// GetAttr retrieves custom handler attributes.
func (m *Message) GetAttr(k string) (any, bool) {
	if m.Attrs == nil {
		return nil, false
	}
	v, ok := m.Attrs[k]
	return v, ok
}
