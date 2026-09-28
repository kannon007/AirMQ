package store

import (
	"mqtt/pkg/protocol"
)

// MessageStore defines the storage interface for retained and offline messages.
type MessageStore interface {
	// Retained messages
	SetRetained(topic string, msg *protocol.PublishPacket) error
	GetRetained(topic string) (*protocol.PublishPacket, error)
	DeleteRetained(topic string) error
	GetAllRetained() ([]*protocol.PublishPacket, error)

	// Offline messages for persistent sessions
	StoreOffline(clientID string, msg *protocol.PublishPacket) error
	FetchOffline(clientID string) ([]*protocol.PublishPacket, error)
	ClearOffline(clientID string) error

	Close() error
}

// StoredMessage is the persisted representation of a Publish packet.
type StoredMessage struct {
	Topic      string               `json:"topic"`
	Payload    []byte               `json:"payload"`
	QoS        byte                 `json:"qos"`
	Retain     bool                 `json:"retain"`
	PacketID   uint16               `json:"packet_id"`
	Properties *protocol.Properties `json:"properties,omitempty"`
}
