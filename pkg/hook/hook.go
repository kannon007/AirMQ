package hook

import (
	"context"

	"mqtt/pkg/protocol"
)

type AuthAction byte

const (
	AuthActionPublish   AuthAction = 1
	AuthActionSubscribe AuthAction = 2
)

// ClientContext holds per-client metadata accessible inside hook callbacks.
type ClientContext struct {
	context.Context
	ClientID   string
	Username   string
	RemoteAddr string
	Attrs      map[string]any
}

func NewClientContext(clientID, username, remoteAddr string) *ClientContext {
	return &ClientContext{
		Context:    context.Background(),
		ClientID:   clientID,
		Username:   username,
		RemoteAddr: remoteAddr,
		Attrs:      make(map[string]any),
	}
}

// Hook defines the pluggable lifecycle interface marker.
// Plugins implement only the sub-interfaces they care about (ConnectHook, AuthorizeHook, PublishHook, etc.).
type Hook interface {
	Name() string
}

// ConnectHook handles client authentication and connection lifecycle.
type ConnectHook interface {
	OnConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error)
}

// AuthorizeHook performs granular ACL checks on topic subscribe and publish actions.
type AuthorizeHook interface {
	OnAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error)
}

// PublishHook inspects, mutates, or filters incoming publish messages.
type PublishHook interface {
	OnPublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error)
}

// DeliveredHook is notified after a publish packet has been dispatched to a subscriber.
type DeliveredHook interface {
	OnDelivered(ctx *ClientContext, pkt *protocol.PublishPacket)
}

// DisconnectHook handles client disconnection events.
type DisconnectHook interface {
	OnDisconnect(ctx *ClientContext, err error)
}

// FullHook aggregates all hook interfaces for backward compatibility or monolithic plugins.
type FullHook interface {
	Hook
	ConnectHook
	AuthorizeHook
	PublishHook
	DeliveredHook
	DisconnectHook
}

// BaseHook provides default no-op implementation of Name for convenient embedding.
type BaseHook struct{}

func (b *BaseHook) Name() string { return "BaseHook" }

// NoopHook provides no-op implementations of all hook methods if a monolithic stub is needed.
type NoopHook struct {
	BaseHook
}

func (n *NoopHook) OnConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error) {
	return true, 0, nil
}
func (n *NoopHook) OnAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error) {
	return true, nil
}
func (n *NoopHook) OnPublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error) {
	return false, nil // don't drop
}
func (n *NoopHook) OnDelivered(ctx *ClientContext, pkt *protocol.PublishPacket) {}
func (n *NoopHook) OnDisconnect(ctx *ClientContext, err error)                  {}
