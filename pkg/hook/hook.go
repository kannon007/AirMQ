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

// Hook defines the pluggable lifecycle interface.
// Plugins implement only the methods they care about.
type Hook interface {
	Name() string
	OnConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error)
	OnAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error)
	OnPublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error)
	OnDelivered(ctx *ClientContext, pkt *protocol.PublishPacket)
	OnDisconnect(ctx *ClientContext, err error)
}

// BaseHook provides default no-op implementations for convenient embedding.
type BaseHook struct{}

func (b *BaseHook) Name() string { return "BaseHook" }
func (b *BaseHook) OnConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error) {
	return true, 0, nil
}
func (b *BaseHook) OnAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error) {
	return true, nil
}
func (b *BaseHook) OnPublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error) {
	return false, nil // don't drop
}
func (b *BaseHook) OnDelivered(ctx *ClientContext, pkt *protocol.PublishPacket) {}
func (b *BaseHook) OnDisconnect(ctx *ClientContext, err error)                  {}
