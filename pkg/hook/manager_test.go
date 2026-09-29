package hook

import (
	"errors"
	"testing"

	"mqtt/pkg/protocol"
)

type dummyConnectHook struct {
	BaseHook
	connectCalled bool
	allowConnect  bool
}

func (d *dummyConnectHook) OnConnect(ctx *ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error) {
	d.connectCalled = true
	if !d.allowConnect {
		return false, 0x05, errors.New("unauthorized connect")
	}
	return true, 0, nil
}

type dummyAuthorizeHook struct {
	BaseHook
	authCalled bool
	allowAuth  bool
}

func (d *dummyAuthorizeHook) OnAuthorize(ctx *ClientContext, action AuthAction, topic string) (bool, error) {
	d.authCalled = true
	if !d.allowAuth {
		return false, nil
	}
	return true, nil
}

type dummyPublishHook struct {
	BaseHook
	publishCalled bool
	dropPublish   bool
}

func (d *dummyPublishHook) OnPublish(ctx *ClientContext, pkt *protocol.PublishPacket) (bool, error) {
	d.publishCalled = true
	return d.dropPublish, nil
}

type dummyDeliveredHook struct {
	BaseHook
	deliveredCalled bool
}

func (d *dummyDeliveredHook) OnDelivered(ctx *ClientContext, pkt *protocol.PublishPacket) {
	d.deliveredCalled = true
}

type dummyDisconnectHook struct {
	BaseHook
	disconnectCalled bool
}

func (d *dummyDisconnectHook) OnDisconnect(ctx *ClientContext, err error) {
	d.disconnectCalled = true
}

func TestManager_GranularHookRegistration(t *testing.T) {
	mgr := NewManager()

	if mgr.HasHooks() {
		t.Fatal("expected HasHooks to be false initially")
	}
	if mgr.HasPublishHooks() {
		t.Fatal("expected HasPublishHooks to be false initially")
	}
	if mgr.HasAuthorizeHooks() {
		t.Fatal("expected HasAuthorizeHooks to be false initially")
	}

	// 1. Register a connect-only hook
	connHook := &dummyConnectHook{allowConnect: true}
	mgr.Register(connHook)

	if !mgr.HasHooks() {
		t.Fatal("expected HasHooks to be true after Register")
	}
	if mgr.HasPublishHooks() {
		t.Fatal("expected HasPublishHooks to be false for connect-only hook")
	}
	if mgr.HasAuthorizeHooks() {
		t.Fatal("expected HasAuthorizeHooks to be false for connect-only hook")
	}

	// 2. Register an authorize hook
	authHook := &dummyAuthorizeHook{allowAuth: true}
	mgr.Register(authHook)

	if !mgr.HasAuthorizeHooks() {
		t.Fatal("expected HasAuthorizeHooks to be true after registering AuthorizeHook")
	}
	if mgr.HasPublishHooks() {
		t.Fatal("expected HasPublishHooks to still be false")
	}

	// 3. Register a publish hook
	pubHook := &dummyPublishHook{dropPublish: false}
	mgr.Register(pubHook)

	if !mgr.HasPublishHooks() {
		t.Fatal("expected HasPublishHooks to be true after registering PublishHook")
	}
}

func TestManager_FireMethods(t *testing.T) {
	mgr := NewManager()

	connHook := &dummyConnectHook{allowConnect: true}
	authHook := &dummyAuthorizeHook{allowAuth: true}
	pubHook := &dummyPublishHook{dropPublish: false}
	delivHook := &dummyDeliveredHook{}
	discHook := &dummyDisconnectHook{}

	mgr.Register(connHook)
	mgr.Register(authHook)
	mgr.Register(pubHook)
	mgr.Register(delivHook)
	mgr.Register(discHook)

	clientCtx := NewClientContext("client-1", "user-1", "127.0.0.1:5000")

	// Test FireConnect
	allow, code, err := mgr.FireConnect(clientCtx, &protocol.ConnectPacket{ClientID: "client-1"})
	if !allow || code != 0 || err != nil || !connHook.connectCalled {
		t.Fatalf("FireConnect failed: allow=%v, code=%d, err=%v", allow, code, err)
	}

	// Test FireAuthorize
	allowAuth, err := mgr.FireAuthorize(clientCtx, AuthActionSubscribe, "sensor/data")
	if !allowAuth || err != nil || !authHook.authCalled {
		t.Fatalf("FireAuthorize failed: allow=%v, err=%v", allowAuth, err)
	}

	// Test FirePublish
	drop, err := mgr.FirePublish(clientCtx, &protocol.PublishPacket{Topic: "sensor/data"})
	if drop || err != nil || !pubHook.publishCalled {
		t.Fatalf("FirePublish failed: drop=%v, err=%v", drop, err)
	}

	// Test FireDelivered
	mgr.FireDelivered(clientCtx, &protocol.PublishPacket{Topic: "sensor/data"})
	if !delivHook.deliveredCalled {
		t.Fatal("FireDelivered was not called")
	}

	// Test FireDisconnect
	mgr.FireDisconnect(clientCtx, nil)
	if !discHook.disconnectCalled {
		t.Fatal("FireDisconnect was not called")
	}
}

func TestManager_Rejection(t *testing.T) {
	mgr := NewManager()

	connHook := &dummyConnectHook{allowConnect: false}
	authHook := &dummyAuthorizeHook{allowAuth: false}
	pubHook := &dummyPublishHook{dropPublish: true}

	mgr.Register(connHook)
	mgr.Register(authHook)
	mgr.Register(pubHook)

	clientCtx := NewClientContext("c1", "u1", "127.0.0.1:5000")

	allowConn, code, err := mgr.FireConnect(clientCtx, &protocol.ConnectPacket{})
	if allowConn || code != 0x05 || err == nil {
		t.Fatalf("expected connect rejection, got allow=%v, code=%d, err=%v", allowConn, code, err)
	}

	allowAuth, err := mgr.FireAuthorize(clientCtx, AuthActionPublish, "topic")
	if allowAuth || err != nil {
		t.Fatalf("expected auth rejection, got allow=%v, err=%v", allowAuth, err)
	}

	drop, err := mgr.FirePublish(clientCtx, &protocol.PublishPacket{})
	if !drop || err != nil {
		t.Fatalf("expected publish drop, got drop=%v, err=%v", drop, err)
	}
}
