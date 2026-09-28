package server

import (
	"net"
	"time"

	"github.com/panjf2000/gnet/v2"

	"mqtt/pkg/protocol"
	"mqtt/pkg/session"
)

// ClientConn defines the common connection interface across TCP (gnet), TLS (crypto/tls), and WebSocket.
type ClientConn interface {
	Write(b []byte) (int, error)
	Close() error
	RemoteAddr() net.Addr
}

// clientEntry bundles an active ClientConn with its per-connection context.
type clientEntry struct {
	conn ClientConn
	ctx  *ConnContext
}

// gnetClientConn wraps a gnet.Conn to satisfy the ClientConn interface thread-safely.
type gnetClientConn struct {
	c gnet.Conn
}

// NewGnetClientConn creates a thread-safe ClientConn from a gnet.Conn.
func NewGnetClientConn(c gnet.Conn) ClientConn {
	return &gnetClientConn{c: c}
}

func (g *gnetClientConn) Write(b []byte) (int, error) {
	err := g.c.AsyncWrite(b, nil)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (g *gnetClientConn) Close() error {
	return g.c.Close()
}

func (g *gnetClientConn) RemoteAddr() net.Addr {
	return g.c.RemoteAddr()
}

func (g *gnetClientConn) RawConn() gnet.Conn {
	return g.c
}

// ConnContext stores per-connection metadata on active client connections.
type ConnContext struct {
	ClientID             string
	Username             string
	Authed               bool
	KeepAlive            time.Duration
	LastActive           time.Time
	Session              *session.Session
	ProtocolLevel        byte // V311 (4) or V50 (5)
	WillTopic            string
	WillMessage          []byte
	WillQoS              byte
	WillRetain           bool
	WillProperties       *protocol.Properties
	CleanDisconnect      bool
	AssignedClientID     bool
	SessionExpirySeconds uint32
	IsTLS                bool
	ClientCertCN         string // Extracted from client certificate in mTLS
	Transport            string // "tcp", "tls", or "ws"
}

func NewConnContext() *ConnContext {
	return &ConnContext{
		LastActive:    time.Now(),
		ProtocolLevel: protocol.V311,
		Transport:     "tcp",
	}
}
