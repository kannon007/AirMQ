package server

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"
)

// quicClientConn wraps a QUIC Connection and its Stream to satisfy the ClientConn interface.
// Supports dynamic Connection Migration: RemoteAddr() dynamically queries the active UDP path.
type quicClientConn struct {
	sess   *quic.Conn
	stream *quic.Stream
	mu     sync.Mutex
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
}

// NewQUICClientConn creates a thread-safe ClientConn from a QUIC connection and stream.
func NewQUICClientConn(sess *quic.Conn, stream *quic.Stream) *quicClientConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &quicClientConn{
		sess:   sess,
		stream: stream,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (q *quicClientConn) Write(b []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed.Load() {
		return 0, net.ErrClosed
	}
	return q.stream.Write(b)
}

func (q *quicClientConn) WriteAndClose(b []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed.Load() {
		return net.ErrClosed
	}
	_, _ = q.stream.Write(b)
	q.cancel()
	q.closed.Store(true)
	return q.stream.Close()
}

// Close terminates the stream and signals cancellation to reading goroutines.
func (q *quicClientConn) Close() error {
	if q.closed.CompareAndSwap(false, true) {
		q.cancel()
		_ = q.stream.Close()
		return q.sess.CloseWithError(0, "client disconnected")
	}
	return nil
}

// RemoteAddr returns the current active UDP endpoint of the QUIC client.
// Supports QUIC Connection Migration: when client switches between 4G/5G/Wi-Fi,
// this immediately returns the newly validated UDP address.
func (q *quicClientConn) RemoteAddr() net.Addr {
	return q.sess.RemoteAddr()
}

// Stream returns the underlying QUIC bidirectional stream.
func (q *quicClientConn) Stream() *quic.Stream {
	return q.stream
}

// Connection returns the underlying QUIC session.
func (q *quicClientConn) Connection() *quic.Conn {
	return q.sess
}

// Context returns the connection cancellation context.
func (q *quicClientConn) Context() context.Context {
	return q.ctx
}
