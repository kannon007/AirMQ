package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"mqtt/pkg/protocol"
)

// TLSConfig defines the TLS server parameters.
type TLSConfig struct {
	Addr              string // e.g. ":8883"
	CertFile          string // Path to broker cert.pem
	KeyFile           string // Path to broker key.pem
	ClientCAFile      string // Path to client CA cert.pem for mTLS (optional)
	RequireClientCert bool   // Whether to require and verify client cert
}

// tlsClientConn wraps a net.Conn (such as *tls.Conn) to implement ClientConn thread-safely.
type tlsClientConn struct {
	conn net.Conn
	mu   sync.Mutex
}

func (t *tlsClientConn) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conn.Write(b)
}

func (t *tlsClientConn) Close() error {
	return t.conn.Close()
}

func (t *tlsClientConn) RemoteAddr() net.Addr {
	return t.conn.RemoteAddr()
}

// TLSServer manages a running TLS listener.
type TLSServer struct {
	server   *Server
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// StartTLS initializes and starts the TLS listener using file paths.
func (s *Server) StartTLS(cfg TLSConfig) (*TLSServer, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS key pair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"mqtt"},
	}

	if cfg.ClientCAFile != "" {
		caData, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse client CA certificates")
		}
		tlsCfg.ClientCAs = caPool
		if cfg.RequireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}

	return s.StartTLSWithConfig(cfg.Addr, tlsCfg)
}

// StartTLSWithConfig initializes and starts the TLS listener with a pre-configured *tls.Config.
func (s *Server) StartTLSWithConfig(addr string, tlsCfg *tls.Config) (*TLSServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on TLS addr %s: %w", addr, err)
	}

	tlsLn := tls.NewListener(ln, tlsCfg)
	ts := &TLSServer{
		server:   s,
		listener: tlsLn,
		stopCh:   make(chan struct{}),
	}

	s.tlsServer = ts

	ts.wg.Add(1)
	go ts.serve()

	return ts, nil
}

// Stop gracefully shuts down the TLS listener and waits for active connections.
func (ts *TLSServer) Stop() error {
	select {
	case <-ts.stopCh:
		return nil
	default:
		close(ts.stopCh)
	}
	err := ts.listener.Close()
	ts.wg.Wait()
	return err
}

// Addr returns the actual listening address (useful for ephemeral ports in tests).
func (ts *TLSServer) Addr() net.Addr {
	return ts.listener.Addr()
}

func (ts *TLSServer) serve() {
	defer ts.wg.Done()
	for {
		conn, err := ts.listener.Accept()
		if err != nil {
			select {
			case <-ts.stopCh:
				return
			default:
				log.Printf("TLS accept error: %v", err)
				return
			}
		}

		ts.wg.Add(1)
		go func(rawConn net.Conn) {
			defer ts.wg.Done()
			ts.handleConn(rawConn)
		}(conn)
	}
}

func (ts *TLSServer) handleConn(rawConn net.Conn) {
	tlsConn, ok := rawConn.(*tls.Conn)
	if !ok {
		_ = rawConn.Close()
		return
	}

	// Perform TLS handshake
	if err := tlsConn.Handshake(); err != nil {
		log.Printf("TLS handshake failed from %s: %v", rawConn.RemoteAddr(), err)
		_ = rawConn.Close()
		return
	}

	ctx := NewConnContext()
	ctx.IsTLS = true
	ctx.Transport = "tls"
	ts.server.metrics.IncConnActive("tls")

	// Extract mTLS client certificate Subject Common Name if available
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) > 0 {
		ctx.ClientCertCN = state.PeerCertificates[0].Subject.CommonName
	}

	clientConn := &tlsClientConn{conn: rawConn}
	defer func() {
		ts.server.closeClient(ctx, rawConn.RemoteAddr().String(), nil)
		_ = clientConn.Close()
	}()

	buf := make([]byte, 64*1024)
	var readBuf []byte

	for {
		select {
		case <-ts.stopCh:
			return
		default:
		}

		if ctx.KeepAlive > 0 {
			timeout := (ctx.KeepAlive * 3) / 2
			_ = rawConn.SetReadDeadline(time.Now().Add(timeout))
		}

		n, err := rawConn.Read(buf)
		if err != nil {
			if err != io.EOF {
				ts.server.closeClient(ctx, rawConn.RemoteAddr().String(), err)
			}
			return
		}
		if n == 0 {
			continue
		}

		ctx.LastActive = time.Now()
		readBuf = append(readBuf, buf[:n]...)

		offset := 0
		for offset < len(readBuf) {
			pkt, consumed, err := protocol.DecodePacket(readBuf[offset:], ctx.ProtocolLevel)
			if err != nil {
				log.Printf("TLS client %s packet decode error: %v", ctx.ClientID, err)
				return
			}
			if consumed == 0 || pkt == nil {
				break
			}
			offset += consumed

			act := ts.server.handlePacket(clientConn, ctx, pkt)
			if act != 0 { // gnet.Close is 1
				return
			}
		}

		if offset > 0 {
			readBuf = readBuf[offset:]
		}
	}
}
