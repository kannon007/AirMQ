package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/quic-go/quic-go"

	"mqtt/pkg/protocol"
)

var quicBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

// QUICConfig defines the configuration for the MQTT over QUIC UDP listener.
type QUICConfig struct {
	Addr            string        // UDP address to listen on (e.g. ":14567" or "0.0.0.0:14567")
	CertFile        string        // Path to TLS 1.3 certificate file (PEM)
	KeyFile         string        // Path to TLS 1.3 private key file (PEM)
	MaxIdleTimeout  time.Duration // QUIC connection idle timeout (default: 60s)
	KeepAlivePeriod time.Duration // QUIC ping keepalive period (default: 15s)
	TLSConfig       *tls.Config   // Optional pre-configured TLS config (takes precedence over CertFile/KeyFile)
}

// QUICListener implements TransportListener for MQTT over QUIC (RFC 9000 over UDP).
type QUICListener struct {
	cfg        QUICConfig
	tlsConfig  *tls.Config
	quicConfig *quic.Config
	listener   *quic.Listener
	dispatcher PacketDispatcher
	stopCh     chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

// NewQUICListener creates a new QUIC UDP transport listener.
func NewQUICListener(cfg QUICConfig) (*QUICListener, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":14567"
	}
	if cfg.MaxIdleTimeout <= 0 {
		cfg.MaxIdleTimeout = 60 * time.Second
	}
	if cfg.KeepAlivePeriod <= 0 {
		cfg.KeepAlivePeriod = 15 * time.Second
	}

	var tlsCfg *tls.Config
	if cfg.TLSConfig != nil {
		tlsCfg = cfg.TLSConfig.Clone()
	} else if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("quic: failed to load TLS certificate: %w", err)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	} else {
		// Ephemeral in-memory TLS 1.3 certificate for testing / dev mode
		cert, err := generateSelfSignedQUICCert()
		if err != nil {
			return nil, fmt.Errorf("quic: failed to generate ephemeral TLS cert: %w", err)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	tlsCfg.MinVersion = tls.VersionTLS13
	// Support both standard MQTT over QUIC and draft ALPNs (e.g. EMQX 5.0 compatibility)
	tlsCfg.NextProtos = []string{"mqtt", "hq-29"}

	quicCfg := &quic.Config{
		MaxIdleTimeout:  cfg.MaxIdleTimeout,
		KeepAlivePeriod: cfg.KeepAlivePeriod,
		Allow0RTT:       true,
	}

	return &QUICListener{
		cfg:        cfg,
		tlsConfig:  tlsCfg,
		quicConfig: quicCfg,
		stopCh:     make(chan struct{}),
	}, nil
}

// Name returns the driver identifier.
func (ql *QUICListener) Name() string {
	return "quic"
}

// Protocol returns "udp".
func (ql *QUICListener) Protocol() string {
	return "udp"
}

// Addr returns the active listening address.
func (ql *QUICListener) Addr() net.Addr {
	if ql.listener != nil {
		return ql.listener.Addr()
	}
	return nil
}

// Start boots the UDP socket and starts accepting QUIC connections.
func (ql *QUICListener) Start(dispatcher PacketDispatcher) error {
	ql.dispatcher = dispatcher

	ln, err := quic.ListenAddr(ql.cfg.Addr, ql.tlsConfig, ql.quicConfig)
	if err != nil {
		return fmt.Errorf("quic: failed to listen on UDP %s: %w", ql.cfg.Addr, err)
	}
	ql.listener = ln

	ql.wg.Add(1)
	go ql.acceptLoop()

	log.Printf("[QUIC] MQTT over QUIC (UDP) listening on %s (ALPN: %v)", ql.listener.Addr(), ql.tlsConfig.NextProtos)
	return nil
}

// Stop gracefully terminates the listener and active sessions.
func (ql *QUICListener) Stop(ctx context.Context) error {
	ql.closeOnce.Do(func() {
		close(ql.stopCh)
		if ql.listener != nil {
			_ = ql.listener.Close()
		}
	})

	done := make(chan struct{})
	go func() {
		ql.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (ql *QUICListener) acceptLoop() {
	defer ql.wg.Done()

	for {
		sess, err := ql.listener.Accept(context.Background())
		if err != nil {
			select {
			case <-ql.stopCh:
				return
			default:
				log.Printf("[QUIC] Accept error: %v", err)
				return
			}
		}

		ql.wg.Add(1)
		go func(s *quic.Conn) {
			defer ql.wg.Done()
			ql.handleConnection(s)
		}(sess)
	}
}

func (ql *QUICListener) handleConnection(sess *quic.Conn) {
	// Loop to accept streams.
	// In Single-Stream mode: first bidirectional stream carries all MQTT packets.
	// In Multi-Stream mode: additional streams (e.g. data streams) share the same connection context.
	var connCtx *ConnContext
	var mainConn *quicClientConn

	for {
		stream, err := sess.AcceptStream(context.Background())
		if err != nil {
			// Connection closed or timed out
			if mainConn != nil && connCtx != nil {
				ql.dispatcher.OnConnClosed(connCtx, mainConn.RemoteAddr().String(), err)
				_ = mainConn.Close()
			}
			return
		}

		if connCtx == nil {
			// Stream 0: Initial Control Stream
			connCtx = NewConnContext()
			connCtx.Transport = "quic"
			mainConn = NewQUICClientConn(sess, stream)
			ql.wg.Add(1)
			go func(c *quicClientConn, ctx *ConnContext) {
				defer ql.wg.Done()
				ql.handleStream(c, ctx)
			}(mainConn, connCtx)
		} else {
			// Stream 1..N: Data Streams sharing existing session context
			streamConn := NewQUICClientConn(sess, stream)
			ql.wg.Add(1)
			go func(c *quicClientConn, ctx *ConnContext) {
				defer ql.wg.Done()
				ql.handleStream(c, ctx)
			}(streamConn, connCtx)
		}
	}
}

func (ql *QUICListener) handleStream(clientConn *quicClientConn, ctx *ConnContext) {
	defer func() {
		_ = clientConn.Stream().Close()
	}()

	bufPtr := quicBufferPool.Get().(*[]byte)
	defer quicBufferPool.Put(bufPtr)

	rawBuf := *bufPtr
	var readBuf []byte

	for {
		select {
		case <-ql.stopCh:
			return
		case <-clientConn.Context().Done():
			return
		default:
		}

		if ctx.KeepAlive > 0 {
			timeout := (ctx.KeepAlive * 3) / 2
			_ = clientConn.Stream().SetReadDeadline(time.Now().Add(timeout))
		}

		n, err := clientConn.Stream().Read(rawBuf)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				ql.dispatcher.OnConnClosed(ctx, clientConn.RemoteAddr().String(), err)
			}
			return
		}
		if n == 0 {
			continue
		}

		ctx.LastActive = time.Now()
		readBuf = append(readBuf, rawBuf[:n]...)

		offset := 0
		for offset < len(readBuf) {
			pkt, consumed, err := protocol.DecodePacket(readBuf[offset:], ctx.ProtocolLevel)
			if err != nil {
				log.Printf("[QUIC] Packet decode error from %s: %v", clientConn.RemoteAddr(), err)
				_ = clientConn.Close()
				return
			}
			if consumed == 0 || pkt == nil {
				break
			}
			offset += consumed

			action := ql.dispatcher.ProcessPacket(clientConn, ctx, pkt)
			if action == gnet.Close {
				_ = clientConn.Close()
				return
			}
		}

		if offset > 0 {
			readBuf = readBuf[offset:]
		}
	}
}

// generateSelfSignedQUICCert creates a local in-memory self-signed certificate for dev and tests.
func generateSelfSignedQUICCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "localhost",
			Organization: []string{"MQTT Broker QUIC"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}, nil
}
