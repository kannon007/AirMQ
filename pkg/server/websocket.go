package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"mqtt/pkg/protocol"
)

// WSConfig defines the WebSocket listener parameters.
type WSConfig struct {
	Addr     string // e.g. ":8083"
	Path     string // e.g. "/mqtt" (defaults to "/mqtt")
	CertFile string // Optional, for WSS
	KeyFile  string // Optional, for WSS
}

// wsClientConn wraps a *websocket.Conn to implement ClientConn thread-safely.
type wsClientConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
	addr net.Addr
}

func (w *wsClientConn) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *wsClientConn) Close() error {
	return w.conn.Close()
}

func (w *wsClientConn) RemoteAddr() net.Addr {
	return w.addr
}

// WSServer represents a running WebSocket HTTP server.
type WSServer struct {
	server     *Server
	cfg        WSConfig
	httpServer *http.Server
	listener   net.Listener
	stopCh     chan struct{}
}

// StartWS starts an HTTP server serving MQTT over WebSocket.
func (s *Server) StartWS(cfg WSConfig) (*WSServer, error) {
	if cfg.Path == "" {
		cfg.Path = "/mqtt"
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on WS addr %s: %w", cfg.Addr, err)
	}

	upgrader := websocket.Upgrader{
		Subprotocols: []string{"mqtt", "mqttv3.1"},
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow cross-origin Web / App clients
		},
		ReadBufferSize:  64 * 1024,
		WriteBufferSize: 64 * 1024,
	}

	mux := http.NewServeMux()
	wsServer := &WSServer{
		server:   s,
		cfg:      cfg,
		listener: ln,
		stopCh:   make(chan struct{}),
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WS upgrade failed from %s: %v", r.RemoteAddr, err)
			return
		}
		wsServer.serveWSConn(wsConn)
	}

	mux.HandleFunc(cfg.Path, handler)
	if cfg.Path != "/" {
		mux.HandleFunc("/", handler)
	}
	mux.Handle("/metrics", s.metrics.Handler())

	httpSrv := &http.Server{
		Handler: mux,
	}
	wsServer.httpServer = httpSrv
	s.wsServer = wsServer

	go func() {
		var serveErr error
		if cfg.CertFile != "" && cfg.KeyFile != "" {
			serveErr = httpSrv.ServeTLS(ln, cfg.CertFile, cfg.KeyFile)
		} else {
			serveErr = httpSrv.Serve(ln)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("WS HTTP server error: %v", serveErr)
		}
	}()

	return wsServer, nil
}

// Addr returns the actual listening address (useful for ephemeral ports in tests).
func (ws *WSServer) Addr() net.Addr {
	return ws.listener.Addr()
}

// Stop gracefully shuts down the WebSocket server.
func (ws *WSServer) Stop(ctx context.Context) error {
	select {
	case <-ws.stopCh:
		return nil
	default:
		close(ws.stopCh)
	}
	return ws.httpServer.Shutdown(ctx)
}

func (ws *WSServer) serveWSConn(c *websocket.Conn) {
	ctx := NewConnContext()
	ctx.Transport = "ws"
	ws.server.metrics.IncConnActive("ws")
	clientConn := &wsClientConn{
		conn: c,
		addr: c.UnderlyingConn().RemoteAddr(),
	}

	defer func() {
		ws.server.closeClient(ctx, clientConn.RemoteAddr().String(), nil)
		_ = clientConn.Close()
	}()

	var readBuf []byte
	for {
		select {
		case <-ws.stopCh:
			return
		default:
		}

		if ctx.KeepAlive > 0 {
			timeout := (ctx.KeepAlive * 3) / 2
			_ = c.SetReadDeadline(time.Now().Add(timeout))
		}

		messageType, payload, err := c.ReadMessage()
		if err != nil {
			if err != io.EOF && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				ws.server.closeClient(ctx, clientConn.RemoteAddr().String(), err)
			}
			return
		}

		if messageType != websocket.BinaryMessage && messageType != websocket.TextMessage {
			continue
		}

		ctx.LastActive = time.Now()
		readBuf = append(readBuf, payload...)

		offset := 0
		for offset < len(readBuf) {
			pkt, consumed, err := protocol.DecodePacket(readBuf[offset:], ctx.ProtocolLevel)
			if err != nil {
				log.Printf("WS client %s packet decode error: %v", ctx.ClientID, err)
				return
			}
			if consumed == 0 || pkt == nil {
				break
			}
			offset += consumed

			act := ws.server.handlePacket(clientConn, ctx, pkt)
			if act != 0 {
				return
			}
		}

		if offset > 0 {
			readBuf = readBuf[offset:]
		}
	}
}
