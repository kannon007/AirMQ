package dashboard

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// BrokerInterface defines the read/management contract implemented by the MQTT broker.
// Both core.Broker and server.Server satisfy this interface.
type BrokerInterface interface {
	GetOverview() OverviewStats
	GetClients(page, limit int, query string) ([]ClientSummary, int)
	GetClientDetail(clientID string) (*ClientDetail, bool)
	KickClient(clientID string) error
	GetSubscriptions(page, limit int, query string) ([]SubscriptionSummary, int)
	UnsubscribeClient(clientID, topic string) error
	GetRetainedMessages() ([]RetainedSummary, error)
	DeleteRetainedMessage(topic string) error
	PublishMessage(topic string, qos byte, retain bool, payload []byte) error
	GetListeners() []ListenerSummary
	GetClusterNodes() ClusterSummary
	GetPipelineStatus() *PipelineSummary
	GetRules() []RuleStatusDTO
	GetRule(id string) (*RuleStatusDTO, error)
	UpdateRule(rule RuleDTO) error
	DeleteRule(id string) error
	PingRule(ctx context.Context, id string) (*RulePingResultDTO, error)
	TestMatchTopic(topic string) []string
	GetBridges() []BridgeStatusDTO
	GetBridge(id string) (*BridgeStatusDTO, error)
	UpdateBridge(bridge BridgeDTO) error
	DeleteBridge(id string) error
	PingBridge(ctx context.Context, id string) (*RulePingResultDTO, error)
	GetRegisteredDrivers() []string
}

// Config defines Dashboard server configuration options.
type Config struct {
	Addr     string
	Username string
	Password string
	WebDir   string // Optional local directory for web hot-reloading
}

// Server serves the management REST API and the embedded web dashboard.
type Server struct {
	cfg        Config
	broker     BrokerInterface
	auth       *AuthManager
	httpServer *http.Server
	listener   net.Listener
	mu         sync.Mutex
	running    bool
}

// NewServer initializes a new Dashboard Server.
func NewServer(cfg Config, broker BrokerInterface) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":18083"
	}
	auth := NewAuthManager(cfg.Username, cfg.Password)
	return &Server{
		cfg:    cfg,
		broker: broker,
		auth:   auth,
	}
}

// Start launches the Dashboard HTTP server.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return errors.New("dashboard: server already running")
	}

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("dashboard: failed to listen on %s: %w", s.cfg.Addr, err)
	}
	s.listener = ln

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Handler:      s.corsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	s.running = true

	log.Printf("[Dashboard] REST API & Web Console listening on http://%s (user: %s)", s.cfg.Addr, s.cfg.Username)

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[Dashboard] HTTP server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the Dashboard HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running || s.httpServer == nil {
		return nil
	}

	s.running = false
	return s.httpServer.Shutdown(ctx)
}

// Addr returns the actual bound network address.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
