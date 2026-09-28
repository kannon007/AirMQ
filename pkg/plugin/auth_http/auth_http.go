package auth_http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"mqtt/pkg/hook"
	"mqtt/pkg/protocol"
)

// HTTPAuthConfig configures the HTTP Auth plugin.
type HTTPAuthConfig struct {
	AuthURL       string
	ACLURL        string
	CacheTTL      time.Duration
	ClientTimeout time.Duration
}

type cacheEntry struct {
	allowed   bool
	expiresAt time.Time
}

// HTTPAuthHook implements HTTP authentication and authorization with an internal cache.
type HTTPAuthHook struct {
	hook.BaseHook
	cfg        HTTPAuthConfig
	httpClient *http.Client
	mu         sync.RWMutex
	cache      map[string]cacheEntry
}

func NewHTTPAuthHook(cfg HTTPAuthConfig) *HTTPAuthHook {
	if cfg.ClientTimeout == 0 {
		cfg.ClientTimeout = 2 * time.Second
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 5 * time.Minute
	}
	return &HTTPAuthHook{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: cfg.ClientTimeout,
		},
		cache: make(map[string]cacheEntry),
	}
}

func (h *HTTPAuthHook) Name() string {
	return "HTTPAuthHook"
}

// OnConnect authenticates a client against the HTTP auth service.
func (h *HTTPAuthHook) OnConnect(ctx *hook.ClientContext, pkt *protocol.ConnectPacket) (bool, byte, error) {
	if h.cfg.AuthURL == "" {
		return true, 0, nil
	}

	cacheKey := fmt.Sprintf("auth:%s:%s", pkt.ClientID, pkt.Username)
	if h.checkCache(cacheKey) {
		return true, 0, nil
	}

	reqBody, _ := json.Marshal(map[string]any{
		"client_id": pkt.ClientID,
		"username":  pkt.Username,
		"password":  string(pkt.Password),
		"peer_ip":   ctx.RemoteAddr,
	})

	resp, err := h.httpClient.Post(h.cfg.AuthURL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return false, 0x05, fmt.Errorf("http auth request failed: %w", err) // 0x05 = Not authorized
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		h.setCache(cacheKey, true)
		return true, 0, nil
	}

	return false, 0x04, nil // 0x04 = Bad user name or password
}

// OnAuthorize validates topic pub/sub permissions against the ACL HTTP service.
func (h *HTTPAuthHook) OnAuthorize(ctx *hook.ClientContext, action hook.AuthAction, topic string) (bool, error) {
	if h.cfg.ACLURL == "" {
		return true, nil
	}

	cacheKey := fmt.Sprintf("acl:%s:%d:%s", ctx.ClientID, action, topic)
	if h.checkCache(cacheKey) {
		return true, nil
	}

	reqBody, _ := json.Marshal(map[string]any{
		"client_id": ctx.ClientID,
		"username":  ctx.Username,
		"action":    action,
		"topic":     topic,
	})

	resp, err := h.httpClient.Post(h.cfg.ACLURL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	allowed := resp.StatusCode == http.StatusOK
	if allowed {
		h.setCache(cacheKey, true)
	}
	return allowed, nil
}

func (h *HTTPAuthHook) checkCache(key string) bool {
	h.mu.RLock()
	entry, ok := h.cache[key]
	h.mu.RUnlock()
	if ok && time.Now().Before(entry.expiresAt) {
		return entry.allowed
	}
	return false
}

func (h *HTTPAuthHook) setCache(key string, allowed bool) {
	h.mu.Lock()
	h.cache[key] = cacheEntry{
		allowed:   allowed,
		expiresAt: time.Now().Add(h.cfg.CacheTTL),
	}
	h.mu.Unlock()
}
