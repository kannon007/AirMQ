package dashboard

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AuthManager handles admin credentials and session tokens.
type AuthManager struct {
	mu        sync.RWMutex
	username  string
	password  string
	secretKey []byte
	tokens    map[string]time.Time // token -> expiresAt
}

// NewAuthManager initializes an authentication manager.
func NewAuthManager(username, password string) *AuthManager {
	if username == "" {
		username = "admin"
	}
	if password == "" {
		password = "public"
	}

	secret := make([]byte, 32)
	_, _ = rand.Read(secret)

	return &AuthManager{
		username:  username,
		password:  password,
		secretKey: secret,
		tokens:    make(map[string]time.Time),
	}
}

// Authenticate verifies username and password, returning an auth token if valid.
func (a *AuthManager) Authenticate(username, password string) (string, time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if username != a.username || password != a.password {
		return "", time.Time{}, false
	}

	expiresAt := time.Now().Add(24 * time.Hour)
	raw := fmt.Sprintf("%s:%d", username, expiresAt.Unix())
	mac := hmac.New(sha256.New, a.secretKey)
	mac.Write([]byte(raw))
	sig := hex.EncodeToString(mac.Sum(nil))

	token := fmt.Sprintf("%s.%s", strconv.FormatInt(expiresAt.Unix(), 10), sig)
	a.tokens[token] = expiresAt
	return token, expiresAt, true
}

// ValidateToken checks whether the token is valid and not expired.
func (a *AuthManager) ValidateToken(token string) bool {
	if token == "" {
		return false
	}

	a.mu.RLock()
	expiresAt, exists := a.tokens[token]
	a.mu.RUnlock()

	if !exists || time.Now().After(expiresAt) {
		return false
	}
	return true
}

// InvalidateToken logs out / revokes a token.
func (a *AuthManager) InvalidateToken(token string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.tokens, token)
}

// AuthMiddleware protects HTTP routes requiring authentication.
func (a *AuthManager) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow CORS Preflight OPTIONS
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Extract token from Authorization header or URL query parameter
		token := ""
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		} else {
			token = r.URL.Query().Get("token")
		}

		if !a.ValidateToken(token) {
			WriteError(w, http.StatusUnauthorized, 401, "unauthorized or token expired")
			return
		}

		next.ServeHTTP(w, r)
	})
}
