package dashboard

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDashboard_EmbeddedWeb_EndToEnd(t *testing.T) {
	broker := &mockBroker{
		overview: OverviewStats{NodeName: "web-test-node"},
	}

	cfg := Config{
		Addr:     "127.0.0.1:18098",
		Username: "admin",
		Password: "public",
	}
	srv := NewServer(cfg, broker)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		_ = srv.Stop(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)
	baseURL := fmt.Sprintf("http://%s", srv.Addr().String())

	// 1. Root / should serve index.html
	resp, err := http.Get(baseURL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Fatalf("expected text/html, got %s", contentType)
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, `<div id="root"></div>`) {
		t.Fatalf("expected index.html with #root element, got: %s", bodyStr)
	}

	// 2. SPA route /clients should fall back to index.html (client-side routing)
	respClients, err := http.Get(baseURL + "/clients")
	if err != nil {
		t.Fatalf("GET /clients failed: %v", err)
	}
	defer respClients.Body.Close()
	if respClients.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for SPA route, got %d", respClients.StatusCode)
	}
	clientsBody, _ := io.ReadAll(respClients.Body)
	if !strings.Contains(string(clientsBody), `<div id="root"></div>`) {
		t.Fatalf("expected SPA fallback to index.html, got: %s", string(clientsBody))
	}

	// 3. API endpoint /api/v1/auth/login should NOT be hijacked by static file server
	loginResp, err := http.Post(baseURL+"/api/v1/auth/login", "application/json", strings.NewReader(`{"username":"admin","password":"bad"}`))
	if err != nil {
		t.Fatalf("POST /api/v1/auth/login failed: %v", err)
	}
	defer loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 from API, got %d", loginResp.StatusCode)
	}
}
