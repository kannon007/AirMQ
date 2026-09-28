package dashboard_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"mqtt/core"
	"mqtt/dashboard"
)

func TestCoreBrokerWithDashboardIntegration(t *testing.T) {
	// 1. Create and start core.Broker standalone
	brokerAddr := "tcp://127.0.0.1:18895"
	b, err := core.NewBroker(
		core.WithTCP(brokerAddr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Failed to create core.Broker: %v", err)
	}

	go func() {
		_ = b.Start()
	}()
	time.Sleep(100 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 2. Attach dashboard.Server directly to core.Broker
	dashAddr := "127.0.0.1:18099"
	dashSrv := dashboard.NewServer(dashboard.Config{
		Addr:     dashAddr,
		Username: "admin",
		Password: "password123",
	}, b) // b directly satisfies dashboard.BrokerInterface!

	if err := dashSrv.Start(); err != nil {
		t.Fatalf("Failed to start dashboard server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = dashSrv.Stop(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	baseURL := "http://" + dashAddr

	// 3. Test /healthz
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("Failed to get healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// 4. Test Login
	loginBody, _ := json.Marshal(dashboard.LoginRequest{
		Username: "admin",
		Password: "password123",
	})
	resp, err = http.Post(baseURL+"/api/v1/auth/login", "application/json", bytes.NewReader(loginBody))
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected login 200, got %d", resp.StatusCode)
	}

	var loginResp struct {
		Code int                     `json:"code"`
		Data dashboard.LoginResponse `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&loginResp)
	token := loginResp.Data.Token
	if token == "" {
		t.Fatal("Expected auth token, got empty")
	}

	// 5. Test Authenticated /api/v1/overview
	req, _ := http.NewRequest(http.MethodGet, baseURL+"/api/v1/overview", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Overview request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected overview 200, got %d: %s", resp.StatusCode, string(body))
	}

	var overviewResp struct {
		Code int                `json:"code"`
		Data core.OverviewStats `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&overviewResp)
	if overviewResp.Data.NodeName == "" {
		t.Errorf("Expected non-empty NodeName from broker")
	}
}
