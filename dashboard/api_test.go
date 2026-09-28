package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockBroker implements BrokerInterface for comprehensive unit testing.
type mockBroker struct {
	overview     OverviewStats
	clients      []ClientSummary
	clientDetail map[string]*ClientDetail
	kicked       []string
	subs         []SubscriptionSummary
	unsubscribed []string
	retained     []RetainedSummary
	published    []PublishRequest
	listeners    []ListenerSummary
	cluster      ClusterSummary
	pipeline     *PipelineSummary
	rules        []RuleStatusDTO
	bridges      []BridgeStatusDTO
}

func (m *mockBroker) GetOverview() OverviewStats {
	return m.overview
}

func (m *mockBroker) GetClients(page, limit int, query string) ([]ClientSummary, int) {
	var filtered []ClientSummary
	for _, c := range m.clients {
		if query != "" && !strings.Contains(c.ClientID, query) && !strings.Contains(c.Username, query) {
			continue
		}
		filtered = append(filtered, c)
	}
	start := (page - 1) * limit
	if start >= len(filtered) {
		return []ClientSummary{}, len(filtered)
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], len(filtered)
}

func (m *mockBroker) GetClientDetail(clientID string) (*ClientDetail, bool) {
	d, ok := m.clientDetail[clientID]
	return d, ok
}

func (m *mockBroker) KickClient(clientID string) error {
	if _, ok := m.clientDetail[clientID]; !ok {
		return errors.New("client not found")
	}
	m.kicked = append(m.kicked, clientID)
	delete(m.clientDetail, clientID)
	return nil
}

func (m *mockBroker) GetSubscriptions(page, limit int, query string) ([]SubscriptionSummary, int) {
	var filtered []SubscriptionSummary
	for _, s := range m.subs {
		if query != "" && !strings.Contains(s.Topic, query) && !strings.Contains(s.ClientID, query) {
			continue
		}
		filtered = append(filtered, s)
	}
	start := (page - 1) * limit
	if start >= len(filtered) {
		return []SubscriptionSummary{}, len(filtered)
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], len(filtered)
}

func (m *mockBroker) UnsubscribeClient(clientID, topic string) error {
	m.unsubscribed = append(m.unsubscribed, clientID+":"+topic)
	return nil
}

func (m *mockBroker) GetRetainedMessages() ([]RetainedSummary, error) {
	return m.retained, nil
}

func (m *mockBroker) DeleteRetainedMessage(topic string) error {
	for i, r := range m.retained {
		if r.Topic == topic {
			m.retained = append(m.retained[:i], m.retained[i+1:]...)
			return nil
		}
	}
	return errors.New("retained message not found")
}

func (m *mockBroker) PublishMessage(topic string, qos byte, retain bool, payload []byte) error {
	if strings.ContainsAny(topic, "+#") {
		return errors.New("wildcard topic invalid")
	}
	m.published = append(m.published, PublishRequest{
		Topic:   topic,
		QoS:     qos,
		Retain:  retain,
		Payload: string(payload),
	})
	return nil
}

func (m *mockBroker) GetListeners() []ListenerSummary {
	return m.listeners
}

func (m *mockBroker) GetClusterNodes() ClusterSummary {
	return m.cluster
}

func (m *mockBroker) GetPipelineStatus() *PipelineSummary {
	return m.pipeline
}

func (m *mockBroker) GetRules() []RuleStatusDTO {
	return m.rules
}

func (m *mockBroker) GetRule(id string) (*RuleStatusDTO, error) {
	for _, r := range m.rules {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, errors.New("rule not found")
}

func (m *mockBroker) UpdateRule(rule RuleDTO) error {
	for i, r := range m.rules {
		if r.ID == rule.ID {
			m.rules[i].Rule = rule
			return nil
		}
	}
	m.rules = append(m.rules, RuleStatusDTO{Rule: rule, Connected: true})
	return nil
}

func (m *mockBroker) DeleteRule(id string) error {
	for i, r := range m.rules {
		if r.ID == id {
			m.rules = append(m.rules[:i], m.rules[i+1:]...)
			return nil
		}
	}
	return errors.New("rule not found")
}

func (m *mockBroker) PingRule(ctx context.Context, id string) (*RulePingResultDTO, error) {
	return &RulePingResultDTO{
		Success:   true,
		LatencyMs: 2,
		Target:    "127.0.0.1:9092",
		Message:   "connected successfully",
	}, nil
}

func (m *mockBroker) TestMatchTopic(topic string) []string {
	var res []string
	for _, r := range m.rules {
		if r.Enabled {
			res = append(res, r.ID)
		}
	}
	return res
}

func (m *mockBroker) GetBridges() []BridgeStatusDTO {
	return m.bridges
}

func (m *mockBroker) GetBridge(id string) (*BridgeStatusDTO, error) {
	for _, b := range m.bridges {
		if b.ID == id {
			return &b, nil
		}
	}
	return nil, errors.New("bridge not found")
}

func (m *mockBroker) UpdateBridge(bridge BridgeDTO) error {
	for i, b := range m.bridges {
		if b.ID == bridge.ID {
			m.bridges[i].Bridge = bridge
			return nil
		}
	}
	m.bridges = append(m.bridges, BridgeStatusDTO{Bridge: bridge, Status: "connected"})
	return nil
}

func (m *mockBroker) DeleteBridge(id string) error {
	for i, b := range m.bridges {
		if b.ID == id {
			m.bridges = append(m.bridges[:i], m.bridges[i+1:]...)
			return nil
		}
	}
	return errors.New("bridge not found")
}

func (m *mockBroker) PingBridge(ctx context.Context, id string) (*RulePingResultDTO, error) {
	return &RulePingResultDTO{
		Success:   true,
		LatencyMs: 1,
		Target:    "127.0.0.1:9092",
		Message:   "bridge connected successfully",
	}, nil
}

func (m *mockBroker) GetRegisteredDrivers() []string {
	return []string{"kafka", "stdout", "mock"}
}

func setupTestServer() (*Server, *mockBroker, *http.ServeMux) {
	broker := &mockBroker{
		bridges: []BridgeStatusDTO{
			{
				Bridge: BridgeDTO{
					ID:      "bridge_kafka_default",
					Name:    "Kafka 默认集群",
					Type:    "kafka",
					Servers: []string{"127.0.0.1:9092"},
				},
				Status: "connected",
			},
		},
		rules: []RuleStatusDTO{
			{
				Rule: RuleDTO{
					ID:           "rule_default",
					Name:         "Default Forwarding Rule",
					Enabled:      true,
					TopicFilters: []string{"#"},
					TargetTopic:  "mqtt_events",
					KeyStrategy:  "client_id",
					SinkType:     "kafka",
					SinkConfig:   map[string]any{"brokers": []string{"127.0.0.1:9092"}},
				},
				Connected:      true,
				CircuitBreaker: "Closed",
			},
		},
		overview: OverviewStats{
			NodeName:          "test-node-1",
			Version:           "1.0.0",
			UptimeSeconds:     3600,
			ActiveConnections: 10,
			TCPConnections:    5,
			TLSConnections:    3,
			QUICConnections:   2,
			Subscriptions:     15,
			RetainedCount:     2,
			TotalMsgReceived:  1000,
			TotalMsgSent:      950,
			OS:                "windows",
			Arch:              "amd64",
			NumCPU:            8,
		},
		clients: []ClientSummary{
			{ClientID: "client-1", Username: "user1", IPAddress: "127.0.0.1:5001", Transport: "tcp", Protocol: "MQTT 3.1.1"},
			{ClientID: "client-2", Username: "user2", IPAddress: "127.0.0.1:5002", Transport: "quic", Protocol: "MQTT 5.0"},
		},
		clientDetail: map[string]*ClientDetail{
			"client-1": {
				ClientSummary: ClientSummary{ClientID: "client-1", Username: "user1", IPAddress: "127.0.0.1:5001", Transport: "tcp"},
				Subscriptions: []string{"sensor/temp (QoS 1)"},
			},
		},
		subs: []SubscriptionSummary{
			{ClientID: "client-1", Topic: "sensor/temp", QoS: 1},
			{ClientID: "client-2", Topic: "sensor/humidity", QoS: 0},
		},
		retained: []RetainedSummary{
			{Topic: "status/node1", QoS: 0, Payload: "online", Size: 6},
		},
		listeners: []ListenerSummary{
			{Name: "tcp", Protocol: "tcp", Address: ":1883", Status: "running", ActiveConns: 5},
			{Name: "quic", Protocol: "udp", Address: ":14567", Status: "running", ActiveConns: 2},
		},
		cluster: ClusterSummary{
			SelfNodeID: "test-node-1",
			SelfAddr:   "127.0.0.1:19991",
			Nodes: []ClusterNodeSummary{
				{NodeID: "peer-node-2", Address: "127.0.0.1:19992", Status: "alive"},
			},
		},
		pipeline: &PipelineSummary{
			Enabled:        true,
			SinkDriver:     "kafka",
			CircuitBreaker: "Closed",
			IngestedTotal:  50000,
			DirectSent:     50000,
			DiskUsageMB:    0,
			MaxDiskQuotaGB: 5,
		},
	}

	cfg := Config{
		Addr:     ":18083",
		Username: "admin",
		Password: "public",
	}
	srv := NewServer(cfg, broker)
	mux := http.NewServeMux()
	srv.registerRoutes(mux)

	return srv, broker, mux
}

func getAuthToken(t *testing.T, mux *http.ServeMux) string {
	loginPayload := `{"username":"admin","password":"public"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewBufferString(loginPayload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Login failed with code %d: %s", w.Code, w.Body.String())
	}

	var resp Response
	_ = json.NewDecoder(w.Body).Decode(&resp)
	dataMap := resp.Data.(map[string]any)
	return dataMap["token"].(string)
}

func TestAPI_Auth_Login_And_Logout(t *testing.T) {
	_, _, mux := setupTestServer()

	// 1. Invalid credentials
	badLogin := `{"username":"admin","password":"wrong"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewBufferString(badLogin))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}

	// 2. Successful login
	token := getAuthToken(t, mux)
	if token == "" {
		t.Fatal("expected non-empty auth token")
	}

	// 3. Me profile
	reqMe := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	reqMe.Header.Set("Authorization", "Bearer "+token)
	wMe := httptest.NewRecorder()
	mux.ServeHTTP(wMe, reqMe)
	if wMe.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wMe.Code)
	}

	// 4. Logout
	reqLogout := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	reqLogout.Header.Set("Authorization", "Bearer "+token)
	wLogout := httptest.NewRecorder()
	mux.ServeHTTP(wLogout, reqLogout)
	if wLogout.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wLogout.Code)
	}

	// 5. Subsequent request with invalidated token should fail
	wMe2 := httptest.NewRecorder()
	mux.ServeHTTP(wMe2, reqMe)
	if wMe2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 after logout, got %d", wMe2.Code)
	}
}

func TestAPI_Overview(t *testing.T) {
	_, _, mux := setupTestServer()
	token := getAuthToken(t, mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/overview", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp Response
	_ = json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if data["node_name"] != "test-node-1" {
		t.Fatalf("unexpected node_name: %v", data["node_name"])
	}
	if data["active_connections"].(float64) != 10 {
		t.Fatalf("unexpected active_connections: %v", data["active_connections"])
	}
	if data["quic_connections"].(float64) != 2 {
		t.Fatalf("unexpected quic_connections: %v", data["quic_connections"])
	}
}

func TestAPI_Clients_List_Detail_Kick(t *testing.T) {
	_, broker, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// List
	req := httptest.NewRequest(http.MethodGet, "/api/v1/clients?page=1&limit=10", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Detail
	reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/clients/client-1", nil)
	reqDetail.Header.Set("Authorization", "Bearer "+token)
	wDetail := httptest.NewRecorder()
	mux.ServeHTTP(wDetail, reqDetail)
	if wDetail.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wDetail.Code, wDetail.Body.String())
	}

	// Kick
	reqKick := httptest.NewRequest(http.MethodDelete, "/api/v1/clients/client-1", nil)
	reqKick.Header.Set("Authorization", "Bearer "+token)
	wKick := httptest.NewRecorder()
	mux.ServeHTTP(wKick, reqKick)
	if wKick.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wKick.Code, wKick.Body.String())
	}
	if len(broker.kicked) != 1 || broker.kicked[0] != "client-1" {
		t.Fatalf("expected client-1 to be kicked, got: %v", broker.kicked)
	}

	// Detail after kick should return 404
	wDetailAfter := httptest.NewRecorder()
	mux.ServeHTTP(wDetailAfter, reqDetail)
	if wDetailAfter.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wDetailAfter.Code)
	}
}

func TestAPI_Subscriptions_List_And_Delete(t *testing.T) {
	_, broker, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// List
	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?topic=sensor", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Delete
	reqDel := httptest.NewRequest(http.MethodDelete, "/api/v1/subscriptions?client_id=client-1&topic=sensor/temp", nil)
	reqDel.Header.Set("Authorization", "Bearer "+token)
	wDel := httptest.NewRecorder()
	mux.ServeHTTP(wDel, reqDel)
	if wDel.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wDel.Code, wDel.Body.String())
	}
	if len(broker.unsubscribed) != 1 || broker.unsubscribed[0] != "client-1:sensor/temp" {
		t.Fatalf("unexpected unsubscribed list: %v", broker.unsubscribed)
	}
}

func TestAPI_Retained_List_And_Delete(t *testing.T) {
	_, broker, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// List
	req := httptest.NewRequest(http.MethodGet, "/api/v1/retained", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Delete
	reqDel := httptest.NewRequest(http.MethodDelete, "/api/v1/retained?topic=status/node1", nil)
	reqDel.Header.Set("Authorization", "Bearer "+token)
	wDel := httptest.NewRecorder()
	mux.ServeHTTP(wDel, reqDel)
	if wDel.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wDel.Code, wDel.Body.String())
	}
	if len(broker.retained) != 0 {
		t.Fatalf("expected retained list to be empty, got: %d", len(broker.retained))
	}
}

func TestAPI_Publish(t *testing.T) {
	_, broker, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// 1. Invalid topic with wildcard
	badPub := `{"topic":"sensor/+","qos":1,"retain":false,"payload":"test"}`
	reqBad := httptest.NewRequest(http.MethodPost, "/api/v1/publish", bytes.NewBufferString(badPub))
	reqBad.Header.Set("Authorization", "Bearer "+token)
	wBad := httptest.NewRecorder()
	mux.ServeHTTP(wBad, reqBad)
	if wBad.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on wildcard topic, got %d", wBad.Code)
	}

	// 2. Valid publish
	validPub := `{"topic":"sensor/temp","qos":1,"retain":true,"payload":"{\"val\": 25.5}"}`
	reqValid := httptest.NewRequest(http.MethodPost, "/api/v1/publish", bytes.NewBufferString(validPub))
	reqValid.Header.Set("Authorization", "Bearer "+token)
	wValid := httptest.NewRecorder()
	mux.ServeHTTP(wValid, reqValid)
	if wValid.Code != http.StatusOK {
		t.Fatalf("expected 200 on publish, got %d: %s", wValid.Code, wValid.Body.String())
	}
	if len(broker.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(broker.published))
	}
	if broker.published[0].Topic != "sensor/temp" || broker.published[0].Payload != `{"val": 25.5}` {
		t.Fatalf("unexpected published message: %+v", broker.published[0])
	}
}

func TestAPI_System_Endpoints(t *testing.T) {
	_, _, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// Healthz (public)
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	wHealth := httptest.NewRecorder()
	mux.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 for healthz, got %d", wHealth.Code)
	}

	// Listeners
	reqListeners := httptest.NewRequest(http.MethodGet, "/api/v1/listeners", nil)
	reqListeners.Header.Set("Authorization", "Bearer "+token)
	wListeners := httptest.NewRecorder()
	mux.ServeHTTP(wListeners, reqListeners)
	if wListeners.Code != http.StatusOK {
		t.Fatalf("expected 200 for listeners, got %d", wListeners.Code)
	}

	// Cluster nodes
	reqCluster := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/nodes", nil)
	reqCluster.Header.Set("Authorization", "Bearer "+token)
	wCluster := httptest.NewRecorder()
	mux.ServeHTTP(wCluster, reqCluster)
	if wCluster.Code != http.StatusOK {
		t.Fatalf("expected 200 for cluster, got %d", wCluster.Code)
	}

	// Pipeline
	reqPipe := httptest.NewRequest(http.MethodGet, "/api/v1/pipeline", nil)
	reqPipe.Header.Set("Authorization", "Bearer "+token)
	wPipe := httptest.NewRecorder()
	mux.ServeHTTP(wPipe, reqPipe)
	if wPipe.Code != http.StatusOK {
		t.Fatalf("expected 200 for pipeline, got %d", wPipe.Code)
	}
}

func TestAPI_Rules(t *testing.T) {
	_, _, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// 1. GET /api/v1/rules
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/rules", nil)
	reqGet.Header.Set("Authorization", "Bearer "+token)
	wGet := httptest.NewRecorder()
	mux.ServeHTTP(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/rules, got %d", wGet.Code)
	}

	// 2. GET /api/v1/rules/rule_default
	reqItem := httptest.NewRequest(http.MethodGet, "/api/v1/rules/rule_default", nil)
	reqItem.Header.Set("Authorization", "Bearer "+token)
	wItem := httptest.NewRecorder()
	mux.ServeHTTP(wItem, reqItem)
	if wItem.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/rules/rule_default, got %d", wItem.Code)
	}

	// 3. PUT /api/v1/rules
	updatePayload := `{"id":"rule_default","name":"Updated Rule","enabled":true,"topic_filters":["sensors/#"],"target_topic":"iot_data","key_strategy":"client_id","sink_type":"kafka","sink_config":{"brokers":["127.0.0.1:9092"]}}`
	reqPut := httptest.NewRequest(http.MethodPut, "/api/v1/rules", bytes.NewBufferString(updatePayload))
	reqPut.Header.Set("Authorization", "Bearer "+token)
	reqPut.Header.Set("Content-Type", "application/json")
	wPut := httptest.NewRecorder()
	mux.ServeHTTP(wPut, reqPut)
	if wPut.Code != http.StatusOK {
		t.Fatalf("expected 200 for PUT /api/v1/rules, got %d", wPut.Code)
	}

	// 4. POST /api/v1/rules/rule_default/test (Ping)
	reqPing := httptest.NewRequest(http.MethodPost, "/api/v1/rules/rule_default/test", nil)
	reqPing.Header.Set("Authorization", "Bearer "+token)
	wPing := httptest.NewRecorder()
	mux.ServeHTTP(wPing, reqPing)
	if wPing.Code != http.StatusOK {
		t.Fatalf("expected 200 for POST /api/v1/rules/rule_default/test, got %d", wPing.Code)
	}

	// 5. GET /api/v1/rules/drivers
	reqDrivers := httptest.NewRequest(http.MethodGet, "/api/v1/rules/drivers", nil)
	reqDrivers.Header.Set("Authorization", "Bearer "+token)
	wDrivers := httptest.NewRecorder()
	mux.ServeHTTP(wDrivers, reqDrivers)
	if wDrivers.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/rules/drivers, got %d", wDrivers.Code)
	}

	// 6. POST /api/v1/rules/match-test
	matchPayload := `{"topic":"sensors/room1/temperature"}`
	reqMatch := httptest.NewRequest(http.MethodPost, "/api/v1/rules/match-test", bytes.NewBufferString(matchPayload))
	reqMatch.Header.Set("Authorization", "Bearer "+token)
	reqMatch.Header.Set("Content-Type", "application/json")
	wMatch := httptest.NewRecorder()
	mux.ServeHTTP(wMatch, reqMatch)
	if wMatch.Code != http.StatusOK {
		t.Fatalf("expected 200 for POST /api/v1/rules/match-test, got %d", wMatch.Code)
	}
}

func TestAPI_Bridges(t *testing.T) {
	_, _, mux := setupTestServer()
	token := getAuthToken(t, mux)

	// 1. GET /api/v1/bridges
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/bridges", nil)
	reqGet.Header.Set("Authorization", "Bearer "+token)
	wGet := httptest.NewRecorder()
	mux.ServeHTTP(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/bridges, got %d", wGet.Code)
	}

	// 2. GET /api/v1/bridges/bridge_kafka_default
	reqItem := httptest.NewRequest(http.MethodGet, "/api/v1/bridges/bridge_kafka_default", nil)
	reqItem.Header.Set("Authorization", "Bearer "+token)
	wItem := httptest.NewRecorder()
	mux.ServeHTTP(wItem, reqItem)
	if wItem.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/bridges/bridge_kafka_default, got %d", wItem.Code)
	}

	// 3. POST /api/v1/bridges/bridge_kafka_default/test (Ping)
	reqPing := httptest.NewRequest(http.MethodPost, "/api/v1/bridges/bridge_kafka_default/test", nil)
	reqPing.Header.Set("Authorization", "Bearer "+token)
	wPing := httptest.NewRecorder()
	mux.ServeHTTP(wPing, reqPing)
	if wPing.Code != http.StatusOK {
		t.Fatalf("expected 200 for POST /api/v1/bridges/bridge_kafka_default/test, got %d", wPing.Code)
	}

	// 4. PUT /api/v1/bridges/bridge_kafka_default
	updatePayload := `{"id":"bridge_kafka_default","name":"Updated Kafka Bridge","type":"kafka","servers":["127.0.0.1:9092","127.0.0.1:9093"]}`
	reqPut := httptest.NewRequest(http.MethodPut, "/api/v1/bridges/bridge_kafka_default", bytes.NewBufferString(updatePayload))
	reqPut.Header.Set("Authorization", "Bearer "+token)
	reqPut.Header.Set("Content-Type", "application/json")
	wPut := httptest.NewRecorder()
	mux.ServeHTTP(wPut, reqPut)
	if wPut.Code != http.StatusOK {
		t.Fatalf("expected 200 for PUT /api/v1/bridges/bridge_kafka_default, got %d", wPut.Code)
	}

	// 5. GET /api/v1/bridges/drivers
	reqDrivers := httptest.NewRequest(http.MethodGet, "/api/v1/bridges/drivers", nil)
	reqDrivers.Header.Set("Authorization", "Bearer "+token)
	wDrivers := httptest.NewRecorder()
	mux.ServeHTTP(wDrivers, reqDrivers)
	if wDrivers.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/bridges/drivers, got %d", wDrivers.Code)
	}
}
