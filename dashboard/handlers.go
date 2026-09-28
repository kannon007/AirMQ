package dashboard

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"mqtt/web"
)

func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Public endpoints
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/auth/login", s.handleLogin)

	// Protected API mux
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/auth/logout", s.handleLogout)
	apiMux.HandleFunc("/api/v1/auth/me", s.handleMe)
	apiMux.HandleFunc("/api/v1/overview", s.handleOverview)
	apiMux.HandleFunc("/api/v1/clients", s.handleClients)
	apiMux.HandleFunc("/api/v1/clients/", s.handleClientItem)
	apiMux.HandleFunc("/api/v1/subscriptions", s.handleSubscriptions)
	apiMux.HandleFunc("/api/v1/retained", s.handleRetained)
	apiMux.HandleFunc("/api/v1/publish", s.handlePublish)
	apiMux.HandleFunc("/api/v1/listeners", s.handleListeners)
	apiMux.HandleFunc("/api/v1/cluster/nodes", s.handleClusterNodes)
	apiMux.HandleFunc("/api/v1/pipeline", s.handlePipeline)
	apiMux.HandleFunc("/api/v1/rules", s.handleRules)
	apiMux.HandleFunc("/api/v1/rules/", s.handleRuleItem)
	apiMux.HandleFunc("/api/v1/bridges", s.handleBridges)
	apiMux.HandleFunc("/api/v1/bridges/", s.handleBridgeItem)

	// Mount protected subrouter
	mux.Handle("/api/v1/", s.auth.AuthMiddleware(apiMux))

	// Mount Web UI static files
	s.registerStaticRoutes(mux)
}

func (s *Server) registerStaticRoutes(mux *http.ServeMux) {
	var fileSystem http.FileSystem

	if s.cfg.WebDir != "" {
		fileSystem = http.Dir(s.cfg.WebDir)
	} else {
		staticFS, err := web.GetFS()
		if err == nil {
			fileSystem = http.FS(staticFS)
		}
	}

	if fileSystem != nil {
		fileServer := http.FileServer(fileSystem)
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				http.NotFound(w, r)
				return
			}
			path := strings.TrimPrefix(r.URL.Path, "/")
			if path == "" {
				path = "index.html"
			}
			f, err := fileSystem.Open(path)
			if err != nil {
				// Fallback to index.html for React SPA client routing
				r.URL.Path = "/"
			} else {
				_ = f.Close()
			}
			fileServer.ServeHTTP(w, r)
		})
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
		return
	}

	token, expiresAt, ok := s.auth.Authenticate(req.Username, req.Password)
	if !ok {
		WriteError(w, http.StatusUnauthorized, 401, "invalid username or password")
		return
	}

	WriteJSON(w, http.StatusOK, LoginResponse{
		Token:     token,
		ExpiresAt: expiresAt,
		Username:  req.Username,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}
	token := ""
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token = strings.TrimPrefix(authHeader, "Bearer ")
	}
	if token != "" {
		s.auth.InvalidateToken(token)
	}
	WriteJSON(w, http.StatusOK, "logged out")
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, UserProfile{
		Username: s.cfg.Username,
		Role:     "administrator",
	})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.GetOverview()
	WriteJSON(w, http.StatusOK, stats)
}

func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page <= 0 {
		page = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 20
	}
	query := r.URL.Query().Get("query")

	clients, total := s.broker.GetClients(page, limit, query)
	WriteJSON(w, http.StatusOK, PaginatedResult[ClientSummary]{
		Items: clients,
		Total: total,
		Page:  page,
		Limit: limit,
	})
}

func (s *Server) handleClientItem(w http.ResponseWriter, r *http.Request) {
	clientID := strings.TrimPrefix(r.URL.Path, "/api/v1/clients/")
	if clientID == "" {
		WriteError(w, http.StatusBadRequest, 400, "missing client id")
		return
	}

	switch r.Method {
	case http.MethodGet:
		detail, found := s.broker.GetClientDetail(clientID)
		if !found {
			WriteError(w, http.StatusNotFound, 404, "client not found")
			return
		}
		WriteJSON(w, http.StatusOK, detail)

	case http.MethodDelete:
		if err := s.broker.KickClient(clientID); err != nil {
			WriteError(w, http.StatusNotFound, 404, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{
			"message": "client disconnected successfully",
		})

	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page <= 0 {
			page = 1
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 500 {
			limit = 20
		}
		query := r.URL.Query().Get("topic")
		if query == "" {
			query = r.URL.Query().Get("query")
		}

		subs, total := s.broker.GetSubscriptions(page, limit, query)
		WriteJSON(w, http.StatusOK, PaginatedResult[SubscriptionSummary]{
			Items: subs,
			Total: total,
			Page:  page,
			Limit: limit,
		})

	case http.MethodDelete:
		clientID := r.URL.Query().Get("client_id")
		topic := r.URL.Query().Get("topic")
		if clientID == "" || topic == "" {
			WriteError(w, http.StatusBadRequest, 400, "client_id and topic are required")
			return
		}
		if err := s.broker.UnsubscribeClient(clientID, topic); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{
			"message": "unsubscribed successfully",
		})

	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handleRetained(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		msgs, err := s.broker.GetRetainedMessages()
		if err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{
			"items": msgs,
			"count": len(msgs),
		})

	case http.MethodDelete:
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			WriteError(w, http.StatusBadRequest, 400, "topic parameter is required")
			return
		}
		if err := s.broker.DeleteRetainedMessage(topic); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{
			"message": "retained message deleted successfully",
		})

	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
		return
	}

	if req.Topic == "" {
		WriteError(w, http.StatusBadRequest, 400, "topic is required")
		return
	}
	if req.QoS > 2 {
		WriteError(w, http.StatusBadRequest, 400, "qos must be 0, 1, or 2")
		return
	}

	if err := s.broker.PublishMessage(req.Topic, req.QoS, req.Retain, []byte(req.Payload)); err != nil {
		WriteError(w, http.StatusInternalServerError, 500, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{
		"message": "message published successfully",
	})
}

func (s *Server) handleListeners(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}
	listeners := s.broker.GetListeners()
	WriteJSON(w, http.StatusOK, map[string]any{
		"items": listeners,
		"count": len(listeners),
	})
}

func (s *Server) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}
	cluster := s.broker.GetClusterNodes()
	WriteJSON(w, http.StatusOK, cluster)
}

func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}
	pipe := s.broker.GetPipelineStatus()
	if pipe == nil {
		WriteJSON(w, http.StatusOK, PipelineSummary{
			Enabled: false,
		})
		return
	}
	WriteJSON(w, http.StatusOK, pipe)
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rules := s.broker.GetRules()
		WriteJSON(w, http.StatusOK, map[string]any{
			"items": rules,
			"count": len(rules),
		})
	case http.MethodPut, http.MethodPost:
		var dto RuleDTO
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
			return
		}
		if err := s.broker.UpdateRule(dto); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"message": "rule updated successfully"})
	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handleRuleItem(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/rules/")
	if path == "drivers" {
		s.handleRuleDrivers(w, r)
		return
	}
	if path == "match-test" {
		s.handleRuleMatchTest(w, r)
		return
	}

	if strings.HasSuffix(path, "/test") {
		id := strings.TrimSuffix(path, "/test")
		if r.Method != http.MethodPost {
			WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
			return
		}
		res, err := s.broker.PingRule(r.Context(), id)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, res)
		return
	}

	id := path
	switch r.Method {
	case http.MethodGet:
		rule, err := s.broker.GetRule(id)
		if err != nil {
			WriteError(w, http.StatusNotFound, 404, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, rule)
	case http.MethodPut:
		var dto RuleDTO
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
			return
		}
		dto.ID = id
		if err := s.broker.UpdateRule(dto); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"message": "rule updated successfully"})
	case http.MethodDelete:
		if err := s.broker.DeleteRule(id); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"message": "rule deleted successfully"})
	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handleRuleMatchTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}
	var req MatchTestRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
		return
	}
	matched := s.broker.TestMatchTopic(req.Topic)
	if matched == nil {
		matched = []string{}
	}
	WriteJSON(w, http.StatusOK, MatchTestResultDTO{MatchedRuleIDs: matched})
}

func (s *Server) handleBridges(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		bridges := s.broker.GetBridges()
		WriteJSON(w, http.StatusOK, map[string]any{
			"items": bridges,
			"count": len(bridges),
		})
	case http.MethodPost, http.MethodPut:
		var dto BridgeDTO
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
			return
		}
		if err := s.broker.UpdateBridge(dto); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"message": "bridge updated successfully"})
	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handleBridgeItem(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/bridges/")
	if path == "drivers" {
		s.handleRuleDrivers(w, r)
		return
	}

	if strings.HasSuffix(path, "/test") {
		id := strings.TrimSuffix(path, "/test")
		if r.Method != http.MethodPost {
			WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
			return
		}
		res, err := s.broker.PingBridge(r.Context(), id)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, res)
		return
	}

	id := path
	switch r.Method {
	case http.MethodGet:
		bridge, err := s.broker.GetBridge(id)
		if err != nil {
			WriteError(w, http.StatusNotFound, 404, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, bridge)
	case http.MethodPut:
		var dto BridgeDTO
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			WriteError(w, http.StatusBadRequest, 400, "invalid json payload")
			return
		}
		dto.ID = id
		if err := s.broker.UpdateBridge(dto); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"message": "bridge updated successfully"})
	case http.MethodDelete:
		if err := s.broker.DeleteBridge(id); err != nil {
			WriteError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"message": "bridge deleted successfully"})
	default:
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
	}
}

func (s *Server) handleRuleDrivers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}
	drivers := s.broker.GetRegisteredDrivers()
	WriteJSON(w, http.StatusOK, map[string]any{
		"drivers": drivers,
	})
}
