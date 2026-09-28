package dashboard

import (
	"encoding/json"
	"net/http"
	"time"

	"mqtt/core"
)

// Response is the standard JSON envelope for API responses.
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// WriteJSON sends a standardized JSON response.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Response{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

// WriteError sends a standardized error JSON response.
func WriteError(w http.ResponseWriter, status int, code int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Response{
		Code:    code,
		Message: message,
	})
}

// LoginRequest defines credentials payload for /api/v1/auth/login.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse defines response payload after successful login.
type LoginResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	Username  string    `json:"username"`
}

// UserProfile defines current logged-in user info.
type UserProfile struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}

// Telemetry and broker model type aliases from core
type OverviewStats = core.OverviewStats
type ClientSummary = core.ClientSummary
type ClientDetail = core.ClientDetail
type SubscriptionSummary = core.SubscriptionSummary
type RetainedSummary = core.RetainedSummary
type ListenerSummary = core.ListenerSummary
type ClusterNodeSummary = core.ClusterNodeSummary
type ClusterSummary = core.ClusterSummary
type PipelineSummary = core.PipelineSummary

// Data integration rule and bridge type aliases from core
type RuleDTO = core.Rule
type RuleActionDTO = core.RuleAction
type RuleStatusDTO = core.RuleStatus
type BridgeDTO = core.Bridge
type BridgeStatusDTO = core.BridgeStatus
type RulePingResultDTO = core.RulePingResult

// PublishRequest defines payload for direct MQTT publishing tool.
type PublishRequest struct {
	Topic   string `json:"topic"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain"`
	Payload string `json:"payload"`
}

// PaginatedResult wraps paged data.
type PaginatedResult[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
	Page  int `json:"page"`
	Limit int `json:"limit"`
}

// MatchTestRequestDTO is used to test whether a topic matches any rules.
type MatchTestRequestDTO struct {
	Topic string `json:"topic"`
}

// MatchTestResultDTO is the result of match testing.
type MatchTestResultDTO struct {
	MatchedRuleIDs []string `json:"matched_rule_ids"`
}
