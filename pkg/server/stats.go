package server

import (
	"time"
)

// OverviewStats defines real-time broker statistics and hardware metrics.
type OverviewStats struct {
	NodeName          string  `json:"node_name"`
	Version           string  `json:"version"`
	UptimeSeconds     int64   `json:"uptime_seconds"`
	ActiveConnections int64   `json:"active_connections"`
	TCPConnections    int64   `json:"tcp_connections"`
	TLSConnections    int64   `json:"tls_connections"`
	WSConnections     int64   `json:"ws_connections"`
	QUICConnections   int64   `json:"quic_connections"`
	Subscriptions     int     `json:"subscriptions_count"`
	RetainedCount     int     `json:"retained_count"`
	TotalMsgReceived  uint64  `json:"total_msg_received"`
	TotalMsgSent      uint64  `json:"total_msg_sent"`
	TotalBytesIn      uint64  `json:"total_bytes_in"`
	TotalBytesOut     uint64  `json:"total_bytes_out"`
	RateLimitDropped  uint64  `json:"rate_limit_dropped"`
	ClusterNodesCount int     `json:"cluster_nodes_count"`
	OS                string  `json:"os"`
	Arch              string  `json:"arch"`
	GoVersion         string  `json:"go_version"`
	Goroutines        int     `json:"goroutines"`
	MemoryAllocMB     float64 `json:"memory_alloc_mb"`
	MemorySysMB       float64 `json:"memory_sys_mb"`
	NumCPU            int     `json:"num_cpu"`
}

// ClientSummary represents client item in list view.
type ClientSummary struct {
	ClientID      string    `json:"client_id"`
	Username      string    `json:"username"`
	IPAddress     string    `json:"ip_address"`
	Protocol      string    `json:"protocol"`  // "MQTT 3.1.1" or "MQTT 5.0"
	Transport     string    `json:"transport"` // "tcp", "tls", "ws", "quic"
	ConnectedAt   time.Time `json:"connected_at"`
	KeepAlive     int       `json:"keep_alive"`
	CleanSession  bool      `json:"clean_session"`
	Subscriptions int       `json:"subscriptions_count"`
}

// ClientDetail represents client details and its active subscriptions.
type ClientDetail struct {
	ClientSummary
	Subscriptions []string `json:"subscriptions"`
	WillTopic     string   `json:"will_topic,omitempty"`
	IsTLS         bool     `json:"is_tls"`
	ClientCertCN  string   `json:"client_cert_cn,omitempty"`
}

// SubscriptionSummary represents a topic subscription across the broker.
type SubscriptionSummary struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
}

// RetainedSummary represents a retained message stored in broker.
type RetainedSummary struct {
	Topic     string    `json:"topic"`
	QoS       byte      `json:"qos"`
	Size      int       `json:"size"`
	Payload   string    `json:"payload"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ListenerSummary describes a network transport listener.
type ListenerSummary struct {
	Name        string `json:"name"`     // "tcp", "tls", "ws", "quic"
	Protocol    string `json:"protocol"` // "tcp" or "udp"
	Address     string `json:"address"`
	Status      string `json:"status"` // "running", "stopped"
	ActiveConns int64  `json:"active_conns"`
}

// ClusterNodeSummary represents a cluster peer.
type ClusterNodeSummary struct {
	NodeID   string    `json:"node_id"`
	Address  string    `json:"address"`
	Status   string    `json:"status"` // "alive", "suspect", "dead"
	LastSeen time.Time `json:"last_seen"`
}

// ClusterSummary represents the overall cluster status.
type ClusterSummary struct {
	SelfNodeID string               `json:"self_node_id"`
	SelfAddr   string               `json:"self_addr"`
	Nodes      []ClusterNodeSummary `json:"nodes"`
}

// PipelineSummary describes the streaming pipeline & spooler status.
type PipelineSummary struct {
	Enabled        bool   `json:"enabled"`
	SinkDriver     string `json:"sink_driver"`
	CircuitBreaker string `json:"circuit_breaker"` // "Closed", "Open", "Half-Open"
	IngestedTotal  uint64 `json:"ingested_total"`
	DirectSent     uint64 `json:"direct_sent"`
	SpooledTotal   uint64 `json:"spooled_total"`
	DrainedTotal   uint64 `json:"drained_total"`
	DroppedTotal   uint64 `json:"dropped_total"`
	DiskUsageMB    int64  `json:"disk_usage_mb"`
	MaxDiskQuotaGB int64  `json:"max_disk_quota_gb"`
}
