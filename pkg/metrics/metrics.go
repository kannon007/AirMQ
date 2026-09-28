package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
)

// Default is the global singleton metrics registry.
var Default = NewRegistry()

// Registry holds hardware atomic counters and gauges for zero-alloc high-throughput metrics.
type Registry struct {
	// Active connections (Gauge)
	ConnsActiveTCP  atomic.Int64
	ConnsActiveTLS  atomic.Int64
	ConnsActiveWS   atomic.Int64
	ConnsActiveQUIC atomic.Int64

	// Connects (Counter)
	ConnectSuccessTotal  atomic.Uint64
	ConnectRejectedTotal atomic.Uint64
	ConnectAuthFailTotal atomic.Uint64

	// Disconnects (Counter)
	DisconnectCleanTotal   atomic.Uint64
	DisconnectTimeoutTotal atomic.Uint64
	DisconnectErrorTotal   atomic.Uint64

	// Messages received (Counter by QoS)
	MsgsReceivedQoS0 atomic.Uint64
	MsgsReceivedQoS1 atomic.Uint64
	MsgsReceivedQoS2 atomic.Uint64

	// Messages sent (Counter by QoS)
	MsgsSentQoS0 atomic.Uint64
	MsgsSentQoS1 atomic.Uint64
	MsgsSentQoS2 atomic.Uint64

	// Bytes (Counter)
	BytesReceivedTotal atomic.Uint64
	BytesSentTotal     atomic.Uint64

	// Retained & Offline (Gauge)
	RetainedMessagesCount atomic.Int64
	OfflineMessagesCount  atomic.Int64

	// Rate limiting drops (Counter)
	RateLimitDroppedConn    atomic.Uint64
	RateLimitDroppedPublish atomic.Uint64

	// Cluster (Gauge & Counter)
	ClusterNodesOnline     atomic.Int64
	ClusterRoutedMsgsTotal atomic.Uint64
}

// NewRegistry initializes a clean Registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// IncConnActive increments active connections by transport.
func (r *Registry) IncConnActive(transport string) {
	switch transport {
	case "tls":
		r.ConnsActiveTLS.Add(1)
	case "ws":
		r.ConnsActiveWS.Add(1)
	case "quic":
		r.ConnsActiveQUIC.Add(1)
	default:
		r.ConnsActiveTCP.Add(1)
	}
}

// DecConnActive decrements active connections by transport.
func (r *Registry) DecConnActive(transport string) {
	switch transport {
	case "tls":
		r.ConnsActiveTLS.Add(-1)
	case "ws":
		r.ConnsActiveWS.Add(-1)
	case "quic":
		r.ConnsActiveQUIC.Add(-1)
	default:
		r.ConnsActiveTCP.Add(-1)
	}
}

// IncConnect records a connection attempt result.
func (r *Registry) IncConnect(status string) {
	switch status {
	case "rejected":
		r.ConnectRejectedTotal.Add(1)
	case "auth_failed":
		r.ConnectAuthFailTotal.Add(1)
	default:
		r.ConnectSuccessTotal.Add(1)
	}
}

// IncDisconnect records a disconnection reason.
func (r *Registry) IncDisconnect(reason string) {
	switch reason {
	case "timeout":
		r.DisconnectTimeoutTotal.Add(1)
	case "error":
		r.DisconnectErrorTotal.Add(1)
	default:
		r.DisconnectCleanTotal.Add(1)
	}
}

// IncMsgReceived records an incoming PUBLISH message.
func (r *Registry) IncMsgReceived(qos byte) {
	switch qos {
	case 1:
		r.MsgsReceivedQoS1.Add(1)
	case 2:
		r.MsgsReceivedQoS2.Add(1)
	default:
		r.MsgsReceivedQoS0.Add(1)
	}
}

// IncMsgSent records an outgoing PUBLISH message delivered to subscriber.
func (r *Registry) IncMsgSent(qos byte) {
	switch qos {
	case 1:
		r.MsgsSentQoS1.Add(1)
	case 2:
		r.MsgsSentQoS2.Add(1)
	default:
		r.MsgsSentQoS0.Add(1)
	}
}

// AddBytesReceived tracks incoming network payload bytes.
func (r *Registry) AddBytesReceived(n int) {
	if n > 0 {
		r.BytesReceivedTotal.Add(uint64(n))
	}
}

// AddBytesSent tracks outgoing network payload bytes.
func (r *Registry) AddBytesSent(n int) {
	if n > 0 {
		r.BytesSentTotal.Add(uint64(n))
	}
}

// SetRetainedCount sets the gauge of stored retained messages.
func (r *Registry) SetRetainedCount(count int64) {
	r.RetainedMessagesCount.Store(count)
}

// SetOfflineCount sets the gauge of stored offline messages.
func (r *Registry) SetOfflineCount(count int64) {
	r.OfflineMessagesCount.Store(count)
}

// IncRateLimitDropped tracks messages or connections rejected by rate limiter.
func (r *Registry) IncRateLimitDropped(dropType string) {
	if dropType == "conn" {
		r.RateLimitDroppedConn.Add(1)
	} else {
		r.RateLimitDroppedPublish.Add(1)
	}
}

// SetClusterNodesOnline sets the gauge of online cluster peer nodes.
func (r *Registry) SetClusterNodesOnline(count int64) {
	r.ClusterNodesOnline.Store(count)
}

// IncClusterRoutedMsgs tracks messages routed across cluster nodes.
func (r *Registry) IncClusterRoutedMsgs(count uint64) {
	r.ClusterRoutedMsgsTotal.Add(count)
}

// WritePrometheus writes standard Prometheus/OpenMetrics text exposition format.
func (r *Registry) WritePrometheus(w io.Writer) error {
	format := `# HELP mqtt_connections_active Number of currently active MQTT connections.
# TYPE mqtt_connections_active gauge
mqtt_connections_active{transport="tcp"} %d
mqtt_connections_active{transport="tls"} %d
mqtt_connections_active{transport="ws"} %d
mqtt_connections_active{transport="quic"} %d

# HELP mqtt_connect_total Total number of MQTT connection attempts.
# TYPE mqtt_connect_total counter
mqtt_connect_total{status="success"} %d
mqtt_connect_total{status="rejected"} %d
mqtt_connect_total{status="auth_failed"} %d

# HELP mqtt_disconnect_total Total number of MQTT client disconnections.
# TYPE mqtt_disconnect_total counter
mqtt_disconnect_total{reason="clean"} %d
mqtt_disconnect_total{reason="timeout"} %d
mqtt_disconnect_total{reason="error"} %d

# HELP mqtt_messages_received_total Total number of MQTT PUBLISH messages received by broker.
# TYPE mqtt_messages_received_total counter
mqtt_messages_received_total{qos="0"} %d
mqtt_messages_received_total{qos="1"} %d
mqtt_messages_received_total{qos="2"} %d

# HELP mqtt_messages_sent_total Total number of MQTT PUBLISH messages sent to subscribers.
# TYPE mqtt_messages_sent_total counter
mqtt_messages_sent_total{qos="0"} %d
mqtt_messages_sent_total{qos="1"} %d
mqtt_messages_sent_total{qos="2"} %d

# HELP mqtt_bytes_received_total Total bytes received by the broker.
# TYPE mqtt_bytes_received_total counter
mqtt_bytes_received_total %d

# HELP mqtt_bytes_sent_total Total bytes sent by the broker.
# TYPE mqtt_bytes_sent_total counter
mqtt_bytes_sent_total %d

# HELP mqtt_retained_messages_count Current number of retained messages stored.
# TYPE mqtt_retained_messages_count gauge
mqtt_retained_messages_count %d

# HELP mqtt_offline_messages_count Current number of offline queued messages stored.
# TYPE mqtt_offline_messages_count gauge
mqtt_offline_messages_count %d

# HELP mqtt_rate_limit_dropped_total Total number of messages or connections dropped by rate limiting.
# TYPE mqtt_rate_limit_dropped_total counter
mqtt_rate_limit_dropped_total{type="conn"} %d
mqtt_rate_limit_dropped_total{type="publish"} %d

# HELP mqtt_cluster_nodes_online Number of currently connected active cluster peers.
# TYPE mqtt_cluster_nodes_online gauge
mqtt_cluster_nodes_online %d

# HELP mqtt_cluster_routed_messages_total Total messages routed across cluster nodes.
# TYPE mqtt_cluster_routed_messages_total counter
mqtt_cluster_routed_messages_total %d
`
	_, err := fmt.Fprintf(w, format,
		r.ConnsActiveTCP.Load(),
		r.ConnsActiveTLS.Load(),
		r.ConnsActiveWS.Load(),
		r.ConnsActiveQUIC.Load(),
		r.ConnectSuccessTotal.Load(),
		r.ConnectRejectedTotal.Load(),
		r.ConnectAuthFailTotal.Load(),
		r.DisconnectCleanTotal.Load(),
		r.DisconnectTimeoutTotal.Load(),
		r.DisconnectErrorTotal.Load(),
		r.MsgsReceivedQoS0.Load(),
		r.MsgsReceivedQoS1.Load(),
		r.MsgsReceivedQoS2.Load(),
		r.MsgsSentQoS0.Load(),
		r.MsgsSentQoS1.Load(),
		r.MsgsSentQoS2.Load(),
		r.BytesReceivedTotal.Load(),
		r.BytesSentTotal.Load(),
		r.RetainedMessagesCount.Load(),
		r.OfflineMessagesCount.Load(),
		r.RateLimitDroppedConn.Load(),
		r.RateLimitDroppedPublish.Load(),
		r.ClusterNodesOnline.Load(),
		r.ClusterRoutedMsgsTotal.Load(),
	)
	return err
}

// Handler returns a standard http.Handler for scraping /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = r.WritePrometheus(w)
	})
}
