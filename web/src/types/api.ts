export interface ApiResponse<T = any> {
  code: number;
  message: string;
  data: T;
}

export interface OverviewStats {
  node_name: string;
  version: string;
  uptime_seconds: number;
  active_connections: number;
  tcp_connections: number;
  tls_connections: number;
  ws_connections: number;
  quic_connections: number;
  subscriptions_count: number;
  retained_count: number;
  total_msg_received: number;
  total_msg_sent: number;
  total_bytes_in: number;
  total_bytes_out: number;
  rate_limit_dropped: number;
  cluster_nodes_count: number;
  os: string;
  arch: string;
  go_version: string;
  goroutines: number;
  memory_alloc_mb: number;
  memory_sys_mb: number;
  num_cpu: number;
}

export interface ClientSummary {
  client_id: string;
  username: string;
  ip_address: string;
  protocol: string;
  transport: string; // "tcp", "tls", "ws", "quic"
  connected_at: string;
  keep_alive: number;
  clean_session: boolean;
  subscriptions_count: number;
}

export interface ClientDetail extends ClientSummary {
  subscriptions: string[];
  will_topic?: string;
  is_tls: boolean;
  client_cert_cn?: string;
}

export interface SubscriptionSummary {
  client_id: string;
  topic: string;
  qos: number;
}

export interface RetainedSummary {
  topic: string;
  qos: number;
  size: number;
  payload: string;
  updated_at: string;
}

export interface ListenerSummary {
  name: string;
  protocol: string;
  address: string;
  status: string;
  active_conns: number;
}

export interface ClusterNodeSummary {
  node_id: string;
  address: string;
  status: 'alive' | 'suspect' | 'dead';
  last_seen: string;
}

export interface ClusterSummary {
  self_node_id: string;
  self_addr: string;
  nodes: ClusterNodeSummary[];
}

export interface PipelineSummary {
  enabled: boolean;
  sink_driver: string;
  circuit_breaker: 'Closed' | 'Open' | 'Half-Open';
  ingested_total: number;
  direct_sent: number;
  spooled_total: number;
  drained_total: number;
  dropped_total: number;
  disk_usage_mb: number;
  max_disk_quota_gb: number;
}

export interface PaginatedResult<T> {
  items: T[];
  total: number;
  page: number;
  limit: number;
}

export interface PublishRequest {
  topic: string;
  qos: number;
  retain: boolean;
  payload: string;
}

export interface LoginResponse {
  token: string;
  expires_at: string;
  username: string;
}

export interface UserProfile {
  username: string;
  role: string;
}

export interface Bridge {
  id: string;
  name: string;
  type: string;
  servers: string[];
  config?: Record<string, any>;
}

export interface BridgeStatus extends Bridge {
  status: 'connected' | 'disconnected' | string;
  circuit_breaker: 'Closed' | 'Open' | 'Half-Open' | string;
  delivered_total: number;
  pending_total: number;
  dropped_total: number;
  latency_ms: number;
}

export interface RuleAction {
  bridge_id: string;
  target_topic: string;
  key_strategy: 'client_id' | 'topic' | 'none';
}

export interface Rule {
  id: string;
  name: string;
  description?: string;
  enabled: boolean;
  topic_filters: string[];
  payload_format?: 'raw' | 'json';
  actions?: RuleAction[];

  // Backward compatibility fields
  target_topic?: string;
  key_strategy?: 'client_id' | 'topic' | 'none';
  sink_type?: string;
  sink_config?: Record<string, any>;
}

export interface RuleStatus extends Rule {
  connected: boolean;
  circuit_breaker: 'Closed' | 'Open' | 'Half-Open';
  total_matched: number;
  total_delivered: number;
  queue_pending: number;
  total_dropped: number;
}

export interface RulePingResult {
  success: boolean;
  latency_ms: number;
  target: string;
  message: string;
}

export interface MatchTestResult {
  matched_rule_ids: string[];
}
