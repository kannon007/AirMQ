package pipeline

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Bridge represents a unified external MQ connection resource (e.g. Kafka cluster, RabbitMQ).
type Bridge struct {
	ID      string         `json:"id"`      // Unique ID, e.g. "bridge_kafka_default"
	Name    string         `json:"name"`    // Friendly display name, e.g. "Kafka 生产集群"
	Type    string         `json:"type"`    // Driver type: "kafka", "stdout", "mock"
	Servers []string       `json:"servers"` // Target broker addresses: ["127.0.0.1:9092"]
	Config  map[string]any `json:"config"`  // Driver specific configurations
}

// BridgeStatus represents runtime connectivity and metrics for a Bridge.
type BridgeStatus struct {
	Bridge
	Status         string `json:"status"`          // "connected", "disconnected"
	CircuitBreaker string `json:"circuit_breaker"` // "Closed", "Open", "Half-Open"
	DeliveredTotal uint64 `json:"delivered_total"`
	PendingTotal   int    `json:"pending_total"`
	DroppedTotal   uint64 `json:"dropped_total"`
	LatencyMs      int64  `json:"latency_ms"`
}

// RuleAction defines the downstream bridge and routing target for a rule.
type RuleAction struct {
	BridgeID    string `json:"bridge_id"`    // Reference to a configured Bridge.ID
	TargetTopic string `json:"target_topic"` // Destination topic (e.g. "mqtt_events" or "${topic}")
	KeyStrategy string `json:"key_strategy"` // Partition key: "client_id", "topic", "none"
}

// Rule defines an MQTT forwarding rule with topic filtering and target actions.
type Rule struct {
	ID            string       `json:"id"`             // Unique rule ID (e.g. "rule_default")
	Name          string       `json:"name"`           // Friendly display name
	Description   string       `json:"description"`    // Purpose or notes for operators
	Enabled       bool         `json:"enabled"`        // Whether forwarding is active
	TopicFilters  []string     `json:"topic_filters"`  // Which MQTT topics to match (e.g. ["telemetry/#"])
	PayloadFormat string       `json:"payload_format"` // "raw" or "json"
	Actions       []RuleAction `json:"actions"`        // Target actions forwarding to bridges

	// Backward compatibility fields
	TargetTopic string         `json:"target_topic,omitempty"`
	KeyStrategy string         `json:"key_strategy,omitempty"`
	SinkType    string         `json:"sink_type,omitempty"`
	SinkConfig  map[string]any `json:"sink_config,omitempty"`
}

// RuleStatus represents runtime statistics for a rule.
type RuleStatus struct {
	Rule
	Connected      bool   `json:"connected"`
	CircuitBreaker string `json:"circuit_breaker"`
	TotalMatched   uint64 `json:"total_matched"`
	TotalDelivered uint64 `json:"total_delivered"`
	QueuePending   int    `json:"queue_pending"`
	TotalDropped   uint64 `json:"total_dropped"`
}

// RulePingResult is the latency and status result of a connectivity probe.
type RulePingResult struct {
	Success   bool   `json:"success"`
	LatencyMs int64  `json:"latency_ms"`
	Target    string `json:"target"`
	Message   string `json:"message"`
}

// CompiledRule encapsulates pre-computed lookup structures for zero-allocation matching.
type CompiledRule struct {
	Rule     Rule
	allMatch bool
	exactMap map[string]struct{}
	prefixes []string
	patterns []string
	matched  atomic.Uint64
}

func compileRule(r Rule) *CompiledRule {
	cr := &CompiledRule{
		Rule:     r,
		exactMap: make(map[string]struct{}),
	}

	filters := r.TopicFilters
	if len(filters) == 0 {
		cr.allMatch = true
		return cr
	}

	for _, f := range filters {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if f == "#" {
			cr.allMatch = true
		} else if strings.HasSuffix(f, "/#") {
			prefix := strings.TrimSuffix(f, "/#")
			cr.prefixes = append(cr.prefixes, prefix)
		} else if strings.Contains(f, "+") {
			cr.patterns = append(cr.patterns, f)
		} else {
			cr.exactMap[f] = struct{}{}
		}
	}

	return cr
}

// Matches tests if the given MQTT topic matches this rule with zero allocations.
func (cr *CompiledRule) Matches(topic string) bool {
	if !cr.Rule.Enabled {
		return false
	}
	if cr.allMatch {
		return true
	}
	if _, ok := cr.exactMap[topic]; ok {
		return true
	}
	for _, pfx := range cr.prefixes {
		if topic == pfx {
			return true
		}
		if strings.HasPrefix(topic, pfx) && len(topic) > len(pfx) && topic[len(pfx)] == '/' {
			return true
		}
	}
	for _, pat := range cr.patterns {
		if matchPattern(pat, topic) {
			return true
		}
	}
	return false
}

func matchPattern(filter, topic string) bool {
	fParts := strings.Split(filter, "/")
	tParts := strings.Split(topic, "/")
	if len(fParts) != len(tParts) {
		return false
	}
	for i := 0; i < len(fParts); i++ {
		if fParts[i] == "+" {
			continue
		}
		if fParts[i] != tParts[i] {
			return false
		}
	}
	return true
}

// Format transforms MQTT metadata into target destination values.
func (cr *CompiledRule) Format(clientID, topic string, payload []byte, qos byte) (targetTopic string, key []byte, value []byte) {
	// 1. Determine destination topic and key strategy from primary action
	targetTopicTpl := cr.Rule.TargetTopic
	keyStrategy := cr.Rule.KeyStrategy
	if len(cr.Rule.Actions) > 0 {
		if cr.Rule.Actions[0].TargetTopic != "" {
			targetTopicTpl = cr.Rule.Actions[0].TargetTopic
		}
		if cr.Rule.Actions[0].KeyStrategy != "" {
			keyStrategy = cr.Rule.Actions[0].KeyStrategy
		}
	}

	// 2. Destination Topic
	if targetTopicTpl == "" || targetTopicTpl == "${topic}" {
		targetTopic = strings.ReplaceAll(topic, "/", ".")
	} else {
		targetTopic = targetTopicTpl
	}

	// 3. Partition Key
	switch keyStrategy {
	case "client_id":
		key = StringToBytes(clientID)
	case "topic":
		key = StringToBytes(topic)
	default:
		key = nil
	}

	// 4. Payload Format
	if cr.Rule.PayloadFormat == "json" {
		env := struct {
			ClientID  string `json:"client_id"`
			Topic     string `json:"topic"`
			QoS       byte   `json:"qos"`
			Payload   string `json:"payload"`
			Timestamp int64  `json:"timestamp"`
		}{
			ClientID:  clientID,
			Topic:     topic,
			QoS:       qos,
			Payload:   BytesToString(payload),
			Timestamp: time.Now().UnixMilli(),
		}
		buf, err := json.Marshal(env)
		if err == nil {
			value = buf
		} else {
			value = payload
		}
	} else {
		// "raw": zero-copy direct wire bytes
		value = payload
	}

	return targetTopic, key, value
}

// CompiledRuleSet is an immutable snapshot of precompiled rules for lock-free read access.
type CompiledRuleSet struct {
	rules []*CompiledRule
}

type dataIntegrationPersist struct {
	Bridges []Bridge `json:"bridges"`
	Rules   []Rule   `json:"rules"`
}

// RuleEngine coordinates topic matching, message formatting, and bridge associations.
type RuleEngine struct {
	mu          sync.RWMutex
	bridges     []Bridge
	rules       []Rule
	compiled    atomic.Pointer[CompiledRuleSet]
	hasEnabled  atomic.Bool
	persistFile string
	seq         atomic.Uint64
}

// NewRuleEngine initializes the rule engine with optional persistence.
func NewRuleEngine(persistFile string, defaultRule *Rule) *RuleEngine {
	re := &RuleEngine{
		persistFile: persistFile,
	}

	loaded := false
	if persistFile != "" {
		// Try data_integration.json or legacy rules.json
		if data, err := os.ReadFile(persistFile); err == nil {
			// First try unified struct
			var unified dataIntegrationPersist
			if err := json.Unmarshal(data, &unified); err == nil && (len(unified.Rules) > 0 || len(unified.Bridges) > 0) {
				re.bridges = unified.Bridges
				re.rules = unified.Rules
				loaded = true
			} else {
				// Fallback: try raw array of rules
				var rList []Rule
				if err := json.Unmarshal(data, &rList); err == nil && len(rList) > 0 {
					re.rules = rList
					loaded = true
				}
			}
		}
	}

	if !loaded {
		re.bridges = []Bridge{DefaultBridge(), DefaultRedpandaBridge(), DefaultNATSBridge()}
		if defaultRule != nil {
			re.rules = []Rule{*defaultRule}
		} else {
			re.rules = []Rule{DefaultRule()}
		}
		re.saveToDisk()
	} else {
		// Ensure default bridges exist if bridges list is empty
		if len(re.bridges) == 0 {
			re.bridges = []Bridge{DefaultBridge(), DefaultRedpandaBridge(), DefaultNATSBridge()}
		}
		// Ensure rule actions point to a bridge
		for i := range re.rules {
			if len(re.rules[i].Actions) == 0 {
				bridgeID := "bridge_kafka_default"
				if len(re.bridges) > 0 {
					bridgeID = re.bridges[0].ID
				}
				re.rules[i].Actions = []RuleAction{
					{
						BridgeID:    bridgeID,
						TargetTopic: re.rules[i].TargetTopic,
						KeyStrategy: re.rules[i].KeyStrategy,
					},
				}
			}
		}
	}

	re.recompile()
	return re
}

// DefaultBridge returns standard Kafka bridge config.
func DefaultBridge() Bridge {
	return Bridge{
		ID:      "bridge_kafka_default",
		Name:    "Kafka 默认集群",
		Type:    "kafka",
		Servers: []string{"127.0.0.1:9092"},
		Config: map[string]any{
			"topic": "mqtt_events",
		},
	}
}

// DefaultRedpandaBridge returns standard Redpanda bridge config.
func DefaultRedpandaBridge() Bridge {
	return Bridge{
		ID:      "bridge_redpanda_default",
		Name:    "Redpanda 极速集群",
		Type:    "redpanda",
		Servers: []string{"127.0.0.1:9092"},
		Config: map[string]any{
			"topic": "mqtt_events",
		},
	}
}

// DefaultNATSBridge returns standard NATS JetStream bridge config.
func DefaultNATSBridge() Bridge {
	return Bridge{
		ID:      "bridge_nats_default",
		Name:    "NATS JetStream 消息流",
		Type:    "nats",
		Servers: []string{"127.0.0.1:4222"},
		Config: map[string]any{
			"subject": "mqtt_telemetry",
			"stream":  "MQTT_STREAM",
		},
	}
}

// DefaultRule returns a preconfigured out-of-the-box rule forwarding to DefaultBridge.
func DefaultRule() Rule {
	return Rule{
		ID:            "rule_default",
		Name:          "默认全量消息流转",
		Description:   "将所有设备与客户端上报的消息流转至默认 Kafka 集群",
		Enabled:       true,
		TopicFilters:  []string{"#"},
		PayloadFormat: "raw",
		Actions: []RuleAction{
			{
				BridgeID:    "bridge_kafka_default",
				TargetTopic: "mqtt_events",
				KeyStrategy: "client_id",
			},
		},
		TargetTopic: "mqtt_events",
		KeyStrategy: "client_id",
		SinkType:    "kafka",
		SinkConfig: map[string]any{
			"brokers": []string{"127.0.0.1:9092"},
		},
	}
}

func (re *RuleEngine) recompile() {
	re.mu.RLock()
	rulesCopy := make([]Rule, len(re.rules))
	copy(rulesCopy, re.rules)
	re.mu.RUnlock()

	var compiled []*CompiledRule
	anyEnabled := false
	for _, r := range rulesCopy {
		if r.Enabled {
			anyEnabled = true
		}
		compiled = append(compiled, compileRule(r))
	}

	set := &CompiledRuleSet{rules: compiled}
	re.compiled.Store(set)
	re.hasEnabled.Store(anyEnabled)
}

// Evaluate performs zero-allocation rule matching on the Fast-Path.
func (re *RuleEngine) Evaluate(clientID, topic string, payload []byte, qos byte) []*Record {
	if !re.hasEnabled.Load() {
		return nil
	}

	set := re.compiled.Load()
	if set == nil || len(set.rules) == 0 {
		return nil
	}

	var records []*Record
	for _, cr := range set.rules {
		if cr.Matches(topic) {
			cr.matched.Add(1)
			rec := AcquireRecord()
			rec.ID = re.seq.Add(1)
			rec.Topic = topic
			rec.Target, rec.Key, rec.Value = cr.Format(clientID, topic, payload, qos)
			rec.Timestamp = time.Now()
			rec.QoS = qos
			records = append(records, rec)
		}
	}

	return records
}

// GetBridges returns a snapshot of all configured bridges.
func (re *RuleEngine) GetBridges() []Bridge {
	re.mu.RLock()
	defer re.mu.RUnlock()
	res := make([]Bridge, len(re.bridges))
	copy(res, re.bridges)
	return res
}

// GetBridge finds a bridge by ID.
func (re *RuleEngine) GetBridge(id string) (*Bridge, error) {
	re.mu.RLock()
	defer re.mu.RUnlock()
	for _, b := range re.bridges {
		if b.ID == id {
			copyB := b
			return &copyB, nil
		}
	}
	return nil, fmt.Errorf("bridge %q not found", id)
}

// UpdateBridge updates or inserts a bridge.
func (re *RuleEngine) UpdateBridge(b Bridge) error {
	if b.Type == "" {
		b.Type = "kafka"
	}
	if b.ID == "" {
		b.ID = fmt.Sprintf("bridge_%s_%d", b.Type, time.Now().Unix())
	}
	if b.Name == "" {
		b.Name = b.ID
	}

	re.mu.Lock()
	found := false
	for i, existing := range re.bridges {
		if existing.ID == b.ID {
			re.bridges[i] = b
			found = true
			break
		}
	}
	if !found {
		re.bridges = append(re.bridges, b)
	}
	re.mu.Unlock()

	re.saveToDisk()
	return nil
}

// DeleteBridge removes a bridge by ID.
func (re *RuleEngine) DeleteBridge(id string) error {
	re.mu.Lock()
	defer re.mu.Unlock()

	idx := -1
	for i, b := range re.bridges {
		if b.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("bridge %q not found", id)
	}

	re.bridges = append(re.bridges[:idx], re.bridges[idx+1:]...)
	re.saveToDisk()
	return nil
}

// GetRules returns a snapshot of all configured rules.
func (re *RuleEngine) GetRules() []Rule {
	re.mu.RLock()
	defer re.mu.RUnlock()
	res := make([]Rule, len(re.rules))
	copy(res, re.rules)
	return res
}

// GetRule finds a rule by ID.
func (re *RuleEngine) GetRule(id string) (*Rule, error) {
	re.mu.RLock()
	defer re.mu.RUnlock()
	for _, r := range re.rules {
		if r.ID == id {
			copyR := r
			return &copyR, nil
		}
	}
	return nil, fmt.Errorf("rule %q not found", id)
}

// UpdateRule updates or inserts a rule, re-compiling the matcher and saving to disk.
func (re *RuleEngine) UpdateRule(r Rule) error {
	if r.ID == "" {
		r.ID = "rule_default"
	}
	if r.Name == "" {
		r.Name = r.ID
	}
	if len(r.Actions) == 0 {
		// Auto-populate action for backwards compatibility
		target := r.TargetTopic
		if target == "" {
			target = "mqtt_events"
		}
		keyStrat := r.KeyStrategy
		if keyStrat == "" {
			keyStrat = "client_id"
		}
		r.Actions = []RuleAction{
			{
				BridgeID:    "bridge_kafka_default",
				TargetTopic: target,
				KeyStrategy: keyStrat,
			},
		}
	}

	// Keep backward compatibility fields in sync
	if len(r.Actions) > 0 {
		if r.TargetTopic == "" {
			r.TargetTopic = r.Actions[0].TargetTopic
		}
		if r.KeyStrategy == "" {
			r.KeyStrategy = r.Actions[0].KeyStrategy
		}
	}

	re.mu.Lock()
	found := false
	for i, existing := range re.rules {
		if existing.ID == r.ID {
			re.rules[i] = r
			found = true
			break
		}
	}
	if !found {
		re.rules = append(re.rules, r)
	}
	re.mu.Unlock()

	re.recompile()
	re.saveToDisk()
	return nil
}

// DeleteRule removes a rule by ID.
func (re *RuleEngine) DeleteRule(id string) error {
	re.mu.Lock()
	idx := -1
	for i, r := range re.rules {
		if r.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		re.mu.Unlock()
		return fmt.Errorf("rule %q not found", id)
	}

	re.rules = append(re.rules[:idx], re.rules[idx+1:]...)
	re.mu.Unlock()

	re.recompile()
	re.saveToDisk()
	return nil
}

// TestMatch evaluates whether a given topic matches any active rules.
func (re *RuleEngine) TestMatch(topic string) []string {
	set := re.compiled.Load()
	if set == nil {
		return nil
	}
	var matchedIDs []string
	for _, cr := range set.rules {
		if cr.Matches(topic) {
			matchedIDs = append(matchedIDs, cr.Rule.ID)
		}
	}
	return matchedIDs
}

func (re *RuleEngine) saveToDisk() {
	if re.persistFile == "" {
		return
	}

	re.mu.RLock()
	payload := dataIntegrationPersist{
		Bridges: re.bridges,
		Rules:   re.rules,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	re.mu.RUnlock()
	if err != nil {
		log.Printf("[RuleEngine] Failed to marshal data integration config: %v", err)
		return
	}

	dir := filepath.Dir(re.persistFile)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", re.persistFile, time.Now().UnixNano())
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		log.Printf("[RuleEngine] Failed to write temporary config file: %v", err)
		return
	}

	_ = os.Rename(tmpFile, re.persistFile)
}

// MatchTopicFilter checks if topic matches a filter (utility function).
func MatchTopicFilter(filter, topic string) bool {
	if filter == "#" || filter == topic {
		return true
	}
	if strings.HasSuffix(filter, "/#") {
		pfx := strings.TrimSuffix(filter, "/#")
		if topic == pfx || (strings.HasPrefix(topic, pfx) && len(topic) > len(pfx) && topic[len(pfx)] == '/') {
			return true
		}
	}
	if strings.Contains(filter, "+") {
		return matchPattern(filter, topic)
	}
	return false
}

// ExtractBrokers is a helper to pull broker addresses from SinkConfig or slice.
func ExtractBrokers(cfg map[string]any) []string {
	if cfg == nil {
		return nil
	}
	if raw, ok := cfg["brokers"]; ok {
		switch v := raw.(type) {
		case []string:
			return v
		case []any:
			var res []string
			for _, item := range v {
				if s, ok := item.(string); ok && s != "" {
					res = append(res, s)
				}
			}
			return res
		case string:
			if v != "" {
				parts := strings.Split(v, ",")
				var res []string
				for _, p := range parts {
					p = strings.TrimSpace(p)
					if p != "" {
						res = append(res, p)
					}
				}
				return res
			}
		}
	}
	return nil
}
