package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRuleEngine_Matching(t *testing.T) {
	rule := Rule{
		ID:           "test_rule",
		Enabled:      true,
		TopicFilters: []string{"telemetry/#", "sensor/+/temp", "alerts/critical"},
		TargetTopic:  "dest_events",
		KeyStrategy:  "client_id",
		SinkType:     "kafka",
	}

	re := NewRuleEngine("", &rule)

	cases := []struct {
		topic   string
		matched bool
	}{
		{"telemetry", true},
		{"telemetry/v1/sensor1", true},
		{"sensor/001/temp", true},
		{"sensor/002/temp", true},
		{"sensor/001/voltage", false},
		{"alerts/critical", true},
		{"alerts/info", false},
		{"other/topic", false},
	}

	for _, c := range cases {
		recs := re.Evaluate("dev_123", c.topic, []byte("hello"), 1)
		got := len(recs) > 0
		if got != c.matched {
			t.Errorf("Topic %s: expected match=%v, got=%v", c.topic, c.matched, got)
		}
		for _, r := range recs {
			ReleaseRecord(r)
		}
	}
}

func TestRuleEngine_DisabledShortCircuit(t *testing.T) {
	rule := Rule{
		ID:           "test_rule",
		Enabled:      false,
		TopicFilters: []string{"#"},
	}

	re := NewRuleEngine("", &rule)
	recs := re.Evaluate("dev_1", "any/topic", []byte("data"), 0)
	if len(recs) != 0 {
		t.Fatalf("Expected 0 records when disabled, got %d", len(recs))
	}
}

func TestRuleEngine_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "rule_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	rulePath := filepath.Join(tmpDir, "rules.json")

	r1 := Rule{
		ID:           "r1",
		Enabled:      true,
		TopicFilters: []string{"iot/#"},
		TargetTopic:  "iot_topic",
		SinkType:     "kafka",
		SinkConfig:   map[string]any{"brokers": []string{"localhost:9092"}},
	}

	re1 := NewRuleEngine(rulePath, &r1)
	if err := re1.UpdateRule(r1); err != nil {
		t.Fatal(err)
	}

	// Read from disk with new engine instance
	re2 := NewRuleEngine(rulePath, nil)
	rules := re2.GetRules()
	if len(rules) != 1 || rules[0].ID != "r1" || len(rules[0].TopicFilters) != 1 || rules[0].TopicFilters[0] != "iot/#" {
		t.Fatalf("Failed to recover rules from persistence: %+v", rules)
	}
}

func BenchmarkRuleEngine_Match(b *testing.B) {
	rule := Rule{
		ID:            "bench_rule",
		Enabled:       true,
		TopicFilters:  []string{"telemetry/#", "sensor/+/data"},
		TargetTopic:   "bench_target",
		KeyStrategy:   "client_id",
		PayloadFormat: "raw",
		SinkType:      "kafka",
	}

	re := NewRuleEngine("", &rule)
	payload := []byte("bench_payload")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		recs := re.Evaluate("client_abc", "telemetry/device1/metrics", payload, 0)
		for _, r := range recs {
			ReleaseRecord(r)
		}
	}
}
