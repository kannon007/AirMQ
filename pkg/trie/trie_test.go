package trie

import (
	"fmt"
	"testing"
)

func TestTopicTreeBasic(t *testing.T) {
	tree := NewTopicTree(nil)

	tree.Subscribe("sensor/temperature/living", "client1", 0)
	tree.Subscribe("sensor/+/living", "client2", 1)
	tree.Subscribe("sensor/#", "client3", 2)
	tree.Subscribe("#", "client4", 0)

	// Publish to sensor/temperature/living: should match all 4 clients
	subs := tree.Match("sensor/temperature/living")
	if len(subs) != 4 {
		t.Fatalf("Expected 4 subscribers, got %d", len(subs))
	}

	matchMap := make(map[string]byte)
	for _, s := range subs {
		matchMap[s.ClientID] = s.QoS
	}

	if matchMap["client1"] != 0 || matchMap["client2"] != 1 || matchMap["client3"] != 2 || matchMap["client4"] != 0 {
		t.Errorf("Unexpected QoS values: %+v", matchMap)
	}

	// Publish to sensor/humidity/living: client1 shouldn't match, but 2, 3, 4 should
	subs2 := tree.Match("sensor/humidity/living")
	if len(subs2) != 3 {
		t.Fatalf("Expected 3 subscribers, got %d", len(subs2))
	}

	// Publish to other/status: only '#' (client4) matches
	subs3 := tree.Match("other/status")
	if len(subs3) != 1 || subs3[0].ClientID != "client4" {
		t.Fatalf("Expected client4 only, got %+v", subs3)
	}

	// Test Unsubscribe
	tree.Unsubscribe("sensor/temperature/living", "client1")
	subsAfter := tree.Match("sensor/temperature/living")
	if len(subsAfter) != 3 {
		t.Fatalf("Expected 3 subscribers after unsub, got %d", len(subsAfter))
	}
}

func TestSharedSubscriptions(t *testing.T) {
	tree := NewTopicTree(nil)

	// Two clients in the same shared group "workers"
	tree.Subscribe("$share/workers/tasks/#", "worker-1", 1)
	tree.Subscribe("$share/workers/tasks/#", "worker-2", 1)

	// One regular independent subscriber
	tree.Subscribe("tasks/#", "auditor", 0)

	counts := map[string]int{"worker-1": 0, "worker-2": 0, "auditor": 0}

	// Publish 100 messages to tasks/job/1
	for i := 0; i < 100; i++ {
		matched := tree.Match("tasks/job/1")
		if len(matched) != 2 {
			t.Fatalf("Expected exactly 2 subscribers (1 shared + 1 auditor), got %d", len(matched))
		}
		for _, m := range matched {
			counts[m.ClientID]++
		}
	}

	// Auditor must have received all 100
	if counts["auditor"] != 100 {
		t.Errorf("Auditor expected 100 messages, got %d", counts["auditor"])
	}

	// Shared workers should share evenly (50 each in round-robin)
	if counts["worker-1"] != 50 || counts["worker-2"] != 50 {
		t.Errorf("Shared workers not balanced: %+v", counts)
	}
}

func TestRouter_AllDataWildcardAndShared(t *testing.T) {
	router := NewRouter(nil)

	// 1. Regular subscriber to ALL data: '#'
	router.Subscribe("#", "all-data-consumer", 1)

	// 2. Shared subscription subscribers for backend service cluster: '$share/backend/#'
	router.Subscribe("$share/backend/#", "srv-node-1", 1)
	router.Subscribe("$share/backend/#", "srv-node-2", 1)

	counts := map[string]int{
		"all-data-consumer": 0,
		"srv-node-1":        0,
		"srv-node-2":        0,
	}

	testTopics := []string{
		"device/sensor/temperature",
		"factory/machine/status",
		"cars/telemetry/speed",
		"alerts/fire",
		"orders/payment/success",
		"home/living_room/light",
	}

	for _, top := range testTopics {
		matched := router.Match(top)
		// Should match 2: all-data-consumer + exactly 1 of the shared nodes
		if len(matched) != 2 {
			t.Fatalf("Topic %s expected 2 subscribers, got %d: %+v", top, len(matched), matched)
		}
		for _, m := range matched {
			counts[m.ClientID]++
		}
	}

	// all-data-consumer should receive every single message (6/6)
	if counts["all-data-consumer"] != len(testTopics) {
		t.Errorf("all-data-consumer expected %d messages, got %d", len(testTopics), counts["all-data-consumer"])
	}

	// Shared cluster should balance the 6 messages across the 2 nodes
	if counts["srv-node-1"]+counts["srv-node-2"] != len(testTopics) {
		t.Errorf("Shared nodes sum mismatch: srv-1=%d, srv-2=%d", counts["srv-node-1"], counts["srv-node-2"])
	}
	if counts["srv-node-1"] == 0 || counts["srv-node-2"] == 0 {
		t.Errorf("Shared nodes should both receive messages, got: %+v", counts)
	}
}

func TestTopicTree_DollarSystemTopic_WildcardIsolation(t *testing.T) {
	tree := NewTopicTree(nil)

	// Sub1 subscribes to '#' (global wildcard)
	tree.Subscribe("#", "wildcard-sub", 1)
	// Sub2 subscribes to '+/+' (single level wildcards)
	tree.Subscribe("+/+", "plus-sub", 1)
	// Sub3 explicitly subscribes to '$SYS/#'
	tree.Subscribe("$SYS/#", "sys-sub", 1)

	// 1. Publishing to a $SYS topic: only Sub3 should match!
	matchedSys := tree.Match("$SYS/broker/uptime")
	if len(matchedSys) != 1 || matchedSys[0].ClientID != "sys-sub" {
		t.Fatalf("Expected only sys-sub for $SYS topic, got: %+v", matchedSys)
	}

	// 2. Publishing to normal topic: Sub1 and Sub2 should match, Sub3 must not!
	matchedNormal := tree.Match("sensor/temp")
	if len(matchedNormal) != 2 {
		t.Fatalf("Expected 2 subscribers for sensor/temp, got: %d", len(matchedNormal))
	}
	foundSys := false
	for _, m := range matchedNormal {
		if m.ClientID == "sys-sub" {
			foundSys = true
		}
	}
	if foundSys {
		t.Fatalf("sys-sub should not match normal topic sensor/temp")
	}
}

func BenchmarkTopicTreeMatch(b *testing.B) {
	tree := NewTopicTree(nil)
	// Seed 1000 subscriptions
	for i := 0; i < 1000; i++ {
		tree.Subscribe("sensor/+/reading", "c1", 1)
		tree.Subscribe("sensor/temperature/#", "c2", 2)
		tree.Subscribe("device/status", "c3", 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tree.Match("sensor/temperature/reading")
	}
}

func BenchmarkRouterFastPath(b *testing.B) {
	router := NewRouter(nil)
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("device/sensor/%d", i)
		router.Subscribe(topic, "sub_client_1", 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = router.Match("device/sensor/500")
	}
}

func BenchmarkRouterMatchBytes(b *testing.B) {
	router := NewRouter(nil)
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("device/sensor/%d", i)
		router.Subscribe(topic, "sub_client_1", 0)
	}
	rawTopic := []byte("device/sensor/500")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = router.MatchBytes(rawTopic)
	}
}
