package session

import (
	"sync"
	"testing"
	"time"

	"mqtt/pkg/protocol"
)

func TestPacketIDAllocator(t *testing.T) {
	alloc := NewPacketIDAllocator()

	id1, err := alloc.Allocate()
	if err != nil || id1 != 1 {
		t.Fatalf("Expected ID 1, got %d, err: %v", id1, err)
	}

	id2, err := alloc.Allocate()
	if err != nil || id2 != 2 {
		t.Fatalf("Expected ID 2, got %d, err: %v", id2, err)
	}

	if !alloc.IsInUse(1) {
		t.Errorf("Expected ID 1 in use")
	}

	alloc.Release(1)
	if alloc.IsInUse(1) {
		t.Errorf("Expected ID 1 freed")
	}

	// Should reallocate 1 or continue sequentially
	id3, err := alloc.Allocate()
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if id3 != 1 && id3 != 3 {
		t.Errorf("Expected ID 1 or 3, got %d", id3)
	}
}

func TestInflightQueue(t *testing.T) {
	q := NewInflightQueue(16)

	msg := &InflightMessage{
		PacketID: 100,
		Packet: &protocol.PublishPacket{
			Topic: "test/topic",
		},
	}

	if err := q.Push(msg); err != nil {
		t.Fatalf("Push error: %v", err)
	}

	if q.Len() != 1 {
		t.Fatalf("Expected Len 1, got %d", q.Len())
	}

	acked, ok := q.Ack(100)
	if !ok || acked == nil || acked.PacketID != 100 {
		t.Fatalf("Ack failed, ok: %v, acked: %+v", ok, acked)
	}

	if q.Len() != 0 {
		t.Fatalf("Expected Len 0 after ack, got %d", q.Len())
	}
}

func TestConcurrentSessionManager(t *testing.T) {
	sm := NewSessionManager()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := "client"
			s, _ := sm.GetOrSet(cid, false)
			s.AddSubscription("sensor/test", 1)
			_ = s.GetSubscriptions()
		}(i)
	}

	wg.Wait()
	sess, found := sm.Get("client")
	if !found {
		t.Fatalf("Expected to find session for 'client'")
	}
	subs := sess.GetSubscriptions()
	if subs["sensor/test"] != 1 {
		t.Fatalf("Subscription missing or incorrect QoS: %+v", subs)
	}
}

func TestSessionExpiry(t *testing.T) {
	sm := NewSessionManager()
	s, _ := sm.GetOrSet("expiring_client", false)
	s.AddSubscription("test/topic", 1)

	// Currently connected
	if s.IsExpired(time.Now()) {
		t.Fatalf("Connected session should not be expired")
	}

	// Disconnected with 2s expiry
	now := time.Now()
	s.SetDisconnected(now, 2)

	// Not yet expired after 1s
	if s.IsExpired(now.Add(1 * time.Second)) {
		t.Fatalf("Session should not be expired after 1s")
	}

	// Expired after 2s
	if !s.IsExpired(now.Add(2 * time.Second)) {
		t.Fatalf("Session should be expired after 2s")
	}

	// Reconnect resets expiry
	s.SetConnected()
	if s.IsExpired(now.Add(5 * time.Second)) {
		t.Fatalf("Reconnected session should not be expired")
	}

	// Never expires with 0xFFFFFFFF
	s.SetDisconnected(now, 0xFFFFFFFF)
	if s.IsExpired(now.Add(1000 * time.Hour)) {
		t.Fatalf("Session with 0xFFFFFFFF expiry should never expire")
	}
}

func TestCleanExpiredSessions(t *testing.T) {
	sm := NewSessionManager()
	s1, _ := sm.GetOrSet("client1", false)
	s2, _ := sm.GetOrSet("client2", false)
	s3, _ := sm.GetOrSet("client3", false)

	now := time.Now()
	s1.SetDisconnected(now.Add(-10*time.Second), 5) // Expired 5s ago
	s2.SetDisconnected(now, 60)                     // Still valid for 60s
	// s3 remains connected
	_ = s3

	expired := sm.CleanExpiredSessions(now)
	if len(expired) != 1 || expired[0].ClientID != "client1" {
		t.Fatalf("Expected only client1 to be expired, got %v", expired)
	}

	if _, found := sm.Get("client1"); found {
		t.Fatalf("client1 should have been deleted from manager")
	}
	if _, found := sm.Get("client2"); !found {
		t.Fatalf("client2 should still be in manager")
	}
	if _, found := sm.Get("client3"); !found {
		t.Fatalf("client3 should still be in manager")
	}
}

