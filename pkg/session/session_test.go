package session

import (
	"sync"
	"testing"

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
