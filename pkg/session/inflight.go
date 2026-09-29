package session

import (
	"errors"
	"sync"
	"time"

	"mqtt/pkg/protocol"
)

var (
	ErrInflightFull = errors.New("inflight message queue is full")
)

// InflightMessage holds an unacknowledged QoS 1 or 2 packet.
type InflightMessage struct {
	PacketID  uint16
	Packet    *protocol.PublishPacket
	Timestamp time.Time
	Retries   int
}

// InflightQueue is a thread-safe, fixed-size circular ring buffer for inflight messages.
// Avoids slice dynamic reallocation and pointer thrashing under high message rates.
type InflightQueue struct {
	mu       sync.Mutex
	capacity int
	entries  []*InflightMessage
	indexMap map[uint16]int // packetID -> buffer index for O(1) ACK removal
	head     int
	tail     int
	count    int
}

// NewInflightQueue initializes an inflight ring buffer with a maximum capacity.
func NewInflightQueue(capacity int) *InflightQueue {
	if capacity <= 0 {
		capacity = 65535
	}
	return &InflightQueue{
		capacity: capacity,
		entries:  make([]*InflightMessage, capacity),
		indexMap: make(map[uint16]int, capacity),
	}
}

// Push appends an inflight message to the tail of the queue.
func (q *InflightQueue) Push(msg *InflightMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count >= q.capacity {
		return ErrInflightFull
	}

	slot := q.tail
	q.entries[slot] = msg
	q.indexMap[msg.PacketID] = slot
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	return nil
}

// Ack removes the message associated with packetID upon receiving PUBACK or PUBCOMP.
func (q *InflightQueue) Ack(packetID uint16) (*InflightMessage, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	slot, exists := q.indexMap[packetID]
	if !exists {
		return nil, false
	}

	msg := q.entries[slot]
	q.entries[slot] = nil
	delete(q.indexMap, packetID)

	// Note: We leave holes if removed out-of-order, which is standard for MQTT sliding windows.
	// When head meets a nil entry, we advance head.
	for q.count > 0 && q.entries[q.head] == nil {
		q.head = (q.head + 1) % q.capacity
		q.count--
	}

	return msg, true
}

// Len returns the current count of pending inflight messages.
func (q *InflightQueue) Len() int {
	q.mu.Lock()
	c := q.count
	q.mu.Unlock()
	return c
}

// GetPending returns a snapshot of all unacknowledged inflight messages in order.
func (q *InflightQueue) GetPending() []*InflightMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	res := make([]*InflightMessage, 0, q.count)
	for i := 0; i < q.capacity; i++ {
		idx := (q.head + i) % q.capacity
		if q.entries[idx] != nil {
			res = append(res, q.entries[idx])
		}
	}
	return res
}
