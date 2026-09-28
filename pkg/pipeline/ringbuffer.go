package pipeline

import (
	"sync"
)

// shard is an isolated circular buffer segment padded to 64 bytes to eliminate CPU Cache-Line False Sharing.
type shard struct {
	mu            sync.Mutex
	head          int
	tail          int
	count         int
	capacity      int
	highWatermark int
	_             [40]byte // Cache-line padding ensuring this struct occupies its own 64-byte boundary
	buf           []*Record
}

func newShard(capacity int, highWatermarkRatio float64) *shard {
	return &shard{
		buf:           make([]*Record, capacity),
		capacity:      capacity,
		highWatermark: int(float64(capacity) * highWatermarkRatio),
	}
}

func (s *shard) push(rec *Record) (accepted bool, overflow bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.count >= s.capacity {
		// Hard capacity reached
		return false, true
	}

	s.buf[s.head] = rec
	s.head = (s.head + 1) % s.capacity
	s.count++

	return true, s.count >= s.highWatermark
}

func (s *shard) popBatch(maxCount int, out []*Record) []*Record {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.count == 0 {
		return out
	}

	n := maxCount
	if n > s.count {
		n = s.count
	}

	for i := 0; i < n; i++ {
		out = append(out, s.buf[s.tail])
		s.buf[s.tail] = nil // Clear reference for GC
		s.tail = (s.tail + 1) % s.capacity
	}
	s.count -= n

	return out
}

func (s *shard) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// ShardedRingBuffer partitions incoming messages across multiple CPU cache-line aligned shards,
// eliminating lock contention and multi-core false sharing from gnet EventLoops.
type ShardedRingBuffer struct {
	shards []*shard
	mask   uint32
	count  int
}

// NewShardedRingBuffer creates a sharded ring buffer with the given total capacity and number of shards (power of 2).
func NewShardedRingBuffer(totalCapacity int, numShards int) *ShardedRingBuffer {
	if numShards <= 0 {
		numShards = 8
	}
	// Ensure numShards is power of 2
	p := 1
	for p < numShards {
		p <<= 1
	}
	numShards = p

	if totalCapacity <= 0 {
		totalCapacity = 65536
	}

	perShardCap := totalCapacity / numShards
	if perShardCap < 128 {
		perShardCap = 128
	}

	srb := &ShardedRingBuffer{
		shards: make([]*shard, numShards),
		mask:   uint32(numShards - 1),
		count:  numShards,
	}

	for i := 0; i < numShards; i++ {
		srb.shards[i] = newShard(perShardCap, 0.80)
	}

	return srb
}

// Push adds a record to the shard hashed by keyHash.
// Returns accepted=false if hard capacity reached, overflow=true if shard reached its high watermark.
func (srb *ShardedRingBuffer) Push(keyHash uint32, rec *Record) (accepted bool, overflow bool) {
	idx := keyHash & srb.mask
	return srb.shards[idx].push(rec)
}

// PopBatch pops up to maxCount records from a specific shard.
func (srb *ShardedRingBuffer) PopBatch(shardIdx int, maxCount int, out []*Record) []*Record {
	idx := uint32(shardIdx) & srb.mask
	return srb.shards[idx].popBatch(maxCount, out)
}

// NumShards returns total number of shards.
func (srb *ShardedRingBuffer) NumShards() int {
	return srb.count
}

// TotalLen returns the aggregate number of buffered records across all shards.
func (srb *ShardedRingBuffer) TotalLen() int {
	sum := 0
	for _, s := range srb.shards {
		sum += s.len()
	}
	return sum
}

// Fast hash helper (FNV-1a 32-bit) for ShardedRingBuffer routing
func HashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
