package session

import (
	"errors"
	"math/bits"
	"sync"
)

var (
	ErrNoAvailablePacketID = errors.New("no available packet identifier (inflight full)")
)

// PacketIDAllocator manages MQTT 16-bit packet identifiers (1-65535).
// It uses a bitset of [1024]uint64 (65,536 bits) and hardware trailing-zero counting
// to achieve O(1) allocation and deallocation with zero memory allocations.
type PacketIDAllocator struct {
	mu      sync.Mutex
	bitset  [1024]uint64
	lastIdx int
}

// NewPacketIDAllocator initializes an allocator with ID 0 reserved (MQTT IDs are 1..65535).
func NewPacketIDAllocator() *PacketIDAllocator {
	a := &PacketIDAllocator{}
	// ID 0 is reserved and marked as used
	a.bitset[0] |= 1
	return a
}

// Allocate acquires the next free PacketID (1-65535).
func (a *PacketIDAllocator) Allocate() (uint16, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i := 0; i < 1024; i++ {
		idx := (a.lastIdx + i) % 1024
		val := a.bitset[idx]
		if val != ^uint64(0) { // If not all 64 bits are set
			tz := bits.TrailingZeros64(^val)
			id := uint16(idx*64 + tz)
			if id == 0 {
				continue
			}
			a.bitset[idx] |= (1 << tz)
			a.lastIdx = idx
			return id, nil
		}
	}
	return 0, ErrNoAvailablePacketID
}

// Release marks a PacketID as free again.
func (a *PacketIDAllocator) Release(id uint16) {
	a.mu.Lock()
	idx := int(id / 64)
	bit := id % 64
	a.bitset[idx] &^= (1 << bit)
	a.mu.Unlock()
}

// IsInUse checks if a specific ID is currently allocated.
func (a *PacketIDAllocator) IsInUse(id uint16) bool {
	a.mu.Lock()
	idx := int(id / 64)
	bit := id % 64
	inUse := (a.bitset[idx] & (1 << bit)) != 0
	a.mu.Unlock()
	return inUse
}
