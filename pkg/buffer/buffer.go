package buffer

import (
	"sync"
	"sync/atomic"
)

// SlabPool provides tiered memory slab pools to minimize GC allocations for common buffer sizes.
type SlabPool struct {
	poolSmall  sync.Pool // <= 512 B
	poolMedium sync.Pool // <= 4 KB
	poolLarge  sync.Pool // <= 64 KB
}

var DefaultSlabPool = NewSlabPool()

// NewSlabPool initializes tiered sync.Pools for byte slices.
func NewSlabPool() *SlabPool {
	return &SlabPool{
		poolSmall: sync.Pool{
			New: func() any {
				b := make([]byte, 512)
				return &b
			},
		},
		poolMedium: sync.Pool{
			New: func() any {
				b := make([]byte, 4096)
				return &b
			},
		},
		poolLarge: sync.Pool{
			New: func() any {
				b := make([]byte, 65536)
				return &b
			},
		},
	}
}

// Alloc acquires a byte slice with at least the requested capacity.
func (p *SlabPool) Alloc(capacity int) *[]byte {
	if capacity <= 512 {
		return p.poolSmall.Get().(*[]byte)
	} else if capacity <= 4096 {
		return p.poolMedium.Get().(*[]byte)
	} else if capacity <= 65536 {
		return p.poolLarge.Get().(*[]byte)
	}
	b := make([]byte, capacity)
	return &b
}

// Free returns a slice back to the slab pool if it matches one of the standard sizes.
func (p *SlabPool) Free(b *[]byte) {
	if b == nil {
		return
	}
	c := cap(*b)
	*b = (*b)[:0]
	if c == 512 {
		p.poolSmall.Put(b)
	} else if c == 4096 {
		p.poolMedium.Put(b)
	} else if c == 65536 {
		p.poolLarge.Put(b)
	}
}

// RefCountedBytes wraps a byte slice with an atomic reference counter
// to enable 1-to-N zero-copy fan-out broadcasting without memory duplication.
type RefCountedBytes struct {
	Data   []byte
	pool   *SlabPool
	rawPtr *[]byte
	refCnt int32
}

// NewRefCountedBytes creates a reference-counted payload with an initial reference of 1.
func NewRefCountedBytes(data []byte, rawPtr *[]byte, pool *SlabPool) *RefCountedBytes {
	return &RefCountedBytes{
		Data:   data,
		pool:   pool,
		rawPtr: rawPtr,
		refCnt: 1,
	}
}

// Retain increments the reference counter by 1.
func (r *RefCountedBytes) Retain() {
	atomic.AddInt32(&r.refCnt, 1)
}

// RetainN increments the reference counter by n. Useful when fanning out to N subscribers at once.
func (r *RefCountedBytes) RetainN(n int32) {
	atomic.AddInt32(&r.refCnt, n)
}

// Release decrements the reference counter. When it reaches zero, the underlying buffer
// is recycled back to the SlabPool.
func (r *RefCountedBytes) Release() {
	if atomic.AddInt32(&r.refCnt, -1) == 0 {
		if r.pool != nil && r.rawPtr != nil {
			r.pool.Free(r.rawPtr)
		}
		r.Data = nil
		r.rawPtr = nil
	}
}
