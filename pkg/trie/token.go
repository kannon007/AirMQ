package trie

import (
	"sync"
	"sync/atomic"
)

// Reserved Token IDs
const (
	TokenRoot uint32 = 0
	TokenPlus uint32 = 1 // single-level wildcard '+'
	TokenHash uint32 = 2 // multi-level wildcard '#'
	TokenUser uint32 = 3 // start of dynamic tokens
)

// TokenDictionary provides bi-directional mapping between string topic segments and uint32 IDs.
// This enables integer-based comparisons in the topic tree, slashing memory and accelerating CPU cache hits.
type TokenDictionary struct {
	mu      sync.RWMutex
	strToID map[string]uint32
	idToStr map[uint32]string
	nextID  uint32
}

var DefaultTokenDict = NewTokenDictionary()

// NewTokenDictionary creates an initialized token dictionary with reserved tokens.
func NewTokenDictionary() *TokenDictionary {
	d := &TokenDictionary{
		strToID: make(map[string]uint32, 1024),
		idToStr: make(map[uint32]string, 1024),
		nextID:  TokenUser,
	}
	d.strToID["+"] = TokenPlus
	d.idToStr[TokenPlus] = "+"
	d.strToID["#"] = TokenHash
	d.idToStr[TokenHash] = "#"
	return d
}

// GetOrIntern returns the token ID for a given string segment.
// If it does not exist, it atomically registers a new ID.
func (d *TokenDictionary) GetOrIntern(token string) uint32 {
	// Fast path: concurrent read lock
	d.mu.RLock()
	id, exists := d.strToID[token]
	d.mu.RUnlock()
	if exists {
		return id
	}

	// Slow path: write lock to register
	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check
	if id, exists = d.strToID[token]; exists {
		return id
	}

	id = atomic.AddUint32(&d.nextID, 1) - 1
	d.strToID[token] = id
	d.idToStr[id] = token
	return id
}

// LookupID returns the ID if found, or 0 if not found.
func (d *TokenDictionary) LookupID(token string) (uint32, bool) {
	d.mu.RLock()
	id, ok := d.strToID[token]
	d.mu.RUnlock()
	return id, ok
}

// LookupString returns the string representation for an ID.
func (d *TokenDictionary) LookupString(id uint32) (string, bool) {
	d.mu.RLock()
	s, ok := d.idToStr[id]
	d.mu.RUnlock()
	return s, ok
}
