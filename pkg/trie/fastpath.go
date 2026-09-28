package trie

import (
	"strings"
	"sync"
	"sync/atomic"
)

// Router combines the TopicTree with an O(1) Fast-Path Hash Table for exact subscriptions.
// Features a Lock-Free Copy-On-Write (COW) fast-path cache for zero-allocation, zero-contention Lookups.
type Router struct {
	tree       *TopicTree
	exactMu    sync.Mutex
	exactSubs  map[string]map[string]Subscriber        // topic -> (clientID -> Subscriber)
	fastCache  atomic.Pointer[map[string][]Subscriber] // Lock-Free read cache for ultra-high throughput
	hasWildSub atomic.Bool                             // if false globally, we can bypass tree traversal entirely!
}

// NewRouter creates a combined fast-path router and wildcard trie.
func NewRouter(dict *TokenDictionary) *Router {
	r := &Router{
		tree:      NewTopicTree(dict),
		exactSubs: make(map[string]map[string]Subscriber),
	}
	emptyMap := make(map[string][]Subscriber)
	r.fastCache.Store(&emptyMap)
	return r
}

func (r *Router) rebuildFastCacheLocked() {
	newCache := make(map[string][]Subscriber, len(r.exactSubs))
	for topic, clients := range r.exactSubs {
		if len(clients) == 0 {
			continue
		}
		subs := make([]Subscriber, 0, len(clients))
		for _, sub := range clients {
			subs = append(subs, sub)
		}
		newCache[topic] = subs
	}
	r.fastCache.Store(&newCache)
}

// Subscribe registers a subscription. If it has wildcards (+ or #), it is added to the TopicTree.
// If it's an exact topic, it is added to both for complete consistency.
func (r *Router) Subscribe(topic, clientID string, qos byte, opts ...SubOption) {
	isWildcard := stringsContainsWildcard(topic)

	var opt SubOption
	if len(opts) > 0 {
		opt = opts[0]
	}
	subObj := Subscriber{
		ClientID:          clientID,
		QoS:               qos,
		NoLocal:           opt.NoLocal,
		RetainAsPublished: opt.RetainAsPublished,
		RetainHandling:    opt.RetainHandling,
	}

	r.exactMu.Lock()
	if isWildcard {
		r.hasWildSub.Store(true)
	} else {
		subMap, exists := r.exactSubs[topic]
		if !exists {
			subMap = make(map[string]Subscriber)
			r.exactSubs[topic] = subMap
		}
		subMap[clientID] = subObj
		r.rebuildFastCacheLocked()
	}
	r.exactMu.Unlock()

	// Tree always stores all subscriptions for unified wildcard resolution
	r.tree.Subscribe(topic, clientID, qos, opts...)
}

// Unsubscribe removes a subscription from both exact cache and topic tree.
func (r *Router) Unsubscribe(topic, clientID string) {
	if !stringsContainsWildcard(topic) {
		r.exactMu.Lock()
		if subMap, exists := r.exactSubs[topic]; exists {
			delete(subMap, clientID)
			if len(subMap) == 0 {
				delete(r.exactSubs, topic)
			}
			r.rebuildFastCacheLocked()
		}
		r.exactMu.Unlock()
	}

	r.tree.Unsubscribe(topic, clientID)
}

// Match finds all matching subscribers.
// Fast path: if there are no wildcard subscriptions anywhere in the system,
// it does an O(1) lock-free atomic hash map lookup with zero allocations!
func (r *Router) Match(topic string) []Subscriber {
	// 1. Lock-free exact fast path (zero mutex, zero slice allocations)
	if !r.hasWildSub.Load() {
		cache := r.fastCache.Load()
		if cache != nil {
			return (*cache)[topic]
		}
		return nil
	}

	// 2. Slow/Wildcard path: query full topic tree
	return r.tree.Match(topic)
}

// MatchBytes finds matching subscribers directly from raw topic bytes with zero heap allocation.
func (r *Router) MatchBytes(topic []byte) []Subscriber {
	if !r.hasWildSub.Load() {
		cache := r.fastCache.Load()
		if cache != nil {
			// Compiler optimization: map access with string(topic) does not allocate heap memory
			return (*cache)[string(topic)]
		}
		return nil
	}
	return r.tree.Match(string(topic))
}

func stringsContainsWildcard(s string) bool {
	if strings.HasPrefix(s, "$share/") {
		return true
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '+' || s[i] == '#' {
			return true
		}
	}
	return false
}
