package trie

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Subscriber represents a client subscription with its granted QoS and MQTT 5.0 options.
type Subscriber struct {
	ClientID          string
	QoS               byte
	NoLocal           bool // MQTT 5.0: If true, publisher will not receive messages published by itself
	RetainAsPublished bool // MQTT 5.0: Forward message with original Retain flag
	RetainHandling    byte // MQTT 5.0: 0, 1, or 2
}

// SubOption configures MQTT 5.0 specific subscription flags.
type SubOption struct {
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// Node represents a node in the Topic Tree.
type Node struct {
	tokenID          uint32
	hasWildcardChild bool
	subscribers      map[string]Subscriber            // clientID -> Subscriber
	sharedSubs       map[string]map[string]Subscriber // group -> (clientID -> Subscriber)
	children         map[uint32]*Node
}

func newNode(tokenID uint32) *Node {
	return &Node{
		tokenID:     tokenID,
		subscribers: make(map[string]Subscriber),
		sharedSubs:  make(map[string]map[string]Subscriber),
		children:    make(map[uint32]*Node),
	}
}

// TopicTree implements a high-performance, wildcard-aware MQTT topic matching trie,
// with full support for MQTT 5.0 Shared Subscriptions ($share/{group}/{topic}) and Subscription Options.
type TopicTree struct {
	mu            sync.RWMutex
	root          *Node
	dict          *TokenDictionary
	sharedCursors sync.Map // groupName -> *uint32
}

// NewTopicTree creates a new topic tree backed by a token dictionary.
func NewTopicTree(dict *TokenDictionary) *TopicTree {
	if dict == nil {
		dict = DefaultTokenDict
	}
	return &TopicTree{
		root: newNode(TokenRoot),
		dict: dict,
	}
}

// ParseSharedTopic checks if a topic filter is an MQTT shared subscription ($share/group/topic).
func ParseSharedTopic(topic string) (group string, actualTopic string, isShared bool) {
	if strings.HasPrefix(topic, "$share/") {
		parts := strings.SplitN(topic[7:], "/", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return parts[0], parts[1], true
		}
	}
	return "", topic, false
}

// Subscribe adds a subscription for a given client, QoS, and optional MQTT 5.0 subscription flags.
func (t *TopicTree) Subscribe(topic string, clientID string, qos byte, opts ...SubOption) {
	group, actualTopic, isShared := ParseSharedTopic(topic)
	tokens := t.tokenize(actualTopic, true)
	if len(tokens) == 0 {
		return
	}

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

	t.mu.Lock()
	defer t.mu.Unlock()

	curr := t.root
	for _, tokID := range tokens {
		if tokID == TokenPlus || tokID == TokenHash {
			curr.hasWildcardChild = true
		}
		child, exists := curr.children[tokID]
		if !exists {
			child = newNode(tokID)
			curr.children[tokID] = child
		}
		curr = child
	}

	if isShared {
		gMap, exists := curr.sharedSubs[group]
		if !exists {
			gMap = make(map[string]Subscriber)
			curr.sharedSubs[group] = gMap
		}
		gMap[clientID] = subObj
	} else {
		curr.subscribers[clientID] = subObj
	}
}

// Unsubscribe removes a client's subscription for a given topic.
func (t *TopicTree) Unsubscribe(topic string, clientID string) bool {
	group, actualTopic, isShared := ParseSharedTopic(topic)
	tokens := t.tokenize(actualTopic, false)
	if len(tokens) == 0 {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.remove(t.root, tokens, 0, clientID, isShared, group)
}

func (t *TopicTree) remove(curr *Node, tokens []uint32, depth int, clientID string, isShared bool, group string) bool {
	if depth == len(tokens) {
		if isShared {
			if gMap, exists := curr.sharedSubs[group]; exists {
				delete(gMap, clientID)
				if len(gMap) == 0 {
					delete(curr.sharedSubs, group)
				}
			}
		} else {
			delete(curr.subscribers, clientID)
		}
		return len(curr.subscribers) == 0 && len(curr.sharedSubs) == 0 && len(curr.children) == 0
	}

	tokID := tokens[depth]
	child, exists := curr.children[tokID]
	if !exists {
		return false
	}

	shouldPrune := t.remove(child, tokens, depth+1, clientID, isShared, group)
	if shouldPrune {
		delete(curr.children, tokID)
		curr.hasWildcardChild = false
		for k := range curr.children {
			if k == TokenPlus || k == TokenHash {
				curr.hasWildcardChild = true
				break
			}
		}
	}
	return len(curr.subscribers) == 0 && len(curr.sharedSubs) == 0 && len(curr.children) == 0
}

// Match finds all clients matching the published topic and returns their highest QoS and options.
// For Shared Subscriptions ($share/group/topic), it balances the load across group members via Round-Robin.
func (t *TopicTree) Match(topic string) []Subscriber {
	tokens := t.tokenize(topic, false)
	if len(tokens) == 0 {
		return nil
	}

	t.mu.RLock()
	regularMap := make(map[string]Subscriber)
	sharedMap := make(map[string][]Subscriber)

	t.matchRecursive(t.root, tokens, 0, regularMap, sharedMap)
	t.mu.RUnlock()

	totalCount := len(regularMap) + len(sharedMap)
	if totalCount == 0 {
		return nil
	}

	subscribers := make([]Subscriber, 0, totalCount)
	for _, sub := range regularMap {
		subscribers = append(subscribers, sub)
	}

	for group, members := range sharedMap {
		if len(members) == 0 {
			continue
		}
		selected := t.selectSharedMember(group, members)
		subscribers = append(subscribers, selected)
	}

	return subscribers
}

func (t *TopicTree) selectSharedMember(group string, members []Subscriber) Subscriber {
	if len(members) == 1 {
		return members[0]
	}

	var cursorPtr *uint32
	val, ok := t.sharedCursors.Load(group)
	if !ok {
		var c uint32
		actual, _ := t.sharedCursors.LoadOrStore(group, &c)
		cursorPtr = actual.(*uint32)
	} else {
		cursorPtr = val.(*uint32)
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].ClientID < members[j].ClientID
	})

	idx := int(atomic.AddUint32(cursorPtr, 1) % uint32(len(members)))
	return members[idx]
}

func (t *TopicTree) matchRecursive(curr *Node, tokens []uint32, depth int, regular map[string]Subscriber, shared map[string][]Subscriber) {
	// 1. Multi-level wildcard '#' matches everything from this level onward
	if curr.hasWildcardChild {
		if hashNode, exists := curr.children[TokenHash]; exists {
			for cid, sub := range hashNode.subscribers {
				if oldSub, ok := regular[cid]; !ok || sub.QoS > oldSub.QoS {
					regular[cid] = sub
				}
			}
			for grp, gMap := range hashNode.sharedSubs {
				for _, sub := range gMap {
					shared[grp] = append(shared[grp], sub)
				}
			}
		}
	}

	// If we've consumed all topic tokens, collect subscribers on this node
	if depth == len(tokens) {
		for cid, sub := range curr.subscribers {
			if oldSub, ok := regular[cid]; !ok || sub.QoS > oldSub.QoS {
				regular[cid] = sub
			}
		}
		for grp, gMap := range curr.sharedSubs {
			for _, sub := range gMap {
				shared[grp] = append(shared[grp], sub)
			}
		}
		return
	}

	currTok := tokens[depth]

	// 2. Exact child match
	if child, exists := curr.children[currTok]; exists {
		t.matchRecursive(child, tokens, depth+1, regular, shared)
	}

	// 3. Single-level wildcard '+' child match
	if curr.hasWildcardChild {
		if plusNode, exists := curr.children[TokenPlus]; exists {
			t.matchRecursive(plusNode, tokens, depth+1, regular, shared)
		}
	}
}

// tokenize splits topic strings by '/' and maps each segment to a uint32 TokenID.
func (t *TopicTree) tokenize(topic string, register bool) []uint32 {
	if topic == "" {
		return nil
	}

	parts := strings.Split(topic, "/")
	tokenIDs := make([]uint32, len(parts))

	for i, part := range parts {
		if register {
			tokenIDs[i] = t.dict.GetOrIntern(part)
		} else {
			id, ok := t.dict.LookupID(part)
			if !ok {
				tokenIDs[i] = 0xFFFFFFFF
			} else {
				tokenIDs[i] = id
			}
		}
	}
	return tokenIDs
}

// TopicFilterMatches checks if an MQTT topic filter (including + and #) matches a given topic name.
func TopicFilterMatches(filter, topic string) bool {
	_, filter, _ = ParseSharedTopic(filter)
	if filter == topic {
		return true
	}
	fParts := strings.Split(filter, "/")
	tParts := strings.Split(topic, "/")

	// MQTT spec: topics starting with '$' cannot be matched by wildcards at the first level
	if len(tParts) > 0 && strings.HasPrefix(tParts[0], "$") {
		if len(fParts) > 0 && (fParts[0] == "+" || fParts[0] == "#") {
			return false
		}
	}

	for i := 0; i < len(fParts); i++ {
		if fParts[i] == "#" {
			return i == len(fParts)-1
		}
		if i >= len(tParts) {
			return false
		}
		if fParts[i] != "+" && fParts[i] != tParts[i] {
			return false
		}
	}
	return len(fParts) == len(tParts)
}
