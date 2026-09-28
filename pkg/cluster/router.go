package cluster

import (
	"sync"
	"sync/atomic"

	"mqtt/pkg/trie"
)

// ClusterRouter manages cross-node routing of MQTT publications and tracks route versions (Epoch).
type ClusterRouter struct {
	mu           sync.RWMutex
	localNodeID  string
	remoteTree   *trie.TopicTree                // Tracks which remote nodes are subscribed to which topics
	nodeTopics   map[string]map[string]struct{} // remoteNodeID -> set of topics subscribed by that node
	localTopics  map[string]int                 // topic -> count of local subscriptions
	localEpoch   uint64                         // Monotonically increasing epoch of local route changes
	remoteEpochs map[string]uint64              // remoteNodeID -> last known epoch
	rpcClient    ClusterRPC
}

func NewClusterRouter(localNodeID string, rpcClient ClusterRPC) *ClusterRouter {
	return &ClusterRouter{
		localNodeID:  localNodeID,
		remoteTree:   trie.NewTopicTree(nil),
		nodeTopics:   make(map[string]map[string]struct{}),
		localTopics:  make(map[string]int),
		remoteEpochs: make(map[string]uint64),
		rpcClient:    rpcClient,
	}
}

// LocalEpoch returns the current route generation epoch of the local node.
func (cr *ClusterRouter) LocalEpoch() uint64 {
	return atomic.LoadUint64(&cr.localEpoch)
}

// IncEpoch increments the local route generation epoch.
func (cr *ClusterRouter) IncEpoch() uint64 {
	return atomic.AddUint64(&cr.localEpoch, 1)
}

// RemoteEpoch returns the last known epoch for a remote node.
func (cr *ClusterRouter) RemoteEpoch(remoteNodeID string) uint64 {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.remoteEpochs[remoteNodeID]
}

// SetRemoteEpoch records the remote node's route epoch.
func (cr *ClusterRouter) SetRemoteEpoch(remoteNodeID string, epoch uint64) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.remoteEpochs[remoteNodeID] = epoch
}

// AddLocalTopic records a locally subscribed topic and bumps local epoch.
func (cr *ClusterRouter) AddLocalTopic(topic string) {
	cr.mu.Lock()
	cr.localTopics[topic]++
	cr.mu.Unlock()
	cr.IncEpoch()
}

// RemoveLocalTopic removes a locally subscribed topic and bumps local epoch.
func (cr *ClusterRouter) RemoveLocalTopic(topic string) {
	cr.mu.Lock()
	if count, exists := cr.localTopics[topic]; exists {
		if count <= 1 {
			delete(cr.localTopics, topic)
		} else {
			cr.localTopics[topic] = count - 1
		}
	}
	cr.mu.Unlock()
	cr.IncEpoch()
}

// LocalTopics returns a snapshot copy of all active unique local subscribed topics.
func (cr *ClusterRouter) LocalTopics() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	res := make([]string, 0, len(cr.localTopics))
	for t := range cr.localTopics {
		res = append(res, t)
	}
	return res
}

// OnRemoteRouteSync updates the routing table when a remote broker advertises a subscription.
func (cr *ClusterRouter) OnRemoteRouteSync(remoteNodeID, topic string, action byte) {
	if remoteNodeID == cr.localNodeID {
		return
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if action == 1 { // Add
		if cr.nodeTopics[remoteNodeID] == nil {
			cr.nodeTopics[remoteNodeID] = make(map[string]struct{})
		}
		cr.nodeTopics[remoteNodeID][topic] = struct{}{}
		cr.remoteTree.Subscribe(topic, remoteNodeID, 1)
	} else if action == 2 { // Remove
		if tMap, exists := cr.nodeTopics[remoteNodeID]; exists {
			delete(tMap, topic)
		}
		cr.remoteTree.Unsubscribe(topic, remoteNodeID)
	}
}

// SyncRouteSnapshot reconciles all subscriptions for a remote node (reconnect reconciliation).
func (cr *ClusterRouter) SyncRouteSnapshot(remoteNodeID string, topics []string) {
	if remoteNodeID == cr.localNodeID {
		return
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Clear old subscriptions for this node
	if oldTopics, exists := cr.nodeTopics[remoteNodeID]; exists {
		for t := range oldTopics {
			cr.remoteTree.Unsubscribe(t, remoteNodeID)
		}
	}

	newMap := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		newMap[t] = struct{}{}
		cr.remoteTree.Subscribe(t, remoteNodeID, 1)
	}
	cr.nodeTopics[remoteNodeID] = newMap
}

// EvictNode cleans up all remote routes when a peer node goes offline or times out.
func (cr *ClusterRouter) EvictNode(remoteNodeID string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if topics, exists := cr.nodeTopics[remoteNodeID]; exists {
		for t := range topics {
			cr.remoteTree.Unsubscribe(t, remoteNodeID)
		}
		delete(cr.nodeTopics, remoteNodeID)
	}
	delete(cr.remoteEpochs, remoteNodeID)
}

// RouteToCluster finds all remote broker nodes that have subscribers matching the topic,
// and forwards exactly ONE copy to each remote node via the RPC mesh.
func (cr *ClusterRouter) RouteToCluster(topic string, qos byte, payload []byte) error {
	if cr.rpcClient == nil {
		return nil
	}

	cr.mu.RLock()
	matches := cr.remoteTree.Match(topic)
	cr.mu.RUnlock()

	if len(matches) == 0 {
		return nil
	}

	// De-duplicate remote nodes
	nodeSet := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		if m.ClientID != cr.localNodeID {
			nodeSet[m.ClientID] = struct{}{}
		}
	}

	for nodeID := range nodeSet {
		_ = cr.rpcClient.ForwardPublish(nodeID, topic, qos, payload)
	}

	return nil
}
