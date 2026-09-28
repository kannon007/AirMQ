package cluster

// PeerState represents the SWIM-inspired health state of a peer node.
type PeerState int

const (
	StateAlive   PeerState = 1 // Node is healthy and responding to heartbeats
	StateSuspect PeerState = 2 // Node missed heartbeats (2s-4s), suspecting failure, routes retained
	StateDead    PeerState = 3 // Node confirmed dead (>4s), evicted and routes removed
)

func (s PeerState) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Cluster internal frame message types
const (
	MsgRouteAdd      byte = 1 // Incremental route subscription addition
	MsgRouteDel      byte = 2 // Incremental route unsubscription
	MsgForwardPub    byte = 3 // Cross-node PUBLISH payload forwarding
	MsgHeartbeat     byte = 4 // Liveness ping carrying 8-byte uint64 local epoch
	MsgRouteSnapshot byte = 5 // Full route reconciliation snapshot
	MsgPeerJoin      byte = 6 // Gossip notification of new peer joining
	MsgPeerList      byte = 7 // Full directory response of known active peers (PEX)
	MsgPeerLeave     byte = 8 // Graceful departure notification
)

// NodeInfo contains information about a peer broker in the cluster.
type NodeInfo struct {
	ID        string
	RPCAddr   string
	MQTTAddr  string
	State     PeerState
	LastEpoch uint64
}

// ClusterRPC defines the RPC methods for cross-node message forwarding and route sync.
type ClusterRPC interface {
	ForwardPublish(targetNodeID string, topic string, qos byte, payload []byte) error
	BroadcastRoute(nodeID string, topic string, action byte) error // 1=Add, 2=Remove
}
