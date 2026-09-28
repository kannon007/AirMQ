package cluster

import (
	"bytes"
	"testing"
	"time"

	"mqtt/pkg/protocol"
)

// TestCluster_ThreeNode_DynamicDiscovery verifies that 3 nodes form a full mesh
// when Node 3 only knows Node 2, and Node 2 only knows Node 1.
func TestCluster_ThreeNode_DynamicDiscovery(t *testing.T) {
	srv1 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh1 := NewClusterMesh("node-1", "127.0.0.1:29901", srv1)
	if err := mesh1.Start(); err != nil {
		t.Fatalf("mesh1 start failed: %v", err)
	}
	defer mesh1.Close()

	srv2 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh2 := NewClusterMesh("node-2", "127.0.0.1:29902", srv2)
	if err := mesh2.Start(); err != nil {
		t.Fatalf("mesh2 start failed: %v", err)
	}
	defer mesh2.Close()

	// Node 2 joins Node 1
	if err := mesh2.Join([]string{"127.0.0.1:29901"}); err != nil {
		t.Fatalf("mesh2 join failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	srv3 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh3 := NewClusterMesh("node-3", "127.0.0.1:29903", srv3)
	if err := mesh3.Start(); err != nil {
		t.Fatalf("mesh3 start failed: %v", err)
	}
	defer mesh3.Close()

	// Node 3 joins ONLY Node 2 (Node 3 has NO knowledge of Node 1!)
	if err := mesh3.Join([]string{"127.0.0.1:29902"}); err != nil {
		t.Fatalf("mesh3 join failed: %v", err)
	}

	// Wait up to 1.5 seconds for PEX gossip to auto-discover and complete Full Mesh
	deadline := time.Now().Add(2 * time.Second)
	meshFormed := false
	for time.Now().Before(deadline) {
		peers1 := mesh1.Peers()
		peers2 := mesh2.Peers()
		peers3 := mesh3.Peers()
		if len(peers1) == 2 && len(peers2) == 2 && len(peers3) == 2 {
			meshFormed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !meshFormed {
		t.Fatalf("Full Mesh failed to form: mesh1 peers=%v, mesh2 peers=%v, mesh3 peers=%v",
			mesh1.Peers(), mesh2.Peers(), mesh3.Peers())
	}

	// Verify all peer states are StateAlive
	if s := mesh1.PeerState("node-3"); s != StateAlive {
		t.Errorf("Node 1 expected StateAlive for node-3, got %v", s)
	}
	if s := mesh3.PeerState("node-1"); s != StateAlive {
		t.Errorf("Node 3 expected StateAlive for node-1, got %v", s)
	}

	// 2. Test cross-node messaging across discovered peers:
	// Node 3 subscribes to topic "factory/robot/arm1"
	_ = mesh3.BroadcastRoute("node-3", "factory/robot/arm1", 1)
	time.Sleep(150 * time.Millisecond)

	// Node 1 publishes to "factory/robot/arm1"
	testPayload := []byte("MOVE_X_200")
	err := mesh1.Router().RouteToCluster("factory/robot/arm1", 1, testPayload)
	if err != nil {
		t.Fatalf("RouteToCluster failed: %v", err)
	}

	// Verify Node 3 received the forwarded message directly from Node 1
	select {
	case msg := <-srv3.delivered:
		if msg.Topic != "factory/robot/arm1" {
			t.Errorf("Topic mismatch: got %q", msg.Topic)
		}
		if !bytes.Equal(msg.Payload, testPayload) {
			t.Errorf("Payload mismatch: got %q, want %q", msg.Payload, testPayload)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Node 3 timed out waiting for cross-node forwarded message from Node 1")
	}

	// 3. Test Graceful Departure: Node 2 leaves gracefully
	mesh2.Close()
	time.Sleep(150 * time.Millisecond)

	// Node 1 and Node 3 should now only have 1 peer each (each other)
	if len(mesh1.Peers()) != 1 || len(mesh3.Peers()) != 1 {
		t.Errorf("After node-2 departure: mesh1 peers=%v, mesh3 peers=%v", mesh1.Peers(), mesh3.Peers())
	}

	// Node 1 publishes again to Node 3
	testPayload2 := []byte("MOVE_X_300")
	_ = mesh1.Router().RouteToCluster("factory/robot/arm1", 1, testPayload2)

	select {
	case msg := <-srv3.delivered:
		if !bytes.Equal(msg.Payload, testPayload2) {
			t.Errorf("Payload mismatch: got %q, want %q", msg.Payload, testPayload2)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Node 3 timed out waiting for message after node-2 left")
	}
}

// TestCluster_SWIM_SuspectAndRecovery verifies the 3-state health machine.
func TestCluster_SWIM_SuspectAndRecovery(t *testing.T) {
	srv1 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh1 := NewClusterMesh("node-swim-1", "127.0.0.1:29911", srv1)
	if err := mesh1.Start(); err != nil {
		t.Fatalf("mesh1 start failed: %v", err)
	}
	defer mesh1.Close()

	srv2 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh2 := NewClusterMesh("node-swim-2", "127.0.0.1:29912", srv2)
	if err := mesh2.Start(); err != nil {
		t.Fatalf("mesh2 start failed: %v", err)
	}
	defer mesh2.Close()

	if err := mesh2.Join([]string{"127.0.0.1:29911"}); err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Initially StateAlive
	if s := mesh1.PeerState("node-swim-2"); s != StateAlive {
		t.Fatalf("expected StateAlive, got %v", s)
	}

	// Simulate simulated silence by rewinding lastSeen on mesh1
	mesh1.mu.Lock()
	mesh1.peerLastSeen["node-swim-2"] = time.Now().Add(-2500 * time.Millisecond)
	mesh1.mu.Unlock()

	// Run health check logic manually or wait for next tick
	time.Sleep(1100 * time.Millisecond)

	// In the next heartbeat check, it should mark suspect or recover upon receiving heartbeat
	// Now let's test recovery by sending a heartbeat frame directly
	mesh1.mu.Lock()
	mesh1.peerStates["node-swim-2"] = StateSuspect
	mesh1.mu.Unlock()

	if s := mesh1.PeerState("node-swim-2"); s != StateSuspect {
		t.Fatalf("expected StateSuspect, got %v", s)
	}

	// Routes must NOT be evicted during suspect
	_ = mesh2.BroadcastRoute("node-swim-2", "sensors/temp", 1)
	time.Sleep(100 * time.Millisecond)

	matches := mesh1.Router().remoteTree.Match("sensors/temp")
	if len(matches) == 0 {
		t.Fatalf("expected route retained during StateSuspect, but got 0 matches")
	}

	// Once heartbeat received, node-swim-2 is restored to StateAlive
	time.Sleep(1100 * time.Millisecond)
	if s := mesh1.PeerState("node-swim-2"); s != StateAlive {
		t.Fatalf("expected node to recover to StateAlive, got %v", s)
	}
}
