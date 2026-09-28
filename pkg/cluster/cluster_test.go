package cluster

import (
	"bytes"
	"testing"
	"time"

	"mqtt/pkg/protocol"
)

type mockServer struct {
	delivered chan *protocol.PublishPacket
}

func (m *mockServer) DeliverFromCluster(topic string, qos byte, payload []byte) {
	m.delivered <- &protocol.PublishPacket{
		Topic:   topic,
		QoS:     qos,
		Payload: payload,
	}
}

func TestClusterMeshRouteAndForward(t *testing.T) {
	srv1 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh1 := NewClusterMesh("node-1", "127.0.0.1:19991", srv1)
	if err := mesh1.Start(); err != nil {
		t.Fatalf("mesh1 start failed: %v", err)
	}
	defer mesh1.Close()

	srv2 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh2 := NewClusterMesh("node-2", "127.0.0.1:19992", srv2)
	if err := mesh2.Start(); err != nil {
		t.Fatalf("mesh2 start failed: %v", err)
	}
	defer mesh2.Close()

	// Connect node-2 to node-1
	if err := mesh2.ConnectPeer("node-1", "127.0.0.1:19991"); err != nil {
		t.Fatalf("ConnectPeer failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Node 1 announces a subscription on "telemetry/+/temperature"
	_ = mesh1.BroadcastRoute("node-1", "telemetry/+/temperature", 1) // 1=Add

	time.Sleep(100 * time.Millisecond)

	// Node 2 has a publisher sending to "telemetry/room1/temperature"
	payload := []byte("27.4")
	err := mesh2.Router().RouteToCluster("telemetry/room1/temperature", 1, payload)
	if err != nil {
		t.Fatalf("RouteToCluster failed: %v", err)
	}

	// Verify Node 1 received the forwarded message
	select {
	case msg := <-srv1.delivered:
		if msg.Topic != "telemetry/room1/temperature" {
			t.Errorf("Topic mismatch: got %q", msg.Topic)
		}
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("Payload mismatch: got %q, want %q", msg.Payload, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Node 1 timed out waiting for cross-node forwarded message")
	}

	// Node 1 un-subscribes
	_ = mesh1.BroadcastRoute("node-1", "telemetry/+/temperature", 2) // 2=Remove
	time.Sleep(100 * time.Millisecond)

	// Node 2 publishes again
	_ = mesh2.Router().RouteToCluster("telemetry/room1/temperature", 1, []byte("28.0"))

	// Verify Node 1 does NOT receive anything
	select {
	case msg := <-srv1.delivered:
		t.Fatalf("Unexpected message received after route removal: %+v", msg)
	case <-time.After(300 * time.Millisecond):
		// Success: no message received
	}
}

func TestClusterMesh_RouteSnapshotAndEviction(t *testing.T) {
	srv1 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh1 := NewClusterMesh("node-snap-1", "127.0.0.1:19993", srv1)
	if err := mesh1.Start(); err != nil {
		t.Fatalf("mesh1 start failed: %v", err)
	}
	defer mesh1.Close()

	srv2 := &mockServer{delivered: make(chan *protocol.PublishPacket, 10)}
	mesh2 := NewClusterMesh("node-snap-2", "127.0.0.1:19994", srv2)
	if err := mesh2.Start(); err != nil {
		t.Fatalf("mesh2 start failed: %v", err)
	}
	defer mesh2.Close()

	if err := mesh2.ConnectPeer("node-snap-1", "127.0.0.1:19993"); err != nil {
		t.Fatalf("ConnectPeer failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Node 1 sends a route snapshot containing 2 topics
	err := mesh1.SendRouteSnapshot("node-snap-2", []string{"devices/sensor-1", "devices/sensor-2"})
	if err != nil {
		t.Fatalf("SendRouteSnapshot failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Node 2 should match both topics to node-snap-1
	matches1 := mesh2.Router().remoteTree.Match("devices/sensor-1")
	if len(matches1) == 0 || matches1[0].ClientID != "node-snap-1" {
		t.Fatalf("expected match for devices/sensor-1, got %v", matches1)
	}
	matches2 := mesh2.Router().remoteTree.Match("devices/sensor-2")
	if len(matches2) == 0 || matches2[0].ClientID != "node-snap-1" {
		t.Fatalf("expected match for devices/sensor-2, got %v", matches2)
	}

	// Evict Node 1 (e.g. peer disconnect / failure)
	mesh2.Router().EvictNode("node-snap-1")

	// Verify all routes for Node 1 are removed
	afterEvict := mesh2.Router().remoteTree.Match("devices/sensor-1")
	if len(afterEvict) != 0 {
		t.Fatalf("expected 0 matches after eviction, got %v", afterEvict)
	}
}

func BenchmarkClusterRouter_RouteToCluster(b *testing.B) {
	cr := NewClusterRouter("node-local", nil)
	cr.OnRemoteRouteSync("node-remote-1", "telemetry/+/temperature", 1)
	payload := []byte("25.5")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cr.RouteToCluster("telemetry/room1/temperature", 0, payload)
		}
	})
}

func BenchmarkClusterMesh_CrossNodeForward(b *testing.B) {
	srv := &mockServer{delivered: make(chan *protocol.PublishPacket, 100000)}
	mesh1 := NewClusterMesh("node-bench-1", "127.0.0.1:29981", nil)
	_ = mesh1.Start()
	defer mesh1.Close()

	mesh2 := NewClusterMesh("node-bench-2", "127.0.0.1:29982", srv)
	_ = mesh2.Start()
	defer mesh2.Close()

	_ = mesh1.ConnectPeer("node-bench-2", "127.0.0.1:29982")
	time.Sleep(50 * time.Millisecond)

	go func() {
		for range srv.delivered {
		}
	}()

	payload := []byte("bench-forward-payload-64-bytes-data-stream-verification-cluster")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mesh1.ForwardPublish("node-bench-2", "telemetry/data", 0, payload)
	}
}
