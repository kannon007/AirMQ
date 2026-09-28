package server

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"mqtt/pkg/cluster"
	"mqtt/pkg/protocol"
)

func TestServer_Cluster_DynamicDiscovery_FullMesh_E2E(t *testing.T) {
	// 1. Server 1 (Node 1 - Seed)
	srv1 := NewServer(Config{Addr: "tcp://127.0.0.1:18871", Multicore: false}, nil, nil, nil)
	mesh1 := cluster.NewClusterMesh("node-e2e-1", "127.0.0.1:29931", srv1)
	if err := mesh1.Start(); err != nil {
		t.Fatalf("mesh1 start failed: %v", err)
	}
	defer mesh1.Close()
	srv1.SetClusterMesh(mesh1)
	go func() { _ = srv1.Start() }()
	defer srv1.Stop(context.Background())

	// 2. Server 2 (Node 2 - intermediate)
	srv2 := NewServer(Config{Addr: "tcp://127.0.0.1:18872", Multicore: false}, nil, nil, nil)
	mesh2 := cluster.NewClusterMesh("node-e2e-2", "127.0.0.1:29932", srv2)
	if err := mesh2.Start(); err != nil {
		t.Fatalf("mesh2 start failed: %v", err)
	}
	defer mesh2.Close()
	srv2.SetClusterMesh(mesh2)
	go func() { _ = srv2.Start() }()
	defer srv2.Stop(context.Background())

	// Node 2 joins Node 1
	if err := mesh2.Join([]string{"127.0.0.1:29931"}); err != nil {
		t.Fatalf("mesh2 join failed: %v", err)
	}

	// 3. Server 3 (Node 3 - only knows Node 2!)
	srv3 := NewServer(Config{Addr: "tcp://127.0.0.1:18873", Multicore: false}, nil, nil, nil)
	mesh3 := cluster.NewClusterMesh("node-e2e-3", "127.0.0.1:29933", srv3)
	if err := mesh3.Start(); err != nil {
		t.Fatalf("mesh3 start failed: %v", err)
	}
	defer mesh3.Close()
	srv3.SetClusterMesh(mesh3)
	go func() { _ = srv3.Start() }()
	defer srv3.Stop(context.Background())

	// Node 3 joins ONLY Node 2
	if err := mesh3.Join([]string{"127.0.0.1:29932"}); err != nil {
		t.Fatalf("mesh3 join failed: %v", err)
	}

	// Wait for PEX discovery to auto-connect Node 1 and Node 3
	deadline := time.Now().Add(2 * time.Second)
	meshConverged := false
	for time.Now().Before(deadline) {
		if len(mesh1.Peers()) == 2 && len(mesh2.Peers()) == 2 && len(mesh3.Peers()) == 2 {
			meshConverged = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !meshConverged {
		t.Fatalf("Cluster mesh failed to converge: node1=%v, node2=%v, node3=%v",
			mesh1.Peers(), mesh2.Peers(), mesh3.Peers())
	}

	time.Sleep(200 * time.Millisecond)

	// 4. Connect MQTT Client B to Server 3 (127.0.0.1:18873)
	connB, err := net.Dial("tcp", "127.0.0.1:18873")
	if err != nil {
		t.Fatalf("Failed to dial Server 3: %v", err)
	}
	defer connB.Close()

	// Handshake Client B
	connectB := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "client-b-on-node3",
	}
	encConnB, _ := connectB.Encode()
	_, _ = connB.Write(encConnB)
	connackBuf := make([]byte, 64)
	_, _ = connB.Read(connackBuf)

	// Client B subscribes to "cluster/dynamic/telemetry"
	subB := &protocol.SubscribePacket{
		ProtocolLevel: protocol.V311,
		PacketID:      1,
		Topics: []protocol.TopicSub{
			{Topic: "cluster/dynamic/telemetry", QoS: 0},
		},
	}
	encSubB, _ := subB.Encode()
	_, _ = connB.Write(encSubB)
	subackBuf := make([]byte, 64)
	_, _ = connB.Read(subackBuf)

	time.Sleep(150 * time.Millisecond)

	// 5. Connect MQTT Client A to Server 1 (127.0.0.1:18871)
	connA, err := net.Dial("tcp", "127.0.0.1:18871")
	if err != nil {
		t.Fatalf("Failed to dial Server 1: %v", err)
	}
	defer connA.Close()

	connectA := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "client-a-on-node1",
	}
	encConnA, _ := connectA.Encode()
	_, _ = connA.Write(encConnA)
	_, _ = connA.Read(connackBuf)

	// Client A publishes to "cluster/dynamic/telemetry"
	testPayload := []byte("payload-routed-across-pex-mesh")
	pubA := &protocol.PublishPacket{
		ProtocolLevel: protocol.V311,
		Topic:         "cluster/dynamic/telemetry",
		Payload:       testPayload,
		QoS:           0,
	}
	encPubA, _ := pubA.Encode()
	_, _ = connA.Write(encPubA)

	// 6. Client B on Server 3 should receive the message!
	_ = connB.SetReadDeadline(time.Now().Add(2 * time.Second))
	readBuf := make([]byte, 512)
	n, err := connB.Read(readBuf)
	if err != nil {
		t.Fatalf("Client B failed to receive routed message from Client A: %v", err)
	}

	if !bytes.Contains(readBuf[:n], testPayload) {
		t.Fatalf("Client B received unexpected payload: %q (want to contain %q)", readBuf[:n], testPayload)
	}

	// 7. Test Graceful Disconnect: Stop Server 2 / Mesh 2
	mesh2.Close()
	srv2.Stop(context.Background())
	time.Sleep(150 * time.Millisecond)

	// Client A publishes another message to Client B
	testPayload2 := []byte("payload-routed-after-node2-departure")
	pubA2 := &protocol.PublishPacket{
		ProtocolLevel: protocol.V311,
		Topic:         "cluster/dynamic/telemetry",
		Payload:       testPayload2,
		QoS:           0,
	}
	encPubA2, _ := pubA2.Encode()
	_, _ = connA.Write(encPubA2)

	// Client B should still receive it directly from Server 1
	_ = connB.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = connB.Read(readBuf)
	if err != nil {
		t.Fatalf("Client B failed to receive message after Node 2 left: %v", err)
	}

	if !bytes.Contains(readBuf[:n], testPayload2) {
		t.Fatalf("Client B received unexpected payload after departure: %q", readBuf[:n])
	}
}
