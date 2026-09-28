package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"mqtt/pkg/protocol"
)

func TestBadgerStoreRetainedAndOffline(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "badger-mqtt-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "data")
	store, err := NewBadgerStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}

	// 1. Test Retained
	pkt := &protocol.PublishPacket{
		Topic:    "home/temp",
		Payload:  []byte("21.5"),
		QoS:      1,
		Retain:   true,
		PacketID: 10,
	}

	if err := store.SetRetained(pkt.Topic, pkt); err != nil {
		t.Fatalf("SetRetained failed: %v", err)
	}

	retrieved, err := store.GetRetained("home/temp")
	if err != nil || retrieved == nil {
		t.Fatalf("GetRetained failed: %v, retrieved: %+v", err, retrieved)
	}
	if !bytes.Equal(retrieved.Payload, pkt.Payload) {
		t.Fatalf("Payload mismatch: got %s, want %s", retrieved.Payload, pkt.Payload)
	}

	all, err := store.GetAllRetained()
	if err != nil || len(all) != 1 {
		t.Fatalf("GetAllRetained expected 1, got %d", len(all))
	}

	// 2. Test Offline Messages
	off1 := &protocol.PublishPacket{Topic: "alerts/1", Payload: []byte("warning"), QoS: 1}
	off2 := &protocol.PublishPacket{Topic: "alerts/2", Payload: []byte("critical"), QoS: 1}

	if err := store.StoreOffline("clientA", off1); err != nil {
		t.Fatalf("StoreOffline off1 failed: %v", err)
	}
	if err := store.StoreOffline("clientA", off2); err != nil {
		t.Fatalf("StoreOffline off2 failed: %v", err)
	}

	fetched, err := store.FetchOffline("clientA")
	if err != nil || len(fetched) != 2 {
		t.Fatalf("FetchOffline expected 2, got %d", len(fetched))
	}

	// Second fetch should be empty
	fetched2, err := store.FetchOffline("clientA")
	if err != nil || len(fetched2) != 0 {
		t.Fatalf("Second FetchOffline expected 0, got %d", len(fetched2))
	}

	_ = store.Close()
}
