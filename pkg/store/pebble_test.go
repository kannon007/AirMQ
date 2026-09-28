package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"mqtt/pkg/protocol"
)

func TestPebbleStoreRetainedAndOffline(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pebble_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "pebble_data")
	store, err := NewPebbleStore(dbPath)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	defer store.Close()

	// 1. Test Retained messages
	msg1 := &protocol.PublishPacket{
		Topic:   "sensor/temp",
		Payload: []byte("25.4C"),
		QoS:     1,
		Retain:  true,
		Properties: &protocol.Properties{
			ContentType: "text/plain",
		},
	}
	msg1.Properties.AddUserProperty("device", "d-001")

	if err := store.SetRetained("sensor/temp", msg1); err != nil {
		t.Fatalf("SetRetained failed: %v", err)
	}

	got, err := store.GetRetained("sensor/temp")
	if err != nil || got == nil {
		t.Fatalf("GetRetained failed: %v, got: %v", err, got)
	}
	if got.Topic != msg1.Topic || !bytes.Equal(got.Payload, msg1.Payload) {
		t.Fatalf("Retained message mismatch: got %v", got)
	}
	if got.Properties == nil || got.Properties.ContentType != "text/plain" {
		t.Fatalf("Expected properties preserved in PebbleStore")
	}

	// GetAllRetained
	all, err := store.GetAllRetained()
	if err != nil || len(all) != 1 {
		t.Fatalf("GetAllRetained failed: %v, count: %d", err, len(all))
	}

	// DeleteRetained
	if err := store.DeleteRetained("sensor/temp"); err != nil {
		t.Fatalf("DeleteRetained failed: %v", err)
	}
	gotAfterDel, err := store.GetRetained("sensor/temp")
	if err != nil || gotAfterDel != nil {
		t.Fatalf("Expected nil after delete, got: %v", gotAfterDel)
	}

	// 2. Test Offline messages
	clientID := "client-test-pebble"
	off1 := &protocol.PublishPacket{
		Topic:   "orders/pending",
		Payload: []byte("order-1001"),
		QoS:     1,
	}
	off2 := &protocol.PublishPacket{
		Topic:   "orders/pending",
		Payload: []byte("order-1002"),
		QoS:     2,
	}

	if err := store.StoreOffline(clientID, off1); err != nil {
		t.Fatalf("StoreOffline 1 failed: %v", err)
	}
	if err := store.StoreOffline(clientID, off2); err != nil {
		t.Fatalf("StoreOffline 2 failed: %v", err)
	}

	// FetchOffline should retrieve both messages and clear them
	fetched, err := store.FetchOffline(clientID)
	if err != nil || len(fetched) != 2 {
		t.Fatalf("FetchOffline failed: %v, count: %d", err, len(fetched))
	}
	if !bytes.Equal(fetched[0].Payload, off1.Payload) || !bytes.Equal(fetched[1].Payload, off2.Payload) {
		t.Fatalf("Fetched messages mismatch: %s, %s", string(fetched[0].Payload), string(fetched[1].Payload))
	}

	// Second fetch should be empty
	fetchedAgain, err := store.FetchOffline(clientID)
	if err != nil || len(fetchedAgain) != 0 {
		t.Fatalf("Expected empty fetchedAgain, got %d", len(fetchedAgain))
	}
}
