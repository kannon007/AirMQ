package store

import (
	"errors"
	"testing"

	"mqtt/pkg/protocol"
)

var errMockDiskFull = errors.New("mock disk full: write failed")

// FaultyStore injects artificial failures for testing resilience
type FaultyStore struct {
	FailSetRetained   bool
	FailGetRetained   bool
	FailDeleteRetain  bool
	FailStoreOffline  bool
	FailFetchOffline  bool
	FailClearOffline  bool
	Underlying        MessageStore
}

func (f *FaultyStore) SetRetained(topic string, msg *protocol.PublishPacket) error {
	if f.FailSetRetained {
		return errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.SetRetained(topic, msg)
	}
	return nil
}

func (f *FaultyStore) GetRetained(topic string) (*protocol.PublishPacket, error) {
	if f.FailGetRetained {
		return nil, errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.GetRetained(topic)
	}
	return nil, nil
}

func (f *FaultyStore) DeleteRetained(topic string) error {
	if f.FailDeleteRetain {
		return errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.DeleteRetained(topic)
	}
	return nil
}

func (f *FaultyStore) GetAllRetained() ([]*protocol.PublishPacket, error) {
	if f.FailGetRetained {
		return nil, errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.GetAllRetained()
	}
	return nil, nil
}

func (f *FaultyStore) StoreOffline(clientID string, msg *protocol.PublishPacket) error {
	if f.FailStoreOffline {
		return errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.StoreOffline(clientID, msg)
	}
	return nil
}

func (f *FaultyStore) FetchOffline(clientID string) ([]*protocol.PublishPacket, error) {
	if f.FailFetchOffline {
		return nil, errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.FetchOffline(clientID)
	}
	return nil, nil
}

func (f *FaultyStore) ClearOffline(clientID string) error {
	if f.FailClearOffline {
		return errMockDiskFull
	}
	if f.Underlying != nil {
		return f.Underlying.ClearOffline(clientID)
	}
	return nil
}

func (f *FaultyStore) Close() error {
	if f.Underlying != nil {
		return f.Underlying.Close()
	}
	return nil
}

func TestFaultyStore_ErrorInjection(t *testing.T) {
	mem := NewMemoryStore()
	faulty := &FaultyStore{
		Underlying: mem,
	}

	pkt := &protocol.PublishPacket{
		Topic:   "test/fault",
		Payload: []byte("val"),
		QoS:     1,
		Retain:  true,
	}

	// 1. Success path
	if err := faulty.SetRetained(pkt.Topic, pkt); err != nil {
		t.Fatalf("Expected success, got: %v", err)
	}

	// 2. Inject SetRetained failure
	faulty.FailSetRetained = true
	if err := faulty.SetRetained(pkt.Topic, pkt); err != errMockDiskFull {
		t.Fatalf("Expected errMockDiskFull, got: %v", err)
	}

	// 3. Inject GetRetained failure
	faulty.FailGetRetained = true
	if _, err := faulty.GetRetained(pkt.Topic); err != errMockDiskFull {
		t.Fatalf("Expected errMockDiskFull on GetRetained, got: %v", err)
	}
	if _, err := faulty.GetAllRetained(); err != errMockDiskFull {
		t.Fatalf("Expected errMockDiskFull on GetAllRetained, got: %v", err)
	}

	// 4. Inject Offline failure
	faulty.FailStoreOffline = true
	if err := faulty.StoreOffline("c1", pkt); err != errMockDiskFull {
		t.Fatalf("Expected errMockDiskFull on StoreOffline, got: %v", err)
	}

	faulty.FailFetchOffline = true
	if _, err := faulty.FetchOffline("c1"); err != errMockDiskFull {
		t.Fatalf("Expected errMockDiskFull on FetchOffline, got: %v", err)
	}
}
