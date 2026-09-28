package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"mqtt/pkg/protocol"
)

// PebbleStore implements MessageStore on top of CockroachDB's Pebble LSM-tree.
type PebbleStore struct {
	db  *pebble.DB
	seq uint64
}

// NewPebbleStore opens or creates a Pebble LSM-tree database at the specified directory.
func NewPebbleStore(dir string) (*PebbleStore, error) {
	opts := &pebble.Options{}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble store at %s: %w", dir, err)
	}
	return &PebbleStore{db: db}, nil
}

func (p *PebbleStore) SetRetained(topic string, msg *protocol.PublishPacket) error {
	stored := StoredMessage{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		QoS:        msg.QoS,
		Retain:     msg.Retain,
		PacketID:   msg.PacketID,
		Properties: msg.Properties,
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	key := []byte(fmt.Sprintf("ret:%s", topic))
	return p.db.Set(key, data, pebble.NoSync)
}

func (p *PebbleStore) GetRetained(topic string) (*protocol.PublishPacket, error) {
	key := []byte(fmt.Sprintf("ret:%s", topic))
	val, closer, err := p.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var stored StoredMessage
	if err := json.Unmarshal(val, &stored); err != nil {
		return nil, err
	}

	return &protocol.PublishPacket{
		Topic:      stored.Topic,
		Payload:    stored.Payload,
		QoS:        stored.QoS,
		Retain:     stored.Retain,
		PacketID:   stored.PacketID,
		Properties: stored.Properties,
	}, nil
}

func (p *PebbleStore) DeleteRetained(topic string) error {
	key := []byte(fmt.Sprintf("ret:%s", topic))
	return p.db.Delete(key, pebble.NoSync)
}

func (p *PebbleStore) GetAllRetained() ([]*protocol.PublishPacket, error) {
	var result []*protocol.PublishPacket
	prefix := []byte("ret:")

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		var stored StoredMessage
		if err := json.Unmarshal(iter.Value(), &stored); err == nil {
			result = append(result, &protocol.PublishPacket{
				Topic:      stored.Topic,
				Payload:    stored.Payload,
				QoS:        stored.QoS,
				Retain:     stored.Retain,
				PacketID:   stored.PacketID,
				Properties: stored.Properties,
			})
		}
	}

	return result, nil
}

func (p *PebbleStore) StoreOffline(clientID string, msg *protocol.PublishPacket) error {
	stored := StoredMessage{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		QoS:        msg.QoS,
		Retain:     msg.Retain,
		PacketID:   msg.PacketID,
		Properties: msg.Properties,
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	seq := atomic.AddUint64(&p.seq, 1)
	key := []byte(fmt.Sprintf("off:%s:%020d_%010d", clientID, time.Now().UnixNano(), seq))
	return p.db.Set(key, data, pebble.NoSync)
}

func (p *PebbleStore) FetchOffline(clientID string) ([]*protocol.PublishPacket, error) {
	var result []*protocol.PublishPacket
	prefix := []byte(fmt.Sprintf("off:%s:", clientID))

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var keysToDelete [][]byte
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		k := append([]byte(nil), iter.Key()...)
		keysToDelete = append(keysToDelete, k)

		var stored StoredMessage
		if err := json.Unmarshal(iter.Value(), &stored); err == nil {
			result = append(result, &protocol.PublishPacket{
				Topic:      stored.Topic,
				Payload:    stored.Payload,
				QoS:        stored.QoS,
				Retain:     stored.Retain,
				PacketID:   stored.PacketID,
				Properties: stored.Properties,
			})
		}
	}

	if len(keysToDelete) > 0 {
		batch := p.db.NewBatch()
		defer batch.Close()
		for _, k := range keysToDelete {
			_ = batch.Delete(k, nil)
		}
		_ = batch.Commit(pebble.NoSync)
	}

	return result, nil
}

func (p *PebbleStore) ClearOffline(clientID string) error {
	prefix := []byte(fmt.Sprintf("off:%s:", clientID))

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var keysToDelete [][]byte
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		k := append([]byte(nil), iter.Key()...)
		keysToDelete = append(keysToDelete, k)
	}

	if len(keysToDelete) > 0 {
		batch := p.db.NewBatch()
		defer batch.Close()
		for _, k := range keysToDelete {
			_ = batch.Delete(k, nil)
		}
		return batch.Commit(pebble.NoSync)
	}

	return nil
}

func (p *PebbleStore) Close() error {
	return p.db.Close()
}
