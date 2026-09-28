package store

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"mqtt/pkg/protocol"
)

// BadgerStore implements MessageStore on top of BadgerDB for crash-resilient persistence.
type BadgerStore struct {
	db     *badger.DB
	seq    uint64
	stopGC chan struct{}
}

// NewBadgerStore opens or creates a BadgerDB at the specified directory.
func NewBadgerStore(dir string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(dir).
		WithLoggingLevel(badger.ERROR).
		WithSyncWrites(false) // Async flush for high performance

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger store at %s: %w", dir, err)
	}

	bs := &BadgerStore{
		db:     db,
		stopGC: make(chan struct{}),
	}

	// Start background ValueLog GC ticker (every 10 minutes)
	go bs.startGCLoop()

	return bs, nil
}

func (b *BadgerStore) startGCLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Run ValueLog GC while more than 50% can be reclaimed
			for {
				if err := b.db.RunValueLogGC(0.5); err != nil {
					break
				}
			}
		case <-b.stopGC:
			return
		}
	}
}

func (b *BadgerStore) SetRetained(topic string, msg *protocol.PublishPacket) error {
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
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *BadgerStore) GetRetained(topic string) (*protocol.PublishPacket, error) {
	key := []byte(fmt.Sprintf("ret:%s", topic))
	var pkt *protocol.PublishPacket

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		return item.Value(func(val []byte) error {
			var stored StoredMessage
			if err := json.Unmarshal(val, &stored); err != nil {
				return err
			}
			pkt = &protocol.PublishPacket{
				Topic:    stored.Topic,
				Payload:  stored.Payload,
				QoS:      stored.QoS,
				Retain:   stored.Retain,
				PacketID: stored.PacketID,
			}
			return nil
		})
	})

	return pkt, err
}

func (b *BadgerStore) DeleteRetained(topic string) error {
	key := []byte(fmt.Sprintf("ret:%s", topic))
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *BadgerStore) GetAllRetained() ([]*protocol.PublishPacket, error) {
	var result []*protocol.PublishPacket
	prefix := []byte("ret:")

	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			_ = item.Value(func(val []byte) error {
				var stored StoredMessage
				if err := json.Unmarshal(val, &stored); err == nil {
					result = append(result, &protocol.PublishPacket{
						Topic:    stored.Topic,
						Payload:  stored.Payload,
						QoS:      stored.QoS,
						Retain:   stored.Retain,
						PacketID: stored.PacketID,
					})
				}
				return nil
			})
		}
		return nil
	})

	return result, err
}

func (b *BadgerStore) StoreOffline(clientID string, msg *protocol.PublishPacket) error {
	stored := StoredMessage{
		Topic:    msg.Topic,
		Payload:  msg.Payload,
		QoS:      msg.QoS,
		Retain:   msg.Retain,
		PacketID: msg.PacketID,
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	seq := atomic.AddUint64(&b.seq, 1)
	key := []byte(fmt.Sprintf("off:%s:%020d_%010d", clientID, time.Now().UnixNano(), seq))
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *BadgerStore) FetchOffline(clientID string) ([]*protocol.PublishPacket, error) {
	var result []*protocol.PublishPacket
	prefix := []byte(fmt.Sprintf("off:%s:", clientID))

	err := b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var keysToDelete [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)

			_ = item.Value(func(val []byte) error {
				var stored StoredMessage
				if err := json.Unmarshal(val, &stored); err == nil {
					result = append(result, &protocol.PublishPacket{
						Topic:    stored.Topic,
						Payload:  stored.Payload,
						QoS:      stored.QoS,
						Retain:   stored.Retain,
						PacketID: stored.PacketID,
					})
				}
				return nil
			})
		}

		for _, k := range keysToDelete {
			_ = txn.Delete(k)
		}
		return nil
	})

	return result, err
}

func (b *BadgerStore) ClearOffline(clientID string) error {
	prefix := []byte(fmt.Sprintf("off:%s:", clientID))
	return b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var keysToDelete [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, k := range keysToDelete {
			_ = txn.Delete(k)
		}
		return nil
	})
}

func (b *BadgerStore) Close() error {
	close(b.stopGC)
	return b.db.Close()
}
