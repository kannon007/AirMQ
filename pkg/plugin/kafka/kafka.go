package kafka

import (
	"strings"
	"sync"
	"time"

	"mqtt/pkg/hook"
	"mqtt/pkg/protocol"
)

// KafkaMessage represents a bridged MQTT message to be pushed to Kafka.
type KafkaMessage struct {
	MQTTTopic string
	ClientID  string
	Payload   []byte
	Timestamp time.Time
}

// KafkaProducer defines the contract for producing batches to Kafka.
// This decouples from any specific Kafka driver (sarama, confluent-kafka-go, segmentio/kafka-go).
type KafkaProducer interface {
	ProduceBatch(messages []*KafkaMessage) error
	Close() error
}

type KafkaBridgeConfig struct {
	TopicFilters []string // which MQTT topics to bridge; empty means bridge all
	BatchSize    int
	FlushTimeout time.Duration
	QueueSize    int
}

// KafkaBridgeHook bridges MQTT PUBLISH packets to Kafka asynchronously.
type KafkaBridgeHook struct {
	hook.BaseHook
	cfg       KafkaBridgeConfig
	producer  KafkaProducer
	queue     chan *KafkaMessage
	stopCh    chan struct{}
	closeOnce sync.Once
}

func NewKafkaBridgeHook(cfg KafkaBridgeConfig, producer KafkaProducer) *KafkaBridgeHook {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 50 * time.Millisecond
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 65536
	}

	hook := &KafkaBridgeHook{
		cfg:      cfg,
		producer: producer,
		queue:    make(chan *KafkaMessage, cfg.QueueSize),
		stopCh:   make(chan struct{}),
	}

	go hook.batchWorker()
	return hook
}

func (k *KafkaBridgeHook) Name() string {
	return "KafkaBridgeHook"
}

// OnPublish captures PUBLISH messages and pushes them to the async batching queue.
func (k *KafkaBridgeHook) OnPublish(ctx *hook.ClientContext, pkt *protocol.PublishPacket) (bool, error) {
	if !k.matchFilters(pkt.Topic) {
		return false, nil // don't bridge, don't drop
	}

	msg := &KafkaMessage{
		MQTTTopic: pkt.Topic,
		ClientID:  ctx.ClientID,
		Payload:   pkt.Payload,
		Timestamp: time.Now(),
	}

	select {
	case k.queue <- msg:
	default:
		// Queue full: non-blocking drop or counter bump to prevent stalling MQTT broker
	}

	return false, nil // do not drop the MQTT packet
}

func (k *KafkaBridgeHook) matchFilters(topic string) bool {
	if len(k.cfg.TopicFilters) == 0 {
		return true
	}
	for _, filter := range k.cfg.TopicFilters {
		if filter == "#" || filter == topic || strings.HasPrefix(topic, strings.TrimSuffix(filter, "#")) {
			return true
		}
	}
	return false
}

func (k *KafkaBridgeHook) batchWorker() {
	batch := make([]*KafkaMessage, 0, k.cfg.BatchSize)
	ticker := time.NewTicker(k.cfg.FlushTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 && k.producer != nil {
			_ = k.producer.ProduceBatch(batch)
			batch = make([]*KafkaMessage, 0, k.cfg.BatchSize)
		}
	}

	for {
		select {
		case <-k.stopCh:
			flush()
			return
		case msg := <-k.queue:
			batch = append(batch, msg)
			if len(batch) >= k.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (k *KafkaBridgeHook) Close() {
	k.closeOnce.Do(func() {
		close(k.stopCh)
		if k.producer != nil {
			_ = k.producer.Close()
		}
	})
}
