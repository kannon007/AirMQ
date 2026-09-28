package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// Sink represents an external streaming consumer (e.g. Kafka, RabbitMQ, TSDB, Webhook).
type Sink interface {
	// Name returns the unique driver name (e.g. "kafka", "stdout", "mock").
	Name() string

	// Init initializes the sink with user-supplied key-value parameters.
	Init(ctx context.Context, config map[string]any) error

	// SendBatch delivers a batch of records. Returns failed records (if any) and error.
	SendBatch(ctx context.Context, records []*Record) (failedRecords []*Record, err error)

	// HealthCheck returns nil if connection to the external sink is healthy.
	HealthCheck(ctx context.Context) error

	// Ping tests connectivity to the external target with latency measurement.
	Ping(ctx context.Context) error

	// Close gracefully flushes remaining buffers and closes network connections.
	Close() error
}

// SinkFactory creates a new instance of a Sink.
type SinkFactory func() Sink

var (
	registryMu sync.RWMutex
	sinks      = make(map[string]SinkFactory)
)

// RegisterSink registers a Sink factory for pluggable extensions.
func RegisterSink(name string, factory SinkFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	sinks[name] = factory
}

// CreateSink instantiates a Sink by registered name.
func CreateSink(name string) (Sink, error) {
	registryMu.RLock()
	factory, ok := sinks[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("pipeline: unknown sink driver %q (available: %v)", name, ListSinks())
	}
	return factory(), nil
}

// ListSinks returns all currently registered sink drivers.
func ListSinks() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	list := make([]string, 0, len(sinks))
	for name := range sinks {
		list = append(list, name)
	}
	return list
}
