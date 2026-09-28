package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// registeredProcessor pairs a Processor with its identity and performance metrics collector.
type registeredProcessor struct {
	name      string
	processor Processor
	collector *processorMetricCollector
}

// Pipe represents a composable sequence of Processors.
// Each processor decides whether to execute via its self-contained Match(c) method,
// while Pipe orchestrates execution and tracks performance metrics automatically.
type Pipe struct {
	mu         sync.RWMutex
	processors []registeredProcessor
	sampleMask int64 // 0 = 100% full profiling (default); 15 = sample 1/16; 63 = sample 1/64
}

// NewPipe creates an empty Pipe ready to receive processors.
func NewPipe() *Pipe {
	return &Pipe{
		processors: make([]registeredProcessor, 0, 8),
	}
}

// SetSampleRate configures the latency profiling sample rate.
// e.g. 1 for 100% full profiling (default), 16 for 1/16 sampled latency profiling.
func (p *Pipe) SetSampleRate(rate int) *Pipe {
	p.mu.Lock()
	defer p.mu.Unlock()

	if rate <= 1 {
		p.sampleMask = 0
	} else {
		// round to nearest power of 2 minus 1
		p.sampleMask = int64(rate - 1)
	}
	return p
}

// Add appends a named Processor to the pipeline.
func (p *Pipe) Add(name string, proc Processor) *Pipe {
	if proc == nil {
		return p
	}
	if name == "" {
		name = fmt.Sprintf("proc_%d", p.Len()+1)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.processors = append(p.processors, registeredProcessor{
		name:      name,
		processor: proc,
		collector: newProcessorMetricCollector(name),
	})
	return p
}

// AddFunc appends a processor defined by match and process functions.
func (p *Pipe) AddFunc(name string, matchFn func(c *Context) bool, processFn func(c *Context) error) *Pipe {
	return p.Add(name, NewFuncProcessor(matchFn, processFn))
}

// Stats returns an immutable point-in-time snapshot of metrics for all registered processors.
func (p *Pipe) Stats() []ProcessorStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	res := make([]ProcessorStats, len(p.processors))
	for i, rp := range p.processors {
		res[i] = rp.collector.Snapshot()
	}
	return res
}

// PrintStats returns a formatted ASCII table string of the current processor metrics.
func (p *Pipe) PrintStats() string {
	return FormatStatsTable(p.Stats())
}

// Len returns the count of registered processors.
func (p *Pipe) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return len(p.processors)
}

// ProcessorNames returns the names of all registered processors in execution order.
func (p *Pipe) ProcessorNames() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	names := make([]string, len(p.processors))
	for i, rp := range p.processors {
		names[i] = rp.name
	}
	return names
}

// Clear removes all processors from the pipeline.
func (p *Pipe) Clear() *Pipe {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.processors = p.processors[:0]
	return p
}

// Clone creates a shallow copy of the pipe with fresh metric collectors.
func (p *Pipe) Clone() *Pipe {
	p.mu.RLock()
	defer p.mu.RUnlock()

	clone := NewPipe()
	clone.sampleMask = p.sampleMask
	for _, rp := range p.processors {
		clone.Add(rp.name, rp.processor)
	}
	return clone
}

// Process executes the pipeline on an active Context.
// For each processor:
// 1. proc.Match(c) is checked (self-contained responsibility).
// 2. If matched, execution is timed and performance stats are recorded.
// 3. Execution stops if c is Dropped or Aborted.
func (p *Pipe) Process(c *Context) error {
	p.mu.RLock()
	procs := p.processors
	mask := p.sampleMask
	p.mu.RUnlock()

	for i := range procs {
		rp := &procs[i]

		// 1. Processor determines if it should handle this context
		if !rp.processor.Match(c) {
			rp.collector.recordSkipped()
			continue
		}

		// 2. Timed execution with optional high-throughput sampling
		var (
			start   time.Time
			sampled bool
		)
		if mask == 0 || (rp.collector.matchedCount.Load()&mask) == 0 {
			sampled = true
			start = time.Now()
		}

		err := rp.processor.Process(c)

		// 3. Record performance metrics
		if sampled {
			elapsed := time.Since(start)
			if mask > 0 {
				elapsed = elapsed * time.Duration(mask+1)
			}
			rp.collector.recordExecution(elapsed, c.IsDropped(), err)
		} else {
			rp.collector.recordQuickExecution(c.IsDropped(), err)
		}

		// 4. Check pipeline flow control
		if err != nil {
			c.Abort(err)
			return err
		}
		if c.IsAborted() {
			return c.Error()
		}
		if c.IsDropped() {
			return ErrMessageDropped
		}
	}

	if c.IsDropped() {
		return ErrMessageDropped
	}
	return c.Error()
}

// Execute runs the pipeline on a Message within a parent context.
// Allocates 0 heap memory on the hot path via pooled Context.
func (p *Pipe) Execute(parent context.Context, msg *Message) error {
	c := acquireContext(parent, p, msg)
	defer releaseContext(c)

	return p.Process(c)
}
