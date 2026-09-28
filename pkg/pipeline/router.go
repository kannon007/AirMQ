package pipeline

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

type routeEntry struct {
	pattern string
	pipe    *Pipe
	isExact bool
	isWild  bool
	prefix  string
}

type compiledRoutes struct {
	entries []routeEntry
}

// Router dispatches incoming messages to matching Pipelines based on topic patterns.
// It supports dynamic binding and an ultra-fast atomic bypass check (<1ns) when empty.
type Router struct {
	mu       sync.RWMutex
	routes   map[string]*Pipe
	compiled atomic.Pointer[compiledRoutes]
	hasRules atomic.Bool
}

// NewRouter creates an empty pipeline Router.
func NewRouter() *Router {
	r := &Router{
		routes: make(map[string]*Pipe),
	}
	r.recompile()
	return r
}

// HasPipelines returns true if any pipeline route is registered.
// This is an atomic read requiring 0 memory allocations and <1ns latency.
func (r *Router) HasPipelines() bool {
	return r.hasRules.Load()
}

// Handle registers a Pipeline for a topic pattern (e.g. "telemetry/#", "sensor/+/metrics").
func (r *Router) Handle(pattern string, p *Pipe) {
	if pattern == "" || p == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.routes[pattern] = p
	r.recompile()
}

// Add appends a named Processor to the pipeline registered for pattern, creating it if needed.
func (r *Router) Add(pattern, name string, proc Processor) {
	if pattern == "" || proc == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	p, ok := r.routes[pattern]
	if !ok {
		p = NewPipe()
		r.routes[pattern] = p
	}
	p.Add(name, proc)
	r.recompile()
}

// AddFunc appends a functional processor to the pattern's pipeline.
func (r *Router) AddFunc(pattern, name string, matchFn func(c *Context) bool, processFn func(c *Context) error) {
	r.Add(pattern, name, NewFuncProcessor(matchFn, processFn))
}

// Remove unregisters a topic pattern route.
func (r *Router) Remove(pattern string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.routes[pattern]; !ok {
		return false
	}
	delete(r.routes, pattern)
	r.recompile()
	return true
}

// Get retrieves the pipeline for a pattern.
func (r *Router) Get(pattern string) (*Pipe, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.routes[pattern]
	return p, ok
}

// Routes returns a copy of all registered pattern bindings.
func (r *Router) Routes() map[string]*Pipe {
	r.mu.RLock()
	defer r.mu.RUnlock()

	copyMap := make(map[string]*Pipe, len(r.routes))
	for k, v := range r.routes {
		copyMap[k] = v
	}
	return copyMap
}

func (r *Router) recompile() {
	var list []routeEntry
	for pat, p := range r.routes {
		entry := routeEntry{
			pattern: pat,
			pipe:    p,
		}
		if pat == "#" {
			entry.isWild = true
		} else if strings.HasSuffix(pat, "/#") {
			entry.prefix = strings.TrimSuffix(pat, "#")
		} else if strings.Contains(pat, "+") || strings.Contains(pat, "#") {
			entry.isWild = true
		} else {
			entry.isExact = true
		}
		list = append(list, entry)
	}

	set := &compiledRoutes{entries: list}
	r.compiled.Store(set)
	r.hasRules.Store(len(list) > 0)
}

// Process evaluates a message across all matching Pipelines.
func (r *Router) Process(ctx context.Context, msg *Message) error {
	if !r.hasRules.Load() {
		return nil
	}

	set := r.compiled.Load()
	if set == nil || len(set.entries) == 0 {
		return nil
	}

	for _, entry := range set.entries {
		if !matchRoute(entry, msg.Topic) {
			continue
		}

		if err := entry.pipe.Execute(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func matchRoute(entry routeEntry, topic string) bool {
	if entry.pattern == "#" {
		return true
	}
	if entry.isExact {
		return entry.pattern == topic
	}
	if entry.prefix != "" && strings.HasPrefix(topic, entry.prefix) {
		return true
	}
	if entry.isWild {
		return MatchTopicFilter(entry.pattern, topic)
	}
	return false
}
