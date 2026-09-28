package pipeline

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrMessageDropped = errors.New("pipeline: message dropped")
	ErrAborted        = errors.New("pipeline: processing aborted")
	ErrUnauthorized   = errors.New("pipeline: unauthorized client")
)

// Context represents the execution context of a message passing through a pipeline of Processors.
// It embeds standard Go context.Context for cancellation, deadlines, and tracing,
// and provides flow control methods like Drop() and Abort().
type Context struct {
	context.Context

	Message *Message // The in-flight message envelope (payload, topic, metadata)

	pipe       *Pipe
	aborted    bool
	dropped    bool
	dropReason string
	err        error
	values     map[string]any
}

var contextPool = sync.Pool{
	New: func() any {
		return &Context{
			values: make(map[string]any, 4),
		}
	},
}

func acquireContext(parent context.Context, p *Pipe, msg *Message) *Context {
	c := contextPool.Get().(*Context)
	if parent == nil {
		parent = context.Background()
	}
	c.Context = parent
	c.Message = msg
	c.pipe = p
	c.aborted = false
	c.dropped = false
	c.dropReason = ""
	c.err = nil
	clear(c.values)
	return c
}

func releaseContext(c *Context) {
	if c != nil {
		c.Context = nil
		c.Message = nil
		c.pipe = nil
		contextPool.Put(c)
	}
}

// Drop silently halts message flow without a fatal error (e.g. filtered out by business logic).
func (c *Context) Drop(reason string) {
	c.dropped = true
	c.dropReason = reason
}

// IsDropped returns true if the message was dropped.
func (c *Context) IsDropped() bool {
	return c.dropped
}

// DropReason returns the reason message was dropped.
func (c *Context) DropReason() string {
	return c.dropReason
}

// Abort halts the remaining processor execution with an error.
func (c *Context) Abort(err error) {
	c.aborted = true
	c.err = err
}

// IsAborted returns true if Abort() was invoked.
func (c *Context) IsAborted() bool {
	return c.aborted
}

// Error returns the error that aborted the pipeline, if any.
func (c *Context) Error() error {
	return c.err
}

// Set stores a key-value pair in this context for downstream processors to consume.
func (c *Context) Set(key string, val any) {
	if c.values == nil {
		c.values = make(map[string]any)
	}
	c.values[key] = val
}

// Get retrieves a value stored in this context.
func (c *Context) Get(key string) (any, bool) {
	if c.values == nil {
		return nil, false
	}
	v, ok := c.values[key]
	return v, ok
}
