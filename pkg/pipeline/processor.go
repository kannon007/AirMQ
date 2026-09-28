package pipeline

// Processor represents a stage in the message processing pipeline.
// Every processor encapsulates its own responsibility via two core methods:
// 1. Match: decides whether this processor wants to handle the message/event (self-contained responsibility).
// 2. Process: executes the concrete processing logic when matched.
type Processor interface {
	// Match determines if this processor should handle the incoming message context.
	// The processor knows its own requirements (e.g. topic, clientID, payload format).
	Match(c *Context) bool

	// Process executes the concrete business logic.
	Process(c *Context) error
}

// FuncProcessor is a convenience adapter implementing Processor using functions.
type FuncProcessor struct {
	matchFn   func(c *Context) bool
	processFn func(c *Context) error
}

// NewFuncProcessor creates a Processor from match and process functions.
func NewFuncProcessor(matchFn func(c *Context) bool, processFn func(c *Context) error) *FuncProcessor {
	return &FuncProcessor{
		matchFn:   matchFn,
		processFn: processFn,
	}
}

func (p *FuncProcessor) Match(c *Context) bool {
	if p.matchFn == nil {
		return true
	}
	return p.matchFn(c)
}

func (p *FuncProcessor) Process(c *Context) error {
	if p.processFn == nil {
		return nil
	}
	return p.processFn(c)
}
