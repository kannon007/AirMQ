package pipeline

// AuthProcessor enforces client credentials and access control.
type AuthProcessor struct {
	topicFilter string
	allowFn     func(clientID, username string) bool
	matchFn     func(c *Context) bool
}

// AuthOption configures AuthProcessor.
type AuthOption func(*AuthProcessor)

// WithAuthTopic restricts authentication check to a specific topic pattern.
func WithAuthTopic(topicFilter string) AuthOption {
	return func(p *AuthProcessor) { p.topicFilter = topicFilter }
}

// WithAuthMatch customizes the Match condition.
func WithAuthMatch(matchFn func(c *Context) bool) AuthOption {
	return func(p *AuthProcessor) { p.matchFn = matchFn }
}

// NewAuthProcessor creates a new AuthProcessor.
func NewAuthProcessor(allowFn func(clientID, username string) bool, opts ...AuthOption) *AuthProcessor {
	ap := &AuthProcessor{allowFn: allowFn}
	for _, opt := range opts {
		opt(ap)
	}
	return ap
}

// Match determines if this processor should authenticate the client.
// Internal system events ($internal) are automatically exempted.
func (p *AuthProcessor) Match(c *Context) bool {
	if c.Message == nil {
		return false
	}
	if c.Message.ClientID == "$internal" {
		return false
	}
	if p.topicFilter != "" && !MatchTopicFilter(p.topicFilter, c.Message.Topic) {
		return false
	}
	if p.matchFn != nil {
		return p.matchFn(c)
	}
	return true
}

// Process performs the authentication logic.
func (p *AuthProcessor) Process(c *Context) error {
	if p.allowFn != nil && !p.allowFn(c.Message.ClientID, c.Message.Username) {
		c.Drop("unauthorized client")
		return ErrUnauthorized
	}
	return nil
}

// ValidateProcessor checks message payload conformance.
type ValidateProcessor struct {
	validator   Validator
	topicFilter string
	onFail      func(c *Context, err error)
	matchFn     func(c *Context) bool
}

// ValidateOption configures ValidateProcessor.
type ValidateOption func(*ValidateProcessor)

// WithValidateTopic specifies which topic filter this validator targets.
func WithValidateTopic(topicFilter string) ValidateOption {
	return func(p *ValidateProcessor) { p.topicFilter = topicFilter }
}

// WithValidateOnFail configures custom error handling on validation failure.
func WithValidateOnFail(onFail func(c *Context, err error)) ValidateOption {
	return func(p *ValidateProcessor) { p.onFail = onFail }
}

// WithValidateMatch customizes the Match condition.
func WithValidateMatch(matchFn func(c *Context) bool) ValidateOption {
	return func(p *ValidateProcessor) { p.matchFn = matchFn }
}

// NewValidateProcessor creates a new ValidateProcessor.
func NewValidateProcessor(v Validator, opts ...ValidateOption) *ValidateProcessor {
	vp := &ValidateProcessor{validator: v}
	for _, opt := range opts {
		opt(vp)
	}
	return vp
}

// Match determines if this message should be validated.
// Empty payloads or non-matching topics are skipped.
func (p *ValidateProcessor) Match(c *Context) bool {
	if c.Message == nil || len(c.Message.Payload) == 0 {
		return false
	}
	if p.topicFilter != "" && !MatchTopicFilter(p.topicFilter, c.Message.Topic) {
		return false
	}
	if p.matchFn != nil {
		return p.matchFn(c)
	}
	return true
}

// Process executes the schema validator.
func (p *ValidateProcessor) Process(c *Context) error {
	if p.validator == nil {
		return nil
	}
	if err := p.validator.Validate(c.Message.Payload); err != nil {
		if p.onFail != nil {
			p.onFail(c, err)
		} else {
			c.Drop("validation failed: " + err.Error())
		}
		return nil
	}
	return nil
}

// ForwardProcessor forwards messages to an external sink (e.g. Kafka, NATS).
type ForwardProcessor struct {
	sinkFn      func(msg *Message) error
	topicFilter string
	passthrough bool
	matchFn     func(c *Context) bool
}

// ForwardOption configures ForwardProcessor.
type ForwardOption func(*ForwardProcessor)

// WithForwardTopic restricts forwarding to matching topics.
func WithForwardTopic(topicFilter string) ForwardOption {
	return func(p *ForwardProcessor) { p.topicFilter = topicFilter }
}

// WithForwardMatch customizes the Match condition.
func WithForwardMatch(matchFn func(c *Context) bool) ForwardOption {
	return func(p *ForwardProcessor) { p.matchFn = matchFn }
}

// NewForwardProcessor creates a new ForwardProcessor.
func NewForwardProcessor(sinkFn func(msg *Message) error, passthrough bool, opts ...ForwardOption) *ForwardProcessor {
	fp := &ForwardProcessor{
		sinkFn:      sinkFn,
		passthrough: passthrough,
	}
	for _, opt := range opts {
		opt(fp)
	}
	return fp
}

// Match determines if this message should be forwarded.
func (p *ForwardProcessor) Match(c *Context) bool {
	if c.Message == nil {
		return false
	}
	if p.topicFilter != "" && !MatchTopicFilter(p.topicFilter, c.Message.Topic) {
		return false
	}
	if p.matchFn != nil {
		return p.matchFn(c)
	}
	return true
}

// Process sends the message to the external sink.
func (p *ForwardProcessor) Process(c *Context) error {
	if p.sinkFn == nil {
		return nil
	}
	if err := p.sinkFn(c.Message); err != nil {
		c.Abort(err)
		return err
	}
	if !p.passthrough {
		c.Drop("egress only")
	}
	return nil
}

// TransformProcessor mutates or enriches the message payload/headers.
type TransformProcessor struct {
	transformFn func(msg *Message) error
	topicFilter string
	matchFn     func(c *Context) bool
}

// TransformOption configures TransformProcessor.
type TransformOption func(*TransformProcessor)

// WithTransformTopic restricts transformation to matching topics.
func WithTransformTopic(topicFilter string) TransformOption {
	return func(p *TransformProcessor) { p.topicFilter = topicFilter }
}

// WithTransformMatch customizes the Match condition.
func WithTransformMatch(matchFn func(c *Context) bool) TransformOption {
	return func(p *TransformProcessor) { p.matchFn = matchFn }
}

// NewTransformProcessor creates a new TransformProcessor.
func NewTransformProcessor(transformFn func(msg *Message) error, opts ...TransformOption) *TransformProcessor {
	tp := &TransformProcessor{transformFn: transformFn}
	for _, opt := range opts {
		opt(tp)
	}
	return tp
}

// Match determines if this message should be transformed.
func (p *TransformProcessor) Match(c *Context) bool {
	if c.Message == nil {
		return false
	}
	if p.topicFilter != "" && !MatchTopicFilter(p.topicFilter, c.Message.Topic) {
		return false
	}
	if p.matchFn != nil {
		return p.matchFn(c)
	}
	return true
}

// Process applies the transformation function.
func (p *TransformProcessor) Process(c *Context) error {
	if p.transformFn != nil {
		return p.transformFn(c.Message)
	}
	return nil
}

// FilterProcessor drops messages that do not meet a predicate.
type FilterProcessor struct {
	predicate func(msg *Message) bool
}

// NewFilterProcessor creates a new FilterProcessor.
func NewFilterProcessor(predicate func(msg *Message) bool) *FilterProcessor {
	return &FilterProcessor{predicate: predicate}
}

// Match determines if this message should be evaluated.
func (p *FilterProcessor) Match(c *Context) bool {
	return c.Message != nil
}

// Process filters out messages that do not satisfy the predicate.
func (p *FilterProcessor) Process(c *Context) error {
	if p.predicate != nil && !p.predicate(c.Message) {
		c.Drop("filtered by predicate")
	}
	return nil
}
