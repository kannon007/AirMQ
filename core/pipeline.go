package core

import (
	"mqtt/pkg/pipeline"
)

// Re-export idiomatic Go pipeline types
type (
	Message        = pipeline.Message
	Context        = pipeline.Context
	Processor      = pipeline.Processor
	ProcessorStats = pipeline.ProcessorStats
	Pipe           = pipeline.Pipe
	Pipeline       = pipeline.Pipe
	Router         = pipeline.Router
	Validator      = pipeline.Validator

	// Concrete processor types
	AuthProcessor      = pipeline.AuthProcessor
	ValidateProcessor  = pipeline.ValidateProcessor
	ForwardProcessor   = pipeline.ForwardProcessor
	TransformProcessor = pipeline.TransformProcessor
	FilterProcessor    = pipeline.FilterProcessor
	FuncProcessor      = pipeline.FuncProcessor
)

// Pipeline error variables
var (
	ErrMessageDropped = pipeline.ErrMessageDropped
	ErrAborted        = pipeline.ErrAborted
	ErrUnauthorized   = pipeline.ErrUnauthorized
)

// Constructors
var (
	NewPipe          = pipeline.NewPipe
	NewPipeline      = pipeline.NewPipe
	NewRouter        = pipeline.NewRouter
	NewFuncProcessor = pipeline.NewFuncProcessor

	// Concrete processor constructors
	NewAuthProcessor      = pipeline.NewAuthProcessor
	NewValidateProcessor  = pipeline.NewValidateProcessor
	NewForwardProcessor   = pipeline.NewForwardProcessor
	NewTransformProcessor = pipeline.NewTransformProcessor
	NewFilterProcessor    = pipeline.NewFilterProcessor

	// Processor options
	WithAuthTopic      = pipeline.WithAuthTopic
	WithAuthMatch      = pipeline.WithAuthMatch
	WithValidateTopic  = pipeline.WithValidateTopic
	WithValidateOnFail = pipeline.WithValidateOnFail
	WithValidateMatch  = pipeline.WithValidateMatch
	WithForwardTopic   = pipeline.WithForwardTopic
	WithForwardMatch   = pipeline.WithForwardMatch
	WithTransformTopic = pipeline.WithTransformTopic
	WithTransformMatch = pipeline.WithTransformMatch

	// Multi-format validator implementations (Raw, Binary TLV, JSON)
	NewRawValidator    = pipeline.NewRawValidator
	NewBinaryValidator = pipeline.NewBinaryValidator
	NewJSONValidator   = pipeline.NewJSONValidator

	// Validator options
	WithMagic          = pipeline.WithMagic
	WithTLVValidation  = pipeline.WithTLVValidation
	WithProtobufFormat = pipeline.WithProtobufFormat

	// Zero-allocation message pool helpers
	AcquireMessage = pipeline.AcquireMessage
	ReleaseMessage = pipeline.ReleaseMessage
)
