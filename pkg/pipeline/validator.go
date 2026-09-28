package pipeline

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrPayloadTooShort = errors.New("validator: payload too short")
	ErrPayloadTooLarge = errors.New("validator: payload exceeds max size")
	ErrMagicMismatch   = errors.New("validator: binary magic number mismatch")
	ErrInvalidJSON     = errors.New("validator: invalid JSON syntax")
	ErrInvalidTLV      = errors.New("validator: invalid TLV header")
)

// PayloadFormat represents the serialization or encoding format of the payload.
type PayloadFormat string

const (
	FormatRaw      PayloadFormat = "raw"
	FormatJSON     PayloadFormat = "json"
	FormatProtobuf PayloadFormat = "protobuf"
	FormatTLV      PayloadFormat = "tlv"
	FormatCustom   PayloadFormat = "custom"
)

// Validator defines a pluggable message payload validator (JSON Schema, Protobuf, TLV, Raw).
type Validator interface {
	Name() string
	Format() PayloadFormat
	Validate(payload []byte) error
}

// RawValidator enforces basic payload size constraints.
type RawValidator struct {
	name   string
	minLen int
	maxLen int
}

// NewRawValidator creates a raw bytes validator.
func NewRawValidator(name string, minLen, maxLen int) *RawValidator {
	if name == "" {
		name = "raw_validator"
	}
	return &RawValidator{name: name, minLen: minLen, maxLen: maxLen}
}

func (v *RawValidator) Name() string          { return v.name }
func (v *RawValidator) Format() PayloadFormat { return FormatRaw }

func (v *RawValidator) Validate(payload []byte) error {
	if v.minLen > 0 && len(payload) < v.minLen {
		return fmt.Errorf("%w: length %d < minimum %d", ErrPayloadTooShort, len(payload), v.minLen)
	}
	if v.maxLen > 0 && len(payload) > v.maxLen {
		return fmt.Errorf("%w: length %d > maximum %d", ErrPayloadTooLarge, len(payload), v.maxLen)
	}
	return nil
}

// BinaryValidator enforces binary framing rules: length limits, magic headers, and optional TLV framing.
type BinaryValidator struct {
	name      string
	minLen    int
	maxLen    int
	magic     []byte
	format    PayloadFormat
	verifyTLV bool
}

// BinaryValidatorOption configures binary validator behavior.
type BinaryValidatorOption func(*BinaryValidator)

// WithMagic requires the payload to start with the given magic byte sequence.
func WithMagic(magic []byte) BinaryValidatorOption {
	return func(bv *BinaryValidator) {
		bv.magic = magic
	}
}

// WithTLVValidation verifies that the binary payload has valid TLV (Type:2B, Length:2B, Value:NB) framing.
func WithTLVValidation() BinaryValidatorOption {
	return func(bv *BinaryValidator) {
		bv.verifyTLV = true
		bv.format = FormatTLV
	}
}

// WithProtobufFormat sets the format label to Protobuf.
func WithProtobufFormat() BinaryValidatorOption {
	return func(bv *BinaryValidator) {
		bv.format = FormatProtobuf
	}
}

// NewBinaryValidator creates a high-performance zero-allocation binary payload validator.
func NewBinaryValidator(name string, minLen, maxLen int, opts ...BinaryValidatorOption) *BinaryValidator {
	if name == "" {
		name = "binary_validator"
	}
	bv := &BinaryValidator{
		name:   name,
		minLen: minLen,
		maxLen: maxLen,
		format: FormatRaw,
	}
	for _, opt := range opts {
		opt(bv)
	}
	return bv
}

func (v *BinaryValidator) Name() string          { return v.name }
func (v *BinaryValidator) Format() PayloadFormat { return v.format }

func (v *BinaryValidator) Validate(payload []byte) error {
	pLen := len(payload)
	if v.minLen > 0 && pLen < v.minLen {
		return fmt.Errorf("%w: length %d < min %d", ErrPayloadTooShort, pLen, v.minLen)
	}
	if v.maxLen > 0 && pLen > v.maxLen {
		return fmt.Errorf("%w: length %d > max %d", ErrPayloadTooLarge, pLen, v.maxLen)
	}
	if len(v.magic) > 0 {
		if pLen < len(v.magic) || !bytes.Equal(payload[:len(v.magic)], v.magic) {
			return ErrMagicMismatch
		}
	}
	if v.verifyTLV {
		// Minimum TLV packet is 4 bytes (2B Type + 2B Length)
		offset := len(v.magic)
		for offset < pLen {
			if offset+4 > pLen {
				return fmt.Errorf("%w: truncated TLV header at offset %d", ErrInvalidTLV, offset)
			}
			valLen := int(binary.BigEndian.Uint16(payload[offset+2 : offset+4]))
			offset += 4 + valLen
			if offset > pLen {
				return fmt.Errorf("%w: TLV value exceeds packet boundary", ErrInvalidTLV)
			}
		}
	}
	return nil
}

// JSONValidator validates that payloads conform to valid JSON formatting.
type JSONValidator struct {
	name   string
	minLen int
	maxLen int
}

// NewJSONValidator creates a JSON syntax validator.
func NewJSONValidator(name string) *JSONValidator {
	if name == "" {
		name = "json_validator"
	}
	return &JSONValidator{name: name}
}

func (v *JSONValidator) Name() string          { return v.name }
func (v *JSONValidator) Format() PayloadFormat { return FormatJSON }

func (v *JSONValidator) Validate(payload []byte) error {
	if len(payload) == 0 {
		return ErrPayloadTooShort
	}
	if !json.Valid(payload) {
		return ErrInvalidJSON
	}
	return nil
}
