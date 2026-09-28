package protocol

import (
	"encoding/binary"
	"errors"
)

// MQTT 5.0 Property Identifiers (OASIS Specification)
const (
	PropPayloadFormatIndicator          byte = 0x01 // Byte
	PropMessageExpiryInterval           byte = 0x02 // Four Byte Integer
	PropContentType                     byte = 0x03 // UTF-8 String
	PropResponseTopic                   byte = 0x08 // UTF-8 String
	PropCorrelationData                 byte = 0x09 // Binary Data
	PropSubscriptionIdentifier          byte = 0x0B // Variable Byte Integer
	PropSessionExpiryInterval           byte = 0x11 // Four Byte Integer
	PropAssignedClientIdentifier        byte = 0x12 // UTF-8 String
	PropServerKeepAlive                 byte = 0x13 // Two Byte Integer
	PropAuthenticationMethod            byte = 0x15 // UTF-8 String
	PropAuthenticationData              byte = 0x16 // Binary Data
	PropRequestProblemInformation       byte = 0x17 // Byte
	PropWillDelayInterval               byte = 0x18 // Four Byte Integer
	PropRequestResponseInformation      byte = 0x19 // Byte
	PropResponseInformation             byte = 0x1A // UTF-8 String
	PropServerReference                 byte = 0x1C // UTF-8 String
	PropReasonString                    byte = 0x1F // UTF-8 String
	PropReceiveMaximum                  byte = 0x21 // Two Byte Integer
	PropTopicAliasMaximum               byte = 0x22 // Two Byte Integer
	PropTopicAlias                      byte = 0x23 // Two Byte Integer
	PropMaximumQoS                      byte = 0x24 // Byte
	PropRetainAvailable                 byte = 0x25 // Byte
	PropUserProperty                    byte = 0x26 // UTF-8 String Pair
	PropMaximumPacketSize               byte = 0x27 // Four Byte Integer
	PropWildcardSubscriptionAvailable   byte = 0x28 // Byte
	PropSubscriptionIdentifierAvailable byte = 0x29 // Byte
	PropSharedSubscriptionAvailable     byte = 0x2A // Byte
)

var (
	ErrMalformedProperty = errors.New("malformed mqtt 5.0 property")
)

// UserPropertyEntry represents a key-value user property pair.
type UserPropertyEntry struct {
	Key   string
	Value string
}

// Properties stores all MQTT 5.0 packet properties in a strongly-typed structure.
type Properties struct {
	PayloadFormatIndicator          *byte
	MessageExpiryInterval           *uint32
	ContentType                     string
	ResponseTopic                   string
	CorrelationData                 []byte
	SubscriptionIdentifier          *int
	SessionExpiryInterval           *uint32
	AssignedClientIdentifier        string
	ServerKeepAlive                 *uint16
	AuthenticationMethod            string
	AuthenticationData              []byte
	RequestProblemInformation       *byte
	WillDelayInterval               *uint32
	RequestResponseInformation      *byte
	ResponseInformation             string
	ServerReference                 string
	ReasonString                    string
	ReceiveMaximum                  *uint16
	TopicAliasMaximum               *uint16
	TopicAlias                      *uint16
	MaximumQoS                      *byte
	RetainAvailable                 *byte
	UserProperties                  []UserPropertyEntry
	MaximumPacketSize               *uint32
	WildcardSubscriptionAvailable   *byte
	SubscriptionIdentifierAvailable *byte
	SharedSubscriptionAvailable     *byte
}

// AddUserProperty appends a user property key-value pair.
func (p *Properties) AddUserProperty(key, value string) {
	p.UserProperties = append(p.UserProperties, UserPropertyEntry{Key: key, Value: value})
}

// GetUserProperty returns the first user property value matching the key.
func (p *Properties) GetUserProperty(key string) (string, bool) {
	for _, up := range p.UserProperties {
		if up.Key == key {
			return up.Value, true
		}
	}
	return "", false
}

// IsEmpty returns true if no properties are configured.
func (p *Properties) IsEmpty() bool {
	if p == nil {
		return true
	}
	return p.PayloadFormatIndicator == nil &&
		p.MessageExpiryInterval == nil &&
		p.ContentType == "" &&
		p.ResponseTopic == "" &&
		len(p.CorrelationData) == 0 &&
		p.SubscriptionIdentifier == nil &&
		p.SessionExpiryInterval == nil &&
		p.AssignedClientIdentifier == "" &&
		p.ServerKeepAlive == nil &&
		p.AuthenticationMethod == "" &&
		len(p.AuthenticationData) == 0 &&
		p.RequestProblemInformation == nil &&
		p.WillDelayInterval == nil &&
		p.RequestResponseInformation == nil &&
		p.ResponseInformation == "" &&
		p.ServerReference == "" &&
		p.ReasonString == "" &&
		p.ReceiveMaximum == nil &&
		p.TopicAliasMaximum == nil &&
		p.TopicAlias == nil &&
		p.MaximumQoS == nil &&
		p.RetainAvailable == nil &&
		len(p.UserProperties) == 0 &&
		p.MaximumPacketSize == nil &&
		p.WildcardSubscriptionAvailable == nil &&
		p.SubscriptionIdentifierAvailable == nil &&
		p.SharedSubscriptionAvailable == nil
}

// EncodeProperties serializes MQTT 5.0 properties into [Property Length (varbyte)][Property Body].
// If p is nil or empty, returns []byte{0x00} representing 0 property length.
func EncodeProperties(p *Properties) []byte {
	if p == nil || p.IsEmpty() {
		return []byte{0x00}
	}

	var raw []byte

	if p.PayloadFormatIndicator != nil {
		raw = append(raw, PropPayloadFormatIndicator, *p.PayloadFormatIndicator)
	}
	if p.MessageExpiryInterval != nil {
		raw = append(raw, PropMessageExpiryInterval)
		raw = binary.BigEndian.AppendUint32(raw, *p.MessageExpiryInterval)
	}
	if p.ContentType != "" {
		raw = append(raw, PropContentType)
		raw = appendString(raw, p.ContentType)
	}
	if p.ResponseTopic != "" {
		raw = append(raw, PropResponseTopic)
		raw = appendString(raw, p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		raw = append(raw, PropCorrelationData)
		raw = appendBinaryData(raw, p.CorrelationData)
	}
	if p.SubscriptionIdentifier != nil {
		raw = append(raw, PropSubscriptionIdentifier)
		var vb [4]byte
		vbl := EncodeRemainingLength(*p.SubscriptionIdentifier, vb[:])
		raw = append(raw, vb[:vbl]...)
	}
	if p.SessionExpiryInterval != nil {
		raw = append(raw, PropSessionExpiryInterval)
		raw = binary.BigEndian.AppendUint32(raw, *p.SessionExpiryInterval)
	}
	if p.AssignedClientIdentifier != "" {
		raw = append(raw, PropAssignedClientIdentifier)
		raw = appendString(raw, p.AssignedClientIdentifier)
	}
	if p.ServerKeepAlive != nil {
		raw = append(raw, PropServerKeepAlive)
		raw = binary.BigEndian.AppendUint16(raw, *p.ServerKeepAlive)
	}
	if p.AuthenticationMethod != "" {
		raw = append(raw, PropAuthenticationMethod)
		raw = appendString(raw, p.AuthenticationMethod)
	}
	if len(p.AuthenticationData) > 0 {
		raw = append(raw, PropAuthenticationData)
		raw = appendBinaryData(raw, p.AuthenticationData)
	}
	if p.RequestProblemInformation != nil {
		raw = append(raw, PropRequestProblemInformation, *p.RequestProblemInformation)
	}
	if p.WillDelayInterval != nil {
		raw = append(raw, PropWillDelayInterval)
		raw = binary.BigEndian.AppendUint32(raw, *p.WillDelayInterval)
	}
	if p.RequestResponseInformation != nil {
		raw = append(raw, PropRequestResponseInformation, *p.RequestResponseInformation)
	}
	if p.ResponseInformation != "" {
		raw = append(raw, PropResponseInformation)
		raw = appendString(raw, p.ResponseInformation)
	}
	if p.ServerReference != "" {
		raw = append(raw, PropServerReference)
		raw = appendString(raw, p.ServerReference)
	}
	if p.ReasonString != "" {
		raw = append(raw, PropReasonString)
		raw = appendString(raw, p.ReasonString)
	}
	if p.ReceiveMaximum != nil {
		raw = append(raw, PropReceiveMaximum)
		raw = binary.BigEndian.AppendUint16(raw, *p.ReceiveMaximum)
	}
	if p.TopicAliasMaximum != nil {
		raw = append(raw, PropTopicAliasMaximum)
		raw = binary.BigEndian.AppendUint16(raw, *p.TopicAliasMaximum)
	}
	if p.TopicAlias != nil {
		raw = append(raw, PropTopicAlias)
		raw = binary.BigEndian.AppendUint16(raw, *p.TopicAlias)
	}
	if p.MaximumQoS != nil {
		raw = append(raw, PropMaximumQoS, *p.MaximumQoS)
	}
	if p.RetainAvailable != nil {
		raw = append(raw, PropRetainAvailable, *p.RetainAvailable)
	}
	for _, up := range p.UserProperties {
		raw = append(raw, PropUserProperty)
		raw = appendString(raw, up.Key)
		raw = appendString(raw, up.Value)
	}
	if p.MaximumPacketSize != nil {
		raw = append(raw, PropMaximumPacketSize)
		raw = binary.BigEndian.AppendUint32(raw, *p.MaximumPacketSize)
	}
	if p.WildcardSubscriptionAvailable != nil {
		raw = append(raw, PropWildcardSubscriptionAvailable, *p.WildcardSubscriptionAvailable)
	}
	if p.SubscriptionIdentifierAvailable != nil {
		raw = append(raw, PropSubscriptionIdentifierAvailable, *p.SubscriptionIdentifierAvailable)
	}
	if p.SharedSubscriptionAvailable != nil {
		raw = append(raw, PropSharedSubscriptionAvailable, *p.SharedSubscriptionAvailable)
	}

	propLen := len(raw)
	var lenBuf [4]byte
	lenBytes := EncodeRemainingLength(propLen, lenBuf[:])

	res := make([]byte, lenBytes+propLen)
	copy(res, lenBuf[:lenBytes])
	copy(res[lenBytes:], raw)
	return res
}

// DecodeProperties parses MQTT 5.0 properties from data.
// Returns the Properties structure, number of bytes consumed (length header + body), and error.
func DecodeProperties(data []byte) (*Properties, int, error) {
	if len(data) == 0 {
		return nil, 0, nil
	}

	propLen, lenBytes, err := DecodeRemainingLength(data)
	if err != nil {
		return nil, 0, err
	}

	if propLen == 0 {
		return &Properties{}, lenBytes, nil
	}

	totalConsumed := lenBytes + propLen
	if len(data) < totalConsumed {
		return nil, 0, ErrIncompletePacket
	}

	raw := data[lenBytes:totalConsumed]
	props := &Properties{}
	offset := 0

	for offset < len(raw) {
		propID := raw[offset]
		offset++

		switch propID {
		case PropPayloadFormatIndicator:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.PayloadFormatIndicator = &b
			offset++

		case PropMessageExpiryInterval:
			if offset+4 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint32(raw[offset:])
			props.MessageExpiryInterval = &v
			offset += 4

		case PropContentType:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.ContentType = s
			offset += consumed

		case PropResponseTopic:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.ResponseTopic = s
			offset += consumed

		case PropCorrelationData:
			b, consumed, err := readBinaryData(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.CorrelationData = b
			offset += consumed

		case PropSubscriptionIdentifier:
			sid, consumed, err := DecodeRemainingLength(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.SubscriptionIdentifier = &sid
			offset += consumed

		case PropSessionExpiryInterval:
			if offset+4 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint32(raw[offset:])
			props.SessionExpiryInterval = &v
			offset += 4

		case PropAssignedClientIdentifier:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.AssignedClientIdentifier = s
			offset += consumed

		case PropServerKeepAlive:
			if offset+2 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint16(raw[offset:])
			props.ServerKeepAlive = &v
			offset += 2

		case PropAuthenticationMethod:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.AuthenticationMethod = s
			offset += consumed

		case PropAuthenticationData:
			b, consumed, err := readBinaryData(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.AuthenticationData = b
			offset += consumed

		case PropRequestProblemInformation:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.RequestProblemInformation = &b
			offset++

		case PropWillDelayInterval:
			if offset+4 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint32(raw[offset:])
			props.WillDelayInterval = &v
			offset += 4

		case PropRequestResponseInformation:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.RequestResponseInformation = &b
			offset++

		case PropResponseInformation:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.ResponseInformation = s
			offset += consumed

		case PropServerReference:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.ServerReference = s
			offset += consumed

		case PropReasonString:
			s, consumed, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			props.ReasonString = s
			offset += consumed

		case PropReceiveMaximum:
			if offset+2 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint16(raw[offset:])
			props.ReceiveMaximum = &v
			offset += 2

		case PropTopicAliasMaximum:
			if offset+2 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint16(raw[offset:])
			props.TopicAliasMaximum = &v
			offset += 2

		case PropTopicAlias:
			if offset+2 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint16(raw[offset:])
			props.TopicAlias = &v
			offset += 2

		case PropMaximumQoS:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.MaximumQoS = &b
			offset++

		case PropRetainAvailable:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.RetainAvailable = &b
			offset++

		case PropUserProperty:
			key, klen, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			offset += klen
			val, vlen, err := readString(raw[offset:])
			if err != nil {
				return nil, 0, err
			}
			offset += vlen
			props.AddUserProperty(key, val)

		case PropMaximumPacketSize:
			if offset+4 > len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			v := binary.BigEndian.Uint32(raw[offset:])
			props.MaximumPacketSize = &v
			offset += 4

		case PropWildcardSubscriptionAvailable:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.WildcardSubscriptionAvailable = &b
			offset++

		case PropSubscriptionIdentifierAvailable:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.SubscriptionIdentifierAvailable = &b
			offset++

		case PropSharedSubscriptionAvailable:
			if offset >= len(raw) {
				return nil, 0, ErrMalformedProperty
			}
			b := raw[offset]
			props.SharedSubscriptionAvailable = &b
			offset++

		default:
			// Ignore unrecognized property or return error
			return nil, 0, ErrMalformedProperty
		}
	}

	return props, totalConsumed, nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	dst = append(dst, s...)
	return dst
}

func appendBinaryData(dst []byte, b []byte) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(b)))
	dst = append(dst, b...)
	return dst
}

func readString(data []byte) (string, int, error) {
	if len(data) < 2 {
		return "", 0, ErrIncompletePacket
	}
	strLen := int(binary.BigEndian.Uint16(data))
	if len(data) < 2+strLen {
		return "", 0, ErrIncompletePacket
	}
	return string(data[2 : 2+strLen]), 2 + strLen, nil
}

func readBinaryData(data []byte) ([]byte, int, error) {
	if len(data) < 2 {
		return nil, 0, ErrIncompletePacket
	}
	dataLen := int(binary.BigEndian.Uint16(data))
	if len(data) < 2+dataLen {
		return nil, 0, ErrIncompletePacket
	}
	b := make([]byte, dataLen)
	copy(b, data[2:2+dataLen])
	return b, 2 + dataLen, nil
}
