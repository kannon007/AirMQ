package protocol

import (
	"errors"
	"strings"
)

// MQTT Control Packet Types
const (
	CONNECT     byte = 1
	CONNACK     byte = 2
	PUBLISH     byte = 3
	PUBACK      byte = 4
	PUBREC      byte = 5
	PUBREL      byte = 6
	PUBCOMP     byte = 7
	SUBSCRIBE   byte = 8
	SUBACK      byte = 9
	UNSUBSCRIBE byte = 10
	UNSUBACK    byte = 11
	PINGREQ     byte = 12
	PINGRESP    byte = 13
	DISCONNECT  byte = 14
	AUTH        byte = 15
)

// QoS Levels
const (
	QoS0 byte = 0
	QoS1 byte = 1
	QoS2 byte = 2
)

// Protocol Versions
const (
	V311 byte = 4
	V50  byte = 5
)

// Common Protocol Errors
var (
	ErrIncompletePacket   = errors.New("incomplete packet data")
	ErrMalformedRemaining = errors.New("malformed remaining length")
	ErrProtocolViolation  = errors.New("mqtt protocol violation")
	ErrInvalidPacketType  = errors.New("invalid packet type")
)

// ValidatePublishTopic verifies that a topic name meets MQTT specification:
// - Non-empty
// - No wildcard characters ('+' or '#')
// - No null character ('\u0000')
func ValidatePublishTopic(topic string) bool {
	if len(topic) == 0 {
		return false
	}
	if strings.ContainsAny(topic, "+#\x00") {
		return false
	}
	return true
}

// ValidateTopicFilter verifies that a subscription topic filter meets MQTT specification:
// - Non-empty
// - No null character ('\u0000')
// - Valid wildcard placements for '#' and '+'
// - Supports $share/{group}/{topic}
func ValidateTopicFilter(filter string) bool {
	if len(filter) == 0 || strings.ContainsRune(filter, 0) {
		return false
	}
	actualFilter := filter
	if strings.HasPrefix(filter, "$share/") {
		parts := strings.SplitN(filter, "/", 3)
		if len(parts) < 3 || len(parts[1]) == 0 || strings.ContainsAny(parts[1], "+#") {
			return false
		}
		actualFilter = parts[2]
		if len(actualFilter) == 0 {
			return false
		}
	}

	levels := strings.Split(actualFilter, "/")
	for i, level := range levels {
		if level == "#" {
			if i != len(levels)-1 {
				return false // '#' must be the last topic level
			}
		} else if strings.Contains(level, "#") {
			return false // '#' cannot be mixed with other characters in a level
		} else if strings.Contains(level, "+") && level != "+" {
			return false // '+' cannot be mixed with other characters in a level
		}
	}
	return true
}

// Packet is the generic interface implemented by all MQTT packet types.
type Packet interface {
	Type() byte
	Encode() ([]byte, error)
}

// DecodeRemainingLength parses the MQTT variable byte integer (1-4 bytes).
// Returns the remaining length, number of bytes consumed, and any error.
func DecodeRemainingLength(buf []byte) (int, int, error) {
	var multiplier int = 1
	var value int = 0
	var offset int = 0

	for {
		if offset >= len(buf) {
			return 0, 0, ErrIncompletePacket
		}
		digit := buf[offset]
		offset++
		value += int(digit&127) * multiplier
		if multiplier > 128*128*128 {
			return 0, 0, ErrMalformedRemaining
		}
		multiplier *= 128
		if (digit & 128) == 0 {
			break
		}
	}
	return value, offset, nil
}

// EncodeRemainingLength serializes a length into MQTT variable byte format.
func EncodeRemainingLength(length int, dst []byte) int {
	var offset int
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		dst[offset] = digit
		offset++
		if length == 0 {
			break
		}
	}
	return offset
}
