package protocol

import (
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidTopicName = errors.New("invalid topic name in PUBLISH")
)

// PublishPacket represents an MQTT PUBLISH packet (MQTT 3.1.1 and MQTT 5.0).
type PublishPacket struct {
	ProtocolLevel byte
	Dup           bool
	QoS           byte
	Retain        bool
	Topic         string
	PacketID      uint16
	Properties    *Properties // MQTT 5.0 Properties
	Payload       []byte
}

func (p *PublishPacket) Type() byte {
	return PUBLISH
}

// Encode serializes a PUBLISH packet into wire format.
func (p *PublishPacket) Encode() ([]byte, error) {
	topicLen := len(p.Topic)
	remLen := 2 + topicLen + len(p.Payload)
	if p.QoS > QoS0 {
		remLen += 2
	}

	var propBytes []byte
	if p.ProtocolLevel == V50 || (p.Properties != nil && !p.Properties.IsEmpty()) {
		propBytes = EncodeProperties(p.Properties)
		remLen += len(propBytes)
	}

	var varByteBuf [4]byte
	varByteLen := EncodeRemainingLength(remLen, varByteBuf[:])

	totalLen := 1 + varByteLen + remLen
	buf := make([]byte, totalLen)

	// Fixed Header Byte 1
	var flags byte
	if p.Dup {
		flags |= 0x08
	}
	flags |= (p.QoS << 1)
	if p.Retain {
		flags |= 0x01
	}
	buf[0] = (PUBLISH << 4) | flags

	// Fixed Header Remaining Length
	copy(buf[1:], varByteBuf[:varByteLen])
	offset := 1 + varByteLen

	// Topic Name
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], p.Topic)
	offset += topicLen

	// Packet Identifier (if QoS > 0)
	if p.QoS > QoS0 {
		binary.BigEndian.PutUint16(buf[offset:], p.PacketID)
		offset += 2
	}

	// Properties (if present)
	if len(propBytes) > 0 {
		copy(buf[offset:], propBytes)
		offset += len(propBytes)
	}

	// Payload
	copy(buf[offset:], p.Payload)

	return buf, nil
}

// DecodePublish parses a PUBLISH packet from the raw buffer given flags and payload.
func DecodePublish(flags byte, data []byte, protoLevel ...byte) (*PublishPacket, error) {
	dup := (flags & 0x08) != 0
	qos := (flags >> 1) & 0x03
	retain := (flags & 0x01) != 0

	if qos > QoS2 {
		return nil, ErrProtocolViolation
	}

	if len(data) < 2 {
		return nil, ErrIncompletePacket
	}

	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if offset+topicLen > len(data) {
		return nil, ErrIncompletePacket
	}

	topic := string(data[offset : offset+topicLen])
	if !ValidatePublishTopic(topic) {
		return nil, ErrProtocolViolation
	}
	offset += topicLen

	var packetID uint16
	if qos > QoS0 {
		if offset+2 > len(data) {
			return nil, ErrIncompletePacket
		}
		packetID = binary.BigEndian.Uint16(data[offset:])
		offset += 2
	}

	var props *Properties
	isV5 := len(protoLevel) > 0 && protoLevel[0] == V50
	if isV5 && offset < len(data) {
		p, consumed, err := DecodeProperties(data[offset:])
		if err == nil && consumed > 0 {
			props = p
			offset += consumed
		}
	}

	// The rest is payload (clone so it does not alias the inbound network buffer)
	var payload []byte
	if offset < len(data) {
		payload = append([]byte(nil), data[offset:]...)
	}

	lvl := V311
	if isV5 {
		lvl = V50
	}

	return &PublishPacket{
		ProtocolLevel: lvl,
		Dup:           dup,
		QoS:           qos,
		Retain:        retain,
		Topic:         topic,
		PacketID:      packetID,
		Properties:    props,
		Payload:       payload,
	}, nil
}

// PubackPacket represents an MQTT PUBACK packet (QoS 1 response).
type PubackPacket struct {
	PacketID   uint16
	ReasonCode byte        // MQTT 5.0 ReasonCode (default 0x00 Success)
	Properties *Properties // MQTT 5.0 Properties
}

func (p *PubackPacket) Type() byte {
	return PUBACK
}

func (p *PubackPacket) Encode() ([]byte, error) {
	return encodeAckPacket(PUBACK, 0x00, p.PacketID, p.ReasonCode, p.Properties)
}

// DecodePuback parses PUBACK packets.
func DecodePuback(data []byte) (*PubackPacket, error) {
	pid, code, props, err := decodeAckPacket(data)
	if err != nil {
		return nil, err
	}
	return &PubackPacket{PacketID: pid, ReasonCode: code, Properties: props}, nil
}

// PubrecPacket represents PUBREC (QoS 2, part 1).
type PubrecPacket struct {
	PacketID   uint16
	ReasonCode byte
	Properties *Properties
}

func (p *PubrecPacket) Type() byte { return PUBREC }
func (p *PubrecPacket) Encode() ([]byte, error) {
	return encodeAckPacket(PUBREC, 0x00, p.PacketID, p.ReasonCode, p.Properties)
}

func DecodePubrec(data []byte) (*PubrecPacket, error) {
	pid, code, props, err := decodeAckPacket(data)
	if err != nil {
		return nil, err
	}
	return &PubrecPacket{PacketID: pid, ReasonCode: code, Properties: props}, nil
}

// PubrelPacket represents PUBREL (QoS 2, part 2).
type PubrelPacket struct {
	PacketID   uint16
	ReasonCode byte
	Properties *Properties
}

func (p *PubrelPacket) Type() byte { return PUBREL }
func (p *PubrelPacket) Encode() ([]byte, error) {
	return encodeAckPacket(PUBREL, 0x02, p.PacketID, p.ReasonCode, p.Properties) // Bit 1 reserved
}

func DecodePubrel(data []byte) (*PubrelPacket, error) {
	pid, code, props, err := decodeAckPacket(data)
	if err != nil {
		return nil, err
	}
	return &PubrelPacket{PacketID: pid, ReasonCode: code, Properties: props}, nil
}

// PubcompPacket represents PUBCOMP (QoS 2, part 3).
type PubcompPacket struct {
	PacketID   uint16
	ReasonCode byte
	Properties *Properties
}

func (p *PubcompPacket) Type() byte { return PUBCOMP }
func (p *PubcompPacket) Encode() ([]byte, error) {
	return encodeAckPacket(PUBCOMP, 0x00, p.PacketID, p.ReasonCode, p.Properties)
}

func DecodePubcomp(data []byte) (*PubcompPacket, error) {
	pid, code, props, err := decodeAckPacket(data)
	if err != nil {
		return nil, err
	}
	return &PubcompPacket{PacketID: pid, ReasonCode: code, Properties: props}, nil
}

// Helper to encode PUBACK/PUBREC/PUBREL/PUBCOMP packets.
func encodeAckPacket(pktType, flags byte, packetID uint16, reasonCode byte, props *Properties) ([]byte, error) {
	if reasonCode == 0 && (props == nil || props.IsEmpty()) {
		// Standard 4-byte 3.1.1 format
		buf := make([]byte, 4)
		buf[0] = (pktType << 4) | flags
		buf[1] = 2
		binary.BigEndian.PutUint16(buf[2:], packetID)
		return buf, nil
	}

	propBytes := EncodeProperties(props)
	remLen := 2 + 1 + len(propBytes) // PacketID (2) + ReasonCode (1) + Properties

	var vb [4]byte
	vbl := EncodeRemainingLength(remLen, vb[:])

	totalLen := 1 + vbl + remLen
	buf := make([]byte, totalLen)
	buf[0] = (pktType << 4) | flags
	copy(buf[1:], vb[:vbl])

	offset := 1 + vbl
	binary.BigEndian.PutUint16(buf[offset:], packetID)
	offset += 2
	buf[offset] = reasonCode
	offset++
	copy(buf[offset:], propBytes)

	return buf, nil
}

// Helper to decode PUBACK/PUBREC/PUBREL/PUBCOMP packets.
func decodeAckPacket(data []byte) (uint16, byte, *Properties, error) {
	if len(data) < 2 {
		return 0, 0, nil, ErrIncompletePacket
	}
	packetID := binary.BigEndian.Uint16(data[:2])
	if len(data) == 2 {
		return packetID, ReasonSuccess, nil, nil
	}

	reasonCode := data[2]
	var props *Properties
	if len(data) > 3 {
		p, _, err := DecodeProperties(data[3:])
		if err != nil {
			return packetID, reasonCode, nil, err
		}
		props = p
	}

	return packetID, reasonCode, props, nil
}
