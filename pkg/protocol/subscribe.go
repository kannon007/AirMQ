package protocol

import (
	"encoding/binary"
	"errors"
)

var (
	ErrEmptySubscription = errors.New("empty subscription list")
)

// TopicSub represents an individual topic filter and its subscription options.
type TopicSub struct {
	Topic             string
	QoS               byte
	NoLocal           bool // MQTT 5.0: Do not send messages published by this client
	RetainAsPublished bool // MQTT 5.0: Preserve retain flag when forwarding
	RetainHandling    byte // MQTT 5.0: 0=send on subscribe, 1=send if new, 2=do not send
}

// SubscribePacket represents an MQTT SUBSCRIBE packet (MQTT 3.1.1 and MQTT 5.0).
type SubscribePacket struct {
	PacketID      uint16
	ProtocolLevel byte        // V311 or V50
	Properties    *Properties // MQTT 5.0 Properties
	Topics        []TopicSub
}

func (p *SubscribePacket) Type() byte {
	return SUBSCRIBE
}

// Encode serializes a SUBSCRIBE packet.
func (p *SubscribePacket) Encode() ([]byte, error) {
	if len(p.Topics) == 0 {
		return nil, ErrEmptySubscription
	}

	var varHeader []byte
	varHeader = binary.BigEndian.AppendUint16(varHeader, p.PacketID)

	isV5 := p.ProtocolLevel == V50
	if p.ProtocolLevel == 0 {
		if p.Properties != nil {
			isV5 = true
		} else {
			for _, sub := range p.Topics {
				if sub.NoLocal || sub.RetainAsPublished || sub.RetainHandling > 0 {
					isV5 = true
					break
				}
			}
		}
	}

	if isV5 {
		propBytes := EncodeProperties(p.Properties)
		varHeader = append(varHeader, propBytes...)
	}

	var payload []byte
	for _, sub := range p.Topics {
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(sub.Topic)))
		payload = append(payload, sub.Topic...)

		subOption := sub.QoS & 0x03
		if sub.NoLocal {
			subOption |= 0x04
		}
		if sub.RetainAsPublished {
			subOption |= 0x08
		}
		subOption |= (sub.RetainHandling & 0x03) << 4
		payload = append(payload, subOption)
	}

	remLen := len(varHeader) + len(payload)
	var vb [4]byte
	vbl := EncodeRemainingLength(remLen, vb[:])

	totalLen := 1 + vbl + remLen
	buf := make([]byte, totalLen)
	buf[0] = (SUBSCRIBE << 4) | 0x02 // Bit 1 reserved must be 1
	copy(buf[1:], vb[:vbl])

	offset := 1 + vbl
	copy(buf[offset:], varHeader)
	offset += len(varHeader)
	copy(buf[offset:], payload)

	return buf, nil
}

// DecodeSubscribe parses a SUBSCRIBE packet payload.
func DecodeSubscribe(data []byte, protoLevel ...byte) (*SubscribePacket, error) {
	if len(data) < 2 {
		return nil, ErrIncompletePacket
	}

	packetID := binary.BigEndian.Uint16(data)
	offset := 2

	var props *Properties
	isV5 := len(protoLevel) > 0 && protoLevel[0] == V50
	if isV5 && offset < len(data) {
		p, consumed, err := DecodeProperties(data[offset:])
		if err != nil {
			return nil, err
		}
		props = p
		offset += consumed
	}

	var topics []TopicSub
	for offset < len(data) {
		if offset+2 > len(data) {
			return nil, ErrIncompletePacket
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if offset+topicLen > len(data) {
			return nil, ErrIncompletePacket
		}
		topic := string(data[offset : offset+topicLen])
		if !ValidateTopicFilter(topic) {
			return nil, ErrProtocolViolation
		}
		offset += topicLen

		if offset >= len(data) {
			return nil, ErrIncompletePacket
		}
		optByte := data[offset]
		offset++

		qos := optByte & 0x03
		noLocal := (optByte & 0x04) != 0
		rap := (optByte & 0x08) != 0
		rh := (optByte >> 4) & 0x03

		topics = append(topics, TopicSub{
			Topic:             topic,
			QoS:               qos,
			NoLocal:           noLocal,
			RetainAsPublished: rap,
			RetainHandling:    rh,
		})
	}

	if len(topics) == 0 {
		return nil, ErrEmptySubscription
	}

	return &SubscribePacket{
		PacketID:   packetID,
		Properties: props,
		Topics:     topics,
	}, nil
}

// SubackPacket represents an MQTT SUBACK packet.
type SubackPacket struct {
	PacketID      uint16
	ProtocolLevel byte
	Properties    *Properties // MQTT 5.0 Properties
	ReturnCodes   []byte      // Return codes (MQTT 3.1.1) or Reason codes (MQTT 5.0)
	ReasonCodes   []byte      // Alias for ReturnCodes
}

func (p *SubackPacket) Type() byte {
	return SUBACK
}

func (p *SubackPacket) Encode() ([]byte, error) {
	codes := p.ReturnCodes
	if len(codes) == 0 && len(p.ReasonCodes) > 0 {
		codes = p.ReasonCodes
	}

	isV5 := p.ProtocolLevel == V50 || (p.ProtocolLevel == 0 && p.Properties != nil && !p.Properties.IsEmpty())
	var propBytes []byte
	if isV5 {
		propBytes = EncodeProperties(p.Properties)
	}

	remLen := 2 + len(propBytes) + len(codes)
	var vb [4]byte
	vbl := EncodeRemainingLength(remLen, vb[:])

	totalLen := 1 + vbl + remLen
	buf := make([]byte, totalLen)
	buf[0] = SUBACK << 4
	copy(buf[1:], vb[:vbl])

	offset := 1 + vbl
	binary.BigEndian.PutUint16(buf[offset:], p.PacketID)
	offset += 2
	if len(propBytes) > 0 {
		copy(buf[offset:], propBytes)
		offset += len(propBytes)
	}
	copy(buf[offset:], codes)

	return buf, nil
}

// DecodeSuback parses SUBACK packet payload.
func DecodeSuback(data []byte, protoLevel ...byte) (*SubackPacket, error) {
	if len(data) < 2 {
		return nil, ErrIncompletePacket
	}
	packetID := binary.BigEndian.Uint16(data[:2])
	offset := 2

	var props *Properties
	isV5 := len(protoLevel) > 0 && protoLevel[0] == V50
	if isV5 && offset < len(data) {
		p, consumed, err := DecodeProperties(data[offset:])
		if err == nil && consumed > 0 {
			props = p
			offset += consumed
		}
	}

	codes := make([]byte, len(data)-offset)
	copy(codes, data[offset:])

	return &SubackPacket{
		PacketID:    packetID,
		Properties:  props,
		ReturnCodes: codes,
		ReasonCodes: codes,
	}, nil
}

// UnsubscribePacket represents an MQTT UNSUBSCRIBE packet.
type UnsubscribePacket struct {
	PacketID      uint16
	ProtocolLevel byte
	Properties    *Properties // MQTT 5.0 Properties
	Topics        []string
}

func (p *UnsubscribePacket) Type() byte {
	return UNSUBSCRIBE
}

func (p *UnsubscribePacket) Encode() ([]byte, error) {
	if len(p.Topics) == 0 {
		return nil, ErrEmptySubscription
	}

	var varHeader []byte
	varHeader = binary.BigEndian.AppendUint16(varHeader, p.PacketID)

	isV5 := p.ProtocolLevel == V50 || (p.ProtocolLevel == 0 && p.Properties != nil && !p.Properties.IsEmpty())
	if isV5 {
		propBytes := EncodeProperties(p.Properties)
		varHeader = append(varHeader, propBytes...)
	}

	var payload []byte
	for _, topic := range p.Topics {
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(topic)))
		payload = append(payload, topic...)
	}

	remLen := len(varHeader) + len(payload)
	var vb [4]byte
	vbl := EncodeRemainingLength(remLen, vb[:])

	totalLen := 1 + vbl + remLen
	buf := make([]byte, totalLen)
	buf[0] = (UNSUBSCRIBE << 4) | 0x02 // Bit 1 reserved must be 1
	copy(buf[1:], vb[:vbl])

	offset := 1 + vbl
	copy(buf[offset:], varHeader)
	offset += len(varHeader)
	copy(buf[offset:], payload)

	return buf, nil
}

// DecodeUnsubscribe parses an UNSUBSCRIBE packet payload.
func DecodeUnsubscribe(data []byte, protoLevel ...byte) (*UnsubscribePacket, error) {
	if len(data) < 2 {
		return nil, ErrIncompletePacket
	}

	packetID := binary.BigEndian.Uint16(data)
	offset := 2

	var props *Properties
	isV5 := len(protoLevel) > 0 && protoLevel[0] == V50
	if isV5 && offset < len(data) {
		p, consumed, err := DecodeProperties(data[offset:])
		if err != nil {
			return nil, err
		}
		props = p
		offset += consumed
	}

	var topics []string
	for offset < len(data) {
		if offset+2 > len(data) {
			return nil, ErrIncompletePacket
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if offset+topicLen > len(data) {
			return nil, ErrIncompletePacket
		}
		topic := string(data[offset : offset+topicLen])
		if !ValidateTopicFilter(topic) {
			return nil, ErrProtocolViolation
		}
		offset += topicLen

		topics = append(topics, topic)
	}

	if len(topics) == 0 {
		return nil, ErrEmptySubscription
	}

	return &UnsubscribePacket{
		PacketID:   packetID,
		Properties: props,
		Topics:     topics,
	}, nil
}

// UnsubackPacket represents an MQTT UNSUBACK packet.
type UnsubackPacket struct {
	PacketID      uint16
	ProtocolLevel byte
	Properties    *Properties // MQTT 5.0 Properties
	ReasonCodes   []byte      // MQTT 5.0 Reason Codes
}

func (p *UnsubackPacket) Type() byte {
	return UNSUBACK
}

func (p *UnsubackPacket) Encode() ([]byte, error) {
	isV5 := p.ProtocolLevel == V50 || len(p.ReasonCodes) > 0 || (p.Properties != nil && !p.Properties.IsEmpty())
	if !isV5 {
		// Standard MQTT 3.1.1 4-byte UNSUBACK
		buf := make([]byte, 4)
		buf[0] = UNSUBACK << 4
		buf[1] = 2
		binary.BigEndian.PutUint16(buf[2:], p.PacketID)
		return buf, nil
	}

	propBytes := EncodeProperties(p.Properties)
	remLen := 2 + len(propBytes) + len(p.ReasonCodes)

	var vb [4]byte
	vbl := EncodeRemainingLength(remLen, vb[:])

	totalLen := 1 + vbl + remLen
	buf := make([]byte, totalLen)
	buf[0] = UNSUBACK << 4
	copy(buf[1:], vb[:vbl])

	offset := 1 + vbl
	binary.BigEndian.PutUint16(buf[offset:], p.PacketID)
	offset += 2
	copy(buf[offset:], propBytes)
	offset += len(propBytes)
	copy(buf[offset:], p.ReasonCodes)

	return buf, nil
}

func DecodeUnsuback(data []byte, protoLevel ...byte) (*UnsubackPacket, error) {
	if len(data) < 2 {
		return nil, ErrIncompletePacket
	}
	packetID := binary.BigEndian.Uint16(data[:2])
	offset := 2

	var props *Properties
	isV5 := len(protoLevel) > 0 && protoLevel[0] == V50
	if isV5 && offset < len(data) {
		p, consumed, err := DecodeProperties(data[offset:])
		if err != nil {
			return nil, err
		}
		props = p
		offset += consumed
	}

	var reasonCodes []byte
	if offset < len(data) {
		reasonCodes = make([]byte, len(data)-offset)
		copy(reasonCodes, data[offset:])
	}

	return &UnsubackPacket{
		PacketID:    packetID,
		Properties:  props,
		ReasonCodes: reasonCodes,
	}, nil
}
