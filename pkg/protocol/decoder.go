package protocol

import (
	"fmt"
)

// DecodePacket tries to parse a complete MQTT packet from the buffer.
// It returns the parsed packet, the total bytes consumed, and any error.
// If the buffer doesn't yet contain a full packet, it returns (nil, 0, nil).
func DecodePacket(buf []byte, protoLevel ...byte) (Packet, int, error) {
	if len(buf) < 2 {
		return nil, 0, nil // need more data
	}

	headerByte := buf[0]
	packetType := headerByte >> 4
	flags := headerByte & 0x0F

	remLen, lenBytes, err := DecodeRemainingLength(buf[1:])
	if err != nil {
		if err == ErrIncompletePacket {
			return nil, 0, nil // need more data
		}
		return nil, 0, err
	}

	totalLen := 1 + lenBytes + remLen
	if len(buf) < totalLen {
		return nil, 0, nil // need more data
	}

	payload := buf[1+lenBytes : totalLen]

	var pkt Packet
	switch packetType {
	case CONNECT:
		pkt, err = DecodeConnect(payload)
	case CONNACK:
		pkt, err = DecodeConnack(payload)
	case PUBLISH:
		pkt, err = DecodePublish(flags, payload, protoLevel...)
	case PUBACK:
		pkt, err = DecodePuback(payload)
	case PUBREC:
		pkt, err = DecodePubrec(payload)
	case PUBREL:
		pkt, err = DecodePubrel(payload)
	case PUBCOMP:
		pkt, err = DecodePubcomp(payload)
	case SUBSCRIBE:
		pkt, err = DecodeSubscribe(payload, protoLevel...)
	case SUBACK:
		pkt, err = DecodeSuback(payload, protoLevel...)
	case UNSUBSCRIBE:
		pkt, err = DecodeUnsubscribe(payload, protoLevel...)
	case UNSUBACK:
		pkt, err = DecodeUnsuback(payload, protoLevel...)
	case PINGREQ:
		pkt = &PingreqPacket{}
	case PINGRESP:
		pkt = &PingrespPacket{}
	case DISCONNECT:
		pkt, err = DecodeDisconnect(payload)
	case AUTH:
		pkt, err = DecodeAuth(payload)
	default:
		return nil, 0, fmt.Errorf("%w: %d", ErrInvalidPacketType, packetType)
	}

	if err != nil {
		return nil, 0, err
	}

	return pkt, totalLen, nil
}
