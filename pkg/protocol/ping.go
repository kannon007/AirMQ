package protocol

// PingreqPacket represents a PINGREQ packet.
type PingreqPacket struct{}

func (p *PingreqPacket) Type() byte { return PINGREQ }
func (p *PingreqPacket) Encode() ([]byte, error) {
	return []byte{PINGREQ << 4, 0}, nil
}

// PingrespPacket represents a PINGRESP packet.
type PingrespPacket struct{}

func (p *PingrespPacket) Type() byte { return PINGRESP }
func (p *PingrespPacket) Encode() ([]byte, error) {
	return []byte{PINGRESP << 4, 0}, nil
}

// DisconnectPacket represents a DISCONNECT packet (MQTT 3.1.1 and MQTT 5.0).
type DisconnectPacket struct {
	ReasonCode byte        // MQTT 5.0 ReasonCode (default 0x00 NormalDisconnection)
	Properties *Properties // MQTT 5.0 Properties
}

func (p *DisconnectPacket) Type() byte { return DISCONNECT }

func (p *DisconnectPacket) Encode() ([]byte, error) {
	if p.ReasonCode == ReasonNormalDisconnection && (p.Properties == nil || p.Properties.IsEmpty()) {
		return []byte{DISCONNECT << 4, 0}, nil
	}

	propBytes := EncodeProperties(p.Properties)
	remLen := 1 + len(propBytes) // 1 byte ReasonCode + property bytes

	var vb [4]byte
	vbl := EncodeRemainingLength(remLen, vb[:])

	totalLen := 1 + vbl + remLen
	buf := make([]byte, totalLen)
	buf[0] = DISCONNECT << 4
	copy(buf[1:], vb[:vbl])

	offset := 1 + vbl
	buf[offset] = p.ReasonCode
	offset++
	copy(buf[offset:], propBytes)

	return buf, nil
}

// DecodeDisconnect parses a DISCONNECT packet payload (excluding fixed header).
func DecodeDisconnect(payload []byte) (*DisconnectPacket, error) {
	if len(payload) == 0 {
		return &DisconnectPacket{ReasonCode: ReasonNormalDisconnection}, nil
	}

	reasonCode := payload[0]
	var props *Properties
	if len(payload) > 1 {
		p, _, err := DecodeProperties(payload[1:])
		if err != nil {
			return nil, err
		}
		props = p
	}

	return &DisconnectPacket{
		ReasonCode: reasonCode,
		Properties: props,
	}, nil
}
