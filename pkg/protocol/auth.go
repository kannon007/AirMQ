package protocol

// AuthPacket represents an MQTT 5.0 AUTH packet (Control Packet Type 15).
type AuthPacket struct {
	ReasonCode byte
	Properties *Properties
}

func (p *AuthPacket) Type() byte {
	return AUTH
}

// Encode serializes the AUTH packet.
func (p *AuthPacket) Encode() ([]byte, error) {
	propBytes := EncodeProperties(p.Properties)
	remLen := 1 + len(propBytes) // 1 byte ReasonCode + property bytes

	var varByteBuf [4]byte
	varByteLen := EncodeRemainingLength(remLen, varByteBuf[:])

	totalLen := 1 + varByteLen + remLen
	buf := make([]byte, totalLen)
	buf[0] = AUTH << 4
	copy(buf[1:], varByteBuf[:varByteLen])

	offset := 1 + varByteLen
	buf[offset] = p.ReasonCode
	offset++
	copy(buf[offset:], propBytes)

	return buf, nil
}

// DecodeAuth parses an AUTH packet from payload (excluding fixed header).
func DecodeAuth(payload []byte) (*AuthPacket, error) {
	if len(payload) == 0 {
		return &AuthPacket{ReasonCode: ReasonSuccess}, nil
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

	return &AuthPacket{
		ReasonCode: reasonCode,
		Properties: props,
	}, nil
}
