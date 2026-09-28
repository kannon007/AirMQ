package protocol

import (
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidProtocolName = errors.New("invalid protocol name in CONNECT")
	ErrUnsupportedVersion  = errors.New("unsupported protocol version")
)

// MQTT 3.1.1 Return Codes
const (
	CodeAccepted                    byte = 0
	CodeUnacceptableProtocolVersion byte = 1
	CodeIdentifierRejected          byte = 2
	CodeServerUnavailable           byte = 3
	CodeBadUsernameOrPassword       byte = 4
	CodeNotAuthorized               byte = 5
)

// ConnectPacket represents an MQTT CONNECT packet (MQTT 3.1.1 and MQTT 5.0).
type ConnectPacket struct {
	ProtocolName   string
	ProtocolLevel  byte
	CleanSession   bool // In MQTT 5.0: CleanStart
	CleanStart     bool // Alias for CleanSession in MQTT 5.0
	WillFlag       bool
	WillQoS        byte
	WillRetain     bool
	PasswordFlag   bool
	UsernameFlag   bool
	KeepAlive      uint16
	Properties     *Properties // MQTT 5.0 Properties
	ClientID       string
	WillProperties *Properties // MQTT 5.0 Will Properties
	WillTopic      string
	WillMessage    []byte
	Username       string
	Password       []byte
}

func (p *ConnectPacket) Type() byte {
	return CONNECT
}

// Encode serializes a CONNECT packet into wire format (supports MQTT 3.1.1 and MQTT 5.0).
func (p *ConnectPacket) Encode() ([]byte, error) {
	protoName := p.ProtocolName
	if protoName == "" {
		protoName = "MQTT"
	}
	protoLevel := p.ProtocolLevel
	if protoLevel == 0 {
		protoLevel = V311
	}

	var varHeader []byte
	varHeader = binary.BigEndian.AppendUint16(varHeader, uint16(len(protoName)))
	varHeader = append(varHeader, protoName...)
	varHeader = append(varHeader, protoLevel)

	var flags byte
	if p.Username != "" || p.UsernameFlag {
		flags |= 0x80
	}
	if len(p.Password) > 0 || p.PasswordFlag {
		flags |= 0x40
	}
	if p.WillRetain {
		flags |= 0x20
	}
	flags |= (p.WillQoS & 0x03) << 3
	if p.WillFlag {
		flags |= 0x04
	}
	if p.CleanSession || p.CleanStart {
		flags |= 0x02
	}
	varHeader = append(varHeader, flags)
	varHeader = binary.BigEndian.AppendUint16(varHeader, p.KeepAlive)

	if protoLevel == V50 {
		propBytes := EncodeProperties(p.Properties)
		varHeader = append(varHeader, propBytes...)
	}

	var payload []byte
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(p.ClientID)))
	payload = append(payload, p.ClientID...)

	if p.WillFlag {
		if protoLevel == V50 {
			willPropBytes := EncodeProperties(p.WillProperties)
			payload = append(payload, willPropBytes...)
		}
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(p.WillTopic)))
		payload = append(payload, p.WillTopic...)
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(p.WillMessage)))
		payload = append(payload, p.WillMessage...)
	}

	if p.Username != "" || p.UsernameFlag {
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(p.Username)))
		payload = append(payload, p.Username...)
	}

	if len(p.Password) > 0 || p.PasswordFlag {
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(p.Password)))
		payload = append(payload, p.Password...)
	}

	remLen := len(varHeader) + len(payload)
	var varByteBuf [4]byte
	varByteLen := EncodeRemainingLength(remLen, varByteBuf[:])

	totalLen := 1 + varByteLen + remLen
	buf := make([]byte, totalLen)
	buf[0] = CONNECT << 4
	copy(buf[1:], varByteBuf[:varByteLen])

	offset := 1 + varByteLen
	copy(buf[offset:], varHeader)
	offset += len(varHeader)
	copy(buf[offset:], payload)

	return buf, nil
}

// DecodeConnect decodes a CONNECT packet payload (excluding fixed header).
func DecodeConnect(payload []byte) (*ConnectPacket, error) {
	if len(payload) < 10 {
		return nil, ErrIncompletePacket
	}

	offset := 0
	protoLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+protoLen > len(payload) {
		return nil, ErrIncompletePacket
	}
	protoName := string(payload[offset : offset+protoLen])
	offset += protoLen

	if protoName != "MQTT" && protoName != "MQIsdp" {
		return nil, ErrInvalidProtocolName
	}

	if offset >= len(payload) {
		return nil, ErrIncompletePacket
	}
	protoLevel := payload[offset]
	offset++

	if protoLevel != V311 && protoLevel != V50 && protoLevel != 3 {
		return nil, ErrUnsupportedVersion
	}

	connectFlags := payload[offset]
	offset++

	cleanSession := (connectFlags & 0x02) != 0
	willFlag := (connectFlags & 0x04) != 0
	willQoS := (connectFlags >> 3) & 0x03
	willRetain := (connectFlags & 0x20) != 0
	passwordFlag := (connectFlags & 0x40) != 0
	usernameFlag := (connectFlags & 0x80) != 0

	keepAlive := binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	var connectProps *Properties
	if protoLevel == V50 {
		props, consumed, err := DecodeProperties(payload[offset:])
		if err != nil {
			return nil, err
		}
		connectProps = props
		offset += consumed
	}

	// Decode ClientID
	if offset+2 > len(payload) {
		return nil, ErrIncompletePacket
	}
	clientIDLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+clientIDLen > len(payload) {
		return nil, ErrIncompletePacket
	}
	clientID := string(payload[offset : offset+clientIDLen])
	offset += clientIDLen

	var willProperties *Properties
	var willTopic string
	var willMessage []byte
	if willFlag {
		if protoLevel == V50 {
			wProps, consumed, err := DecodeProperties(payload[offset:])
			if err != nil {
				return nil, err
			}
			willProperties = wProps
			offset += consumed
		}

		if offset+2 > len(payload) {
			return nil, ErrIncompletePacket
		}
		wtLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+wtLen > len(payload) {
			return nil, ErrIncompletePacket
		}
		willTopic = string(payload[offset : offset+wtLen])
		offset += wtLen

		if offset+2 > len(payload) {
			return nil, ErrIncompletePacket
		}
		wmLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+wmLen > len(payload) {
			return nil, ErrIncompletePacket
		}
		willMessage = payload[offset : offset+wmLen]
		offset += wmLen
	}

	var username string
	if usernameFlag {
		if offset+2 > len(payload) {
			return nil, ErrIncompletePacket
		}
		unLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+unLen > len(payload) {
			return nil, ErrIncompletePacket
		}
		username = string(payload[offset : offset+unLen])
		offset += unLen
	}

	var password []byte
	if passwordFlag {
		if offset+2 > len(payload) {
			return nil, ErrIncompletePacket
		}
		pwLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+pwLen > len(payload) {
			return nil, ErrIncompletePacket
		}
		password = payload[offset : offset+pwLen]
		offset += pwLen
	}

	return &ConnectPacket{
		ProtocolName:   protoName,
		ProtocolLevel:  protoLevel,
		CleanSession:   cleanSession,
		CleanStart:     cleanSession,
		WillFlag:       willFlag,
		WillQoS:        willQoS,
		WillRetain:     willRetain,
		PasswordFlag:   passwordFlag,
		UsernameFlag:   usernameFlag,
		KeepAlive:      keepAlive,
		Properties:     connectProps,
		ClientID:       clientID,
		WillProperties: willProperties,
		WillTopic:      willTopic,
		WillMessage:    willMessage,
		Username:       username,
		Password:       password,
	}, nil
}

// ConnackPacket represents an MQTT CONNACK packet (MQTT 3.1.1 and MQTT 5.0).
type ConnackPacket struct {
	ProtocolLevel  byte
	SessionPresent bool
	ReturnCode     byte        // In MQTT 3.1.1: ReturnCode; in MQTT 5.0: ReasonCode
	ReasonCode     byte        // Alias for ReturnCode in MQTT 5.0
	Properties     *Properties // MQTT 5.0 Properties
}

func (p *ConnackPacket) Type() byte {
	return CONNACK
}

func (p *ConnackPacket) Encode() ([]byte, error) {
	code := p.ReturnCode
	if code == 0 && p.ReasonCode != 0 {
		code = p.ReasonCode
	}

	var sessFlag byte
	if p.SessionPresent {
		sessFlag = 1
	}

	if p.ProtocolLevel == V50 || (p.Properties != nil && !p.Properties.IsEmpty()) {
		// MQTT 5.0 CONNACK with Properties
		propBytes := EncodeProperties(p.Properties)
		remLen := 2 + len(propBytes)

		var varByteBuf [4]byte
		varByteLen := EncodeRemainingLength(remLen, varByteBuf[:])

		totalLen := 1 + varByteLen + remLen
		buf := make([]byte, totalLen)
		buf[0] = CONNACK << 4
		copy(buf[1:], varByteBuf[:varByteLen])

		offset := 1 + varByteLen
		buf[offset] = sessFlag
		offset++
		buf[offset] = code
		offset++
		copy(buf[offset:], propBytes)

		return buf, nil
	}

	// Standard MQTT 3.1.1 4-byte CONNACK
	buf := []byte{CONNACK << 4, 2, sessFlag, code}
	return buf, nil
}

// DecodeConnack parses a CONNACK packet from payload (excluding fixed header).
func DecodeConnack(payload []byte) (*ConnackPacket, error) {
	if len(payload) < 2 {
		return nil, ErrIncompletePacket
	}

	sessPresent := (payload[0] & 0x01) != 0
	code := payload[1]

	var props *Properties
	protoLevel := V311
	if len(payload) > 2 {
		protoLevel = V50
		p, _, err := DecodeProperties(payload[2:])
		if err != nil {
			return nil, err
		}
		props = p
	}

	return &ConnackPacket{
		ProtocolLevel:  protoLevel,
		SessionPresent: sessPresent,
		ReturnCode:     code,
		ReasonCode:     code,
		Properties:     props,
	}, nil
}
