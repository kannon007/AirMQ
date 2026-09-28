package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRemainingLength(t *testing.T) {
	testCases := []int{0, 64, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	var buf [4]byte

	for _, length := range testCases {
		encodedLen := EncodeRemainingLength(length, buf[:])
		decodedLen, consumed, err := DecodeRemainingLength(buf[:encodedLen])
		if err != nil {
			t.Fatalf("Failed to decode length %d: %v", length, err)
		}
		if consumed != encodedLen {
			t.Fatalf("Consumed mismatch for %d: got %d, want %d", length, consumed, encodedLen)
		}
		if decodedLen != length {
			t.Fatalf("Decoded length mismatch: got %d, want %d", decodedLen, length)
		}
	}
}

func TestPublishEncodeDecode(t *testing.T) {
	pub := &PublishPacket{
		Dup:      false,
		QoS:      QoS1,
		Retain:   true,
		Topic:    "sensor/temperature/room1",
		PacketID: 1024,
		Payload:  []byte("25.6"),
	}

	encoded, err := pub.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	pkt, consumed, err := DecodePacket(encoded)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("Consumed %d, expected %d", consumed, len(encoded))
	}

	decodedPub, ok := pkt.(*PublishPacket)
	if !ok {
		t.Fatalf("Expected *PublishPacket, got %T", pkt)
	}

	if decodedPub.Topic != pub.Topic {
		t.Errorf("Topic: got %q, want %q", decodedPub.Topic, pub.Topic)
	}
	if decodedPub.QoS != pub.QoS {
		t.Errorf("QoS: got %d, want %d", decodedPub.QoS, pub.QoS)
	}
	if decodedPub.Retain != pub.Retain {
		t.Errorf("Retain: got %v, want %v", decodedPub.Retain, pub.Retain)
	}
	if decodedPub.PacketID != pub.PacketID {
		t.Errorf("PacketID: got %d, want %d", decodedPub.PacketID, pub.PacketID)
	}
	if !bytes.Equal(decodedPub.Payload, pub.Payload) {
		t.Errorf("Payload: got %q, want %q", string(decodedPub.Payload), string(pub.Payload))
	}
}

func TestPingreqPingresp(t *testing.T) {
	req := &PingreqPacket{}
	data, err := req.Encode()
	if err != nil {
		t.Fatalf("Pingreq encode failed: %v", err)
	}

	pkt, consumed, err := DecodePacket(data)
	if err != nil {
		t.Fatalf("Pingreq decode failed: %v", err)
	}
	if consumed != 2 || pkt.Type() != PINGREQ {
		t.Fatalf("Expected PINGREQ, got type %d", pkt.Type())
	}

	resp := &PingrespPacket{}
	data, err = resp.Encode()
	if err != nil {
		t.Fatalf("Pingresp encode failed: %v", err)
	}
	pkt, consumed, err = DecodePacket(data)
	if err != nil {
		t.Fatalf("Pingresp decode failed: %v", err)
	}
	if consumed != 2 || pkt.Type() != PINGRESP {
		t.Fatalf("Expected PINGRESP, got type %d", pkt.Type())
	}
}
