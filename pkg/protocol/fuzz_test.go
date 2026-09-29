package protocol

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func TestFuzz_MalformedRemainingLength(t *testing.T) {
	// 5-byte continuation should trigger ErrMalformedRemaining
	fiveByteContin := []byte{0x80, 0x80, 0x80, 0x80, 0x80}
	_, _, err := DecodeRemainingLength(fiveByteContin)
	if err != ErrMalformedRemaining {
		t.Fatalf("Expected ErrMalformedRemaining, got %v", err)
	}

	// Incomplete length
	incomplete := []byte{0x80, 0x80}
	_, _, err = DecodeRemainingLength(incomplete)
	if err != ErrIncompletePacket {
		t.Fatalf("Expected ErrIncompletePacket, got %v", err)
	}

	// Normal valid variable byte integers
	cases := []struct {
		val int
		exp []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{2097151, []byte{0xFF, 0xFF, 0x7F}},
		{2097152, []byte{0x80, 0x80, 0x80, 0x01}},
		{268435455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}
	var dst [4]byte
	for _, tc := range cases {
		n := EncodeRemainingLength(tc.val, dst[:])
		if !bytes.Equal(dst[:n], tc.exp) {
			t.Fatalf("Encode remaining mismatch for %d: got %x, exp %x", tc.val, dst[:n], tc.exp)
		}
		gotVal, consumed, err := DecodeRemainingLength(dst[:n])
		if err != nil || gotVal != tc.val || consumed != n {
			t.Fatalf("Decode remaining mismatch for %d: got %d (consumed %d, err %v)", tc.val, gotVal, consumed, err)
		}
	}
}

func TestFuzz_TopicValidation(t *testing.T) {
	validPubs := []string{
		"sensor/temperature",
		"a/b/c/d/e",
		"/",
		"home/living-room",
		"devices/001/telemetry",
	}
	for _, topic := range validPubs {
		if !ValidatePublishTopic(topic) {
			t.Errorf("Expected valid publish topic: %q", topic)
		}
	}

	invalidPubs := []string{
		"",                     // empty
		"sensor/+/temperature", // wildcard '+'
		"sensor/#",             // wildcard '#'
		"sensor/\x00/temp",     // null byte
		"+",
		"#",
	}
	for _, topic := range invalidPubs {
		if ValidatePublishTopic(topic) {
			t.Errorf("Expected invalid publish topic: %q", topic)
		}
	}

	validSubs := []string{
		"sensor/temperature",
		"sensor/+/temperature",
		"sensor/#",
		"+",
		"#",
		"+/+",
		"sensor/+/temp/+",
		"$share/group1/sensor/#",
		"$share/consumer_A/devices/+/telemetry",
	}
	for _, sub := range validSubs {
		if !ValidateTopicFilter(sub) {
			t.Errorf("Expected valid subscription filter: %q", sub)
		}
	}

	invalidSubs := []string{
		"",                          // empty
		"sensor/\x00/temperature",   // null byte
		"sensor/#/temperature",      // '#' not at the end
		"sensor/temp#",              // '#' mixed with other chars
		"sensor/+temp",              // '+' mixed with other chars
		"sensor/temp+",              // '+' mixed with other chars
		"$share//sensor/#",          // empty share group
		"$share/group1",             // missing topic filter in share
		"$share/group+/topic",       // group contains wildcard
	}
	for _, sub := range invalidSubs {
		if ValidateTopicFilter(sub) {
			t.Errorf("Expected invalid subscription filter: %q", sub)
		}
	}
}

func TestFuzz_MalformedPackets(t *testing.T) {
	// 1. Invalid CONNECT protocol name
	badProtoName := []byte{
		0x00, 0x04, 'H', 'T', 'T', 'P', // Protocol name: "HTTP"
		0x04,       // Protocol level
		0x02,       // Connect flags
		0x00, 0x3C, // KeepAlive 60
	}
	_, err := DecodeConnect(badProtoName)
	if err != ErrInvalidProtocolName {
		t.Fatalf("Expected ErrInvalidProtocolName, got %v", err)
	}

	// 2. Invalid CONNECT protocol version
	badProtoVer := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x99,       // Protocol level 153 (invalid)
		0x02,       // Connect flags
		0x00, 0x3C, // KeepAlive 60
	}
	_, err = DecodeConnect(badProtoVer)
	if err != ErrUnsupportedVersion {
		t.Fatalf("Expected ErrUnsupportedVersion, got %v", err)
	}

	// 3. Truncated ClientID in CONNECT
	truncatedClientID := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x04,       // MQTT 3.1.1
		0x02,       // CleanSession
		0x00, 0x3C, // KeepAlive
		0x00, 0x10, // ClientID length 16, but followed by only 2 bytes!
		'a', 'b',
	}
	_, err = DecodeConnect(truncatedClientID)
	if err != ErrIncompletePacket {
		t.Fatalf("Expected ErrIncompletePacket, got %v", err)
	}

	// 4. PUBLISH with QoS 3 (illegal)
	qos3Flags := byte(0x06) // (3 << 1)
	rawPublish := []byte{
		0x00, 0x04, 't', 'e', 's', 't', // Topic "test"
		0x00, 0x01, // PacketID 1
		'h', 'e', 'l', 'l', 'o',
	}
	_, err = DecodePublish(qos3Flags, rawPublish)
	if err != ErrProtocolViolation {
		t.Fatalf("Expected ErrProtocolViolation on QoS 3, got %v", err)
	}

	// 5. PUBLISH with wildcard topic
	wildcardPub := []byte{
		0x00, 0x08, 't', 'e', 's', 't', '/', '+', '/', 'a',
		'p', 'a', 'y', 'l', 'o', 'a', 'd',
	}
	_, err = DecodePublish(0x00, wildcardPub)
	if err != ErrProtocolViolation {
		t.Fatalf("Expected ErrProtocolViolation on wildcard PUBLISH, got %v", err)
	}

	// 6. Unknown MQTT 5.0 Property ID
	unknownProp := []byte{
		0x02, // Property length = 2
		0xFD, // Unknown property ID
		0x01, // Value
	}
	_, _, err = DecodeProperties(unknownProp)
	if err != ErrMalformedProperty {
		t.Fatalf("Expected ErrMalformedProperty on unknown property ID, got %v", err)
	}
}

func TestFuzz_RandomMutationDecoderNoPanic(t *testing.T) {
	// Feed 10,000 random byte sequences into DecodePacket to ensure no panics occur
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 10000; i++ {
		length := rng.Intn(128)
		buf := make([]byte, length)
		rng.Read(buf)

		// Test with MQTT 3.1.1 and MQTT 5.0
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodePacket panicked on input %x: %v", buf, r)
				}
			}()
			_, _, _ = DecodePacket(buf, V311)
			_, _, _ = DecodePacket(buf, V50)
		}()
	}
}
