package protocol

import (
	"bytes"
	"testing"
)

func TestMQTT5PropertiesEncodeDecode(t *testing.T) {
	expiry := uint32(3600)
	payloadFormat := byte(1)
	subID := 105
	keepAlive := uint16(120)

	orig := &Properties{
		MessageExpiryInterval:    &expiry,
		PayloadFormatIndicator:   &payloadFormat,
		ContentType:              "application/json",
		ResponseTopic:            "response/client1",
		CorrelationData:          []byte("correlation-12345"),
		SubscriptionIdentifier:   &subID,
		ServerKeepAlive:          &keepAlive,
		AssignedClientIdentifier: "auto-generated-client-id",
	}
	orig.AddUserProperty("region", "ap-southeast-1")
	orig.AddUserProperty("env", "production")

	encoded := EncodeProperties(orig)
	if len(encoded) == 0 {
		t.Fatalf("EncodeProperties returned empty bytes")
	}

	decoded, consumed, err := DecodeProperties(encoded)
	if err != nil {
		t.Fatalf("DecodeProperties failed: %v", err)
	}
	if consumed != len(encoded) {
		t.Errorf("Consumed %d bytes, expected %d", consumed, len(encoded))
	}

	if decoded.MessageExpiryInterval == nil || *decoded.MessageExpiryInterval != expiry {
		t.Errorf("MessageExpiry mismatch")
	}
	if decoded.PayloadFormatIndicator == nil || *decoded.PayloadFormatIndicator != payloadFormat {
		t.Errorf("PayloadFormat mismatch")
	}
	if decoded.ContentType != "application/json" {
		t.Errorf("ContentType mismatch: %s", decoded.ContentType)
	}
	if decoded.ResponseTopic != "response/client1" {
		t.Errorf("ResponseTopic mismatch: %s", decoded.ResponseTopic)
	}
	if !bytes.Equal(decoded.CorrelationData, []byte("correlation-12345")) {
		t.Errorf("CorrelationData mismatch")
	}
	if decoded.SubscriptionIdentifier == nil || *decoded.SubscriptionIdentifier != subID {
		t.Errorf("SubscriptionIdentifier mismatch")
	}
	if decoded.ServerKeepAlive == nil || *decoded.ServerKeepAlive != keepAlive {
		t.Errorf("ServerKeepAlive mismatch")
	}
	if decoded.AssignedClientIdentifier != "auto-generated-client-id" {
		t.Errorf("AssignedClientID mismatch")
	}

	regionVal, ok := decoded.GetUserProperty("region")
	if !ok || regionVal != "ap-southeast-1" {
		t.Errorf("UserProperty 'region' mismatch: %s", regionVal)
	}
	envVal, ok := decoded.GetUserProperty("env")
	if !ok || envVal != "production" {
		t.Errorf("UserProperty 'env' mismatch: %s", envVal)
	}
}

func TestMQTT5ConnectAndConnack(t *testing.T) {
	sessionExpiry := uint32(7200)
	connectPkt := &ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: V50,
		CleanStart:    true,
		KeepAlive:     60,
		ClientID:      "test-client-v5",
		Properties: &Properties{
			SessionExpiryInterval: &sessionExpiry,
		},
		WillFlag:    true,
		WillTopic:   "status/test-client-v5",
		WillMessage: []byte("offline"),
		WillQoS:     1,
		WillProperties: &Properties{
			ContentType: "text/plain",
		},
	}

	raw, err := connectPkt.Encode()
	if err != nil {
		t.Fatalf("Connect Encode error: %v", err)
	}

	decodedPkt, totalLen, err := DecodePacket(raw, V50)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if totalLen != len(raw) {
		t.Errorf("Length mismatch: got %d, want %d", totalLen, len(raw))
	}

	cp, ok := decodedPkt.(*ConnectPacket)
	if !ok {
		t.Fatalf("Expected *ConnectPacket, got %T", decodedPkt)
	}
	if cp.ClientID != "test-client-v5" {
		t.Errorf("ClientID mismatch: %s", cp.ClientID)
	}
	if cp.Properties == nil || cp.Properties.SessionExpiryInterval == nil || *cp.Properties.SessionExpiryInterval != sessionExpiry {
		t.Errorf("SessionExpiryInterval mismatch")
	}
	if !cp.WillFlag || cp.WillTopic != "status/test-client-v5" || string(cp.WillMessage) != "offline" {
		t.Errorf("Will message mismatch")
	}
	if cp.WillProperties == nil || cp.WillProperties.ContentType != "text/plain" {
		t.Errorf("Will properties mismatch")
	}

	// Test MQTT 5.0 CONNACK with AssignedClientID
	connack := &ConnackPacket{
		SessionPresent: false,
		ReasonCode:     ReasonSuccess,
		Properties: &Properties{
			AssignedClientIdentifier: "assigned-by-broker-999",
		},
	}
	connackRaw, err := connack.Encode()
	if err != nil {
		t.Fatalf("Connack encode failed: %v", err)
	}

	decConnack, _, err := DecodePacket(connackRaw, V50)
	if err != nil {
		t.Fatalf("Connack decode failed: %v", err)
	}
	ca := decConnack.(*ConnackPacket)
	if ca.ReasonCode != ReasonSuccess {
		t.Errorf("ReasonCode mismatch: %x", ca.ReasonCode)
	}
	if ca.Properties == nil || ca.Properties.AssignedClientIdentifier != "assigned-by-broker-999" {
		t.Errorf("AssignedClientID in CONNACK mismatch")
	}
}

func TestMQTT5QoS2AckPackets(t *testing.T) {
	// 1. PUBACK
	puback := &PubackPacket{
		PacketID:   1001,
		ReasonCode: ReasonNoMatchingSubscribers,
		Properties: &Properties{ReasonString: "no subs active"},
	}
	data, err := puback.Encode()
	if err != nil {
		t.Fatalf("Puback encode error: %v", err)
	}
	decPkt, _, err := DecodePacket(data, V50)
	if err != nil {
		t.Fatalf("Puback decode error: %v", err)
	}
	decPuback := decPkt.(*PubackPacket)
	if decPuback.PacketID != 1001 || decPuback.ReasonCode != ReasonNoMatchingSubscribers {
		t.Errorf("Puback mismatch: %+v", decPuback)
	}

	// 2. PUBREC
	pubrec := &PubrecPacket{PacketID: 2002, ReasonCode: ReasonSuccess}
	data, _ = pubrec.Encode()
	decPkt, _, _ = DecodePacket(data, V50)
	decPubrec := decPkt.(*PubrecPacket)
	if decPubrec.PacketID != 2002 || decPubrec.ReasonCode != ReasonSuccess {
		t.Errorf("Pubrec mismatch: %+v", decPubrec)
	}

	// 3. PUBREL
	pubrel := &PubrelPacket{PacketID: 3003, ReasonCode: ReasonPacketIdentifierNotFound}
	data, _ = pubrel.Encode()
	decPkt, _, _ = DecodePacket(data, V50)
	decPubrel := decPkt.(*PubrelPacket)
	if decPubrel.PacketID != 3003 || decPubrel.ReasonCode != ReasonPacketIdentifierNotFound {
		t.Errorf("Pubrel mismatch: %+v", decPubrel)
	}

	// 4. PUBCOMP
	pubcomp := &PubcompPacket{PacketID: 4004, ReasonCode: ReasonSuccess}
	data, _ = pubcomp.Encode()
	decPkt, _, _ = DecodePacket(data, V50)
	decPubcomp := decPkt.(*PubcompPacket)
	if decPubcomp.PacketID != 4004 || decPubcomp.ReasonCode != ReasonSuccess {
		t.Errorf("Pubcomp mismatch: %+v", decPubcomp)
	}
}

func TestMQTT5SubscriptionOptions(t *testing.T) {
	subPkt := &SubscribePacket{
		PacketID: 555,
		Topics: []TopicSub{
			{Topic: "sensor/temp", QoS: 1, NoLocal: true, RetainAsPublished: true, RetainHandling: 1},
			{Topic: "alerts/#", QoS: 2, NoLocal: false, RetainAsPublished: false, RetainHandling: 2},
		},
	}
	data, err := subPkt.Encode()
	if err != nil {
		t.Fatalf("Subscribe encode error: %v", err)
	}

	decPkt, _, err := DecodePacket(data, V50)
	if err != nil {
		t.Fatalf("Subscribe decode error: %v", err)
	}
	decSub := decPkt.(*SubscribePacket)
	if decSub.PacketID != 555 || len(decSub.Topics) != 2 {
		t.Fatalf("Subscribe packet mismatch: %+v", decSub)
	}

	s0 := decSub.Topics[0]
	if s0.Topic != "sensor/temp" || s0.QoS != 1 || !s0.NoLocal || !s0.RetainAsPublished || s0.RetainHandling != 1 {
		t.Errorf("Topic 0 options mismatch: %+v", s0)
	}

	s1 := decSub.Topics[1]
	if s1.Topic != "alerts/#" || s1.QoS != 2 || s1.NoLocal || s1.RetainAsPublished || s1.RetainHandling != 2 {
		t.Errorf("Topic 1 options mismatch: %+v", s1)
	}

	// SUBACK with Reason Codes
	suback := &SubackPacket{
		PacketID:    555,
		ReasonCodes: []byte{ReasonGrantedQoS1, ReasonGrantedQoS2},
	}
	subackData, err := suback.Encode()
	if err != nil {
		t.Fatalf("Suback encode error: %v", err)
	}

	decSubackPkt, _, err := DecodePacket(subackData, V50)
	if err != nil {
		t.Fatalf("Suback decode error: %v", err)
	}
	decSuback := decSubackPkt.(*SubackPacket)
	if decSuback.PacketID != 555 || len(decSuback.ReasonCodes) != 2 {
		t.Fatalf("Suback mismatch: %+v", decSuback)
	}
	if decSuback.ReasonCodes[0] != ReasonGrantedQoS1 || decSuback.ReasonCodes[1] != ReasonGrantedQoS2 {
		t.Errorf("Suback reason codes mismatch: %v", decSuback.ReasonCodes)
	}
}

func TestMQTT5AuthAndDisconnect(t *testing.T) {
	// AUTH packet
	auth := &AuthPacket{
		ReasonCode: ReasonContinueAuth,
		Properties: &Properties{
			AuthenticationMethod: "SCRAM-SHA-256",
			AuthenticationData:   []byte("client-first-message"),
		},
	}
	authData, err := auth.Encode()
	if err != nil {
		t.Fatalf("Auth encode error: %v", err)
	}
	decPkt, _, err := DecodePacket(authData, V50)
	if err != nil {
		t.Fatalf("Auth decode error: %v", err)
	}
	decAuth := decPkt.(*AuthPacket)
	if decAuth.ReasonCode != ReasonContinueAuth {
		t.Errorf("Auth ReasonCode mismatch: %x", decAuth.ReasonCode)
	}
	if decAuth.Properties == nil || decAuth.Properties.AuthenticationMethod != "SCRAM-SHA-256" {
		t.Errorf("Auth properties mismatch: %+v", decAuth.Properties)
	}

	// DISCONNECT packet with Reason Code and Reason String
	disc := &DisconnectPacket{
		ReasonCode: ReasonDisconnectWithWill,
		Properties: &Properties{
			ReasonString: "emergency shutdown",
		},
	}
	discData, err := disc.Encode()
	if err != nil {
		t.Fatalf("Disconnect encode error: %v", err)
	}
	decDiscPkt, _, err := DecodePacket(discData, V50)
	if err != nil {
		t.Fatalf("Disconnect decode error: %v", err)
	}
	decDisc := decDiscPkt.(*DisconnectPacket)
	if decDisc.ReasonCode != ReasonDisconnectWithWill {
		t.Errorf("Disconnect ReasonCode mismatch: %x", decDisc.ReasonCode)
	}
	if decDisc.Properties == nil || decDisc.Properties.ReasonString != "emergency shutdown" {
		t.Errorf("Disconnect ReasonString mismatch: %+v", decDisc.Properties)
	}
}
