package protocol

// MQTT 5.0 Reason Codes (OASIS Specification)
const (
	// Generic / Success
	ReasonSuccess             byte = 0x00
	ReasonNormalDisconnection byte = 0x00
	ReasonGrantedQoS0         byte = 0x00
	ReasonGrantedQoS1         byte = 0x01
	ReasonGrantedQoS2         byte = 0x02
	ReasonDisconnectWithWill  byte = 0x04

	// Informational / Non-fatal
	ReasonNoMatchingSubscribers byte = 0x10
	ReasonNoSubscriptionExisted byte = 0x11
	ReasonContinueAuth          byte = 0x18
	ReasonReAuthenticate        byte = 0x19

	// Errors
	ReasonUnspecifiedError            byte = 0x80
	ReasonMalformedPacket             byte = 0x81
	ReasonProtocolError               byte = 0x82
	ReasonImplementationSpecificError byte = 0x83
	ReasonUnsupportedProtocolVersion  byte = 0x84
	ReasonClientIdentifierNotValid    byte = 0x85
	ReasonBadUserOrPassword           byte = 0x86
	ReasonNotAuthorized               byte = 0x87
	ReasonServerUnavailable           byte = 0x88
	ReasonServerBusy                  byte = 0x89
	ReasonBanned                      byte = 0x8A
	ReasonServerShuttingDown          byte = 0x8B
	ReasonBadAuthenticationMethod     byte = 0x8C
	ReasonKeepAliveTimeout            byte = 0x8D
	ReasonSessionTakenOver            byte = 0x8E
	ReasonTopicFilterInvalid          byte = 0x8F
	ReasonTopicNameInvalid            byte = 0x90
	ReasonPacketIdentifierInUse       byte = 0x91
	ReasonPacketIdentifierNotFound    byte = 0x92
	ReasonReceiveMaximumExceeded      byte = 0x93
	ReasonTopicAliasInvalid           byte = 0x94
	ReasonPacketTooLarge              byte = 0x95
	ReasonMessageRateTooHigh          byte = 0x96
	ReasonQuotaExceeded               byte = 0x97
	ReasonAdministrativeAction        byte = 0x98
	ReasonPayloadFormatInvalid        byte = 0x99
	ReasonRetainNotSupported          byte = 0x9A
	ReasonQoSNotSupported             byte = 0x9B
	ReasonUseAnotherServer            byte = 0x9C
	ReasonServerMoved                 byte = 0x9D
	ReasonSharedSubsNotSupported      byte = 0x9E
	ReasonConnectionRateExceeded      byte = 0x9F
	ReasonMaximumConnectTime          byte = 0xA0
	ReasonSubscriptionIDsNotSupported byte = 0xA1
	ReasonWildcardSubsNotSupported    byte = 0xA2
)

// ReasonCodeText returns a descriptive string for standard MQTT 5.0 reason codes.
func ReasonCodeText(code byte) string {
	switch code {
	case ReasonSuccess:
		return "Success"
	case ReasonGrantedQoS1:
		return "Granted QoS 1"
	case ReasonGrantedQoS2:
		return "Granted QoS 2"
	case ReasonDisconnectWithWill:
		return "Disconnect with Will Message"
	case ReasonNoMatchingSubscribers:
		return "No matching subscribers"
	case ReasonNoSubscriptionExisted:
		return "No subscription existed"
	case ReasonContinueAuth:
		return "Continue authentication"
	case ReasonReAuthenticate:
		return "Re-authenticate"
	case ReasonUnspecifiedError:
		return "Unspecified error"
	case ReasonMalformedPacket:
		return "Malformed Packet"
	case ReasonProtocolError:
		return "Protocol Error"
	case ReasonImplementationSpecificError:
		return "Implementation specific error"
	case ReasonUnsupportedProtocolVersion:
		return "Unsupported Protocol Version"
	case ReasonClientIdentifierNotValid:
		return "Client Identifier not valid"
	case ReasonBadUserOrPassword:
		return "Bad User Name or Password"
	case ReasonNotAuthorized:
		return "Not authorized"
	case ReasonServerUnavailable:
		return "Server unavailable"
	case ReasonServerBusy:
		return "Server busy"
	case ReasonBanned:
		return "Banned"
	case ReasonServerShuttingDown:
		return "Server shutting down"
	case ReasonBadAuthenticationMethod:
		return "Bad authentication method"
	case ReasonKeepAliveTimeout:
		return "Keep Alive timeout"
	case ReasonSessionTakenOver:
		return "Session taken over"
	case ReasonTopicFilterInvalid:
		return "Topic Filter invalid"
	case ReasonTopicNameInvalid:
		return "Topic Name invalid"
	case ReasonPacketIdentifierInUse:
		return "Packet Identifier in use"
	case ReasonPacketIdentifierNotFound:
		return "Packet Identifier not found"
	case ReasonReceiveMaximumExceeded:
		return "Receive Maximum exceeded"
	case ReasonTopicAliasInvalid:
		return "Topic Alias invalid"
	case ReasonPacketTooLarge:
		return "Packet too large"
	case ReasonMessageRateTooHigh:
		return "Message rate too high"
	case ReasonQuotaExceeded:
		return "Quota exceeded"
	case ReasonAdministrativeAction:
		return "Administrative action"
	case ReasonPayloadFormatInvalid:
		return "Payload format invalid"
	case ReasonRetainNotSupported:
		return "Retain not supported"
	case ReasonQoSNotSupported:
		return "QoS not supported"
	case ReasonUseAnotherServer:
		return "Use another server"
	case ReasonServerMoved:
		return "Server moved"
	case ReasonSharedSubsNotSupported:
		return "Shared Subscriptions not supported"
	case ReasonConnectionRateExceeded:
		return "Connection rate exceeded"
	case ReasonMaximumConnectTime:
		return "Maximum connect time"
	case ReasonSubscriptionIDsNotSupported:
		return "Subscription Identifiers not supported"
	case ReasonWildcardSubsNotSupported:
		return "Wildcard Subscriptions not supported"
	default:
		return "Unknown reason code"
	}
}
