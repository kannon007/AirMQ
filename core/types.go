package core

import (
	"mqtt/pkg/hook"
	"mqtt/pkg/pipeline"
	"mqtt/pkg/protocol"
	"mqtt/pkg/server"
	"mqtt/pkg/store"
)

// Hook and security type aliases
type Hook = hook.Hook
type BaseHook = hook.BaseHook
type ClientContext = hook.ClientContext
type AuthAction = hook.AuthAction
type HookManager = hook.Manager

const (
	AuthActionPublish   = hook.AuthActionPublish
	AuthActionSubscribe = hook.AuthActionSubscribe
)

func NewHookManager() *hook.Manager {
	return hook.NewManager()
}

// Protocol packet type aliases
type ConnectPacket = protocol.ConnectPacket
type PublishPacket = protocol.PublishPacket

// Broker telemetry and model type aliases (decoupled from dashboard)
type OverviewStats = server.OverviewStats
type ClientSummary = server.ClientSummary
type ClientDetail = server.ClientDetail
type SubscriptionSummary = server.SubscriptionSummary
type RetainedSummary = server.RetainedSummary
type ListenerSummary = server.ListenerSummary
type ClusterNodeSummary = server.ClusterNodeSummary
type ClusterSummary = server.ClusterSummary
type PipelineSummary = server.PipelineSummary

// Data integration rule and bridge type aliases
type Rule = pipeline.Rule
type RuleAction = pipeline.RuleAction
type RuleStatus = pipeline.RuleStatus
type Bridge = pipeline.Bridge
type BridgeStatus = pipeline.BridgeStatus
type RulePingResult = pipeline.RulePingResult

// Storage type alias
type MessageStore = store.MessageStore

// MessageHandler is the callback function for in-process subscribers.
type MessageHandler func(topic string, payload []byte)
