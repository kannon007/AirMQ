package core_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"mqtt/core"
)

// 自动化回归测试：验证 3 大物联网真实业务场景（简单、一般、复杂）的正确性与边界行为

func TestIoTScenario1_SmartMetering(t *testing.T) {
	pipe := core.NewPipe()

	// 1. ClientID 格式认证
	pipe.Add("meter_auth", core.NewFuncProcessor(
		func(c *core.Context) bool {
			return c.Message != nil && c.Message.ClientID != "$internal"
		},
		func(c *core.Context) error {
			if !strings.HasPrefix(c.Message.ClientID, "meter_") {
				c.Drop("unauthorized meter")
				return core.ErrUnauthorized
			}
			return nil
		},
	))

	// 2. 空包过滤
	pipe.Add("empty_filter", core.NewFuncProcessor(
		func(c *core.Context) bool { return c.Message != nil },
		func(c *core.Context) error {
			if len(c.Message.Payload) == 0 || bytes.Equal(c.Message.Payload, []byte("PING")) {
				c.Drop("empty or ping")
			}
			return nil
		},
	))

	ctx := context.Background()

	// Case 1: 合规上报
	msg1 := &core.Message{ClientID: "meter_001", Topic: "meter/v1", Payload: []byte(`{"kwh":10}`)}
	if err := pipe.Execute(ctx, msg1); err != nil {
		t.Fatalf("msg1 should pass: %v", err)
	}

	// Case 2: 非法扫描客户端 -> 认证拦截
	msg2 := &core.Message{ClientID: "attacker_scan", Topic: "meter/v1", Payload: []byte(`{}`)}
	if err := pipe.Execute(ctx, msg2); err != core.ErrUnauthorized {
		t.Fatalf("msg2 should be ErrUnauthorized, got: %v", err)
	}

	// Case 3: 冗余心跳 -> 丢弃过滤
	msg3 := &core.Message{ClientID: "meter_002", Topic: "meter/v1", Payload: []byte("PING")}
	if err := pipe.Execute(ctx, msg3); err != core.ErrMessageDropped {
		t.Fatalf("msg3 should be dropped, got: %v", err)
	}

	stats := pipe.Stats()
	if stats[1].DroppedCount != 1 {
		t.Fatalf("Expected empty_filter DroppedCount=1, got %d", stats[1].DroppedCount)
	}
}

func TestIoTScenario2_IndustrialFactoryGateway(t *testing.T) {
	pipe := core.NewPipe()
	var kafkaDelivered atomic.Int64

	// 1. 网关白名单
	pipe.Add("gw_auth", core.NewFuncProcessor(
		func(c *core.Context) bool { return c.Message != nil && c.Message.ClientID != "$internal" },
		func(c *core.Context) error {
			if c.Message.ClientID != "gw_workshop_1" {
				c.Drop("unknown gateway")
				return core.ErrUnauthorized
			}
			return nil
		},
	))

	// 2. JSON 校验
	pipe.Add("json_validator", core.NewValidateProcessor(
		core.NewJSONValidator("ind_json"),
		core.WithValidateTopic("factory/+/metrics"),
	))

	// 3. 数据清洗增强
	pipe.Add("enricher", core.NewTransformProcessor(func(msg *core.Message) error {
		msg.SetPayload([]byte(fmt.Sprintf(`{"raw":%s,"edge":"plant_02"}`, string(msg.Payload))))
		return nil
	}, core.WithTransformTopic("factory/+/metrics")))

	// 4. 投递 Kafka
	pipe.Add("kafka_sink", core.NewForwardProcessor(func(msg *core.Message) error {
		kafkaDelivered.Add(1)
		return nil
	}, true, core.WithForwardTopic("factory/+/metrics")))

	ctx := context.Background()

	// Case 1: 合规报文 -> 增强并投递 Kafka
	msg1 := &core.Message{
		ClientID: "gw_workshop_1",
		Topic:    "factory/line1/metrics",
		Payload:  []byte(`{"machine":"CNC_01","rpm":5000}`),
	}
	if err := pipe.Execute(ctx, msg1); err != nil {
		t.Fatalf("Valid industrial message failed: %v", err)
	}
	if kafkaDelivered.Load() != 1 {
		t.Fatalf("Expected Kafka delivery=1, got %d", kafkaDelivered.Load())
	}
	if !strings.Contains(string(msg1.Payload), `"edge":"plant_02"`) {
		t.Fatalf("Payload should be enriched: %s", string(msg1.Payload))
	}

	// Case 2: 非法 JSON 脏数据 -> 拦截丢弃，不送 Kafka
	msg2 := &core.Message{
		ClientID: "gw_workshop_1",
		Topic:    "factory/line1/metrics",
		Payload:  []byte(`ILLEGAL_JSON`),
	}
	if err := pipe.Execute(ctx, msg2); err != core.ErrMessageDropped {
		t.Fatalf("Illegal JSON should be dropped, got: %v", err)
	}
	if kafkaDelivered.Load() != 1 {
		t.Fatalf("Kafka should NOT receive illegal JSON")
	}
}

func TestIoTScenario3_V2XTelematics(t *testing.T) {
	v2xMagic := []byte{0x56, 0x32, 0x58, 0x01}
	var urgentAlarms atomic.Int64
	var canPackets atomic.Int64

	pipe := core.NewPipe()

	// 1. VIN 认证
	pipe.Add("vin_auth", core.NewFuncProcessor(
		func(c *core.Context) bool { return c.Message != nil && c.Message.ClientID != "$internal" },
		func(c *core.Context) error {
			if !strings.HasPrefix(c.Message.ClientID, "VIN_") {
				c.Drop("invalid vin")
				return core.ErrUnauthorized
			}
			return nil
		},
	))

	// 2. CAN TLV 二进制魔数校验
	pipe.Add("can_validator", core.NewValidateProcessor(
		core.NewBinaryValidator("can_val", 8, 1024, core.WithMagic(v2xMagic)),
		core.WithValidateTopic("v2x/+/can"),
	))

	// 3. 碰撞急救告警识别
	pipe.Add("alarm_eval", core.NewFuncProcessor(
		func(c *core.Context) bool {
			return c.Message != nil && strings.Contains(c.Message.Topic, "/alarms")
		},
		func(c *core.Context) error {
			var a struct{ Level int }
			if err := json.Unmarshal(c.Message.Payload, &a); err == nil && a.Level >= 3 {
				c.Set("urgent", true)
				urgentAlarms.Add(1)
			}
			return nil
		},
	))

	// 4. 双路智能投递
	pipe.Add("v2x_forwarder", core.NewFuncProcessor(
		func(c *core.Context) bool { return c.Message != nil },
		func(c *core.Context) error {
			if strings.Contains(c.Message.Topic, "/can") {
				canPackets.Add(1)
			}
			return nil
		},
	))

	ctx := context.Background()

	// Case 1: 合法 CAN 二进制报文
	canPayload := make([]byte, 12)
	copy(canPayload[0:4], v2xMagic)
	binary.BigEndian.PutUint32(canPayload[4:8], 1)
	binary.BigEndian.PutUint32(canPayload[8:12], 380)

	msgCAN := &core.Message{
		ClientID: "VIN_12345678901234567",
		Topic:    "v2x/vin1/can",
		Payload:  canPayload,
	}
	if err := pipe.Execute(ctx, msgCAN); err != nil {
		t.Fatalf("CAN binary packet failed: %v", err)
	}
	if canPackets.Load() != 1 {
		t.Fatalf("CAN packet should be counted")
	}

	// Case 2: 坏魔数 CAN 报文 -> 校验拦截
	badCAN := make([]byte, 12)
	msgBadCAN := &core.Message{
		ClientID: "VIN_12345678901234567",
		Topic:    "v2x/vin1/can",
		Payload:  badCAN,
	}
	if err := pipe.Execute(ctx, msgBadCAN); err != core.ErrMessageDropped {
		t.Fatalf("Bad CAN magic should be dropped, got: %v", err)
	}

	// Case 3: 紧急碰撞告警 (Level 4) -> 触发 urgent 统计
	msgAlarm := &core.Message{
		ClientID: "VIN_12345678901234567",
		Topic:    "v2x/vin1/alarms",
		Payload:  []byte(`{"level":4,"code":"CRASH"}`),
	}
	if err := pipe.Execute(ctx, msgAlarm); err != nil {
		t.Fatalf("Emergency alarm failed: %v", err)
	}
	if urgentAlarms.Load() != 1 {
		t.Fatalf("Urgent alarm should be counted")
	}
}
