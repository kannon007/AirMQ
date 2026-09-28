package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"mqtt/core"
)

// TelemetryPayload 定义严格的业务数据结构规范
type TelemetryPayload struct {
	DeviceID    string             `json:"device_id"`
	Timestamp   int64              `json:"ts"`
	Temperature float64            `json:"temperature"`
	Humidity    float64            `json:"humidity"`
	Readings    map[string]float64 `json:"readings,omitempty"`
}

// DataValidatorHook 是一个在核心网络入口处执行前置校验的 Hook
type DataValidatorHook struct {
	core.BaseHook
	DroppedCount atomic.Uint64
}

func (h *DataValidatorHook) Name() string {
	return "DataValidatorHook"
}

// OnPublish 在任何客户端发布消息时立即触发 (在推入主题树分发之前)
// 返回:
//   - drop: 若返回 true，Broker 将物理丢弃该消息，绝不分发给任何订阅者，也不存入 Retained
//   - err: 可选的错误信息，若不为 nil 且 drop 为 true 则输出警告
func (h *DataValidatorHook) OnPublish(ctx *core.ClientContext, pkt *core.PublishPacket) (bool, error) {
	// 仅对遥测数据主题进行强制数据校验 (如 "sensors/#" 或 "telemetry/#")
	if !strings.HasPrefix(pkt.Topic, "sensors/") && !strings.HasPrefix(pkt.Topic, "telemetry/") {
		return false, nil // 其他主题 (如控制命令/心跳) 放行
	}

	// 1. 基础报文校验: Payload 不能为空
	if len(pkt.Payload) == 0 {
		h.DroppedCount.Add(1)
		log.Printf("[Validator 拦截] 丢弃空报文: 来自客户端 %s, 主题: %s", ctx.ClientID, pkt.Topic)
		return true, fmt.Errorf("payload cannot be empty")
	}

	// 2. 报文格式校验: 校验是否为标准 JSON 格式
	var data TelemetryPayload
	if err := json.Unmarshal(pkt.Payload, &data); err != nil {
		h.DroppedCount.Add(1)
		log.Printf("[Validator 拦截] 丢弃畸形 JSON 数据: 来自客户端 %s, 主题: %s, 原始数据: %s, 解析错误: %v",
			ctx.ClientID, pkt.Topic, string(pkt.Payload), err)
		// 返回 drop = true 阻断消息流转
		return true, fmt.Errorf("invalid json structure: %w", err)
	}

	// 3. 业务必填字段完整性校验
	if data.DeviceID == "" {
		h.DroppedCount.Add(1)
		log.Printf("[Validator 拦截] 丢弃缺失必填字段 (device_id) 报文: 来自客户端 %s, 主题: %s", ctx.ClientID, pkt.Topic)
		return true, fmt.Errorf("missing required field: device_id")
	}
	if data.Timestamp <= 0 {
		h.DroppedCount.Add(1)
		log.Printf("[Validator 拦截] 丢弃时间戳无效报文: 来自客户端 %s, ts=%d", ctx.ClientID, data.Timestamp)
		return true, fmt.Errorf("invalid timestamp")
	}

	// 4. 业务阈值与数值范围有效性校验 (防止传感器故障产生荒谬离群脏数据)
	if data.Temperature < -50.0 || data.Temperature > 120.0 {
		h.DroppedCount.Add(1)
		log.Printf("[Validator 拦截] 丢弃传感器物理量超限数据: 温度 %.2f°C 超出工业安全工作区间 [-50, 120]", data.Temperature)
		return true, fmt.Errorf("temperature out of physical range: %.2f", data.Temperature)
	}

	if data.Humidity < 0.0 || data.Humidity > 100.0 {
		h.DroppedCount.Add(1)
		log.Printf("[Validator 拦截] 丢弃湿度物理量超限数据: 湿度 %.2f%% 超出 [0, 100]", data.Humidity)
		return true, fmt.Errorf("humidity out of range: %.2f", data.Humidity)
	}

	// 所有规则校验通过，允许分发至全网订阅者与存储引擎
	return false, nil
}

func main() {
	validator := &DataValidatorHook{}

	// 1. 初始化纯 core Broker 并装配数据校验器
	broker, err := core.NewBroker(
		core.WithTCP(":1885"),
		core.WithMemoryStore(),
		core.WithHook(validator), // 挂载数据校验 Hook
	)
	if err != nil {
		log.Fatalf("初始化 Broker 失败: %v", err)
	}

	// 2. 模拟业务后端订阅合规数据
	var receivedValidCount atomic.Int32
	unsub, err := broker.Subscribe("telemetry/#", 0, func(topic string, payload []byte) {
		receivedValidCount.Add(1)
		log.Printf(">>> [业务层成功接收合规消息] Topic=%s | Payload=%s", topic, string(payload))
	})
	if err != nil {
		log.Fatalf("订阅失败: %v", err)
	}
	defer unsub()

	go func() {
		log.Println("MQTT Broker (带数据格式与字段校验) 正在运行在 :1885")
		_ = broker.Start()
	}()
	time.Sleep(100 * time.Millisecond)

	// -------------------------------------------------------------------------
	// 测试用例 1: 发送完全合规的 JSON 数据 -> 预期: 校验通过，业务后端成功接收
	// -------------------------------------------------------------------------
	log.Println("\n--- 发送测试 1: 合规数据 ---")
	validMsg := `{"device_id":"sensor-01","ts":1710000000,"temperature":24.5,"humidity":55.0}`
	_ = broker.Publish("telemetry/sensor-01", []byte(validMsg), 0, false)

	// -------------------------------------------------------------------------
	// 测试用例 2: 发送非 JSON 畸形文本 -> 预期: 校验失败，被 Hook 物理丢弃
	// -------------------------------------------------------------------------
	log.Println("\n--- 发送测试 2: 畸形文本数据 (非法 JSON) ---")
	malformedMsg := `THIS_IS_NOT_A_VALID_JSON_STRING`
	_ = broker.Publish("telemetry/sensor-02", []byte(malformedMsg), 0, false)

	// -------------------------------------------------------------------------
	// 测试用例 3: 发送缺失必填字段 (无 device_id) -> 预期: 校验失败，被 Hook 物理丢弃
	// -------------------------------------------------------------------------
	log.Println("\n--- 发送测试 3: 缺失必填字段 ---")
	missingFieldMsg := `{"ts":1710000000,"temperature":25.0,"humidity":50.0}`
	_ = broker.Publish("telemetry/sensor-03", []byte(missingFieldMsg), 0, false)

	// -------------------------------------------------------------------------
	// 测试用例 4: 发送离群超限物理脏数据 (温度 999°C) -> 预期: 校验失败，被 Hook 物理丢弃
	// -------------------------------------------------------------------------
	log.Println("\n--- 发送测试 4: 温度数值离群异常 ---")
	outOfRangeMsg := `{"device_id":"sensor-04","ts":1710000000,"temperature":999.0,"humidity":50.0}`
	_ = broker.Publish("telemetry/sensor-04", []byte(outOfRangeMsg), 0, false)

	time.Sleep(200 * time.Millisecond)

	// 打印校验审计统计
	log.Printf("\n=== 数据校验与拦截总结 ===")
	log.Printf("成功进入业务订阅的合规消息数: %d (预期: 1)", receivedValidCount.Load())
	log.Printf("被入口 Hook 拦截并丢弃的非法消息数: %d (预期: 3)", validator.DroppedCount.Load())

	_ = broker.Stop(context.Background())
}
