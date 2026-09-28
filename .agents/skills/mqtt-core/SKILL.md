---
name: mqtt-core
description: >-
  Guide AI agents and developers on how to embed, configure, and extend the pure Go
  high-performance MQTT broker core (`mqtt/core`). Activate this skill when the user asks
  how to import `core`, build custom authentication, create self-contained processors (Match/Process),
  validate JSON/Binary payloads, stream data to Kafka/external sinks, or benchmark broker performance.
---

# AirMQ Core 开发者与 AI 协同实战指南 (AI-Native Skill & Handbook)

本文档是 `AirMQ (mqtt/core)` 的核心使用手册，采用 **AI Skill 与人类使用文档深度融合** 的组织范式。既作为人类开发者的架构规范与实操手册，也作为 AI 编程助手直接激活并严格遵循的行动准则。

---

## 🧭 核心心智模型 (Mental Model)

在指导或编写基于 `core` 的代码时，必须牢记以下三大核心架构定律：

1. **三层物理解耦，`core` 纯净独立**：
   - `core` 是纯 Go 实现的独立内核（`import "mqtt/core"`），**绝不反向依赖 `dashboard` 或 `web`**；
   - 任何第三方应用或微服务均可单依赖引入 `core`，无需启动任何 HTTP 控制台。
2. **职责自洽的处理器契约 (`Processor`)**：
   - 处理器严格收敛为且仅为 2 个方法：
     - `Match(c *Context) bool`：**必须由处理器自主实现**。底层不干预业务，由处理器自己决定是否处理当前消息或事件；
     - `Process(c *Context) error`：**具体处理逻辑**。当 `Match` 返回 `true` 时执行计算与流转。
3. **性能黄金法则 (Zero-Allocation & Sub-Nanosecond Bypass)**：
   - 热路径严禁高频创建临时对象，必须使用 `core.AcquireMessage()` 与 `core.ReleaseMessage()`；
   - 未配置管道的主题原子穿透仅需 **1.09ns**，不可破坏快速路径（Fast-Path）。

---

## ⚡ 极速行动手册 (Actionable Recipes)

### 配方 1：3 行代码启动独立 Broker 并订阅全量数据

```go
package main

import (
	"context"
	"log"
	"time"

	"mqtt/core"
)

func main() {
	// 1. 初始化纯净的 Broker 实例 (支持 Memory / Pebble / BadgerDB)
	b, err := core.NewBroker(
		core.WithTCP(":1883"),
		core.WithMemoryStore(),
		core.WithMulticore(true),
	)
	if err != nil {
		log.Fatalf("初始化失败: %v", err)
	}

	// 2. 进程内极速订阅全量数据 (支持 # / + 通配符，零网络损耗)
	unsub, _ := b.Subscribe("#", 0, func(topic string, payload []byte) {
		log.Printf("[全量监控] 主题: %s, 内容: %s", topic, string(payload))
	})
	defer unsub()

	// 3. 启动监听
	go func() { _ = b.Start() }()
	time.Sleep(100 * time.Millisecond)

	// 4. 进程内极速推流
	_ = b.Publish("devices/sensor1", []byte(`{"temp":25.4}`), 0, false)

	time.Sleep(1 * time.Second)
	_ = b.Stop(context.Background())
}
```

---

### 配方 2：编写并挂载自洽处理器 (`Processor`)

编写任何自定义业务处理时，**必须且只需** 实现 `core.Processor` 接口（`Match` + `Process`）：

```go
package main

import (
	"strings"
	"mqtt/core"
)

// 自定义业务结构体
type DeviceAlarmProcessor struct {
	MaxAllowedTemp float64
}

// 1. Match: 处理器自己最清楚何时处理 (例如只管 alarms/ 且非内部消息)
func (p *DeviceAlarmProcessor) Match(c *core.Context) bool {
	return c.Message != nil &&
		c.Message.ClientID != "$internal" &&
		strings.HasPrefix(c.Message.Topic, "alarms/")
}

// 2. Process: 具体处理业务
func (p *DeviceAlarmProcessor) Process(c *core.Context) error {
	// 处理消息，可读取/修改 Payload
	// 如果需要中断/丢弃，调用 c.Drop("reason")
	// 如果需要跨处理器共享数据，调用 c.Set("key", val)
	return nil
}
```

**挂载到管道**：
```go
pipe := core.NewPipe()

// 统一通过 Add(name, Processor) 注册
pipe.Add("alarm_filter", &DeviceAlarmProcessor{MaxAllowedTemp: 80.0})

// 也支持快捷闭包处理器 (Match, Process)
pipe.AddFunc("quick_tagger",
	func(c *core.Context) bool { return true },
	func(c *core.Context) error {
		c.Set("tagged", true)
		return nil
	},
)

// 绑定到主题模式
b, _ := core.NewBroker(
	core.WithTCP(":1883"),
	core.WithPipe("alarms/#", pipe),
)
```

---

### 配方 3：开箱即用的工业级中间件装配

`core` 内置了高频通用业务处理器，直接实例化挂载：

```go
pipe := core.NewPipe()

// 1. 认证拦截 (AuthProcessor): 自动跳过 $internal，校验外部客户端
pipe.Add("auth_guard", core.NewAuthProcessor(func(clientID, username string) bool {
	return strings.HasPrefix(clientID, "edge_") || username == "operator"
}))

// 2. JSON 格式合规校验 (ValidateProcessor)
pipe.Add("json_validator", core.NewValidateProcessor(
	core.NewJSONValidator("syntax_check"),
	core.WithValidateTopic("telemetry/#"),
))

// 3. 二进制魔数与 TLV 帧结构校验 (BinaryValidator)
pipe.Add("can_validator", core.NewValidateProcessor(
	core.NewBinaryValidator("can_check", 8, 1024, core.WithMagic([]byte{0x56, 0x32, 0x58, 0x01})),
	core.WithValidateTopic("v2x/+/can"),
))

// 4. 数据增强清洗 (TransformProcessor)
pipe.Add("enricher", core.NewTransformProcessor(func(msg *core.Message) error {
	msg.SetPayload([]byte(`{"raw":` + string(msg.Payload) + `,"enriched":true}`))
	return nil
}))

// 5. 外部 Kafka 异步解耦投递 (ForwardProcessor)
pipe.Add("kafka_sink", core.NewForwardProcessor(func(msg *core.Message) error {
	return myKafkaProducer.Send(msg.Topic, msg.Payload)
}, true)) // passthrough=true 允许本地同时分发
```

---

### 配方 4：提取与输出实时性能与健康度报表

底层的 `Pipe` 会自动在纳秒级探针中测量各处理器的调用量、跳过数、耗时等：

```go
// 获取结构化切片快照 (供 REST API 或 Prometheus 导出)
statsList := pipe.Stats()

// 直接输出 ASCII 格式化监控报表 (控制台调试神器)
fmt.Println(pipe.PrintStats())
```

**输出格式示例**：
```text
================================= 管道各处理器实时性能与健康度 =================================
处理器名称              | 调度总数       | 匹配执行       | 预判跳过       | 拦截丢弃       | 平均耗时       | 最大耗时      
-----------------------------------------------------------------------------------------------
auth_guard         | 50000      | 50000      | 0          | 0          | 11ns       | 576.6µs   
json_validator     | 47500      | 45000      | 2500       | 500        | 625ns      | 736.6µs   
kafka_sink         | 44500      | 44500      | 0          | 0          | 61ns       | 530.0µs   
===============================================================================================
```

---

## 🚫 典型禁忌与反模式 (Anti-Patterns to Avoid)

| 错误做法 (Anti-Pattern) | 正确做法 (Best Practice) | 为什么 (Rationale) |
| :--- | :--- | :--- |
| ❌ 在 `Match()` 中执行沉重的网络 I/O 或复杂正则 | ✅ `Match()` 仅作轻量级前置判断（前缀/位运算/布尔） | `Match()` 属于第一级门禁，耗时必须控制在 10ns 级别。 |
| ❌ 在热路径中频繁 `new(core.Message)` | ✅ 使用 `core.AcquireMessage()` 与 `ReleaseMessage()` | 避免在百万 QPS 下引发频繁的 Go 垃圾回收 (GC STW)。 |
| ❌ 在 `Process()` 中抛出 panic | ✅ 遇到错误返回 `error` 或调用 `c.Abort(err)` / `c.Drop(reason)` | 管道内建优雅控制流，严禁破坏宿主进程稳定性。 |
| ❌ 在自定义库中反向 `import "mqtt/dashboard"` | ✅ 仅引入 `import "mqtt/core"` | 保证微服务依赖的绝对轻量纯净。 |

---

## 📂 进阶模块参考导航

更深度的规范与参考已模块化归档，AI 与开发者可根据需要进一步查阅：

- [API 完整清单与 Options 配置项](references/api_reference.md)
- [典型业务场景处理器设计范式](references/processor_patterns.md)
- [AirMQ 核心算法与数据结构深度解析 (ART/Slab/Lock-Free)](../../docs/algorithms_and_data_structures.md)
- [端到端物联网性能评测源码](../../examples/05_iot_scenarios_bench/main.go)
