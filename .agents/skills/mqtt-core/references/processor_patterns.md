# 物联网高频业务处理器设计范式 (Processor Design Patterns)

在真实工业物联网（IIoT）、车联网（V2X）与智能硬件场景中，核心流水线（`Pipe`）承担着从消息接入、协议校验、边缘计算到多路分发的重要职责。

本文档汇总了基于 `core.Processor` 接口（`Match(c)` + `Process(c)`）的 **5 大工业级经典设计范式**，提供开箱即用的代码蓝本与性能优化建议。

---

## 目录

1. [范式 1：动态设备认证与多租户权限隔离](#1-范式-1动态设备认证与多租户权限隔离)
2. [范式 2：车联网二进制协议校验与 TLV 解析](#2-范式-2车联网二进制协议校验与-tlv-解析)
3. [范式 3：边缘数据增强与协议规范化 (Normalization)](#3-范式-3边缘数据增强与协议规范化-normalization)
4. [范式 4：双通道投递与外部 Kafka 异步桥接](#4-范式-4双通道投递与外部-kafka-异步桥接)
5. [范式 5：滑动窗口告警防抖与频控 (Deduplication & Debounce)](#5-范式-5滑动窗口告警防抖与频控-deduplication--debounce)

---

## 1. 范式 1：动态设备认证与多租户权限隔离

### 业务背景
在多租户物联网平台中，设备通过形如 `tenant-A/device-01/telemetry` 的主题上报数据。需校验：
1. 客户端是否具备该租户命名空间的访问权限；
2. 内部系统分发消息（`$internal`）必须自动放行，不得被外部鉴权规则拦截。

### 实现代码

```go
package main

import (
	"strings"
	"sync"

	"mqtt/core"
)

type MultiTenantAuthProcessor struct {
	mu           sync.RWMutex
	tenantTokens map[string]string // 租户对应的合法签名或密钥
}

func NewMultiTenantAuthProcessor() *MultiTenantAuthProcessor {
	return &MultiTenantAuthProcessor{
		tenantTokens: map[string]string{
			"factory-shanghai": "token-secret-001",
			"factory-beijing":  "token-secret-002",
		},
	}
}

// 1. Match: 处理器自主决策是否关照当前消息
func (p *MultiTenantAuthProcessor) Match(c *core.Context) bool {
	// 忽略空消息与内部进程投递
	if c.Message == nil || c.Message.ClientID == "$internal" {
		return false
	}
	// 仅对以 tenants/ 开头的业务消息进行拦截鉴权
	return strings.HasPrefix(c.Message.Topic, "tenants/")
}

// 2. Process: 执行租户级鉴权与绑定
func (p *MultiTenantAuthProcessor) Process(c *core.Context) error {
	// 主题结构: tenants/{tenant_id}/{device_id}/data
	parts := strings.Split(c.Message.Topic, "/")
	if len(parts) < 4 {
		c.Drop("invalid tenant topic structure")
		return nil
	}

	tenantID := parts[1]
	token, ok := c.Message.GetHeader("X-Tenant-Token")
	if !ok {
		token = c.Message.Username // 降级从 MQTT CONNECT 用户名提取
	}

	p.mu.RLock()
	expectedToken, exists := p.tenantTokens[tenantID]
	p.mu.RUnlock()

	if !exists || token != expectedToken {
		c.Drop("unauthorized tenant token")
		return core.ErrUnauthorized
	}

	// 将解析出的租户 ID 注入上下文，供后续流转的处理器直接读取
	c.Set("tenant_id", tenantID)
	return nil
}
```

---

## 2. 范式 2：车联网二进制协议校验与 TLV 解析

### 业务背景
新能源汽车车载终端（T-Box）采用紧凑的二进制帧上报状态（如国标 GB/T 32960 或企业私有协议）：
- 帧头魔数：`0x23, 0x23`（固定 2 字节）；
- 紧随 2 字节命令 ID、2 字节数据单元长度（TLV 结构）；
- 严禁在热路径反序列化整个大对象，仅需在入口校验帧结构合法性并拦截篡改数据。

### 实现代码

```go
package main

import (
	"encoding/binary"
	"errors"
	"strings"

	"mqtt/core"
)

var (
	ErrMalformedFrame = errors.New("malformed binary frame")
)

type V2XFrameValidator struct {
	expectedMagic uint16
}

func NewV2XFrameValidator() *V2XFrameValidator {
	return &V2XFrameValidator{
		expectedMagic: 0x2323, // 示例: ## 标识
	}
}

// 1. Match: 仅匹配车联网 CAN/T-Box 主题
func (v *V2XFrameValidator) Match(c *core.Context) bool {
	return c.Message != nil &&
		(strings.HasPrefix(c.Message.Topic, "v2x/") || strings.HasSuffix(c.Message.Topic, "/raw"))
}

// 2. Process: 极速零内存拷贝（0-Alloc）边界与魔数检查
func (v *V2XFrameValidator) Process(c *core.Context) error {
	payload := c.Message.Payload
	// 最小帧长：魔数(2B) + 命令字(2B) + 长度(2B) = 6B
	if len(payload) < 6 {
		c.Drop("frame too short")
		return ErrMalformedFrame
	}

	// 校验魔数
	magic := binary.BigEndian.Uint16(payload[0:2])
	if magic != v.expectedMagic {
		c.Drop("invalid magic header")
		return ErrMalformedFrame
	}

	// 校验数据单元长度字段是否与物理切片一致
	bodyLen := int(binary.BigEndian.Uint16(payload[4:6]))
	if len(payload)-6 < bodyLen {
		c.Drop("incomplete frame payload")
		return ErrMalformedFrame
	}

	// 标记校验通过，并记录命令字
	cmdID := binary.BigEndian.Uint16(payload[2:4])
	c.Set("v2x_cmd", cmdID)
	return nil
}
```

---

## 3. 范式 3：边缘数据增强与协议规范化 (Normalization)

### 业务背景
边缘网关或异构传感器上报的 Payload 字段往往缺少时间戳、网关路由标识。为了下游系统统一消费，需要在 Broker 层注入标准元数据（如采集时间、边缘节点 ID、服务端摄入时间）。

### 实现代码

```go
package main

import (
	"fmt"
	"strings"
	"time"

	"mqtt/core"
)

type MetadataEnricher struct {
	EdgeNodeID string
}

func (p *MetadataEnricher) Match(c *core.Context) bool {
	// 仅对遥测数据执行增强
	return c.Message != nil && strings.HasPrefix(c.Message.Topic, "telemetry/")
}

func (p *MetadataEnricher) Process(c *core.Context) error {
	msg := c.Message

	// 1. 注入协议头元数据 (Headers 零切片分配)
	msg.SetHeader("X-Ingest-Time", fmt.Sprintf("%d", time.Now().UnixMilli()))
	msg.SetHeader("X-Node-ID", p.EdgeNodeID)

	// 2. 若 Payload 为 JSON，可包装或附加外层信封
	// 在高频场景下，建议使用高效字符串拼接或 sonic/simdjson 库
	raw := msg.Payload
	if len(raw) > 0 && raw[0] == '{' && raw[len(raw)-1] == '}' {
		// 移除收尾的大括号，包裹平台标准字段
		enriched := fmt.Sprintf(`{"_node":"%s","_ts":%d,"data":%s}`,
			p.EdgeNodeID,
			time.Now().UnixMilli(),
			string(raw),
		)
		msg.SetPayload([]byte(enriched))
	}

	return nil
}
```

---

## 4. 范式 4：双通道投递与外部 Kafka 异步桥接

### 业务背景
物联网平台既需要将设备状态下发给前端 WebSockets/本地移动端做实时控制（本地 Broker 分发），又需要将所有数据全量同步至外部 Kafka 或 ClickHouse 用于大数据分析。

### 实现代码

```go
package main

import (
	"log"
	"strings"

	"mqtt/core"
)

type KafkaBridgeProducer interface {
	Send(topic string, key string, value []byte) error
}

type KafkaBridgeProcessor struct {
	producer    KafkaBridgeProducer
	passthrough bool
}

func NewKafkaBridgeProcessor(producer KafkaBridgeProducer, passthrough bool) *KafkaBridgeProcessor {
	return &KafkaBridgeProcessor{
		producer:    producer,
		passthrough: passthrough,
	}
}

func (p *KafkaBridgeProcessor) Match(c *core.Context) bool {
	// 过滤系统或心跳主题，只转发核心业务数据
	return c.Message != nil && !strings.HasSuffix(c.Message.Topic, "/ping")
}

func (p *KafkaBridgeProcessor) Process(c *core.Context) error {
	msg := c.Message

	// 异步或极速缓冲推送到 Kafka
	key := msg.ClientID
	if key == "" {
		key = msg.Topic
	}

	if err := p.producer.Send(msg.Topic, key, msg.Payload); err != nil {
		log.Printf("[KafkaBridge] 推送失败 topic=%s: %v", msg.Topic, err)
		// 如需强一致性中断可调用 c.Abort(err)；如需弱依赖降级可只记录日志
	}

	// 若非 passthrough，则在此拦截，阻止消息继续被本地 MQTT 客户端订阅消费（纯出站网关）
	if !p.passthrough {
		c.Drop("egress only to kafka")
	}

	return nil
}
```

---

## 5. 范式 5：滑动窗口告警防抖与频控 (Deduplication & Debounce)

### 业务背景
工业现场传感器由于电压波动或临界阈值震荡，可能会在 1 秒内连续触发数百次告警（告警风暴）。流水线处理器必须具备本地微秒级滑动窗口防抖能力，丢弃高频抖动消息。

### 实现代码

```go
package main

import (
	"sync"
	"time"

	"mqtt/core"
)

type AlarmDebounceProcessor struct {
	mu           sync.Mutex
	window       time.Duration
	lastAlarmMap map[string]time.Time
}

func NewAlarmDebounceProcessor(window time.Duration) *AlarmDebounceProcessor {
	return &AlarmDebounceProcessor{
		window:       window,
		lastAlarmMap: make(map[string]time.Time),
	}
}

// 1. Match: 仅关注报警主题
func (p *AlarmDebounceProcessor) Match(c *core.Context) bool {
	return c.Message != nil && strings.HasPrefix(c.Message.Topic, "alarms/")
}

// 2. Process: 检查是否处于防抖抑制窗口
func (p *AlarmDebounceProcessor) Process(c *core.Context) error {
	alarmKey := c.Message.Topic // 或结合 clientID + 报警代码计算特征键

	p.mu.Lock()
	lastTime, exists := p.lastAlarmMap[alarmKey]
	now := time.Now()

	if exists && now.Sub(lastTime) < p.window {
		p.mu.Unlock()
		// 在防抖窗口内，静默丢弃
		c.Drop("alarm debounced (storm suppression)")
		return nil
	}

	p.lastAlarmMap[alarmKey] = now
	p.mu.Unlock()

	return nil
}
```

---

## 6. 范式 6：云边双向指令下发与回执闭环 (Bidirectional Command & ACK Closed-Loop)

### 业务背景
物联网应用不仅有设备上报遥测，更有云端对设备的反向控制（下发指令）。
下发通常面临两大核心需求：
1. **下发前置防御**：下发指令必须经过严格的参数边界校验（如限制变频器最大工作频率、机械臂最大角速度），杜绝非法参数下发损坏设备硬件；
2. **异步回执闭环**：设备接收并执行动作后，异步上报 ACK 报文（携带相同 `req_id`）。服务端管道需即时捕获回执，完成事务状态标记并测量往返时延（RTT）。

### 实现代码

```go
package main

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"mqtt/core"
)

var (
	ErrParamOutOfBounds = errors.New("command parameter out of safety bounds")
)

// 1. 下发控制指令防御校验处理器
type DownlinkSafetyGuard struct {
	MaxAllowedHz float64
}

func (p *DownlinkSafetyGuard) Match(c *core.Context) bool {
	// 仅拦截下行控制主题: devices/{device_id}/cmd
	return c.Message != nil && strings.HasPrefix(c.Message.Topic, "devices/") && strings.HasSuffix(c.Message.Topic, "/cmd")
}

func (p *DownlinkSafetyGuard) Process(c *core.Context) error {
	var cmd struct {
		ReqID  string  `json:"req_id"`
		Action string  `json:"action"`
		Hz     float64 `json:"hz"`
	}
	if err := json.Unmarshal(c.Message.Payload, &cmd); err != nil {
		c.Drop("malformed json command")
		return nil
	}

	// 物理边界防御
	if cmd.Hz > p.MaxAllowedHz {
		c.Drop("command rejected: frequency exceeds physical safety limit")
		return ErrParamOutOfBounds
	}

	// 在上下文中标记下发时间戳
	c.Set("dispatch_ts", time.Now().UnixNano())
	return nil
}

// 2. 上行回执异步关联处理器
type UplinkAckTracker struct {
	mu           sync.Mutex
	pendingTasks map[string]chan string // req_id -> 回执结果通知通道
}

func NewUplinkAckTracker() *UplinkAckTracker {
	return &UplinkAckTracker{
		pendingTasks: make(map[string]chan string),
	}
}

func (t *UplinkAckTracker) RegisterWait(reqID string) chan string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan string, 1)
	t.pendingTasks[reqID] = ch
	return ch
}

func (t *UplinkAckTracker) Match(c *core.Context) bool {
	// 仅拦截上行回执主题: devices/{device_id}/ack
	return c.Message != nil && strings.HasPrefix(c.Message.Topic, "devices/") && strings.HasSuffix(c.Message.Topic, "/ack")
}

func (t *UplinkAckTracker) Process(c *core.Context) error {
	var ack struct {
		ReqID  string `json:"req_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(c.Message.Payload, &ack); err != nil {
		return nil
	}

	t.mu.Lock()
	ch, exists := t.pendingTasks[ack.ReqID]
	if exists {
		delete(t.pendingTasks, ack.ReqID)
	}
	t.mu.Unlock()

	if exists {
		select {
		case ch <- ack.Status:
		default:
		}
	}
	return nil
}
```

---

## 💡 最佳实战建议

1. **先轻后重**：在 `pipe.Add` 编排中，务必将过滤最快、拦截率最高的处理器放在最前端（例如：`AuthProcessor` -> `ValidateProcessor` -> `DebounceProcessor` -> `TransformProcessor` -> `ForwardProcessor`）。
2. **零拷贝共享**：如果多个下游处理器需要使用解析后的中间结构（如已解析的 JSON 结构体），应通过 `c.Set("parsed_data", obj)` 共享，避免下游重复序列化解析。
3. **安全使用 Pool**：在自定义处理器内部如果衍生了新的异步 goroutine 处理逻辑，如需跨 goroutine 持有消息体，必须调用 `bytes.Clone(c.Message.Payload)` 制作拷贝，防止原消息在主链路中被 `ReleaseMessage` 回收后发生数据竞争。
4. **双向指令隔离**：强烈建议将上行遥测主题（如 `telemetry/#`）与下行控制主题（如 `devices/+/cmd`）绑定独立的 `Pipe` 管道实例，保持上报通道与控制通道的物理隔离，避免上行高频遥测争抢下行实时控制指令的调度窗口。
