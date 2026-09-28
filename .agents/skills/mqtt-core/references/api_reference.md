# AirMQ Core 完整 API 规范与配置速查 (API Reference)

本文档是 `AirMQ (mqtt/core)` 的全量导出接口、结构体与配置项技术规格手册。无论是 AI 编程助手生成嵌入式代码，还是架构师与研发人员定制开发，均可据此快速查阅。

---

## 目录

1. [Broker 核心与选项 (Broker & Options)](#1-broker-核心与选项-broker--options)
2. [进程内通信 (In-Process Pub/Sub)](#2-进程内通信-in-process-pubsub)
3. [管道编排与运行上下文 (Pipe & Context)](#3-管道编排与运行上下文-pipe--context)
4. [消息信封与内存池 (Message & Pool)](#4-消息信封与内存池-message--pool)
5. [开箱即用处理器与校验器 (Built-in Processors & Validators)](#5-开箱即用处理器与校验器-built-in-processors--validators)

---

## 1. Broker 核心与选项 (Broker & Options)

### 构造与生命周期

```go
func NewBroker(opts ...Option) (*Broker, error)
func (b *Broker) Start() error
func (b *Broker) Stop(ctx context.Context) error
func (b *Broker) Server() *server.Server
```

- `Start()`: 阻塞启动已配置的所有传输层监听器（TCP、TLS、WS、QUIC）及集群 Mesh 引擎。
- `Stop(ctx)`: 优雅关闭连接池、集群广播下线、持久化存储落盘与释放网络端口。

### 配置选项列表 (`Option`)

| 配置函数 | 入参类型 | 默认值 | 详细说明 |
| :--- | :--- | :--- | :--- |
| `WithTCP(addr)` | `string` | `":1883"` | 绑定 MQTT 监听地址，支持带协议格式（如 `tcp://0.0.0.0:1883`）。 |
| `WithMulticore(multicore)` | `bool` | `true` | 是否启用 gnet 多核事件循环（Event-Loop）提高并发吞吐。 |
| `WithReusePort(reusePort)` | `bool` | `true` | 是否开启系统底层套接字 `SO_REUSEPORT` 特性。 |
| `WithMemoryStore()` | - | 默认启用 | 内存级会话与 Retain 存储，纳秒级读写，重启不保留。 |
| `WithPebbleStore(dataDir)` | `string` | - | 挂载基于 CockroachDB Pebble LSM-Tree 的工业级持久化存储引擎。 |
| `WithBadgerStore(dataDir)` | `string` | - | 挂载 BadgerDB Key-Value 高性能嵌入式持久化存储。 |
| `WithStore(s)` | `store.MessageStore`| - | 注入开发者自定义实现的底层存储引擎。 |
| `WithPipe(topicFilter, pipe)` | `string, *Pipe` | - | 为指定主题通配符绑定一个预组装的 `Pipe` 管道。 |
| `WithProcessor(topic, name, proc)` | `string, string, Processor` | - | 快速为指定主题绑定单个命名处理器。 |
| `WithWebSocket(addr, path)` | `string, string` | - | 开启 MQTT over WebSocket 监听（如 `":8083"`, `"/mqtt"`）。 |
| `WithTLS(cfg)` | `server.TLSConfig` | - | 开启 TLS 监听，支持配置证书、私钥以及 mTLS 双向认证。 |
| `WithQUIC(cfg)` | `server.QUICConfig`| - | 开启基于 UDP 的 MQTT over QUIC 传输协议监听。 |
| `WithRateLimit(cRate, cBst, pRate, pBst)` | `float64...` | - | 细粒度令牌桶限流：连接建连速率/突发与发布 QPS/突发。 |
| `WithMetrics(addr)` | `string` | - | 启用专用的 Prometheus HTTP 监控指标拉取端点（如 `":8080"`）。 |
| `WithCluster(nodeID, addr, seeds)` | `string, string, []string` | - | 启动去中心化 Gossip 分布式集群网格。 |
| `WithClusterPeers(peers)` | `[]string` | - | 指定静态集群节点（形如 `["node-2=192.168.1.12:19992"]`）。 |
| `WithHook(h)` | `Hook` | - | 挂载传统 MQTT 事件生命周期 Hook（连接、断开、鉴权等）。 |

---

## 2. 进程内通信 (In-Process Pub/Sub)

无需经过 TCP/IP 协议栈序列化与反序列化，可在宿主进程内以极低开销分发与消费数据：

```go
// 发布消息到 Broker，自动触发主题路由、规则管道、本地订阅者及集群广播
func (b *Broker) Publish(topic string, payload []byte, qos byte, retain bool) error

// 进程内订阅指定主题（支持 MQTT 通配符 '+' 和 '#'）
// 返回一个取消订阅的闭包函数
func (b *Broker) Subscribe(filter string, qos byte, handler MessageHandler) (func(), error)

// 进程内订阅并显式获取唯一订阅 ID
func (b *Broker) SubscribeInternal(filter string, handler func(topic string, payload []byte)) (string, func(), error)

// 根据订阅 ID 注销内部监听
func (b *Broker) UnsubscribeInternal(id string) error
```

---

## 3. 管道编排与运行上下文 (Pipe & Context)

### 核心接口契约：`Processor`

每个处理器严格具备以下两个方法，由处理器自己决定是否处理，实现自治与解耦：

```go
type Processor interface {
    // Match 决定当前处理器是否应该处理该上下文。
    // 返回 true 进入 Process，返回 false 零开销跳过。
    Match(c *Context) bool

    // Process 执行具体业务处理逻辑。
    // 处理成功返回 nil；中断或拦截调用 c.Drop() 或 c.Abort()。
    Process(c *Context) error
}
```

### 管道管理器：`Pipe`

```go
// 创建一个全新管道
func NewPipe() *Pipe

// 链式追加命名处理器 (推荐明确命名以利于监控)
func (p *Pipe) Add(name string, proc Processor) *Pipe

// 链式追加闭包形式的处理器
func (p *Pipe) AddFunc(name string, matchFn func(c *Context) bool, processFn func(c *Context) error) *Pipe

// 获取所有处理器的实时指标快照（无锁/低开销）
func (p *Pipe) Stats() []ProcessorStats

// 输出对齐格式化的 ASCII 监控报表
func (p *Pipe) PrintStats() string

// 配置延迟探针采样率（1 为 100% 全量采集；16 为 1/16 采样降低时钟系统调用）
func (p *Pipe) SetSampleRate(rate int) *Pipe

// 获取已注册处理器数量
func (p *Pipe) Len() int

// 获取所有处理器的名称切片
func (p *Pipe) ProcessorNames() []string

// 清空所有处理器
func (p *Pipe) Clear() *Pipe

// 复制一份带有独立统计指标的管道副本
func (p *Pipe) Clone() *Pipe
```

### 运行上下文：`Context`

`Context` 贯穿整个处理管道，内嵌标准 `context.Context`，并提供流转控制：

```go
type Context struct {
    context.Context
    Message *Message // 当前流转的消息信封
}

// 业务主动过滤/丢弃消息，不再向下游处理器或本地客户端分发（不产生 Fatal 错误）
func (c *Context) Drop(reason string)
func (c *Context) IsDropped() bool
func (c *Context) DropReason() string

// 致命异常中断管道执行，后续处理器不再执行
func (c *Context) Abort(err error)
func (c *Context) IsAborted() bool
func (c *Context) Error() error

// 跨处理器传递元数据（基于轻量上下文 Map）
func (c *Context) Set(key string, val any)
func (c *Context) Get(key string) (any, bool)
```

---

## 4. 消息信封与内存池 (Message & Pool)

### 数据结构 `Message`

```go
type Message struct {
    ID        string            // 消息全局唯一标识
    Topic     string            // MQTT 发布主题
    Payload   []byte            // 消息体原始切片
    QoS       byte              // 服务质量等级 (0, 1, 2)
    Retain    bool              // 是否为保留消息
    Timestamp time.Time         // 进入管道的时间戳
    ClientID  string            // 发布者客户端 ID（内部事件为 "$internal"）
    Username  string            // 发布者用户名
    Headers   map[string]string // 协议元数据
    Attrs     map[string]any    // 处理器之间共享的自定义属性
}
```

### 零分配内存池 (Zero-Allocation Pool)

在百万级 QPS 处理链路中，**严禁使用 `new(Message)`**，必须通过对象池获取与回收：

```go
// 从复用池获取干净的 Message 实例
msg := core.AcquireMessage()
defer core.ReleaseMessage(msg)

// 修改或更新 Payload
msg.SetPayload(newBytes)
msg.SetTopic("new/topic")
msg.SetHeader("X-Trace-ID", "trace-12345")
msg.SetAttr("parsed_data", myStruct)
```

---

## 5. 开箱即用处理器与校验器 (Built-in Processors & Validators)

### 常用内置处理器

1. **`AuthProcessor`（认证拦截）**：
   ```go
   core.NewAuthProcessor(
       allowFn func(clientID, username string) bool,
       opts ...AuthOption,
   )
   // Options:
   // core.WithAuthTopic("secure/#")
   // core.WithAuthMatch(customMatchFn)
   ```
2. **`ValidateProcessor`（格式校验）**：
   ```go
   core.NewValidateProcessor(
       v Validator,
       opts ...ValidateOption,
   )
   // Options:
   // core.WithValidateTopic("telemetry/#")
   // core.WithValidateOnFail(func(c *Context, err error))
   // core.WithValidateMatch(customMatchFn)
   ```
3. **`ForwardProcessor`（数据桥接与转发）**：
   ```go
   core.NewForwardProcessor(
       sinkFn func(msg *Message) error,
       passthrough bool, // true 表示同时允许消息继续投递给本地订阅者
       opts ...ForwardOption,
   )
   // Options:
   // core.WithForwardTopic("export/#")
   // core.WithForwardMatch(customMatchFn)
   ```
4. **`TransformProcessor`（消息修改与清洗）**：
   ```go
   core.NewTransformProcessor(
       transformFn func(msg *Message) error,
       opts ...TransformOption,
   )
   ```
5. **`FilterProcessor`（谓词过滤）**：
   ```go
   core.NewFilterProcessor(func(msg *Message) bool {
       return len(msg.Payload) > 0
   })
   ```

### 校验器 (`Validator`)

| 构造器 | 适用格式 | 特性与配置项 |
| :--- | :--- | :--- |
| `NewJSONValidator(name)` | `json` | 极速 JSON 语法合规性检查（0 堆内存分配校验）。 |
| `NewBinaryValidator(name, min, max, opts...)` | `raw` / `tlv` / `protobuf` | 校验最小/最大字节数。<br>• `WithMagic([]byte)`: 头部魔数匹配；<br>• `WithTLVValidation()`: TLV 帧长一致性校验；<br>• `WithProtobufFormat()`: 标记为 Protobuf。 |
| `NewRawValidator(name, min, max)` | `raw` | 极简字节长度边界守卫。 |
