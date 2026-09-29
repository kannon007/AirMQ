# AirMQ 上游问题反馈报告

- **日期**：2026-09-29
- **被测版本**：`github.com/kannon007/AirMQ@v0.0.0-20260928073258-511e1e6586ae`（main 分支伪版本）
- **反馈来源**：IoT 平台接入层集成 + 真实 MQTT 双客户端 E2E 测试
- **问题总数**：4（2 安全 / 1 可靠性 / 1 性能）

## 问题总览

| # | 严重度 | 类型 | 一句话描述 |
|---|--------|------|-----------|
| 1 | 🔴 高 | 安全 | `FireAuthorize`/`OnAuthorize` 是死代码——全仓库无任何调用点，订阅授权钩子从未生效 |
| 2 | 🔴 高 | 安全 | `handleSubscribe` 保留消息（Retained）回放直接投递，绕过 `OnPublish` 钩子管道 |
| 3 | 🟡 中 | 可靠性 | `WithPebbleStore` 静默吞掉初始化错误，失败时无提示降级为内存存储 |
| 4 | 🟢 低 | 性能 | `hook.Manager.Register` 无条件置 `hasPublish=true`，仅关心连接的钩子也迫使所有发布走钩子分发 |

---

## 问题 1：OnAuthorize 授权钩子从未被调用（死代码）

**严重度**：高（安全）

**证据**（全仓库非测试代码搜索 `FireAuthorize` / `.OnAuthorize(`，仅命中定义自身）：

```
pkg/hook/manager.go:57  // FireAuthorize runs all OnAuthorize hooks.
pkg/hook/manager.go:58  func (m *Manager) FireAuthorize(...) (bool, error) {
pkg/hook/manager.go:63      allow, err := h.OnAuthorize(ctx, action, topic)
```

`pkg/server/server.go` 中钩子调用点仅有：

```
server.go:581  drop, err := s.hookMgr.FirePublish(hookCtx, p)   // 唯一的 FirePublish 调用
```

**无任何 `FireAuthorize` 调用点。**

**影响**：
- 文档（`pkg/hook/hook.go` 的 `Hook` 接口注释）宣称 `OnAuthorize` 用于 Publish/Subscribe 授权，但实际从未生效
- 嵌入式使用者按文档实现 `OnAuthorize` 做 Topic ACL 会得到**虚假的安全感**——ACL 完全不执行
- 订阅（SUBSCRIBE）授权完全缺失：任意已认证客户端可订阅任意主题过滤器并获得 SUBACK

**复现**（我们已实测确认）：
1. 设备 A（`t1:pk:A`）与设备 B（`t1:pk:B`）分别通过认证连接
2. A 订阅 `t1/pk/B/#` → broker 授予订阅（SUBACK 成功）
3. B 发布自身遥测 → **A 收到了 B 的消息**（我们 E2E 输出：`CONFIRMED LEAK: cross-device subscriber received "{\"temperature\":22}"`）

**建议修复**：
1. 在 `handleSubscribe` 中、写订阅表之前调用 `hookMgr.FireAuthorize(ctx, AuthActionSubscribe, sub.Topic)`，返回 false 时按 MQTT 5.0 规则返回 SUBACK 0x87（Not authorized）或 MQTT 3.1.1 直接拒绝该 filter
2. 在 `server.go:581` 的发布路径同样调用 `FireAuthorize(ctx, AuthActionPublish, p.Topic)`（与现有 FirePublish 并存或合并）
3. 在 `Manager.Register` 中区分钩子能力（反射检查是否覆写了 `OnAuthorize`），避免无效分发

**我们的临时规避**：把 ACL 实现在 `OnPublish`（真实调用路径）：每次发布前枚举 `Broker.GetSubscriptions()`，对未授权订阅调用 `UnsubscribeClient()` 摘除——但这只能在**消息到达后补救**，无法阻止 SUBACK 授予。

---

## 问题 2：Retained 消息回放绕过全部钩子

**严重度**：高（安全，与问题 1 叠加形成完整泄露链）

**证据**：`pkg/server/server.go:814-889` `handleSubscribe`：

```go
// server.go:816
allRetained, _ := s.store.GetAllRetained()
// server.go:867-889
if shouldSendRetained && len(allRetained) > 0 {
    for _, rMsg := range allRetained {
        if trie.TopicFilterMatches(sub.Topic, rMsg.Topic) {
            outPub := &protocol.PublishPacket{ ... Retain: true, ... }
            retainedToSend = append(retainedToSend, outPub)  // 直接投递
        }
    }
}
```

该路径构造 `outPub` 后直接发送，**不经过 `FirePublish`（server.go:581 才是唯一的发布钩子调用点，且不在订阅路径）**，也不经过任何授权检查。

**影响**：即使使用者在 `OnPublish` 中实现了 ACL（问题 1 的规避方案），订阅方的保留消息回放仍会绕过它——我们在修复问题 1 的泄露后，再次实测确认：`LEAK: A received retained B topic t/p/b/telemetry`。

**复现**：
1. 设备 B 以 `retain=true` 发布一条消息
2. 设备 A 订阅 `t/p/B/#`（即使 `OnPublish` 有 ACL）
3. SUBACK 后保留消息直接送达 A

**建议修复**：`handleSubscribe` 回放保留消息前，对 `(订阅客户端, rMsg.Topic)` 调用 `FireAuthorize`（问题 1 修复后）或至少 `FirePublish`，未授权则跳过该条。

**我们的临时规避**：在 `OnPublish` 中对外部客户端（非空且非 `$internal` ClientID）强制 `pkt.Retain = false`，从源头禁止设备产生保留消息。代价：平台设备失去 broker 级 retain 能力（改由应用层影子承担）。

---

## 问题 3：`WithPebbleStore` 静默吞错误并降级内存存储

**严重度**：中（可靠性/数据丢失风险）

**证据**：`core/options.go`：

```go
func WithPebbleStore(dataDir string) Option {
    return func(c *brokerConfig) {
        ps, err := store.NewPebbleStore(dataDir)
        if err == nil {
            c.store = ps
        }
        // err != nil 时：既不返回，也不记录 —— 静默保留默认 MemoryStore
    }
}
```

**影响**：`dataDir` 非法（如指向普通文件、无写权限）时，`NewBroker` 返回 `nil` error，服务正常启动，但 QoS1/2 消息、会话、保留消息全部不落盘且**无任何告警**。对宣称"工业级持久化"的场景是静默数据丢失。

**复现**：`core.NewBroker(core.WithPebbleStore(指向普通文件的路径))` → 无错误，实际运行于内存存储。

**建议修复**：`Option` 无法返回错误，可改为：
1. `WithPebbleStore` 内部 `panic`（构造期失败显式暴露）——最简单；
2. 或把初始化错误存入 `brokerConfig.initErr`，`NewBroker` 末尾统一检查返回；
3. 或提供 `core.NewPebbleStore(dataDir) (*PebbleStore, error)` + `WithStore(s)`（当前签名已存在），文档引导显式构造。

**我们的规避**：适配器内显式调用 `store.NewPebbleStore` + `core.WithStore`，错误包装后从构造函数返回。

---

## 问题 4：`Register` 无条件置 `hasPublish`，拖慢纯连接钩子场景

**严重度**：低（性能）

**证据**：`pkg/hook/manager.go:36-41`：

```go
func (m *Manager) Register(h Hook) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.hooks = append(m.hooks, h)
    m.hasPublish.Store(true)   // 不论钩子是否覆写 OnPublish
}
```

**影响**：只关心 `OnConnect/OnDisconnect` 的钩子（如审计、在线统计）也会把 `hasPublish` 置真，导致**每条发布消息**都走一遍 `FirePublish` 的钩子遍历。在 56 万 msg/s 推流场景下是不必要的开销（`BaseHook.OnPublish` 默认返回 false 不丢弃，但遍历本身有成本）。

**建议修复**：注册时检测钩子是否覆写了 `OnPublish`（如 `reflect.TypeOf(h)` 与 `BaseHook` 的方法指针比较，或改 API 为 `RegisterPublish(h)` / `RegisterConnect(h)` 分类注册），仅当存在真正的发布钩子时置 `hasPublish`。

---

## 附：我们的集成环境

- Go 1.26（AirMQ 要求 ≥1.26.0）
- 嵌入式用法：`core.NewBroker(core.WithTCP, core.WithHook, core.WithStore)` + 进程内 `Subscribe("#")`
- 验证方式：`paho.mqtt.golang` 真实双客户端 E2E（正确/错误密钥 CONNACK、跨设备 SUBSCRIBE、retain 回放、生命周期事件）
- 以上 4 个问题的规避代码与回归测试见我们的适配层（如需细节可联系）

---

*本报告基于 main@511e1e6 实测与源码取证，若上游已修复请忽略对应条目。*
