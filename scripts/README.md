# MQTT Broker C1000K (百万并发连接) 调优与压测全指南

本指南系统性地解决了 MQTT Broker 冲击单机百万并发长连接（C1000K+）时的操作系统内核限制、内存膨胀、端口枯竭与压测方法。

---

## 一、 百万并发连接的“四大物理天花板”与攻克方案

### 1. 文件句柄上限 (File Descriptors)
- **物理现象**：Linux 默认单个进程最大打开文件数为 `1024`。当连接数达到 1024 时，Broker 将报 `Too many open files` 并拒绝后续连接。
- **攻克方案**：将 `nofile`（软硬限制）及系统级 `fs.nr_open` 提升至 **2,097,152**（200 万以上）。

### 2. 单源 IP 端口上限 (Ephemeral Port Exhaustion)
- **物理现象**：TCP 四元组为 `(Source IP, Source Port, Dest IP, Dest Port)`。由于 TCP 端口号为 16 位无符号整数，单个源 IP 最大可用端口为 65,535（实际扣除保留端口后可用约 60,000 个）。
- **攻克方案**：**单台压测机要发起 100 万长连接，必须绑定多个辅助本地 IP**（例如 20 个 IP，每个 IP 承载 50,000 个连接）。

### 3. TCP 内核缓冲区内存膨胀 (TCP Socket Buffer Memory)
- **物理现象**：Linux 默认 TCP 读缓冲区 (`tcp_rmem`) 和写缓冲区 (`tcp_wmem`) 默认分配约为 8KB ~ 16KB。若 100 万空闲连接均分配 16KB，内核仅 Socket 缓冲区就将吃掉 **16GB ~ 32GB** 内存，极易导致内核 OOM。
- **攻克方案**：将 `tcp_rmem` 和 `tcp_wmem` 的默认及最小值精细化下调至 **2048 字节**（2KB）。此时 100 万空闲长连接的 Socket 内存占用仅约 **2.5GB**。

### 4. 连接建立半连接/全连接队列溢出 (SYN & Accept Backlog)
- **物理现象**：以每秒 5,000 ~ 10,000 个连接的速度瞬时冲击时，Linux 默认的 `somaxconn` (128 或 1024) 会瞬间被打满，导致握手丢包与客户端重传。
- **攻克方案**：将 `net.core.somaxconn` 和 `net.ipv4.tcp_max_syn_backlog` 调大至 **65,535**。

---

## 二、 系统内核一键调优脚本

### 1. Linux 生产环境调优 (推荐)
在 Broker 运行机与压测机上分别执行：
```bash
sudo bash scripts/sys_tune.sh
```

**调优参数生效验证**：
```bash
ulimit -n             # 应输出 2097152
cat /proc/sys/fs/file-max
cat /proc/sys/net/core/somaxconn
```

### 2. Windows 开发环境调优
在管理员 PowerShell 中执行：
```powershell
powershell -ExecutionPolicy Bypass -File scripts/tune_windows.ps1
```

---

## 三、 工业级五大实战压测场景指南

本项目已随工程内置编译了超高性能的纯 Go 压测工具：`bench.exe`（在 Linux 下编译为 `bench`）。

支持通过 `-scenario` 参数自由切换以下 5 大核心场景：

| 场景编号 | 场景名称 | 模式标识 (`-scenario`) | 衡量核心指标 | 典型生产业务对齐 |
| :--- | :--- | :--- | :--- | :--- |
| **场景 1** | **C1000K 百万长连接保持** | `conn` | 握手速率、连接数、内存占用、心跳保活 | 大规模设备在线状态维护 |
| **场景 2** | **1-to-N 广播风暴** | `fanout` | 扇出投递吞吐 (msg/s)、端到端延迟 P99 | 紧急通知、全网 OTA 广播、行情分发 |
| **场景 3** | **N-to-N 海量设备遥测上报** | `telemetry` | 消息摄入 TPS、通配符多级聚合耗时 | 车联网、智能电表、传感器高频上报 |
| **场景 4** | **$share 共享订阅消费均衡** | `shared` | 组内成员分发均匀度、任务积压消化 | 后台流式计算、微服务消费者池 |
| **场景 5** | **客户端断线重连风暴** | `reconnect` | Thundering Herd 惊群吞吐、Session 清理效率 | 网络基站割接、区域停电后设备瞬间上线 |

---

### 场景 1：C1000K 百万长连接保活测试
```bash
./bench -broker="192.168.1.100:1883" \
        -scenario=conn \
        -conns=1000000 \
        -rate=10000 \
        -keepalive=60 \
        -local-ips="192.168.1.10,192.168.1.11,...,192.168.1.30"
```

### 场景 2：1-to-N 广播风暴 (Fan-Out 极速扇出与延迟分布)
启动 20,000 个订阅者订阅 `bench/fanout`，单发布者以 200 msg/s 频率广播，**实时测试 400 万 msg/s 的微秒级端到端投递延迟 (P50/P90/P99)**：
```powershell
.\bench.exe -broker="127.0.0.1:1883" `
            -scenario=fanout `
            -conns=20000 `
            -pub-rate=200 `
            -duration=30
```
**输出指标**：
```
[Fan-Out 实时] 投递吞吐: 4000000 msg/s | 延迟 P50: 1.2ms | P99: 4.8ms
=================== [Fan-Out 广播测试完成] ===================
总接收投递数: 120000000 | 端到端延迟 P50: 1.2ms | P90: 2.6ms | P99: 4.8ms | Max: 12.3ms
```

### 场景 3：N-to-N 海量设备遥测并发上报与通配符聚合
启动 5,000 台独立虚拟设备，各自上报私有主题 `devices/{id}/data`，后台消费者订阅 `devices/+/data` 进行聚合消费：
```powershell
.\bench.exe -broker="127.0.0.1:1883" `
            -scenario=telemetry `
            -conns=5000 `
            -pub-rate=10 `
            -duration=30
```

### 场景 4：$share 共享订阅负载均衡压测
启动 10 个工作节点加入 `$share/workers/jobs/+`，生产者瞬时投递 50,000 条任务消息，验证组内任务是否被严格平滑均摊：
```powershell
.\bench.exe -broker="127.0.0.1:1883" `
            -scenario=shared `
            -workers=10 `
            -conns=50000
```
**输出指标**：
```
Worker #1 处理任务数: 5000 (占比 10.00%)
Worker #2 处理任务数: 5000 (占比 10.00%)
...
Worker #10 处理任务数: 5000 (占比 10.00%)
```

### 场景 5：海量客户端频繁断线重连风暴 (Reconnect Storm)
模拟区域网络断开、基站重启场景，5,000 个客户端瞬时断开并重复发起 10 轮高频重连冲击，压测 Acceptor 队列与 Session 重建性能：
```powershell
.\bench.exe -broker="127.0.0.1:1883" `
            -scenario=reconnect `
            -conns=5000 `
            -cycles=10
```

### 场景 6：QoS 1 饱和推流防丢严格对账 (Loss Proof)
基于 PacketID 环形队列与滑动窗口严格反压，20 万条 QoS 1 报文逐条端到端严格核对：
```powershell
.\bench.exe -broker="127.0.0.1:1883" `
            -scenario=loss_proof `
            -conns=200000 `
            -inflight=100 `
            -qos=1
```

### 场景 7：200,000+ msg/s 极限高吞吐零丢包压测 (Speed 200K)
测试多通道并发线缆旁路推流能力，100 万条消息饱和冲击，实时校验丢失率与吞吐：
```powershell
.\bench.exe -broker="127.0.0.1:1883" `
            -scenario=speed200k `
            -conns=1000000 `
            -pubs=50 `
            -subs=50 `
            -batch=500 `
            -qos=0
```

---

## 四、 使用开源 `emqtt-bench` 工具压测

如果你希望与业界主流 MQTT 压测工具对齐，也可以直接使用 EMQX 开源的 `emqtt-bench`：

```bash
# 1. 启动 100 万订阅者连接 (需配置 local_ips)
./emqtt_bench sub -h 192.168.1.100 -p 1883 -c 1000000 -i 10 -t "test/%i" --ifaddr 192.168.1.10,192.168.1.11...

# 2. 启动发布者进行吞吐压测 (每秒发布 50,000 消息)
./emqtt_bench pub -h 192.168.1.100 -p 1883 -c 100 -I 2 -t "test/%i" -s 64
```
