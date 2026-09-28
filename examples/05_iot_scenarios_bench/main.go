package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mqtt/core"
)

// ==============================================================================
// 场景 1: 简单业务场景 —— 智能表计与水文环境遥测 (Smart Metering / Environmental)
// 业务驱动：
// - 10万+ 离散智能电表/水表高频上报，追求极致吞吐与极低算力开销；
// - 过滤非法扫描客户端；
// - 过滤电池耗尽/网络断续导致的 0 字节空包与无效心跳；
// - 合规数据直通 Fast-Path 交付本地实时监控流。
// ==============================================================================

type MeterAuthProcessor struct{}

func (p *MeterAuthProcessor) Match(c *core.Context) bool {
	// 自洽判断：只对非内部系统消息进行表计 ClientID 格式认证
	return c.Message != nil && c.Message.ClientID != "$internal"
}

func (p *MeterAuthProcessor) Process(c *core.Context) error {
	// 业务规则：智能表计 ClientID 必须以 "meter_" 或 "sensor_" 开头
	if !strings.HasPrefix(c.Message.ClientID, "meter_") && !strings.HasPrefix(c.Message.ClientID, "sensor_") {
		c.Drop("unauthorized client ID format")
		return core.ErrUnauthorized
	}
	return nil
}

type HeartbeatFilterProcessor struct{}

func (p *HeartbeatFilterProcessor) Match(c *core.Context) bool {
	return c.Message != nil
}

func (p *HeartbeatFilterProcessor) Process(c *core.Context) error {
	// 业务规则：过滤掉 0 字节空包或 "PING" 纯心跳，不向下分发
	if len(c.Message.Payload) == 0 || bytes.Equal(c.Message.Payload, []byte("PING")) {
		c.Drop("empty payload or redundant ping")
	}
	return nil
}

// ==============================================================================
// 场景 2: 一般业务场景 —— 工业 4.0 智能制造车间网关 (Industrial Factory Gateway)
// 业务驱动：
// - 车间数控冲压机、PLC 通过边缘网关聚合工艺参数上报；
// - 1. 严格网关权限校验；
// - 2. 报文必须是合法 JSON 且必须包含 machine_id 与 metrics，防止坏包污染时序数据库；
// - 3. 数据清洗增强：注入网关微秒级接收时间戳与工厂分区标签；
// - 4. 异步写入外部 Kafka 工厂数据总线，同时 passthrough=true 供本地 SCADA 界面监控。
// ==============================================================================

type FactoryGatewayAuthProcessor struct {
	allowedGateways map[string]bool
}

func (p *FactoryGatewayAuthProcessor) Match(c *core.Context) bool {
	return c.Message != nil && c.Message.ClientID != "$internal"
}

func (p *FactoryGatewayAuthProcessor) Process(c *core.Context) error {
	if !p.allowedGateways[c.Message.ClientID] {
		c.Drop("unauthorized factory edge gateway")
		return core.ErrUnauthorized
	}
	return nil
}

// ==============================================================================
// 场景 3: 复杂业务场景 —— 新能源车联网 (Connected Vehicle / V2X Telematics TSP)
// 业务驱动：
// 百万新能源车在线，行驶中同时产生 3 类异构高频数据流：
// 1. CAN 动力总成总线流 (80% 流量): 二进制 TLV 格式，校验国标魔数 0x56 0x32 0x58 0x01 与长度；
// 2. 车辆主动安全碰撞与热失控告警 (5% 流量): JSON 格式，严重等级 (Level >= 3) 必须打标并紧急分流；
// 3. 车辆常态遥测流 (15% 流量): JSON 格式，GPS 经纬度保留 2 位小数脱敏保护隐私；
// 4. 双路外部 MQ 分流：紧急告警投递应急响应队列，普通数据投递冷数据湖。
// ==============================================================================

// CAN 二进制魔数: "V2X\x01" (0x56, 0x32, 0x58, 0x01)
var V2XCANMagic = []byte{0x56, 0x32, 0x58, 0x01}

type V2XAuthProcessor struct{}

func (p *V2XAuthProcessor) Match(c *core.Context) bool {
	return c.Message != nil && c.Message.ClientID != "$internal"
}

func (p *V2XAuthProcessor) Process(c *core.Context) error {
	// 业务规则：车辆客户端必须持有合法 17 位 VIN 码前缀 "VIN_"
	if !strings.HasPrefix(c.Message.ClientID, "VIN_") || len(c.Message.ClientID) != 21 {
		c.Drop("invalid vehicle VIN identifier")
		return core.ErrUnauthorized
	}
	return nil
}

// 紧急故障与急救告警识别处理器
type V2XEmergencyAlarmProcessor struct{}

func (p *V2XEmergencyAlarmProcessor) Match(c *core.Context) bool {
	// 职责自洽：只关心主题包含 /alarms 的报文
	return c.Message != nil && strings.Contains(c.Message.Topic, "/alarms")
}

type V2XAlarmPayload struct {
	Level int    `json:"level"`
	Code  string `json:"code"`
}

var v2xAlarmPool = sync.Pool{
	New: func() any { return new(V2XAlarmPayload) },
}

func (p *V2XEmergencyAlarmProcessor) Process(c *core.Context) error {
	alarm := v2xAlarmPool.Get().(*V2XAlarmPayload)
	defer v2xAlarmPool.Put(alarm)

	if err := json.Unmarshal(c.Message.Payload, alarm); err != nil {
		c.Drop("corrupted alarm payload")
		return nil
	}

	// 等级 >= 3 为严重事故或火警，打标为 urgent
	if alarm.Level >= 3 {
		c.Set("urgent", true)
		c.Set("alarm_code", alarm.Code)
	}
	return nil
}

// GPS 隐私脱敏与数据规范化处理器 (使用强类型对象池，0 动态 map 分配)
type V2XTelemetryPayload struct {
	Speed     float64 `json:"speed"`
	SOC       int     `json:"soc"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Masked    bool    `json:"masked"`
	TSGateway int64   `json:"ts_gateway"`
}

var v2xTelemetryPool = sync.Pool{
	New: func() any { return new(V2XTelemetryPayload) },
}

type V2XPrivacyMaskProcessor struct{}

func (p *V2XPrivacyMaskProcessor) Match(c *core.Context) bool {
	// 职责自洽：只关心常态遥测 /telemetry 报文
	return c.Message != nil && strings.Contains(c.Message.Topic, "/telemetry")
}

func (p *V2XPrivacyMaskProcessor) Process(c *core.Context) error {
	t := v2xTelemetryPool.Get().(*V2XTelemetryPayload)
	defer v2xTelemetryPool.Put(t)

	if err := json.Unmarshal(c.Message.Payload, t); err != nil {
		c.Drop("corrupted telemetry json")
		return nil
	}

	// 模糊化处理经纬度
	t.Lat = float64(int(t.Lat*100)) / 100
	t.Lng = float64(int(t.Lng*100)) / 100
	t.Masked = true
	t.TSGateway = time.Now().UnixMilli()

	maskedBytes, _ := json.Marshal(t)
	c.Message.SetPayload(maskedBytes)
	return nil
}

// 双路智能分流投递处理器
type V2XSmartSplitForwardProcessor struct {
	urgentCount atomic.Int64
	lakeCount   atomic.Int64
	canCount    atomic.Int64
}

func (p *V2XSmartSplitForwardProcessor) Match(c *core.Context) bool {
	return c.Message != nil
}

func (p *V2XSmartSplitForwardProcessor) Process(c *core.Context) error {
	// 1. 判断是否是紧急告警
	if isUrgent, ok := c.Get("urgent"); ok && isUrgent.(bool) {
		p.urgentCount.Add(1)
		// 投递至高优先级应急响应通道 (Kafka: v2x.urgent.emergency)
		return nil
	}

	// 2. 判断是否是 CAN 二进制流
	if strings.Contains(c.Message.Topic, "/can") {
		p.canCount.Add(1)
		// 投递至高速冷数据湖 (Kafka: v2x.can.lake)
		return nil
	}

	// 3. 常规遥测
	p.lakeCount.Add(1)
	return nil
}

// ==============================================================================
// 综合压测统计分析与结果展示器
// ==============================================================================

type BenchMetrics struct {
	TotalRequests int64
	SuccessCount  int64
	DroppedCount  int64
	ErrorCount    int64
	Duration      time.Duration
	Latencies     []time.Duration
}

func (m *BenchMetrics) QPS() float64 {
	if m.Duration.Seconds() == 0 {
		return 0
	}
	return float64(m.TotalRequests) / m.Duration.Seconds()
}

func (m *BenchMetrics) LatencyPercentile(pct float64) time.Duration {
	if len(m.Latencies) == 0 {
		return 0
	}
	idx := int(float64(len(m.Latencies)) * pct)
	if idx >= len(m.Latencies) {
		idx = len(m.Latencies) - 1
	}
	return m.Latencies[idx]
}

func main() {
	fmt.Println("=================================================================================")
	fmt.Println("      物联网真实业务驱动架构与性能综合评估 (IoT Real-World Business Benchmarks) ")
	fmt.Println("=================================================================================")

	// --------------------------------------------------------------------------
	// 评测 1: 简单业务场景 (智能表计与水文遥测)
	// --------------------------------------------------------------------------
	runScenario1()

	// --------------------------------------------------------------------------
	// 评测 2: 一般业务场景 (工业 4.0 智能制造车间网关)
	// --------------------------------------------------------------------------
	runScenario2()

	// --------------------------------------------------------------------------
	// 评测 3: 复杂业务场景 (新能源车联网 V2X 异构多流治理)
	// --------------------------------------------------------------------------
	runScenario3()

	fmt.Println("\n=================================================================================")
	fmt.Println("                       全部物联网业务场景评测与性能评估执行完毕                     ")
	fmt.Println("=================================================================================")
}

// ------------------------------------------------------------------------------
// 执行场景 1 评测
// ------------------------------------------------------------------------------
func runScenario1() {
	fmt.Println("\n>>> [场景 1: 简单] 智能表计与水文环境遥测 (海量设备高频透传与心跳过滤)")
	fmt.Println("    业务规则: 1. ClientID 格式认证; 2. 0字节空包与冗余心跳过滤; 3. 合规数据极速直通")

	pipe := core.NewPipe()
	pipe.Add("meter_auth", &MeterAuthProcessor{})
	pipe.Add("heartbeat_filter", &HeartbeatFilterProcessor{})

	totalMsgs := 50000
	concurrency := 10

	var (
		wg           sync.WaitGroup
		successCnt   atomic.Int64
		droppedCnt   atomic.Int64
		errCnt       atomic.Int64
		allLatencies = make([]time.Duration, totalMsgs)
	)

	msgsPerWorker := totalMsgs / concurrency
	startTotal := time.Now()

	for w := 0; w < concurrency; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()

			for i := 0; i < msgsPerWorker; i++ {
				idx := workerID*msgsPerWorker + i
				msg := core.AcquireMessage()

				// 构造测试流量：
				// 90% 正常表计数据；5% 离线空报文/PING（应被过滤）；5% 非法扫描客户端（应被拦截）
				mod := i % 100
				if mod < 90 {
					msg.ClientID = fmt.Sprintf("meter_%d", workerID*1000+i)
					msg.Topic = fmt.Sprintf("meter/electric/%s", msg.ClientID)
					msg.Payload = []byte(`{"kwh":124.5,"v":220}`)
				} else if mod < 95 {
					msg.ClientID = fmt.Sprintf("meter_%d", workerID*1000+i)
					msg.Topic = fmt.Sprintf("meter/electric/%s", msg.ClientID)
					msg.Payload = []byte("PING") // 触发过滤
				} else {
					msg.ClientID = "malicious_scanner_88" // 触发未授权
					msg.Topic = "meter/electric/scanner"
					msg.Payload = []byte(`{"attack":true}`)
				}

				t0 := time.Now()
				err := pipe.Execute(ctx, msg)
				lat := time.Since(t0)
				allLatencies[idx] = lat

				if err == nil {
					successCnt.Add(1)
				} else if err == core.ErrMessageDropped {
					droppedCnt.Add(1)
				} else {
					errCnt.Add(1)
				}

				core.ReleaseMessage(msg)
			}
		}()
	}

	wg.Wait()
	dur := time.Since(startTotal)

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	m := &BenchMetrics{
		TotalRequests: int64(totalMsgs),
		SuccessCount:  successCnt.Load(),
		DroppedCount:  droppedCnt.Load(),
		ErrorCount:    errCnt.Load(),
		Duration:      dur,
		Latencies:     allLatencies,
	}

	printMetricsReport("场景 1: 智能表计", m)
	fmt.Println(pipe.PrintStats())
}

// ------------------------------------------------------------------------------
// 执行场景 2 评测
// ------------------------------------------------------------------------------
func runScenario2() {
	fmt.Println("\n>>> [场景 2: 一般] 工业 4.0 智能制造车间网关 (工业报文校验、数据增强与外部 Kafka 投递)")
	fmt.Println("    业务规则: 1. 车间网关白名单鉴权; 2. 工艺 JSON 格式校验; 3. 注入网关接收时间戳与工厂标签; 4. 投递 Kafka")

	allowed := map[string]bool{
		"gateway_cnc_01":   true,
		"gateway_laser_02": true,
		"gateway_press_03": true,
	}

	var kafkaReceivedCount atomic.Int64

	pipe := core.NewPipe()
	pipe.Add("gateway_auth", &FactoryGatewayAuthProcessor{allowedGateways: allowed})
	pipe.Add("json_validator", core.NewValidateProcessor(
		core.NewJSONValidator("factory_json_check"),
		core.WithValidateTopic("factory/+/metrics"),
	))
	pipe.Add("enricher", core.NewTransformProcessor(func(msg *core.Message) error {
		enriched := fmt.Sprintf(`{"raw":%s,"gateway_recv_ts":%d,"plant":"suzhou_plant_02"}`,
			string(msg.Payload), time.Now().UnixMicro())
		msg.SetPayload([]byte(enriched))
		return nil
	}, core.WithTransformTopic("factory/+/metrics")))
	pipe.Add("kafka_sink", core.NewForwardProcessor(func(msg *core.Message) error {
		kafkaReceivedCount.Add(1)
		return nil
	}, true, core.WithForwardTopic("factory/+/metrics")))

	totalMsgs := 30000
	concurrency := 10

	var (
		wg           sync.WaitGroup
		successCnt   atomic.Int64
		droppedCnt   atomic.Int64
		errCnt       atomic.Int64
		allLatencies = make([]time.Duration, totalMsgs)
	)

	msgsPerWorker := totalMsgs / concurrency
	startTotal := time.Now()

	for w := 0; w < concurrency; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()

			for i := 0; i < msgsPerWorker; i++ {
				idx := workerID*msgsPerWorker + i
				msg := core.AcquireMessage()

				// 构造测试流量：
				// 85% 正常机床工艺参数；10% 脏数据/坏 JSON（应被校验器拦截丢弃）；5% 未认证网关
				mod := i % 100
				if mod < 85 {
					gwName := "gateway_cnc_01"
					if i%2 == 0 {
						gwName = "gateway_laser_02"
					}
					msg.ClientID = gwName
					msg.Topic = "factory/workshop1/metrics"
					msg.Payload = []byte(`{"machine_id":"CNC_402","rpm":12000,"temp":65.4,"vibration":0.02}`)
				} else if mod < 95 {
					msg.ClientID = "gateway_press_03"
					msg.Topic = "factory/workshop1/metrics"
					msg.Payload = []byte(`CORRUPTED_NOT_A_VALID_JSON{:::}`) // 脏数据
				} else {
					msg.ClientID = "untrusted_rogue_gateway" // 未授权网关
					msg.Topic = "factory/workshop1/metrics"
					msg.Payload = []byte(`{"rpm":100}`)
				}

				t0 := time.Now()
				err := pipe.Execute(ctx, msg)
				lat := time.Since(t0)
				allLatencies[idx] = lat

				if err == nil {
					successCnt.Add(1)
				} else if err == core.ErrMessageDropped {
					droppedCnt.Add(1)
				} else {
					errCnt.Add(1)
				}

				core.ReleaseMessage(msg)
			}
		}()
	}

	wg.Wait()
	dur := time.Since(startTotal)

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	m := &BenchMetrics{
		TotalRequests: int64(totalMsgs),
		SuccessCount:  successCnt.Load(),
		DroppedCount:  droppedCnt.Load(),
		ErrorCount:    errCnt.Load(),
		Duration:      dur,
		Latencies:     allLatencies,
	}

	printMetricsReport("场景 2: 工业制造车间", m)
	fmt.Printf("   -> Kafka 异步接收成功投递数: %d 条\n", kafkaReceivedCount.Load())
	fmt.Println(pipe.PrintStats())
}

// ------------------------------------------------------------------------------
// 执行场景 3 评测
// ------------------------------------------------------------------------------
func runScenario3() {
	fmt.Println("\n>>> [场景 3: 复杂] 新能源车联网 TSP 异构多流治理 (CAN二进制校验、碰撞告警紧急分流、遥测脱敏)")
	fmt.Println("    业务规则: 1. 车辆 VIN 码认证; 2. CAN TLV二进制魔数校验; 3. 严重急救告警紧急拦截打标; 4. GPS脱敏; 5. 双路异步分流")

	splitForwarder := &V2XSmartSplitForwardProcessor{}

	pipe := core.NewPipe()
	// 处理器 1: VIN 码认证
	pipe.Add("v2x_vin_auth", &V2XAuthProcessor{})

	// 处理器 2: CAN 二进制魔数与 TLV 校验
	pipe.Add("can_bin_validator", core.NewValidateProcessor(
		core.NewBinaryValidator("can_validator", 8, 1024, core.WithMagic(V2XCANMagic)),
		core.WithValidateTopic("v2x/+/can"),
	))

	// 处理器 3: 急救告警紧急分流识别
	pipe.Add("emergency_alarm_eval", &V2XEmergencyAlarmProcessor{})

	// 处理器 4: GPS 敏感数据脱敏规范化
	pipe.Add("privacy_masker", &V2XPrivacyMaskProcessor{})

	// 处理器 5: 外部 Kafka 双路智能分流 (紧急告警 vs 常规湖仓)
	pipe.Add("kafka_smart_router", splitForwarder)

	totalMsgs := 30000
	concurrency := 15

	var (
		wg           sync.WaitGroup
		successCnt   atomic.Int64
		droppedCnt   atomic.Int64
		errCnt       atomic.Int64
		allLatencies = make([]time.Duration, totalMsgs)
	)

	msgsPerWorker := totalMsgs / concurrency
	startTotal := time.Now()

	for w := 0; w < concurrency; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()

			for i := 0; i < msgsPerWorker; i++ {
				idx := workerID*msgsPerWorker + i
				msg := core.AcquireMessage()
				vin := fmt.Sprintf("VIN_LSVAA218%09d", workerID*10000+i)

				// 构造车联网真实混合多流：
				// 75% CAN 动力总成二进制 (魔数 0x56 0x32 0x58 0x01)
				// 15% 车辆常规遥测 (含经纬度待脱敏)
				// 5%  紧急故障告警 (安全气囊/热失控，紧急路由)
				// 3%  损坏的 CAN 二进制报文 (魔数不符，触发校验丢弃)
				// 2%  非法假冒 VIN 码 (未授权触发中断)
				mod := i % 100
				if mod < 75 {
					msg.ClientID = vin
					msg.Topic = fmt.Sprintf("v2x/%s/can", vin)
					// 构造 12 字节标准 CAN 二进制包: 4B Magic + 4B Type + 4B Value
					buf := make([]byte, 12)
					copy(buf[0:4], V2XCANMagic)
					binary.BigEndian.PutUint32(buf[4:8], 0x0001) // Type: 电池电压
					binary.BigEndian.PutUint32(buf[8:12], 380)   // Value: 380V
					msg.Payload = buf
				} else if mod < 90 {
					msg.ClientID = vin
					msg.Topic = fmt.Sprintf("v2x/%s/telemetry", vin)
					msg.Payload = []byte(`{"speed":92.4,"soc":78,"lat":31.298912,"lng":120.585311}`)
				} else if mod < 95 {
					msg.ClientID = vin
					msg.Topic = fmt.Sprintf("v2x/%s/alarms", vin)
					msg.Payload = []byte(`{"level":4,"code":"AIRBAG_DEPLOYED","desc":"front collision detected"}`)
				} else if mod < 98 {
					msg.ClientID = vin
					msg.Topic = fmt.Sprintf("v2x/%s/can", vin)
					msg.Payload = []byte{0x00, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44} // 坏魔数
				} else {
					msg.ClientID = "FAKE_VIN_BAD" // 非法客户端
					msg.Topic = "v2x/FAKE_VIN_BAD/telemetry"
					msg.Payload = []byte(`{"speed":0}`)
				}

				t0 := time.Now()
				err := pipe.Execute(ctx, msg)
				lat := time.Since(t0)
				allLatencies[idx] = lat

				if err == nil {
					successCnt.Add(1)
				} else if err == core.ErrMessageDropped {
					droppedCnt.Add(1)
				} else {
					errCnt.Add(1)
				}

				core.ReleaseMessage(msg)
			}
		}()
	}

	wg.Wait()
	dur := time.Since(startTotal)

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	m := &BenchMetrics{
		TotalRequests: int64(totalMsgs),
		SuccessCount:  successCnt.Load(),
		DroppedCount:  droppedCnt.Load(),
		ErrorCount:    errCnt.Load(),
		Duration:      dur,
		Latencies:     allLatencies,
	}

	printMetricsReport("场景 3: 车联网 V2X", m)
	fmt.Printf("   -> 智能分流统计: 严重急救告警=%d 条 | CAN动力流=%d 条 | 常规遥测=%d 条\n",
		splitForwarder.urgentCount.Load(), splitForwarder.canCount.Load(), splitForwarder.lakeCount.Load())
	fmt.Println(pipe.PrintStats())
}

// ------------------------------------------------------------------------------
// 指标格式化输出辅助函数
// ------------------------------------------------------------------------------
func printMetricsReport(scenarioName string, m *BenchMetrics) {
	fmt.Printf("\n--- [%s 性能评估结果] ---\n", scenarioName)
	fmt.Printf("总报文吞吐量 (QPS) : %.2f msg/sec\n", m.QPS())
	fmt.Printf("处理总量 / 耗时   : %d 条 / %v\n", m.TotalRequests, m.Duration)
	fmt.Printf("执行结果统计      : 成功通过=%d | 预判拦截丢弃=%d | 异常阻断=%d\n",
		m.SuccessCount, m.DroppedCount, m.ErrorCount)
	fmt.Printf("端到端延时分布    : P50=%-8v | P90=%-8v | P99=%-8v | Max=%-8v\n",
		m.LatencyPercentile(0.50),
		m.LatencyPercentile(0.90),
		m.LatencyPercentile(0.99),
		m.LatencyPercentile(1.00),
	)
}
