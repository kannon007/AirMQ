package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"mqtt/core"
)

// 1. 用户自定义业务处理器：完全遵循统一契约，职责自洽
// 实现且仅需实现 2 个方法：Match(c) 与 Process(c)
type DeviceHealthMonitorProcessor struct {
	alertThreshold float64
}

// 1. 自己判断是否处理：只有上报的 topic 为 devices/+/metrics 且包含 health 字段才关注
func (p *DeviceHealthMonitorProcessor) Match(c *core.Context) bool {
	return strings.HasPrefix(c.Message.Topic, "devices/") && strings.Contains(string(c.Message.Payload), "health")
}

// 2. 具体的处理逻辑
func (p *DeviceHealthMonitorProcessor) Process(c *core.Context) error {
	log.Printf("[♥] [HealthMonitor] 捕获设备健康指标: topic=%s, payload=%s", c.Message.Topic, string(c.Message.Payload))
	return nil
}

func main() {
	log.Println("==================================================================")
	log.Println(" [Demo 04] 统一管道与自洽处理器架构 (Self-Contained Processor Architecture)")
	log.Println("==================================================================")

	// Step 1: 创建统一管道
	pipe := core.NewPipe()

	// Step 2: 统一添加各业务处理器 (每个处理器自带 Match 预判与 Process 业务逻辑)
	pipe.
		// 处理器 1: 认证鉴权 (AuthProcessor 自主判断：跳过 $internal，校验外部客户端)
		Add("auth_guard", core.NewAuthProcessor(func(clientID, username string) bool {
			allowed := strings.HasPrefix(clientID, "edge_") || clientID == "$internal"
			if !allowed {
				log.Printf("[-] [AuthProcessor] 拒绝未授权发布: clientID=%s", clientID)
			} else {
				log.Printf("[+] [AuthProcessor] 鉴权通过: clientID=%s", clientID)
			}
			return allowed
		})).

		// 处理器 2: 数据报文格式校验 (ValidateProcessor 自主判断：空报文跳过，只针对 telemetry/# 校验合法 JSON)
		Add("json_validator", core.NewValidateProcessor(
			core.NewJSONValidator("json_syntax_check"),
			core.WithValidateTopic("telemetry/#"),
			core.WithValidateOnFail(func(c *core.Context, err error) {
				log.Printf("[-] [ValidateProcessor] 格式校验失败，静默丢弃: %v", err)
				c.Drop("corrupted json payload")
			}),
		)).

		// 处理器 3: 报文清洗与元数据增强 (TransformProcessor 自主判断：针对 telemetry/# 进行数据增强)
		Add("data_enricher", core.NewTransformProcessor(func(msg *core.Message) error {
			enriched := fmt.Sprintf(`{"raw":%s,"gateway_ts":%d}`, string(msg.Payload), time.Now().UnixMilli())
			msg.SetPayload([]byte(enriched))
			log.Printf("[*] [TransformProcessor] 报文增强完成: %s", enriched)
			return nil
		}, core.WithTransformTopic("telemetry/#"))).

		// 处理器 4: 外部 Kafka 异步转发驱动 (ForwardProcessor 自主判断：出站投递，passthrough=true 允许本地同时接收)
		Add("kafka_sink", core.NewForwardProcessor(func(msg *core.Message) error {
			log.Printf("[>] [ForwardProcessor] 投递至外部 MQ [iot.telemetry.kafka]: 字节数=%d", len(msg.Payload))
			return nil
		}, true, core.WithForwardTopic("telemetry/#"))).

		// 处理器 5: 用户自定义业务处理器 (DeviceHealthMonitorProcessor)
		Add("health_monitor", &DeviceHealthMonitorProcessor{alertThreshold: 90.0})

	// Step 3: 初始化并启动轻量独立 Broker，挂载管道至 "telemetry/#" 与 "devices/#"
	b, err := core.NewBroker(
		core.WithTCP(":18835"),
		core.WithMemoryStore(),
		core.WithMulticore(false),
		core.WithPipe("telemetry/#", pipe),
		core.WithPipe("devices/#", pipe),
	)
	if err != nil {
		log.Fatalf("Broker 启动失败: %v", err)
	}

	go func() {
		if err := b.Start(); err != nil {
			log.Printf("Broker 运行退出: %v", err)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 注册本地 MQTT 订阅端，观察洗炼后的最终交付
	_, _ = b.Subscribe("telemetry/#", 0, func(topic string, payload []byte) {
		log.Printf(">>> [MQTT Local Subscriber 收到报文] topic=%s, payload=%s", topic, string(payload))
	})

	// 测试 1: 发送非法 JSON 报文 -> json_validator 匹配并校验失败拦截，后续处理器不再执行
	log.Println("\n--- [测试 1] 发布非法 JSON 报文 (将在 json_validator 阶段拦截并 Drop) ---")
	_ = b.Publish("telemetry/motor_01", []byte("ILLEGAL_JSON_PAYLOAD"), 0, false)
	time.Sleep(100 * time.Millisecond)

	// 测试 2: 发送合规 JSON 报文 -> 全链路处理器执行并交付
	log.Println("\n--- [测试 2] 发布合规 JSON 报文 (全链路流转交付) ---")
	_ = b.Publish("telemetry/motor_01", []byte(`{"voltage":220,"current":5.2}`), 0, false)
	time.Sleep(100 * time.Millisecond)

	// 测试 3: 发送设备健康指标 -> health_monitor 自行 Match 成功并执行，json_validator 因主题不匹配自动 Skip
	log.Println("\n--- [测试 3] 发布设备健康报文 (只有 health_monitor 匹配，其他处理器自洽跳过) ---")
	_ = b.Publish("devices/edge_gw01", []byte(`{"health":"OK","cpu_usage":12.5}`), 0, false)
	time.Sleep(100 * time.Millisecond)

	// Step 4: 打印底层自动采集的各处理器性能指标与健康报表！
	log.Println("\n--- [实时性能参数与速度监控报表] ---")
	fmt.Println(pipe.PrintStats())

	log.Println("=== 演示执行完毕 ===")
}
