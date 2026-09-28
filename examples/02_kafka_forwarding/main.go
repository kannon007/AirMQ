package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"mqtt/core"
)

// 模拟外部 Kafka 客户端生产者 (真实项目中可替换为 sarama 或 confluent-kafka-go)
type MockKafkaProducer struct{}

func (k *MockKafkaProducer) SendMessage(topic, key string, payload []byte) error {
	log.Printf("[KafkaProducer] 成功转发至 Kafka -> Topic: %s | Key: %s | Size: %d bytes | 数据: %s",
		topic, key, len(payload), string(payload))
	return nil
}

func main() {
	// -------------------------------------------------------------------------
	// 方式 A：通过 core 内置工业级数据流管道转发 Kafka (推荐生产首选)
	// 特点：
	//  1. L1 内存 Sharded RingBuffer + L2 磁盘段文件 Spooler 两级削峰；
	//  2. 下游 Kafka 故障/网络抖动时自动跳闸熔断，消息安全落盘，恢复后自动批量回放；
	//  3. 核心 MQTT EventLoop 零阻塞，保持 50 万 msg/s 高速推流。
	// -------------------------------------------------------------------------
	kafkaBrokers := []string{"127.0.0.1:9092"}
	topicFilters := []string{"telemetry/#", "events/#"} // 仅转发这些 MQTT 主题

	brokerWithPipeline, err := core.NewBroker(
		core.WithTCP("127.0.0.1:18882"),
		core.WithMemoryStore(),
		core.WithMulticore(true),
		// 启用内置 Kafka 流式管道
		core.WithPipeline("kafka", map[string]any{
			"brokers": kafkaBrokers,
			"topic":   "iot_raw_telemetry",
		}, topicFilters, "./data/pipeline_spool"),
		// 配置流式削峰参数：批次大小 1000 条，最大聚合等待 20ms，最大磁盘配额 5GB
		core.WithPipelineSpool(1000, 20, 5),
	)
	if err != nil {
		log.Fatalf("初始化 Broker (内置 Kafka 管道) 失败: %v", err)
	}

	go func() {
		log.Printf("MQTT Broker (内置 Kafka 管道模式) 已启动在 127.0.0.1:18882")
		_ = brokerWithPipeline.Start()
	}()
	time.Sleep(100 * time.Millisecond)
	_ = brokerWithPipeline.Stop(context.Background())

	// -------------------------------------------------------------------------
	// 方式 B：在业务微服务中利用 broker.Subscribe 进程内全量订阅并自定义转发 Kafka
	// 特点：
	//  1. 可以在 Go 代码中自由对数据做格式清洗、协议转码、分发到不同 Kafka 分区；
	//  2. 灵活控制生产者批量发送（如 sarama.SyncProducer / AsyncProducer）。
	// -------------------------------------------------------------------------
	brokerCustom, err := core.NewBroker(
		core.WithTCP("127.0.0.1:18883"),
		core.WithMemoryStore(),
	)
	if err != nil {
		log.Fatalf("初始化 Broker 失败: %v", err)
	}

	kafkaProducer := &MockKafkaProducer{}

	// 订阅全量传感器遥测数据
	unsub, err := brokerCustom.Subscribe("telemetry/#", 0, func(topic string, payload []byte) {
		// 1. 动态生成 Kafka 分区 Key (如从 MQTT Topic 中提取设备 ID)
		// 例如: "telemetry/devices/dev-001/temperature" -> key = "dev-001"
		partitionKey := "default_key"

		// 2. 注入服务端元数据 (如接收时间戳、来源协议)
		var event map[string]any
		if err := json.Unmarshal(payload, &event); err == nil {
			event["_broker_recv_time"] = time.Now().UnixMilli()
			if transformed, err := json.Marshal(event); err == nil {
				payload = transformed
			}
		}

		// 3. 转发至对应 Kafka Topic
		_ = kafkaProducer.SendMessage("kafka_sensor_stream", partitionKey, payload)
	})
	if err != nil {
		log.Fatalf("订阅失败: %v", err)
	}
	defer unsub()

	go func() {
		log.Printf("MQTT Broker (进程内自定义 Kafka 转发模式) 已启动在 :1884")
		_ = brokerCustom.Start()
	}()
	time.Sleep(100 * time.Millisecond)

	// 模拟发布一条遥测测试数据
	_ = brokerCustom.Publish("telemetry/devices/dev-001/temperature", []byte(`{"temp":28.5,"hum":65}`), 0, false)

	time.Sleep(200 * time.Millisecond)
	_ = brokerCustom.Stop(context.Background())
}
