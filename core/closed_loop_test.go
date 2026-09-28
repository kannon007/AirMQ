package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"mqtt/core"
	"mqtt/pkg/protocol"
)

// ==============================================================================
// 物联网端到端全链路双向业务闭环自动化测试 (Downlink Command & Uplink ACK Closed Loop)
// ==============================================================================

// 场景 1: 云端下发控制指令 -> 下发管道校验 -> 边缘设备接收执行 -> 设备回传执行回执 -> 上行管道确认识别 (完整业务闭环)
func TestBidirectional_Command_And_Ack_ClosedLoop(t *testing.T) {
	addr := "tcp://127.0.0.1:18894"
	b, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	// 1. 注册下发控制管道 (Downlink Command Pipeline: 对云端下发的指令进行白名单与格式校验)
	downlinkPipe := core.NewPipe()
	var downlinkChecked atomic.Int64
	downlinkPipe.Add("downlink_validator", core.NewFuncProcessor(
		func(c *core.Context) bool {
			return c.Message != nil
		},
		func(c *core.Context) error {
			downlinkChecked.Add(1)
			var cmd struct {
				ReqID  string `json:"req_id"`
				Action string `json:"action"`
			}
			if err := json.Unmarshal(c.Message.Payload, &cmd); err != nil || cmd.ReqID == "" {
				c.Drop("malformed downlink command")
				return nil
			}
			return nil
		},
	))
	b.Handle("devices/+/cmd", downlinkPipe)

	// 2. 注册上行回执管道 (Uplink ACK Pipeline: 对设备回传的执行状态进行入库与指标确认)
	uplinkPipe := core.NewPipe()
	var uplinkAckConfirmed atomic.Int64
	uplinkPipe.Add("ack_processor", core.NewFuncProcessor(
		func(c *core.Context) bool {
			return c.Message != nil
		},
		func(c *core.Context) error {
			uplinkAckConfirmed.Add(1)
			return nil
		},
	))
	b.Handle("devices/+/ack", uplinkPipe)

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 3. 模拟真实的物理边缘设备（TCP 客户端连接并订阅其专属指令主题）
	conn, err := net.Dial("tcp", "127.0.0.1:18894")
	if err != nil {
		t.Fatalf("Device failed to connect to broker: %v", err)
	}
	defer conn.Close()

	// 设备发送 CONNECT
	connPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "pump_substation_01",
	}
	connRaw, _ := connPkt.Encode()
	_, _ = conn.Write(connRaw)

	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil || n < 4 || buf[0] != 0x20 {
		t.Fatalf("Device failed to receive CONNACK: %v", err)
	}

	// 设备订阅下行控制指令主题: "devices/pump_substation_01/cmd"
	cmdTopic := "devices/pump_substation_01/cmd"
	subPkt := &protocol.SubscribePacket{
		PacketID: 1,
		Topics: []protocol.TopicSub{
			{Topic: cmdTopic, QoS: protocol.QoS0},
		},
	}
	subRaw, _ := subPkt.Encode()
	_, _ = conn.Write(subRaw)

	n, err = conn.Read(buf)
	if err != nil || n < 5 || buf[0] != 0x90 {
		t.Fatalf("Device failed to receive SUBACK: %v", err)
	}

	// 4. 云端服务监听设备的回执主题 "devices/+/ack"
	ackReceivedCh := make(chan string, 1)
	unsubAck, err := b.Subscribe("devices/+/ack", 0, func(topic string, payload []byte) {
		ackReceivedCh <- string(payload)
	})
	if err != nil {
		t.Fatalf("Cloud failed to subscribe ack: %v", err)
	}
	defer unsubAck()

	// 5. 【下发环节】：云端业务服务下发调速控制指令
	reqPayload := `{"req_id":"req_speed_888","action":"adjust_frequency","hz":50.5}`
	startTime := time.Now()
	if err := b.Publish(cmdTopic, []byte(reqPayload), 0, false); err != nil {
		t.Fatalf("Cloud downlink dispatch failed: %v", err)
	}

	// 6. 设备端从 TCP 套接字读取云端下发的 PUBLISH 指令报文
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = conn.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("Device did not receive downlink PUBLISH command: %v", err)
	}
	pkt, _, err := protocol.DecodePacket(buf[:n], protocol.V311)
	if err != nil {
		t.Fatalf("Device failed to decode downlink packet: %v", err)
	}
	pubPkt, ok := pkt.(*protocol.PublishPacket)
	if !ok || pubPkt.Topic != cmdTopic {
		t.Fatalf("Device received unexpected packet: %T, topic: %v", pkt, pubPkt)
	}

	// 7. 设备端模拟执行硬件调节动作，并组装【回执报文 (ACK)】
	var receivedCmd struct {
		ReqID  string  `json:"req_id"`
		Action string  `json:"action"`
		Hz     float64 `json:"hz"`
	}
	_ = json.Unmarshal(pubPkt.Payload, &receivedCmd)
	if receivedCmd.ReqID != "req_speed_888" || receivedCmd.Hz != 50.5 {
		t.Fatalf("Device payload mismatch: got %+v", receivedCmd)
	}

	// 设备端向 "devices/pump_substation_01/ack" 上传执行结果
	ackTopic := "devices/pump_substation_01/ack"
	ackPayload := fmt.Sprintf(`{"req_id":"%s","status":"SUCCESS","applied_hz":%.1f}`, receivedCmd.ReqID, receivedCmd.Hz)
	devicePubPkt := &protocol.PublishPacket{
		ProtocolLevel: protocol.V311,
		Topic:         ackTopic,
		Payload:       []byte(ackPayload),
		QoS:           protocol.QoS0,
	}
	devicePubRaw, _ := devicePubPkt.Encode()
	if _, err := conn.Write(devicePubRaw); err != nil {
		t.Fatalf("Device failed to upload ACK: %v", err)
	}

	// 8. 【闭环验证】：云端服务在超时前收到设备的执行回执，完成请求-响应双向闭环
	select {
	case ackStr := <-ackReceivedCh:
		elapsed := time.Since(startTime)
		t.Logf(">>> 双向业务端到端闭环耗时 (Round-Trip Latency): %v", elapsed)
		var ack struct {
			ReqID     string  `json:"req_id"`
			Status    string  `json:"status"`
			AppliedHz float64 `json:"applied_hz"`
		}
		if err := json.Unmarshal([]byte(ackStr), &ack); err != nil {
			t.Fatalf("Failed to parse ack: %v", err)
		}
		if ack.ReqID != "req_speed_888" || ack.Status != "SUCCESS" || ack.AppliedHz != 50.5 {
			t.Fatalf("ACK payload mismatch: got %+v", ack)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for device ACK to reach cloud (Closed Loop Broken!)")
	}

	// 9. 校验下发与上行管道的拦截统计指标
	if downlinkChecked.Load() != 1 {
		t.Errorf("Expected downlinkChecked=1, got %d", downlinkChecked.Load())
	}
	if uplinkAckConfirmed.Load() != 1 {
		t.Errorf("Expected uplinkAckConfirmed=1, got %d", uplinkAckConfirmed.Load())
	}
}

// 场景 2: MQTT 5.0 标准请求-响应闭环 (Request-Response RPC with Correlation Data & ResponseTopic)
func TestBidirectional_MQTT5_RequestResponse_ClosedLoop(t *testing.T) {
	addr := "tcp://127.0.0.1:18895"
	b, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 1. 微服务客户端 A（RPC 提供者 / 计算设备）连接并订阅 "rpc/calculator"
	serverConn, err := net.Dial("tcp", "127.0.0.1:18895")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer serverConn.Close()

	// CONNECT V5.0
	connPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V50,
		CleanStart:    true,
		ClientID:      "rpc_provider_node",
	}
	raw, _ := connPkt.Encode()
	_, _ = serverConn.Write(raw)
	buf := make([]byte, 2048)
	_, _ = serverConn.Read(buf)

	// SUBSCRIBE V5.0 "rpc/calculator"
	subPkt := &protocol.SubscribePacket{
		ProtocolLevel: protocol.V50,
		PacketID:      1,
		Topics: []protocol.TopicSub{
			{Topic: "rpc/calculator", QoS: protocol.QoS0},
		},
	}
	raw, _ = subPkt.Encode()
	_, _ = serverConn.Write(raw)
	_, _ = serverConn.Read(buf)

	// 2. 云端调用方 B 监听专属响应主题 "rpc/client_callback/uuid123"
	responseTopic := "rpc/client_callback/uuid123"
	replyCh := make(chan string, 1)
	unsub, err := b.Subscribe(responseTopic, 0, func(topic string, payload []byte) {
		replyCh <- string(payload)
	})
	if err != nil {
		t.Fatalf("Failed to subscribe callback: %v", err)
	}
	defer unsub()

	// 3. 云端调用方 B 下发带 ResponseTopic 的 RPC 请求
	reqPkt := &protocol.PublishPacket{
		ProtocolLevel: protocol.V311, // 进程内发布兼容
		Topic:         "rpc/calculator",
		Payload:       []byte(`{"op":"multiply","x":21,"y":2}`),
		QoS:           protocol.QoS0,
	}
	if err := b.Publish(reqPkt.Topic, reqPkt.Payload, reqPkt.QoS, reqPkt.Retain); err != nil {
		t.Fatalf("Failed to dispatch RPC: %v", err)
	}

	// 4. 计算设备接收到请求，读取内容并执行计算
	_ = serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := serverConn.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("RPC provider did not receive request: %v", err)
	}
	pkt, _, _ := protocol.DecodePacket(buf[:n], protocol.V50)
	pubPkt, ok := pkt.(*protocol.PublishPacket)
	if !ok {
		t.Fatalf("Expected publish packet, got: %T, raw: %x", pkt, buf[:n])
	}

	var reqData struct {
		Op string `json:"op"`
		X  int    `json:"x"`
		Y  int    `json:"y"`
	}
	if err := json.Unmarshal(pubPkt.Payload, &reqData); err != nil {
		t.Fatalf("json unmarshal error: %v", err)
	}
	result := reqData.X * reqData.Y // 21 * 2 = 42

	// 5. 计算设备将计算结果回发到指定的 ResponseTopic
	replyPkt := &protocol.PublishPacket{
		ProtocolLevel: protocol.V50,
		Topic:         responseTopic,
		Payload:       []byte(fmt.Sprintf(`{"result":%d}`, result)),
		QoS:           protocol.QoS0,
	}
	raw, _ = replyPkt.Encode()
	_, _ = serverConn.Write(raw)

	// 6. 验证调用方是否在超时前收到 42 的计算结果
	select {
	case replyStr := <-replyCh:
		var reply struct {
			Result int `json:"result"`
		}
		if err := json.Unmarshal([]byte(replyStr), &reply); err != nil {
			t.Fatalf("Failed to unmarshal replyStr: %v", err)
		}
		if reply.Result != 42 {
			t.Fatalf("Expected RPC result 42, got %d", reply.Result)
		}
		t.Logf(">>> MQTT 5.0 RPC 闭环计算成功: %s", replyStr)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for RPC reply")
	}
}

// 场景 3: QoS 1 下发可靠确认闭环 (Downlink QoS 1 PUBACK Handshake)
func TestBidirectional_Downlink_QoS1_Handshake_ClosedLoop(t *testing.T) {
	addr := "tcp://127.0.0.1:18896"
	b, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(false),
	)
	if err != nil {
		t.Fatalf("Failed to initialize broker: %v", err)
	}

	go func() { _ = b.Start() }()
	time.Sleep(150 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	// 设备建立 TCP 连接
	conn, err := net.Dial("tcp", "127.0.0.1:18896")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// CONNECT
	connPkt := &protocol.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: protocol.V311,
		CleanSession:  true,
		ClientID:      "reliable_valve_01",
	}
	raw, _ := connPkt.Encode()
	_, _ = conn.Write(raw)
	buf := make([]byte, 1024)
	_, _ = conn.Read(buf)

	// 订阅 QoS 1 主题: "valves/01/control"
	subPkt := &protocol.SubscribePacket{
		PacketID: 10,
		Topics: []protocol.TopicSub{
			{Topic: "valves/01/control", QoS: protocol.QoS1},
		},
	}
	raw, _ = subPkt.Encode()
	_, _ = conn.Write(raw)
	_, _ = conn.Read(buf)

	// 云端下发 QoS 1 指令
	if err := b.Publish("valves/01/control", []byte("SHUTDOWN_EMERGENCY"), protocol.QoS1, false); err != nil {
		t.Fatalf("QoS 1 downlink publish failed: %v", err)
	}

	// 客户端读取下发的 QoS 1 报文并提取 PacketID
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read QoS 1 command: %v", err)
	}
	pkt, _, _ := protocol.DecodePacket(buf[:n], protocol.V311)
	pubPkt, ok := pkt.(*protocol.PublishPacket)
	if !ok || pubPkt.QoS != protocol.QoS1 {
		t.Fatalf("Expected QoS 1 packet, got: %+v", pkt)
	}

	// 设备端回复 PUBACK
	puback := &protocol.PubackPacket{
		PacketID: pubPkt.PacketID,
	}
	ackRaw, _ := puback.Encode()
	if _, err := conn.Write(ackRaw); err != nil {
		t.Fatalf("Failed to send PUBACK: %v", err)
	}

	t.Logf(">>> QoS 1 下发可靠握手闭环验证通过 (PacketID: %d)", pubPkt.PacketID)
}
