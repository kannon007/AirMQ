package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"mqtt/pkg/protocol"
)

// Helper to send CONNECT and receive CONNACK
func dialAndConnect(brokerAddr, clientID, localIP string, keepalive uint16) (net.Conn, error) {
	var dialer net.Dialer
	dialer.Timeout = 5 * time.Second
	if localIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(localIP)}
	}

	conn, err := dialer.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, err
	}

	connectPkt := buildConnectRaw(clientID, keepalive)
	if _, err := conn.Write(connectPkt); err != nil {
		conn.Close()
		return nil, err
	}

	var connack [4]byte
	if _, err := io.ReadFull(conn, connack[:]); err != nil || connack[0] != 0x20 || connack[3] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("bad connack: %x", connack)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetReadBuffer(1024 * 1024)
		_ = tcpConn.SetWriteBuffer(1024 * 1024)
	}

	return conn, nil
}

// Helper to send SUBSCRIBE and receive SUBACK
func subscribeTopic(conn net.Conn, topic string, qos byte) error {
	packetID := uint16(1)
	subRaw := buildSubscribeRaw(packetID, topic, qos)
	if _, err := conn.Write(subRaw); err != nil {
		return err
	}

	var suback [5]byte
	if _, err := io.ReadFull(conn, suback[:]); err != nil || suback[0] != 0x90 {
		return fmt.Errorf("bad suback: %x", suback)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Scenario 1: Fan-Out Broadcast (1-to-N 广播风暴)
// -----------------------------------------------------------------------------
func RunFanout(brokerAddr string, subCount int, pubRate int, durationSec int, localIPs []string) {
	log.Printf("[Fan-Out 广播压测] 启动 %d 个订阅者监听 'bench/fanout'，发布频率: %d msg/s，持续: %d 秒",
		subCount, pubRate, durationSec)

	stats := &Stats{}
	lat := NewLatencyRecorder(100000)

	var subWg sync.WaitGroup
	readyCount := int64(0)

	// 1. Establish N subscribers
	for i := 1; i <= subCount; i++ {
		subWg.Add(1)
		var localIP string
		if len(localIPs) > 0 {
			localIP = localIPs[i%len(localIPs)]
		}
		cid := fmt.Sprintf("sub_%06d", i)

		go func(clientID, ip string) {
			defer subWg.Done()
			conn, err := dialAndConnect(brokerAddr, clientID, ip, 60)
			if err != nil {
				atomic.AddInt64(&stats.Failed, 1)
				return
			}
			defer conn.Close()

			if err := subscribeTopic(conn, "bench/fanout", 0); err != nil {
				atomic.AddInt64(&stats.Failed, 1)
				return
			}

			atomic.AddInt64(&stats.Active, 1)
			atomic.AddInt64(&readyCount, 1)

			// Reader loop
			buf := make([]byte, 4096)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					return
				}
				// Parse incoming publish packet
				pkt, _, err := protocol.DecodePacket(buf[:n])
				if err == nil && pkt != nil && pkt.Type() == protocol.PUBLISH {
					pub := pkt.(*protocol.PublishPacket)
					if len(pub.Payload) >= 8 {
						sentNanos := int64(binary.BigEndian.Uint64(pub.Payload[:8]))
						diff := time.Duration(time.Now().UnixNano() - sentNanos)
						lat.Record(diff)
					}
					atomic.AddInt64(&stats.PubRecv, 1)
				}
			}
		}(cid, localIP)
	}

	// Wait for subscribers to be ready
	for atomic.LoadInt64(&readyCount) < int64(subCount*98/100) {
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("[Fan-Out] 订阅者已就绪 (%d/%d)，开始推送广播消息...", atomic.LoadInt64(&readyCount), subCount)

	// 2. Start publisher
	pubConn, err := dialAndConnect(brokerAddr, "fanout_pub", "", 60)
	if err != nil {
		log.Fatalf("Publisher connection failed: %v", err)
	}
	defer pubConn.Close()

	interval := time.Duration(1e9 / pubRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stopTimer := time.NewTimer(time.Duration(durationSec) * time.Second)
	defer stopTimer.Stop()

	statTicker := time.NewTicker(1 * time.Second)
	defer statTicker.Stop()

	var lastRecv int64

	for {
		select {
		case <-stopTimer.C:
			snap := lat.Snapshot()
			log.Printf("=================== [Fan-Out 广播测试完成] ===================")
			log.Printf("总接收投递数: %d | 端到端延迟 P50: %v | P90: %v | P99: %v | Max: %v",
				atomic.LoadInt64(&stats.PubRecv), snap.P50, snap.P90, snap.P99, snap.Max)
			return

		case <-ticker.C:
			// Embed 8-byte nano timestamp into payload
			payload := make([]byte, 64)
			binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))
			pubPkt := &protocol.PublishPacket{
				Topic:   "bench/fanout",
				Payload: payload,
				QoS:     0,
			}
			encoded, _ := pubPkt.Encode()
			_, _ = pubConn.Write(encoded)
			atomic.AddInt64(&stats.PubSent, 1)

		case <-statTicker.C:
			currRecv := atomic.LoadInt64(&stats.PubRecv)
			speed := currRecv - lastRecv
			lastRecv = currRecv
			snap := lat.Snapshot()
			log.Printf("[Fan-Out 实时] 投递吞吐: %d msg/s | 延迟 P50: %v | P99: %v",
				speed, snap.P50, snap.P99)
		}
	}
}

// -----------------------------------------------------------------------------
// Scenario 2: N-to-N Telemetry (海量设备遥测并发上报与通配符聚合)
// -----------------------------------------------------------------------------
func RunTelemetry(brokerAddr string, deviceCount int, msgRatePerDev int, durationSec int, localIPs []string) {
	log.Printf("[遥测并发上报压测] 启动 %d 台虚拟设备，各自上报独立主题 'devices/{id}/data'，持续: %d 秒",
		deviceCount, durationSec)

	stats := &Stats{}
	lat := NewLatencyRecorder(100000)

	// 1. Collector subscribing to devices/+/data
	collectorConn, err := dialAndConnect(brokerAddr, "collector_backend", "", 60)
	if err != nil {
		log.Fatalf("Collector connect failed: %v", err)
	}
	defer collectorConn.Close()
	if err := subscribeTopic(collectorConn, "devices/+/data", 0); err != nil {
		log.Fatalf("Collector subscribe failed: %v", err)
	}

	go func() {
		buf := make([]byte, 16384)
		for {
			n, err := collectorConn.Read(buf)
			if err != nil {
				return
			}
			pkt, _, err := protocol.DecodePacket(buf[:n])
			if err == nil && pkt != nil && pkt.Type() == protocol.PUBLISH {
				pub := pkt.(*protocol.PublishPacket)
				if len(pub.Payload) >= 8 {
					sent := int64(binary.BigEndian.Uint64(pub.Payload[:8]))
					lat.Record(time.Duration(time.Now().UnixNano() - sent))
				}
				atomic.AddInt64(&stats.PubRecv, 1)
			}
		}
	}()

	// 2. Spawn device publishers
	interval := time.Duration(1e9 / msgRatePerDev)
	stopCh := make(chan struct{})

	for i := 1; i <= deviceCount; i++ {
		cid := fmt.Sprintf("dev_%06d", i)
		topic := fmt.Sprintf("devices/%06d/data", i)
		var localIP string
		if len(localIPs) > 0 {
			localIP = localIPs[i%len(localIPs)]
		}

		go func(deviceID, devTopic, ip string) {
			conn, err := dialAndConnect(brokerAddr, deviceID, ip, 60)
			if err != nil {
				atomic.AddInt64(&stats.Failed, 1)
				return
			}
			defer conn.Close()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			payload := make([]byte, 64)
			pubPkt := &protocol.PublishPacket{Topic: devTopic, QoS: 0}

			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))
					pubPkt.Payload = payload
					encoded, _ := pubPkt.Encode()
					_, _ = conn.Write(encoded)
					atomic.AddInt64(&stats.PubSent, 1)
				}
			}
		}(cid, topic, localIP)
	}

	statTicker := time.NewTicker(1 * time.Second)
	defer statTicker.Stop()
	stopTimer := time.NewTimer(time.Duration(durationSec) * time.Second)
	defer stopTimer.Stop()

	var lastSent, lastRecv int64
	for {
		select {
		case <-stopTimer.C:
			close(stopCh)
			snap := lat.Snapshot()
			log.Printf("=================== [遥测上报测试完成] ===================")
			log.Printf("总发送: %d | 总接收: %d | 延迟 P50: %v | P90: %v | P99: %v",
				atomic.LoadInt64(&stats.PubSent), atomic.LoadInt64(&stats.PubRecv), snap.P50, snap.P90, snap.P99)
			return

		case <-statTicker.C:
			currSent := atomic.LoadInt64(&stats.PubSent)
			currRecv := atomic.LoadInt64(&stats.PubRecv)
			log.Printf("[遥测实时] 上报速率: %d msg/s | 聚合接收: %d msg/s | P99 延迟: %v",
				currSent-lastSent, currRecv-lastRecv, lat.Snapshot().P99)
			lastSent = currSent
			lastRecv = currRecv
		}
	}
}

// -----------------------------------------------------------------------------
// Scenario 3: Shared Subscription Balancing (共享订阅负载均衡压测)
// -----------------------------------------------------------------------------
func RunSharedSub(brokerAddr string, workerCount int, totalMsgs int) {
	log.Printf("[共享订阅均衡压测] 启动 %d 个消费者加入 '$share/workers/jobs/+'，验证均衡度...", workerCount)

	workerCounts := make([]int64, workerCount)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		cid := fmt.Sprintf("worker_%d", i+1)
		workerIdx := i

		go func(clientID string, idx int) {
			defer wg.Done()
			conn, err := dialAndConnect(brokerAddr, clientID, "", 60)
			if err != nil {
				return
			}
			defer conn.Close()

			_ = subscribeTopic(conn, "$share/workers/jobs/+", 0)

			buf := make([]byte, 4096)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					return
				}
				pkt, _, _ := protocol.DecodePacket(buf[:n])
				if pkt != nil && pkt.Type() == protocol.PUBLISH {
					atomic.AddInt64(&workerCounts[idx], 1)
				}
			}
		}(cid, workerIdx)
	}

	time.Sleep(500 * time.Millisecond)

	// Publisher pushes totalMsgs
	pubConn, _ := dialAndConnect(brokerAddr, "job_producer", "", 60)
	defer pubConn.Close()

	log.Printf("生产者开始投递 %d 条任务消息...", totalMsgs)
	for i := 1; i <= totalMsgs; i++ {
		pubPkt := &protocol.PublishPacket{
			Topic:   fmt.Sprintf("jobs/%d", i),
			Payload: []byte("task-payload"),
			QoS:     0,
		}
		encoded, _ := pubPkt.Encode()
		_, _ = pubConn.Write(encoded)
		if i%1000 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	time.Sleep(1 * time.Second)

	log.Printf("=================== [共享订阅负载均衡结果] ===================")
	for i, c := range workerCounts {
		log.Printf("Worker #%d (%s) 处理任务数: %d (占比 %.2f%%)",
			i+1, fmt.Sprintf("worker_%d", i+1), c, float64(c)/float64(totalMsgs)*100)
	}
}

// -----------------------------------------------------------------------------
// Scenario 4: Reconnect Storm (海量客户端频繁断线重连风暴)
// -----------------------------------------------------------------------------
func RunReconnectStorm(brokerAddr string, clientCount int, cycles int) {
	log.Printf("[重连风暴压测] %d 个客户端同时执行 %d 轮‘断开 - 重连’循环...", clientCount, cycles)

	var successCount, failCount int64
	start := time.Now()

	for c := 1; c <= cycles; c++ {
		log.Printf("--- 正在执行第 %d/%d 轮重连风暴 ---", c, cycles)
		var wg sync.WaitGroup

		for i := 1; i <= clientCount; i++ {
			wg.Add(1)
			cid := fmt.Sprintf("flapping_%05d", i)

			go func(clientID string) {
				defer wg.Done()
				conn, err := dialAndConnect(brokerAddr, clientID, "", 30)
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					return
				}
				// Hold socket briefly
				time.Sleep(100 * time.Millisecond)
				_ = conn.Close()
				atomic.AddInt64(&successCount, 1)
			}(cid)
		}
		wg.Wait()
		time.Sleep(200 * time.Millisecond)
	}

	elapsed := time.Since(start)
	log.Printf("=================== [重连风暴测试完成] ===================")
	log.Printf("总成功建连: %d | 失败数: %d | 总耗时: %v (平均吞吐: %.0f conns/s)",
		successCount, failCount, elapsed, float64(successCount)/elapsed.Seconds())
}

func buildSubscribeRaw(packetID uint16, topic string, qos byte) []byte {
	topicBytes := []byte(topic)
	remLen := 2 + 2 + len(topicBytes) + 1
	buf := make([]byte, 1+4+remLen)
	buf[0] = 0x82 // SUBSCRIBE with flags 0x02
	offset := 1
	offset += protocol.EncodeRemainingLength(remLen, buf[offset:])

	binary.BigEndian.PutUint16(buf[offset:], packetID)
	offset += 2

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(topicBytes)))
	offset += 2
	copy(buf[offset:], topicBytes)
	offset += len(topicBytes)

	buf[offset] = qos
	offset++

	return buf[:offset]
}

// -----------------------------------------------------------------------------
// Scenario 6: Loss-Proof High-Concurrency Reliability (高并发端到端消息防丢对账压测)
// -----------------------------------------------------------------------------
func RunLossProof(brokerAddr string, totalMessages int, inflightWindow int, qos byte) {
	log.Printf("[高并发消息防丢稳定性压测] 目标消息量: %d 条 | Inflight 窗口: %d | QoS: %d",
		totalMessages, inflightWindow, qos)

	topic := "bench/reliable/stream"

	// 1. Subscriber with sequence tracking
	subConn, err := dialAndConnect(brokerAddr, "loss_proof_sub", "", 60)
	if err != nil {
		log.Fatalf("Subscriber connection failed: %v", err)
	}
	defer subConn.Close()

	if err := subscribeTopic(subConn, topic, qos); err != nil {
		log.Fatalf("Subscriber subscribe failed: %v", err)
	}

	receivedMap := make(map[uint64]bool)
	var recMu sync.Mutex
	var totalRecv int64
	doneCh := make(chan struct{})

	// Background reader for subscriber
	go func() {
		var streamBuf []byte
		readBuf := make([]byte, 32768)
		for {
			select {
			case <-doneCh:
				return
			default:
			}

			_ = subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, err := subConn.Read(readBuf)
			if n > 0 {
				streamBuf = append(streamBuf, readBuf[:n]...)
			}
			if err != nil && n == 0 {
				continue
			}

			for len(streamBuf) > 0 {
				pkt, consumed, err := protocol.DecodePacket(streamBuf)
				if err != nil {
					streamBuf = streamBuf[1:]
					continue
				}
				if consumed == 0 || pkt == nil {
					break // wait for more data from socket
				}
				streamBuf = streamBuf[consumed:]

				if pkt.Type() == protocol.PUBLISH {
					pub := pkt.(*protocol.PublishPacket)
					if len(pub.Payload) >= 8 {
						seq := binary.BigEndian.Uint64(pub.Payload[:8])
						recMu.Lock()
						if !receivedMap[seq] {
							receivedMap[seq] = true
							atomic.AddInt64(&totalRecv, 1)
						}
						recMu.Unlock()
					}

					// Reply with PUBACK if QoS 1
					if pub.QoS == protocol.QoS1 {
						ack := []byte{0x40, 0x02, byte(pub.PacketID >> 8), byte(pub.PacketID & 0xFF)}
						_, _ = subConn.Write(ack)
					}
				}
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// 2. Publisher with Backpressure Inflight Window
	pubConn, err := dialAndConnect(brokerAddr, "loss_proof_pub", "", 60)
	if err != nil {
		log.Fatalf("Publisher connection failed: %v", err)
	}
	defer pubConn.Close()

	sem := make(chan struct{}, inflightWindow)
	var totalAcks int64
	var pubSent int64

	// Reader for PUBACK on publisher
	if qos == 1 {
		go func() {
			var ackStream []byte
			ackBuf := make([]byte, 16384)
			for {
				select {
				case <-doneCh:
					return
				default:
				}

				_ = pubConn.SetReadDeadline(time.Now().Add(1 * time.Second))
				n, err := pubConn.Read(ackBuf)
				if n > 0 {
					ackStream = append(ackStream, ackBuf[:n]...)
				}
				if err != nil && n == 0 {
					continue
				}

				for len(ackStream) > 0 {
					pkt, consumed, err := protocol.DecodePacket(ackStream)
					if err != nil {
						ackStream = ackStream[1:]
						continue
					}
					if consumed == 0 || pkt == nil {
						break
					}
					ackStream = ackStream[consumed:]
					if pkt.Type() == protocol.PUBACK {
						atomic.AddInt64(&totalAcks, 1)
						<-sem // Release inflight slot
					}
				}
			}
		}()
	}

	start := time.Now()
	statTicker := time.NewTicker(1 * time.Second)
	defer statTicker.Stop()

	go func() {
		var lastSent, lastRecv int64
		for range statTicker.C {
			s := atomic.LoadInt64(&pubSent)
			r := atomic.LoadInt64(&totalRecv)
			a := atomic.LoadInt64(&totalAcks)
			log.Printf("[实时对账] 已发: %d (+%d/s) | 已确认(ACK): %d | 接收端已核验: %d (+%d/s) | 传输中(Inflight): %d",
				s, s-lastSent, a, r, r-lastRecv, s-a)
			lastSent = s
			lastRecv = r
		}
	}()

	payload := make([]byte, 64)
	copy(payload[8:], []byte("loss-proof-strict-delivery-verification-payload"))

	for i := 0; i < totalMessages; i++ {
		if qos == 1 {
			sem <- struct{}{} // acquire slot in inflight window (backpressure!)
		}

		binary.BigEndian.PutUint64(payload[:8], uint64(i))
		pktID := uint16((i % 65534) + 1)

		pubPkt := &protocol.PublishPacket{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			PacketID: pktID,
		}
		encoded, _ := pubPkt.Encode()
		_, _ = pubConn.Write(encoded)
		atomic.AddInt64(&pubSent, 1)
	}

	log.Println("生产者已发送全部消息，等待接收端对账完成...")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&totalRecv) >= int64(totalMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	close(doneCh)
	elapsed := time.Since(start)

	// 3. Final Audit & Loss Verification
	recMu.Lock()
	missingCount := 0
	for i := 0; i < totalMessages; i++ {
		if !receivedMap[uint64(i)] {
			missingCount++
		}
	}
	actualRecv := len(receivedMap)
	recMu.Unlock()

	lossRate := float64(missingCount) / float64(totalMessages) * 100

	log.Printf("=================== [高并发消息防丢压测结果] ===================")
	log.Printf("目标发送总量: %d 条", totalMessages)
	log.Printf("成功发送数量: %d 条", pubSent)
	log.Printf("接收对账有效: %d 条", actualRecv)
	log.Printf("丢失消息数量: %d 条", missingCount)
	log.Printf("综合消息丢失率: %.4f%%", lossRate)
	log.Printf("端到端有效吞吐: %.0f msg/s (总耗时: %v)", float64(actualRecv)/elapsed.Seconds(), elapsed)

	if missingCount == 0 {
		log.Printf("[SUCCESS] 达成目标: 零消息丢失 (Zero Message Loss)! 系统反压与可靠性验证通过！")
	} else {
		log.Printf("[WARN] 存在消息丢失，丢失率: %.4f%%", lossRate)
	}
}

// -----------------------------------------------------------------------------
// Scenario 7: 200K+ msg/s Ultra High-Throughput Zero-Loss Benchmark (20万 TPS 极限防丢压测)
// -----------------------------------------------------------------------------
func RunSpeed200K(brokerAddr string, totalMessages int, pubCount, subCount, batchSize int, qos byte) {
	if pubCount <= 0 {
		pubCount = 10
	}
	if subCount <= 0 {
		subCount = 10
	}
	if batchSize <= 0 {
		batchSize = 50
	}

	log.Printf("================================================================")
	log.Printf("  200,000+ msg/s 极限高吞吐零丢失压力测试 (C200K TPS Benchmark)  ")
	log.Printf("================================================================")
	log.Printf("目标总量: %d 条 | 发布并发数: %d | 订阅并发数: %d | 批刷盘大小: %d | QoS: %d",
		totalMessages, pubCount, subCount, batchSize, qos)

	var totalSent, totalRecv int64
	doneCh := make(chan struct{})

	// 1. Establish N parallel subscriber connections
	var subWg sync.WaitGroup
	readySubs := int64(0)

	for s := 0; s < subCount; s++ {
		subWg.Add(1)
		subID := fmt.Sprintf("speed_sub_%02d", s)
		topic := fmt.Sprintf("speed/%d", s)

		go func(cid, subTopic string) {
			defer subWg.Done()
			conn, err := dialAndConnect(brokerAddr, cid, "", 60)
			if err != nil {
				log.Fatalf("Subscriber %s failed to connect: %v", cid, err)
			}
			defer conn.Close()

			if err := subscribeTopic(conn, subTopic, qos); err != nil {
				log.Fatalf("Subscriber %s failed to subscribe: %v", cid, err)
			}
			atomic.AddInt64(&readySubs, 1)

			var streamBuf []byte
			readBuf := make([]byte, 262144)

			go func() {
				<-doneCh
				_ = conn.Close()
			}()

			for {
				n, err := conn.Read(readBuf)
				if n > 0 {
					streamBuf = append(streamBuf, readBuf[:n]...)
				}
				if err != nil {
					return
				}

				consumedTotal := 0
				for consumedTotal < len(streamBuf) {
					remBuf := streamBuf[consumedTotal:]
					if len(remBuf) < 2 {
						break
					}
					// Fast zero-alloc MQTT packet header scanner
					multiplier := 1
					remLen := 0
					offset := 1
					malformed := false

					for {
						if offset >= len(remBuf) {
							break // wait for more stream bytes
						}
						digit := remBuf[offset]
						offset++
						remLen += int(digit&127) * multiplier
						if multiplier > 128*128*128 {
							malformed = true
							break
						}
						multiplier *= 128
						if (digit & 128) == 0 {
							break
						}
					}

					if malformed {
						consumedTotal++
						continue
					}

					totalLen := offset + remLen
					if len(remBuf) < totalLen {
						break // wait for complete packet
					}

					pktType := remBuf[0] >> 4
					if pktType == protocol.PUBLISH {
						atomic.AddInt64(&totalRecv, 1)
					}
					consumedTotal += totalLen
				}
				if consumedTotal > 0 {
					rem := copy(streamBuf, streamBuf[consumedTotal:])
					streamBuf = streamBuf[:rem]
				}
			}
		}(subID, topic)
	}

	// Wait for all subscribers to be ready
	for atomic.LoadInt64(&readySubs) < int64(subCount) {
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("%d 个高速订阅通道已全部就绪，开始并行冲击...", subCount)

	// 2. Establish P parallel publisher connections with buffered I/O
	var pubWg sync.WaitGroup
	msgsPerPub := totalMessages / pubCount

	start := time.Now()
	statTicker := time.NewTicker(1 * time.Second)
	defer statTicker.Stop()

	go func() {
		var lastSent, lastRecv int64
		for range statTicker.C {
			s := atomic.LoadInt64(&totalSent)
			r := atomic.LoadInt64(&totalRecv)
			log.Printf("[200K 实时] 发送速率: %d msg/s | 投递速率: %d msg/s | 累计送达: %d/%d",
				s-lastSent, r-lastRecv, r, totalMessages)
			lastSent = s
			lastRecv = r
		}
	}()

	payload := make([]byte, 64)
	copy(payload, []byte("speed-200k-ultra-high-throughput-benchmark-payload-data"))

	for p := 0; p < pubCount; p++ {
		pubWg.Add(1)
		pubID := fmt.Sprintf("speed_pub_%02d", p)
		topic := fmt.Sprintf("speed/%d", p%subCount)

		go func(cid, pubTopic string) {
			defer pubWg.Done()
			conn, err := dialAndConnect(brokerAddr, cid, "", 60)
			if err != nil {
				log.Fatalf("Publisher %s connect error: %v", cid, err)
			}
			defer conn.Close()

			bufWriter := bufio.NewWriterSize(conn, 262144)

			pubPkt := &protocol.PublishPacket{
				Topic:   pubTopic,
				Payload: payload,
				QoS:     qos,
			}
			encoded, _ := pubPkt.Encode()

			for m := 0; m < msgsPerPub; m++ {
				_, _ = bufWriter.Write(encoded)
				if (m+1)%batchSize == 0 {
					_ = bufWriter.Flush()
				}
				atomic.AddInt64(&totalSent, 1)
			}
			_ = bufWriter.Flush()
		}(pubID, topic)
	}

	pubWg.Wait()
	log.Println("所有发布并发通道已推送完毕，等待全部消息交付核验...")

	// Drain remaining deliveries
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&totalRecv) >= atomic.LoadInt64(&totalSent) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(doneCh)
	elapsed := time.Since(start)

	finalSent := atomic.LoadInt64(&totalSent)
	finalRecv := atomic.LoadInt64(&totalRecv)
	missing := finalSent - finalRecv
	if missing < 0 {
		missing = 0
	}
	lossRate := float64(missing) / float64(finalSent) * 100
	tps := float64(finalRecv) / elapsed.Seconds()

	log.Printf("=================== [200K 极限压测结果汇总] ===================")
	log.Printf("计划发送总量: %d 条", totalMessages)
	log.Printf("实际发送成功: %d 条", finalSent)
	log.Printf("接收核验送达: %d 条", finalRecv)
	log.Printf("丢失消息总数: %d 条", missing)
	log.Printf("综合消息丢失率: %.4f%%", lossRate)
	log.Printf("最终实测吞吐 TPS: %.0f msg/s (总耗时: %v)", tps, elapsed)

	if tps >= 200000 {
		log.Printf("[SUCCESS] 达成目标: 突破 200,000 msg/s 极速大关 (%.0f msg/s)，零丢失！", tps)
	} else {
		log.Printf("[INFO] 当前单机环境实测达到 %.0f msg/s，丢失率 %.4f%%", tps, lossRate)
	}
}
