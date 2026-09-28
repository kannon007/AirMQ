package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mqtt/pkg/protocol"
)

type Stats struct {
	Connected int64
	Failed    int64
	Active    int64
	PubSent   int64
	PubRecv   int64
	PingsSent int64
	PingsRecv int64
}

func main() {
	scenario := flag.String("scenario", "conn", "Benchmark scenario: 'conn' (C1000K), 'fanout' (1-to-N), 'telemetry' (N-to-N), 'shared' ($share), 'reconnect' (storm)")
	brokerAddr := flag.String("broker", "127.0.0.1:1883", "MQTT broker target address (host:port)")

	// Connection scenario options
	targetConns := flag.Int("conns", 10000, "Target total concurrent connections / clients")
	connRate := flag.Int("rate", 2000, "Connection rate (connections per second)")
	keepalive := flag.Int("keepalive", 60, "MQTT KeepAlive interval in seconds")
	localIPsStr := flag.String("local-ips", "", "Comma-separated local source IPs for C1000K multi-IP binding")

	// Throughput & Duration options
	pubRate := flag.Int("pub-rate", 100, "Message publish rate (msg/sec)")
	durationSec := flag.Int("duration", 30, "Benchmark run duration in seconds")
	workersCount := flag.Int("workers", 10, "Number of workers for shared subscription scenario")
	cycles := flag.Int("cycles", 5, "Number of flapping cycles for reconnect storm scenario")
	inflight := flag.Int("inflight", 100, "Inflight window size for loss_proof scenario backpressure")
	qos := flag.Int("qos", 1, "MQTT QoS level (0 or 1) for loss_proof scenario")
	pubsCount := flag.Int("pubs", 10, "Number of publisher connections for high-throughput tests")
	subsCount := flag.Int("subs", 10, "Number of subscriber connections for high-throughput tests")
	batchSize := flag.Int("batch", 50, "Batch size per TCP socket flush for high-throughput tests")

	flag.Parse()

	var localIPs []string
	if *localIPsStr != "" {
		for _, s := range strings.Split(*localIPsStr, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				localIPs = append(localIPs, s)
			}
		}
	}

	fmt.Println("================================================================")
	fmt.Println("         MQTT 全场景工业级压力测试套件 (C1000K & High-TPS)       ")
	fmt.Println("================================================================")
	log.Printf("目标 Broker: %s | 压测场景: [%s]", *brokerAddr, strings.ToUpper(*scenario))

	switch strings.ToLower(*scenario) {
	case "fanout":
		RunFanout(*brokerAddr, *targetConns, *pubRate, *durationSec, localIPs)

	case "telemetry":
		RunTelemetry(*brokerAddr, *targetConns, *pubRate, *durationSec, localIPs)

	case "shared":
		RunSharedSub(*brokerAddr, *workersCount, *targetConns)

	case "reconnect":
		RunReconnectStorm(*brokerAddr, *targetConns, *cycles)

	case "loss_proof":
		RunLossProof(*brokerAddr, *targetConns, *inflight, byte(*qos))

	case "speed200k":
		RunSpeed200K(*brokerAddr, *targetConns, *pubsCount, *subsCount, *batchSize, byte(*qos))

	case "conn":
		runConnHold(*brokerAddr, *targetConns, *connRate, *keepalive, localIPs)

	default:
		log.Fatalf("未知场景 '%s'，支持: conn, fanout, telemetry, shared, reconnect, loss_proof, speed200k", *scenario)
	}
}

func runConnHold(brokerAddr string, targetConns, connRate, keepalive int, localIPs []string) {
	log.Printf("[百万长连接压测] 目标并发数: %d | 握手速率: %d conns/s", targetConns, connRate)
	if len(localIPs) > 0 {
		log.Printf("启用了多源 IP 绑定 (突破 65K 端口上限): %d 个 IP", len(localIPs))
	} else {
		log.Printf("单源 IP 模式 (最大支持 ~60,000 连接，压测 100 万请使用 -local-ips)")
	}

	stats := &Stats{}

	// Real-time metrics dashboard printer
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastConnected int64

		for range ticker.C {
			currConnected := atomic.LoadInt64(&stats.Connected)
			currActive := atomic.LoadInt64(&stats.Active)
			currFailed := atomic.LoadInt64(&stats.Failed)

			rateConn := currConnected - lastConnected
			lastConnected = currConnected

			log.Printf("[Stats] 活跃连接: %d | 累计连接: %d (+%d/s) | 失败: %d",
				currActive, currConnected, rateConn, currFailed)

			if currConnected >= int64(targetConns) && currActive == currConnected {
				log.Println("[INFO] 已达到目标连接数！保持心跳保活连接中...")
			}
		}
	}()

	// Connection rate limiter
	interval := time.Duration(1e9 / connRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	ipIndex := 0

	for i := 1; i <= targetConns; i++ {
		<-ticker.C
		wg.Add(1)

		var localIP string
		if len(localIPs) > 0 {
			localIP = localIPs[ipIndex%len(localIPs)]
			ipIndex++
		}

		clientID := fmt.Sprintf("bench_%07d", i)
		go func(cid, srcIP string) {
			defer wg.Done()
			runConnClient(cid, brokerAddr, srcIP, keepalive, stats)
		}(clientID, localIP)
	}

	wg.Wait()
	select {} // Keep running
}

func runConnClient(clientID, brokerAddr, localIP string, keepalive int, stats *Stats) {
	var dialer net.Dialer
	dialer.Timeout = 5 * time.Second
	if localIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(localIP)}
	}

	conn, err := dialer.Dial("tcp", brokerAddr)
	if err != nil {
		atomic.AddInt64(&stats.Failed, 1)
		return
	}
	defer conn.Close()

	// 1. Send CONNECT
	connectPkt := buildConnectRaw(clientID, uint16(keepalive))
	if _, err := conn.Write(connectPkt); err != nil {
		atomic.AddInt64(&stats.Failed, 1)
		return
	}

	// 2. Read CONNACK (4 bytes)
	var connack [4]byte
	if _, err := io.ReadFull(conn, connack[:]); err != nil || connack[0] != 0x20 || connack[3] != 0x00 {
		atomic.AddInt64(&stats.Failed, 1)
		return
	}

	atomic.AddInt64(&stats.Connected, 1)
	atomic.AddInt64(&stats.Active, 1)
	defer atomic.AddInt64(&stats.Active, -1)

	// 3. Keepalive PING loop
	pingInterval := time.Duration(keepalive/2) * time.Second
	if pingInterval < 5*time.Second {
		pingInterval = 5 * time.Second
	}
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	pingreq := []byte{0xC0, 0x00}
	var pingresp [2]byte

	for range pingTicker.C {
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(pingreq); err != nil {
			return
		}
		atomic.AddInt64(&stats.PingsSent, 1)

		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, pingresp[:]); err != nil || pingresp[0] != 0xD0 {
			return
		}
		atomic.AddInt64(&stats.PingsRecv, 1)
	}
}

func buildConnectRaw(clientID string, keepalive uint16) []byte {
	varHeader := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, byte(keepalive >> 8), byte(keepalive & 0xFF)}

	cidBytes := []byte(clientID)
	payloadLen := 2 + len(cidBytes)
	remLen := len(varHeader) + payloadLen

	buf := make([]byte, 1+4+remLen)
	buf[0] = 0x10 // CONNECT
	offset := 1
	offset += protocol.EncodeRemainingLength(remLen, buf[offset:])

	copy(buf[offset:], varHeader)
	offset += len(varHeader)

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(cidBytes)))
	offset += 2
	copy(buf[offset:], cidBytes)
	offset += len(cidBytes)

	return buf[:offset]
}
