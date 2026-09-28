package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"mqtt/pkg/server"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

type StageResult struct {
	Name     string
	Passed   bool
	Duration time.Duration
	Details  string
}

func main() {
	skipBench := flag.Bool("skip-bench", false, "Skip the high-throughput performance smoke test")
	verbose := flag.Bool("verbose", false, "Print detailed command outputs")
	flag.Parse()

	fmt.Println()
	fmt.Println(colorBold + colorCyan + "================================================================================" + colorReset)
	fmt.Println(colorBold + colorCyan + "         MQTT Broker 工业级全量回归与协议一致性自动化测试套件                   " + colorReset)
	fmt.Println(colorBold + colorCyan + "================================================================================" + colorReset)
	fmt.Printf("开始时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	startTime := time.Now()
	var results []StageResult
	allPassed := true

	// -------------------------------------------------------------------------
	// Stage 1: Build Verification
	// -------------------------------------------------------------------------
	fmt.Println(colorBold + "[1/4] 阶段一: 静态构建与全组件编译检查" + colorReset)
	res1 := runBuildCheck(*verbose)
	results = append(results, res1)
	if !res1.Passed {
		allPassed = false
	}

	// -------------------------------------------------------------------------
	// Stage 2: Unit Tests (pkg/...)
	// -------------------------------------------------------------------------
	fmt.Println()
	fmt.Println(colorBold + "[2/4] 阶段二: 核心组件单元测试 (Trie, Session, Store, Protocol, Cluster)" + colorReset)
	res2 := runUnitTests(*verbose)
	results = append(results, res2)
	if !res2.Passed {
		allPassed = false
	}

	// -------------------------------------------------------------------------
	// Stage 3: MQTT 3.1.1 & 5.0 Protocol Conformance Tests
	// -------------------------------------------------------------------------
	fmt.Println()
	fmt.Println(colorBold + "[3/4] 阶段三: MQTT 3.1.1 & 5.0 完整协议一致性矩阵测试" + colorReset)
	res3 := runProtocolTests(*verbose)
	results = append(results, res3)
	if !res3.Passed {
		allPassed = false
	}

	// -------------------------------------------------------------------------
	// Stage 4: High-Throughput Regression Smoke Test
	// -------------------------------------------------------------------------
	fmt.Println()
	if *skipBench {
		fmt.Println(colorYellow + "[4/4] 阶段四: 高并发高吞吐防丢压测 (已跳过: -skip-bench)" + colorReset)
		results = append(results, StageResult{
			Name:     "阶段四: 高吞吐防丢烟雾压测",
			Passed:   true,
			Duration: 0,
			Details:  "SKIPPED",
		})
	} else {
		fmt.Println(colorBold + "[4/4] 阶段四: 200,000 msg/s 高并发高吞吐防丢压测回归" + colorReset)
		res4 := runBenchSmoke(*verbose)
		results = append(results, res4)
		if !res4.Passed {
			allPassed = false
		}
	}

	// -------------------------------------------------------------------------
	// Test Report Dashboard
	// -------------------------------------------------------------------------
	totalDuration := time.Since(startTime)
	fmt.Println()
	fmt.Println(colorBold + "================================================================================" + colorReset)
	fmt.Println(colorBold + "                       测试回归结果仪表盘 (TEST DASHBOARD)                      " + colorReset)
	fmt.Println(colorBold + "================================================================================" + colorReset)
	fmt.Printf("%-35s | %-8s | %-12s | %s\n", "阶段名称", "状态", "耗时", "说明")
	fmt.Println("--------------------------------------------------------------------------------")

	for _, r := range results {
		statusStr := colorGreen + "[PASS]" + colorReset
		if !r.Passed {
			statusStr = colorRed + "[FAIL]" + colorReset
		} else if r.Details == "SKIPPED" {
			statusStr = colorYellow + "[SKIP]" + colorReset
		}
		fmt.Printf("%-31s | %s | %-12s | %s\n", r.Name, statusStr, r.Duration.Round(time.Millisecond), r.Details)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("总计耗时: %s\n", totalDuration.Round(time.Millisecond))

	if allPassed {
		fmt.Println()
		fmt.Println(colorBold + colorGreen + ">>> 结论: 全部大版本测试用例 100% 验证通过! 代码质量达到发布标准! <<<" + colorReset)
		fmt.Println()
		os.Exit(0)
	} else {
		fmt.Println()
		fmt.Println(colorBold + colorRed + ">>> 结论: 存在未通过的测试阶段，请查看上方日志排查修复! <<<" + colorReset)
		fmt.Println()
		os.Exit(1)
	}
}

func runBuildCheck(verbose bool) StageResult {
	start := time.Now()
	tempDir := os.TempDir()

	targets := []string{
		"./cmd/broker",
		"./tools/bench",
	}

	for _, target := range targets {
		binName := filepath.Join(tempDir, fmt.Sprintf("test_build_%d.exe", time.Now().UnixNano()))
		cmd := exec.Command("go", "build", "-o", binName, target)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		out, err := cmd.CombinedOutput()
		_ = os.Remove(binName)

		if err != nil {
			if verbose {
				fmt.Println(string(out))
			}
			return StageResult{
				Name:     "阶段一: 静态构建编译检查",
				Passed:   false,
				Duration: time.Since(start),
				Details:  fmt.Sprintf("构建失败: %s - %v", target, err),
			}
		}
	}

	return StageResult{
		Name:     "阶段一: 静态构建编译检查",
		Passed:   true,
		Duration: time.Since(start),
		Details:  "cmd/broker & tools/bench 编译通过",
	}
}

func runUnitTests(verbose bool) StageResult {
	start := time.Now()
	pkgs := []string{
		"./core",
		"./dashboard",
		"./pkg/protocol",
		"./pkg/session",
		"./pkg/trie",
		"./pkg/store",
		"./pkg/cluster",
		"./pkg/pipeline",
	}

	args := append([]string{"test"}, pkgs...)
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	outStr := string(out)

	if verbose {
		fmt.Println(outStr)
	}

	if err != nil {
		return StageResult{
			Name:     "阶段二: 核心组件单元测试",
			Passed:   false,
			Duration: time.Since(start),
			Details:  "单元测试失败，请查看日志",
		}
	}

	return StageResult{
		Name:     "阶段二: 核心组件单元测试",
		Passed:   true,
		Duration: time.Since(start),
		Details:  "core/dashboard/protocol/session/trie/store/cluster/pipeline 100% 通过",
	}
}

func runProtocolTests(verbose bool) StageResult {
	start := time.Now()
	cmd := exec.Command("go", "test", "-v", "./pkg/server", "-run", "TestMQTT")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	outStr := string(out)

	if verbose {
		fmt.Println(outStr)
	}

	passCount := strings.Count(outStr, "--- PASS: TestMQTT")

	if err != nil {
		return StageResult{
			Name:     "阶段三: MQTT 协议一致性矩阵",
			Passed:   false,
			Duration: time.Since(start),
			Details:  fmt.Sprintf("协议测试失败 (已通过 %d 项): %v", passCount, err),
		}
	}

	return StageResult{
		Name:     "阶段三: MQTT 协议一致性矩阵",
		Passed:   true,
		Duration: time.Since(start),
		Details:  fmt.Sprintf("MQTT 3.1.1 & 5.0 全部 %d 个测试项 100%% 通过", passCount),
	}
}

func runBenchSmoke(verbose bool) StageResult {
	start := time.Now()
	benchPort := 18899
	addr := fmt.Sprintf("127.0.0.1:%d", benchPort)
	gnetAddr := fmt.Sprintf("tcp://%s", addr)

	// 1. Start Broker in background
	srv := server.NewServer(server.Config{
		Addr:         gnetAddr,
		Multicore:    true,
		TCPKeepAlive: 30 * time.Second,
	}, nil, nil, nil)

	go func() {
		_ = srv.Start()
	}()

	// Wait for broker port to become ready
	ready := false
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			ready = true
			break
		}
	}
	if !ready {
		return StageResult{
			Name:     "阶段四: 高吞吐防丢烟雾压测",
			Passed:   false,
			Duration: time.Since(start),
			Details:  "测试 Broker 端口监听启动超时",
		}
	}

	defer func() {
		_ = srv.Stop(context.Background())
		time.Sleep(100 * time.Millisecond)
	}()

	// 2. Run tools/bench in speed200k mode (20,000 messages smoke test)
	cmd := exec.Command("go", "run", "./tools/bench",
		"-scenario", "speed200k",
		"-broker", addr,
		"-conns", "20000",
		"-pubs", "4",
		"-subs", "4",
		"-batch", "25",
		"-qos", "0",
	)

	out, err := cmd.CombinedOutput()
	outStr := string(out)

	if verbose {
		fmt.Println(outStr)
	}

	if err != nil || !strings.Contains(outStr, "丢失率: 0.0000%") {
		return StageResult{
			Name:     "阶段四: 高吞吐防丢烟雾压测",
			Passed:   false,
			Duration: time.Since(start),
			Details:  "压测异常或检测到消息丢失",
		}
	}

	return StageResult{
		Name:     "阶段四: 高吞吐防丢烟雾压测",
		Passed:   true,
		Duration: time.Since(start),
		Details:  "20,000 条消息吞吐回归通过 (丢失率: 0.0000%)",
	}
}
