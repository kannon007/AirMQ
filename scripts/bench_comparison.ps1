# scripts/bench_comparison.ps1
# Comprehensive Performance Benchmark: Baseline vs Full Features (Metrics + Cluster + Limiter)

param(
    [int]$TotalMessages = 200000
)

$rootDir = Split-Path -Parent $PSScriptRoot
Set-Location $rootDir

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "        MQTT Broker 架构演进性能影响与全链路压测对比 (Benchmark Suite)         " -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "测试硬件 CPU: 12th Gen Intel Core i5-12450H (12 Cores)" -ForegroundColor Gray
Write-Host "测试单次消息量: $TotalMessages 条 (10 Publishers, 10 Subscribers, Batch=50)`n" -ForegroundColor Gray

# Ensure binaries
& go build -o broker.exe ./cmd/broker
& go build -o bench.exe ./tools/bench

# -----------------------------------------------------------------------------
# TEST 1: Single Node Baseline (No Metrics, No Cluster)
# -----------------------------------------------------------------------------
Write-Host "--------------------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host ">>> [测试 1/3] 单机极速基准 (Baseline: 无监控、无集群、纯内存转发)" -ForegroundColor Yellow
Write-Host "--------------------------------------------------------------------------------" -ForegroundColor Yellow

$proc1 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1883 -store=memory" -PassThru -NoNewWindow
Start-Sleep -Seconds 1

& .\bench.exe -scenario speed200k -conns 200000 -pubs 10 -subs 10 -batch 50 -qos 0
Stop-Process -Id $proc1.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800

# -----------------------------------------------------------------------------
# TEST 2: Full Production Features (Prometheus Metrics + Limiter + Cluster Mesh)
# -----------------------------------------------------------------------------
Write-Host "`n--------------------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host ">>> [测试 2/3] 全功能生产模式 (Full Features: 启用 Prometheus /metrics + 限流 + 集群 Mesh)" -ForegroundColor Yellow
Write-Host "--------------------------------------------------------------------------------" -ForegroundColor Yellow

$proc2 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1883 -metrics-addr=:8080 -cluster-node=node-1 -cluster-listen=127.0.0.1:19991 -store=memory" -PassThru -NoNewWindow
Start-Sleep -Seconds 1

& .\bench.exe -scenario speed200k -conns 200000 -pubs 10 -subs 10 -batch 50 -qos 0


# Scrape metrics to verify Prometheus is actively counting in real-time
Write-Host "`n[Prometheus 指标采样核验]:" -ForegroundColor Cyan
try {
    $resp = Invoke-WebRequest -Uri "http://127.0.0.1:8080/metrics" -UseBasicParsing -TimeoutSec 2
    $metricsLines = $resp.Content -split "`n" | Where-Object { $_ -match "mqtt_messages_(received|sent)_total" }
    foreach ($line in $metricsLines) {
        Write-Host "  $line" -ForegroundColor Green
    }
} catch {
    Write-Host "  [WARN] Failed to scrape metrics: $_" -ForegroundColor Red
}

Stop-Process -Id $proc2.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800

# -----------------------------------------------------------------------------
# TEST 3: Fanout Broadcast Latency (1-to-50 Subscribers, 10,000 messages)
# -----------------------------------------------------------------------------
Write-Host "`n--------------------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host ">>> [测试 3/4] 广播风暴与延迟分布测试 (Fan-Out: 1-to-50 订阅者, 1,000 msg/s, 持续 5 秒)" -ForegroundColor Yellow
Write-Host "--------------------------------------------------------------------------------" -ForegroundColor Yellow

$proc3 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1883 -metrics-addr=:8080 -cluster-node=node-1 -cluster-listen=127.0.0.1:19991 -store=memory" -PassThru -NoNewWindow
Start-Sleep -Seconds 1

& .\bench.exe -scenario=fanout -conns=50 -pub-rate=1000 -duration=5
Stop-Process -Id $proc3.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800

# -----------------------------------------------------------------------------
# TEST 4: Industrial Streaming Pipeline (RingBuffer + Disk Spool + Egress Worker)
# -----------------------------------------------------------------------------
Write-Host "`n--------------------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host ">>> [测试 4/4] 工业级流式投递管道模式 (Pipeline: Sharded RingBuffer + 磁盘削峰 + 批量投递)" -ForegroundColor Yellow
Write-Host "--------------------------------------------------------------------------------" -ForegroundColor Yellow

$proc4 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1883 -pipeline-enable -pipeline-sink=stdout -pipeline-spool-dir=./test_pipeline_spool -store=memory" -PassThru -NoNewWindow
Start-Sleep -Seconds 1

& .\bench.exe -scenario speed200k -conns 200000 -pubs 10 -subs 10 -batch 50 -qos 0
Stop-Process -Id $proc4.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800
Remove-Item -Recurse -Force "./test_pipeline_spool" -ErrorAction SilentlyContinue

Write-Host "`n================================================================================" -ForegroundColor Cyan
Write-Host "                           性能影响对比评估测试完毕                              " -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
