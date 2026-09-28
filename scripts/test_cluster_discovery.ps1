# scripts/test_cluster_discovery.ps1
# E2E Multi-Process Dynamic Cluster Discovery Test for Local Windows Environment

param(
    [switch]$KeepRunning = $false
)

$rootDir = Split-Path -Parent $PSScriptRoot
Set-Location $rootDir

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "   MQTT Broker Decentralized Dynamic Discovery Multi-Process Test (Windows)    " -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

# 1. Build broker.exe
Write-Host "`n[1/4] Building broker.exe..." -ForegroundColor Yellow
& go build -o broker.exe ./cmd/broker
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Build failed!" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] broker.exe build ready." -ForegroundColor Green

# 2. Launch 3 separate Broker processes locally
Write-Host "`n[2/4] Starting 3 independent Broker nodes locally..." -ForegroundColor Yellow
Write-Host "  - Node-1: MQTT :1883 | Cluster RPC :19991 (Seed Node)" -ForegroundColor Gray
Write-Host "  - Node-2: MQTT :1884 | Cluster RPC :19992 (Seed -> Node-1)" -ForegroundColor Gray
Write-Host "  - Node-3: MQTT :1885 | Cluster RPC :19993 (Seed -> Node-2 ONLY, no Node-1 config)" -ForegroundColor Gray

$p1 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1883 -cluster-node=node-1 -cluster-listen=127.0.0.1:19991 -store=memory" -PassThru -NoNewWindow
Start-Sleep -Milliseconds 400

$p2 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1884 -cluster-node=node-2 -cluster-listen=127.0.0.1:19992 -cluster-seeds=127.0.0.1:19991 -store=memory" -PassThru -NoNewWindow
Start-Sleep -Milliseconds 400

$p3 = Start-Process -FilePath ".\broker.exe" -ArgumentList "-addr=tcp://127.0.0.1:1885 -cluster-node=node-3 -cluster-listen=127.0.0.1:19993 -cluster-seeds=127.0.0.1:19992 -store=memory" -PassThru -NoNewWindow

Write-Host "`n[3/4] Waiting 2 seconds for PEX gossip topology convergence..." -ForegroundColor Yellow
Start-Sleep -Seconds 2

# 3. Run automated tests
Write-Host "`n[4/4] Executing E2E cluster discovery validation..." -ForegroundColor Yellow
& go test -v ./pkg/cluster -run "TestCluster_ThreeNode_DynamicDiscovery"
$clusterRes = $LASTEXITCODE

& go test -v ./pkg/server -run "TestServer_Cluster_DynamicDiscovery_FullMesh_E2E"
$serverRes = $LASTEXITCODE

# 4. Cleanup or KeepRunning
if (-not $KeepRunning) {
    Write-Host "`nShutting down background Broker processes..." -ForegroundColor Gray
    Stop-Process -Id $p1.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $p2.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $p3.Id -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "`n[INFO] KeepRunning specified. Broker processes are kept alive (PIDs: $($p1.Id), $($p2.Id), $($p3.Id))." -ForegroundColor Magenta
}

Write-Host "`n================================================================================" -ForegroundColor Cyan
if ($clusterRes -eq 0 -and $serverRes -eq 0) {
    Write-Host "[SUCCESS] Decentralized Dynamic Discovery & Full-Mesh E2E PASS 100%!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "[FAILURE] Discovery tests failed, please check output above." -ForegroundColor Red
    exit 1
}
