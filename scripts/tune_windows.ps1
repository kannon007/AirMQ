# ==============================================================================
# Windows 环境高并发 TCP 性能与端口调优脚本 (PowerShell)
# 适用: Windows 10 / 11 / Windows Server 2019 / 2022
# 需以管理员身份运行 PowerShell 执行此脚本
# ==============================================================================

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "           正在调优 Windows 高并发 TCP/IP 注册表与网络栈         " -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

# 1. 扩大动态端口范围 (从 1025 到 65535，可用端口达 64510 个)
Write-Host "[1/3] 调整 IPv4 动态端口范围 (1025 - 65535)..." -ForegroundColor Yellow
netsh int ipv4 set dynamicport tcp start=1025 num=64510
netsh int ipv6 set dynamicport tcp start=1025 num=64510

# 2. 优化 TCP 注册表参数
$tcpKey = "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"

Write-Host "[2/3] 配置 Tcpip 注册表参数 (MaxUserPort, TcpTimedWaitDelay)..." -ForegroundColor Yellow

# MaxUserPort = 65534
Set-ItemProperty -Path $tcpKey -Name "MaxUserPort" -Value 65534 -Type DWord -Force

# TcpTimedWaitDelay = 30 (缩短 TIME_WAIT 状态至 30 秒，加速端口回收)
Set-ItemProperty -Path $tcpKey -Name "TcpTimedWaitDelay" -Value 30 -Type DWord -Force

# StrictTimeWaitSeqCheck = 1 (开启严格 TIME_WAIT 序列检查)
Set-ItemProperty -Path $tcpKey -Name "StrictTimeWaitSeqCheck" -Value 1 -Type DWord -Force

# 3. 提高并发握手积压队列 (SynRetransmissions)
Set-ItemProperty -Path $tcpKey -Name "TcpMaxConnectRetransmissions" -Value 2 -Type DWord -Force

Write-Host "[3/3] 注册表修改成功！" -ForegroundColor Green
Write-Host "当前动态端口范围配置:" -ForegroundColor Gray
netsh int ipv4 show dynamicport tcp

Write-Host "`n[提示] 更改已保存，建议重启电脑或重启网络适配器使部分内核参数完全生效。" -ForegroundColor Cyan
