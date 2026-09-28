# ==============================================================================
# MQTT Broker 统一自动化测试与全量回归套件 (Windows PowerShell)
# ==============================================================================
# 用法:
#   .\scripts\run_all_tests.ps1              # 运行全量测试 (构建 + 单元 + 协议一致性 + 高吞吐压测)
#   .\scripts\run_all_tests.ps1 -SkipBench   # 快速模式 (跳过压测，适合提交前快速自测)
#   .\scripts\run_all_tests.ps1 -Verbose     # 输出每个步骤的详细终端日志
# ==============================================================================

param(
    [switch]$SkipBench = $false,
    [switch]$Verbose = $false
)

$rootDir = Split-Path -Parent $PSScriptRoot
Set-Location $rootDir

$argsList = @("run", "./tools/testsuite")
if ($SkipBench) {
    $argsList += "-skip-bench"
}
if ($Verbose) {
    $argsList += "-verbose"
}

Write-Host ">>> 正在启动统一测试套件 (目录: $rootDir) ..." -ForegroundColor Cyan
& go $argsList
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`n>>> [SUCCESS] 全量自动化测试顺利通过! 可以安全发布或合并版本! <<<`n" -ForegroundColor Green
} else {
    Write-Host "`n>>> [FAILURE] 测试未通过，请检查上方日志排查错误! <<<`n" -ForegroundColor Red
}

exit $exitCode
