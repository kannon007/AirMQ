#!/usr/bin/env bash
# ==============================================================================
# MQTT Broker 统一自动化测试与全量回归套件 (Linux / macOS / CI)
# ==============================================================================
# 用法:
#   ./scripts/run_all_tests.sh              # 运行全量测试 (构建 + 单元 + 协议一致性 + 高吞吐压测)
#   ./scripts/run_all_tests.sh -skip-bench  # 快速模式 (跳过压测，适合提交前快速自测)
#   ./scripts/run_all_tests.sh -verbose     # 输出每个步骤的详细终端日志
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo -e "\033[36m>>> 正在启动统一测试套件 (目录: ${ROOT_DIR}) ...\033[0m"
go run ./tools/testsuite "$@"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "\n\033[32m>>> [SUCCESS] 全量自动化测试顺利通过! 可以安全发布或合并版本! <<<\033[0m\n"
else
    echo -e "\n\033[31m>>> [FAILURE] 测试未通过，请检查上方日志排查错误! <<<\033[0m\n"
fi

exit $EXIT_CODE
