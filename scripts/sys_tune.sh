#!/usr/bin/env bash
# ==============================================================================
# MQTT Broker C1000K (百万并发连接) 生产环境操作系统内核优化脚本
# 适用操作系统: Linux (Ubuntu 20.04+, Debian 11+, CentOS 8+, Rocky/RHEL 8+)
# ==============================================================================

set -e

if [ "$EUID" -ne 0 ]; then
  echo "请以 root 权限运行此脚本: sudo bash $0"
  exit 1
fi

echo "================================================================="
echo "        正在应用 MQTT Broker 百万连接系统级内核参数调优...       "
echo "================================================================="

# 1. 调整系统全局最大文件描述符
echo "[1/6] 调整文件描述符上限 (File Descriptors)..."
cat << 'EOF' > /etc/security/limits.d/99-mqtt-limits.conf
* soft nofile 2097152
* hard nofile 2097152
* soft nproc 2097152
* hard nproc 2097152
root soft nofile 2097152
root hard nofile 2097152
EOF

# 2. 优化 TCP 网络栈与内核内存
echo "[2/6] 应用 sysctl 网络参数 (TCP Stack & Buffers)..."
cat << 'EOF' > /etc/sysctl.d/99-mqtt-broker.conf
# 全局文件句柄
fs.file-max = 2097152
fs.nr_open = 2097152

# TCP 接收/发送缓冲区精细化调优 (防止 100 万连接打爆内存)
# 默认读写缓冲区降为 2KB ~ 4KB，确保单个空闲 Socket 仅占约 2.5KB ~ 4KB 内存
# 100 万连接内存占用控制在 2.5GB ~ 4GB 左右
net.ipv4.tcp_rmem = 1024 2048 4096
net.ipv4.tcp_wmem = 1024 2048 4096
net.core.rmem_default = 2048
net.core.wmem_default = 2048
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216

# TCP 全局内存页限制 (4KB/页，限制最大可用 TCP 内存约 6GB ~ 12GB)
net.ipv4.tcp_mem = 786432 1048576 1572864

# 连接建立队列溢出与拥塞控制
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 65535

# 端口快速回收与临时端口范围
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15

# KeepAlive 保活参数 (快速探测死连接)
net.ipv4.tcp_keepalive_time = 300
net.ipv4.tcp_keepalive_intvl = 15
net.ipv4.tcp_keepalive_probes = 3

# 禁用 SYN Cookies (超大并发握手时避免哈希计算损耗，配合大 backlog 使用)
net.ipv4.tcp_syncookies = 1

# 拥塞控制算法选用 BBR (若内核支持)
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

# 连接跟踪表容量调优 (若启用了 iptables/firewalld)
net.netfilter.nf_conntrack_max = 1048576
net.nf_conntrack_max = 1048576
EOF

sysctl --system > /dev/null 2>&1 || true

# 3. 优化 PAM 会话限制
echo "[3/6] 配置 PAM 会话限制..."
if ! grep -q "pam_limits.so" /etc/pam.d/common-session; then
  echo "session required pam_limits.so" >> /etc/pam.d/common-session
fi

# 4. 配置 Systemd 全局服务限制
echo "[4/6] 配置 Systemd 服务文件句柄上限..."
mkdir -p /etc/systemd/system.conf.d/
cat << 'EOF' > /etc/systemd/system.conf.d/99-mqtt-nofile.conf
[Manager]
DefaultLimitNOFILE=2097152
DefaultLimitNPROC=2097152
EOF
systemctl daemon-reexec

# 5. 网卡 Ring Buffer 与中断优化
echo "[5/6] 尝试调优物理网卡 RX/TX 环形队列深度 (以主网卡为例)..."
DEFAULT_NIC=$(ip route show default 2>/dev/null | awk '/default/ {print $5}' | head -n 1)
if [ -n "$DEFAULT_NIC" ] && command -v ethtool >/dev/null 2>&1; then
  echo "发现默认网络设备: $DEFAULT_NIC，调整 Ring Buffer 至最大..."
  ethtool -G "$DEFAULT_NIC" rx 4096 tx 4096 2>/dev/null || true
fi

# 6. 压测客户端多 IP 虚拟网卡配置说明
echo "[6/6] 压测说明..."
cat << 'EOF'

=================================================================
[提示] 单机发起百万连接 (C1000K) 客户端特别注意:
  因为单个源 IP 的可用 TCP 端口受 16 位限制最大为 65,535 (实际可用约 60,000)，
  单台压测机器如果要向同一个 Broker 发起 100 万连接，必须在客户端配置多个辅助 IP:
  
  示例 (批量绑定 20 个本地虚拟 IP，每个 IP 承载 50,000 连接):
    for i in $(seq 10 30); do
      ip addr add 192.168.1.$i/24 dev eth0 label eth0:$i
    done

调优完成！建议重启系统或重新登录会话以确保所有参数生效。
=================================================================
EOF
