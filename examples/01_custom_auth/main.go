package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"mqtt/core"
)

// CustomAuthHook 实现了 core.Hook 接口，用于自定义客户端认证与发布/订阅鉴权 (ACL)
type CustomAuthHook struct {
	core.BaseHook
}

func (h *CustomAuthHook) Name() string {
	return "CustomAuthHook"
}

// OnConnect 在客户端发起 CONNECT 握手时触发
// 返回:
//   - allow: 是否允许连接 (true=通过, false=拒绝)
//   - reasonCode: MQTT 返回原因码 (如 0x00 成功, 0x86 凭据无效, 0x87 未授权)
//   - err: 错误日志 (可选)
func (h *CustomAuthHook) OnConnect(ctx *core.ClientContext, pkt *core.ConnectPacket) (bool, byte, error) {
	log.Printf("[Auth] 收到连接握手: ClientID=%s, Username=%s, RemoteAddr=%s",
		pkt.ClientID, pkt.Username, ctx.RemoteAddr)

	// 1. ClientID 基础格式校验 (如必须以 "device-" 或 "admin-" 开头)
	if pkt.ClientID == "" {
		log.Printf("[Auth] 拒绝连接: ClientID 不能为空")
		return false, 0x85, fmt.Errorf("client identifier not valid")
	}

	// 2. 自定义鉴权逻辑 (模拟查询数据库或校验设备凭据 / JWT)
	// 例如：管理员账号校验
	if pkt.Username == "admin" {
		if string(pkt.Password) != "secret_admin_pass" {
			log.Printf("[Auth] 拒绝连接: 管理员密码错误")
			return false, 0x86, fmt.Errorf("bad user name or password")
		}
		log.Printf("[Auth] 认证通过: 管理员 %s", pkt.ClientID)
		return true, 0x00, nil
	}

	// 例如：物联网设备账号 (设备用户名必须以 dev_ 开头，密码与设备密钥比对)
	if strings.HasPrefix(pkt.Username, "dev_") {
		if string(pkt.Password) == "device_token_2026" {
			log.Printf("[Auth] 认证通过: 工业终端设备 %s", pkt.ClientID)
			return true, 0x00, nil
		}
	}

	log.Printf("[Auth] 拒绝连接: 未授权的用户凭据 (User: %s)", pkt.Username)
	return false, 0x86, fmt.Errorf("bad user name or password")
}

// OnAuthorize 在客户端发布消息 (PUBLISH) 或订阅主题 (SUBSCRIBE) 时触发细粒度 ACL 访问控制
func (h *CustomAuthHook) OnAuthorize(ctx *core.ClientContext, action core.AuthAction, topic string) (bool, error) {
	// 管理员拥有全部权限
	if ctx.Username == "admin" {
		return true, nil
	}

	// 终端设备权限控制：
	// 每个设备只能发布/订阅属于自己 ClientID 的主题前缀，严禁跨设备越权
	expectedPrefix := fmt.Sprintf("devices/%s/", ctx.ClientID)

	switch action {
	case core.AuthActionPublish:
		if strings.HasPrefix(topic, expectedPrefix) {
			return true, nil
		}
		log.Printf("[ACL] 越权发布拦截: Client %s 试图向未授权主题 %s 发布消息", ctx.ClientID, topic)
		return false, fmt.Errorf("permission denied for publish on topic %s", topic)

	case core.AuthActionSubscribe:
		// 允许订阅自己所属的主题树或全局公共广播
		if strings.HasPrefix(topic, expectedPrefix) || topic == "broadcast/#" {
			return true, nil
		}
		log.Printf("[ACL] 越权订阅拦截: Client %s 试图订阅未授权主题 %s", ctx.ClientID, topic)
		return false, fmt.Errorf("permission denied for subscribe on topic %s", topic)
	}

	return false, nil
}

func main() {
	port := 18881
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// 启动纯 core Broker，仅注册自定义认证钩子，无需引入 dashboard
	broker, err := core.NewBroker(
		core.WithTCP(addr),
		core.WithMemoryStore(),
		core.WithMulticore(true),
		core.WithHook(&CustomAuthHook{}), // 注入自定义认证插件
	)
	if err != nil {
		log.Fatalf("初始化 Broker 失败: %v", err)
	}

	go func() {
		log.Printf("MQTT Broker (带自定义认证) 正在监听 %s ...", addr)
		_ = broker.Start()
	}()
	time.Sleep(150 * time.Millisecond)

	// -------------------------------------------------------------------------
	// 场景 1: 合法终端设备连接 (Username: "dev_sensor_01", Password: "device_token_2026")
	// -------------------------------------------------------------------------
	log.Println("\n--- 模拟客户端测试 1: 合法设备账号凭据认证 ---")
	conn1, err := coreDial(addr)
	if err == nil {
		_ = sendConnect(conn1, "device-001", "dev_sensor_01", "device_token_2026")
		ack, _ := readConnack(conn1)
		log.Printf(">>> 客户端 1 连接结果: CONNACK ReasonCode = 0x%02X (0x00 表示认证成功通过)", ack)
		_ = conn1.Close()
	}

	// -------------------------------------------------------------------------
	// 场景 2: 非法密码攻击连接 (Password 错误)
	// -------------------------------------------------------------------------
	log.Println("\n--- 模拟客户端测试 2: 错误密码凭据被拒绝 ---")
	conn2, err := coreDial(addr)
	if err == nil {
		_ = sendConnect(conn2, "device-002", "dev_sensor_02", "WRONG_PASSWORD_HACK")
		ack, _ := readConnack(conn2)
		log.Printf(">>> 客户端 2 连接结果: CONNACK ReasonCode = 0x%02X (0x86 拒绝连接 / 凭据错误)", ack)
		_ = conn2.Close()
	}

	// -------------------------------------------------------------------------
	// 场景 3: 管理员凭据连接
	// -------------------------------------------------------------------------
	log.Println("\n--- 模拟客户端测试 3: 管理员账号凭据认证 ---")
	conn3, err := coreDial(addr)
	if err == nil {
		_ = sendConnect(conn3, "admin-console", "admin", "secret_admin_pass")
		ack, _ := readConnack(conn3)
		log.Printf(">>> 客户端 3 连接结果: CONNACK ReasonCode = 0x%02X (0x00 管理员登录成功)", ack)
		_ = conn3.Close()
	}

	time.Sleep(100 * time.Millisecond)
	_ = broker.Stop(context.Background())
}

func coreDial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 1*time.Second)
}

func sendConnect(c net.Conn, clientID, user, pass string) error {
	var body []byte
	// Protocol Name "MQTT"
	body = append(body, 0x00, 0x04, 'M', 'Q', 'T', 'T')
	// Protocol Level 4 (3.1.1)
	body = append(body, 0x04)
	// Connect Flags (CleanSession + User + Password)
	flags := byte(0x02)
	if user != "" {
		flags |= 0x80
	}
	if pass != "" {
		flags |= 0x40
	}
	body = append(body, flags)
	// KeepAlive 60s
	body = append(body, 0x00, 0x3C)
	// Payload: ClientID
	body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
	body = append(body, []byte(clientID)...)
	// Payload: Username
	if user != "" {
		body = append(body, byte(len(user)>>8), byte(len(user)))
		body = append(body, []byte(user)...)
	}
	// Payload: Password
	if pass != "" {
		body = append(body, byte(len(pass)>>8), byte(len(pass)))
		body = append(body, []byte(pass)...)
	}

	header := []byte{0x10, byte(len(body))}
	_, err := c.Write(append(header, body...))
	return err
}

func readConnack(c net.Conn) (byte, error) {
	_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 4)
	_, err := c.Read(buf)
	if err != nil {
		return 0xFF, err
	}
	// byte 3 is return code
	return buf[3], nil
}
