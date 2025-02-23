package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Registry 是一个简单的注册中心，提供以下功能：
// 1. 添加服务器并接收心跳以保持其活跃状态。
// 2. 同时返回所有活跃的服务器，并同步删除已死亡的服务器。
type Registry struct {
	timeout time.Duration          // 心跳超时时间
	mu      sync.Mutex             // 保护以下字段
	servers map[string]*ServerItem // 存储服务器信息
}

// ServerItem 表示一个服务器的信息。
type ServerItem struct {
	Addr  string    // 服务器地址
	start time.Time // 服务器注册时间
}

const (
	defaultPath    = "/_geerpc_/registry" // 默认的注册路径
	defaultTimeout = time.Minute * 5      // 默认的心跳超时时间（5分钟）
)

// New 创建一个带有超时设置的注册中心实例。
func New(timeout time.Duration) *Registry {
	return &Registry{
		servers: make(map[string]*ServerItem), // 初始化服务器映射
		timeout: timeout,                      // 设置心跳超时时间
	}
}

// DefaultGeeRegister 是一个默认的注册中心实例，使用默认的超时时间。
var DefaultGeeRegister = New(defaultTimeout)

// putServer 添加或更新一个服务器到注册中心。
func (r *Registry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()} // 新增服务器
	} else {
		s.start = time.Now() // 如果服务器已存在，更新其启动时间以保持活跃
	}
}

// aliveServers 返回所有活跃的服务器地址。
func (r *Registry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) { // 检查是否超时
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr) // 删除超时的服务器
		}
	}
	sort.Strings(alive) // 对活跃服务器地址进行排序
	return alive
}

// ServeHTTP 实现了 http.Handler 接口，用于处理注册中心的 HTTP 请求。
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// 返回所有活跃的服务器地址，通过 HTTP 头部返回
		w.Header().Set("X-krpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		// 注册一个服务器，服务器地址通过 HTTP 头部传递
		addr := req.Header.Get("X-krpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError) // 如果地址为空，返回 500 错误
			return
		}
		r.putServer(addr) // 添加或更新服务器
	default:
		w.WriteHeader(http.StatusMethodNotAllowed) // 不支持的 HTTP 方法
	}
}

// HandleHTTP 注册一个 HTTP 处理器，用于处理注册中心的消息。
func (r *Registry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)                    // 注册 HTTP 处理器
	log.Println("rpc registry path:", registryPath) // 打印注册路径
}

// HandleHTTP 是一个便捷方法，用于默认注册中心实例注册 HTTP 处理器。
func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath) // 调用默认注册中心的 HandleHTTP 方法
}

// Heartbeat 定期向注册中心发送心跳消息。
// 它是一个辅助函数，用于服务器注册或发送心跳。
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 确保在被注册中心移除之前有足够的时间发送心跳。
		// 默认心跳间隔为超时时间减去 1 分钟。
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr) // 首次发送心跳
	go func() {
		t := time.NewTicker(duration) // 创建一个定时器
		for err == nil {
			<-t.C                               // 等待下一次心跳时间
			err = sendHeartbeat(registry, addr) // 发送心跳
		}
	}()
}

// sendHeartbeat 向注册中心发送心跳消息。
func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}                     // 创建 HTTP 客户端
	req, _ := http.NewRequest("POST", registry, nil) // 创建 POST 请求
	req.Header.Set("X-Geerpc-Server", addr)          // 设置服务器地址
	if _, err := httpClient.Do(req); err != nil {    // 发送请求
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
