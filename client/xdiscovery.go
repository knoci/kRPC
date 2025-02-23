package client

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type RegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
}

const defaultUpdateTimeout = time.Second * 10

func NewRegistryDiscovery(registerAddr string, timeout time.Duration) *RegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &RegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

// Update 更新服务发现的服务器列表。
func (d *RegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers       // 更新服务器列表
	d.lastUpdate = time.Now() // 更新最后更新时间
	return nil
}

// Refresh 从注册中心刷新服务器列表。
func (d *RegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// 如果上次更新时间加上超时时间大于当前时间，则不需要刷新
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry) // 从注册中心获取服务器列表
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	// 从响应头中解析服务器列表
	servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers)) // 重新初始化服务器列表
	for _, server := range servers {
		if strings.TrimSpace(server) != "" { // 去除空字符串
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now() // 更新最后更新时间
	return nil
}

// Get 根据选择模式获取一个服务器。
func (d *RegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil { // 刷新服务器列表
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode) // 调用 MultiServersDiscovery 获取服务器
}

// GetAll 返回所有服务器。
func (d *RegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil { // 刷新服务器列表
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll() // 调用 MultiServersDiscovery 获取所有服务器
}
