package client

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// SelectMode 定义了选择服务器的模式。
type SelectMode int

const (
	// 随机选择服务器
	RandomSelect SelectMode = iota
	// 使用轮询算法选择服务器
	RoundRobinSelect
)

// Discovery 是服务发现接口，定义了服务发现的基本操作。
type Discovery interface {
	// 刷新服务器列表（从远程注册中心获取）
	Refresh() error
	// 更新服务器列表
	Update(servers []string) error
	// 根据选择模式获取一个服务器
	Get(mode SelectMode) (string, error)
	// 获取所有服务器
	GetAll() ([]string, error)
}

// MultiServersDiscovery 是一个不依赖于注册中心的服务发现实现。
// 用户需要显式提供服务器地址。
type MultiServersDiscovery struct {
	r       *rand.Rand   // 生成随机数
	mu      sync.RWMutex // 保护以下字段
	servers []string     // 服务器列表
	index   int          // 轮询算法的当前位置
}

// NewMultiServerDiscovery 创建一个 MultiServersDiscovery 实例。
func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())), // 初始化随机数生成器
	}
	d.index = d.r.Intn(math.MaxInt32 - 1) // 初始化轮询算法的起始位置
	return d
}

// Refresh 方法在 MultiServersDiscovery 中没有实际意义，因此直接返回 nil。
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update 方法动态更新服务器列表。
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers // 更新服务器列表
	return nil
}

// Get 方法根据选择模式获取一个服务器。
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers") // 如果服务器列表为空，返回错误
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil // 随机选择一个服务器
	case RoundRobinSelect:
		s := d.servers[d.index%n]   // 使用轮询算法选择服务器
		d.index = (d.index + 1) % n // 更新轮询算法的当前位置
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode") // 不支持的选择模式
	}
}

// GetAll 方法返回所有服务器。
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	// 返回服务器列表的副本
	servers := make([]string, len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
