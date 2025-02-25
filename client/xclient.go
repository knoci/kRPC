package client

import (
	"context"
	"kRPC/server"
	"reflect"
	"sync"
)

// XClient 是一个分布式 RPC 客户端，通过服务发现机制动态选择服务器。
type XClient struct {
	d       Discovery          // 服务发现接口
	mode    SelectMode         // 选择模式（随机选择或轮询选择）
	opt     *server.Option     // RPC 客户端选项
	mu      sync.Mutex         // 保护以下字段
	clients map[string]*Client // 存储已连接的 RPC 客户端
}

// NewXClient 创建一个新的 XClient 实例。
func NewXClient(d Discovery, mode SelectMode, opt *server.Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client),
	}
}

// Close 关闭所有已连接的 RPC 客户端。
func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		// 忽略关闭时的错误
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

// dial 尝试连接到指定的 RPC 服务器地址。
func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	client, ok := xc.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		// 如果客户端不可用，关闭并移除
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	if client == nil {
		// 如果尚未连接，创建新的客户端
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

// call 在指定的 RPC 服务器上执行调用。
func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// Call 在一个合适的服务器上执行调用。
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode) // 根据选择模式获取服务器地址
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast 在所有注册的服务器上广播调用。
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll() // 获取所有服务器地址
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex // 保护 e 和 replyDone
	var e error
	replyDone := reply == nil // 如果 reply 为 nil，则不需要设置值
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // 确保在函数返回时调用 cancel
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				// 如果 reply 不为 nil，克隆 reply 以避免并发写入
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel() // 如果任何调用失败，取消未完成的调用
			}
			if err == nil && !replyDone {
				// 如果调用成功且 reply 不为 nil，设置 reply 的值
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
