package main

import (
	"context"
	"kRPC/client"
	"kRPC/registry"
	"kRPC/server"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

// startRegistry 启动一个注册中心服务。
func startRegistry(wg *sync.WaitGroup) {
	l, _ := net.Listen("tcp", ":9999") // 监听 9999 端口
	registry.HandleHTTP()              // 注册 HTTP 处理器
	wg.Done()                          // 通知等待组
	_ = http.Serve(l, nil)             // 启动 HTTP 服务
}

// startServer 启动一个 RPC 服务器，并向注册中心发送心跳。
func startServer(registryAddr string, wg *sync.WaitGroup) {
	var foo Foo                                                   // 定义一个 RPC 服务
	l, _ := net.Listen("tcp", ":0")                               // 监听一个随机端口
	server := server.NewServer()                                  // 创建 RPC 服务器
	_ = server.Register(&foo)                                     // 注册服务
	registry.Heartbeat(registryAddr, "tcp@"+l.Addr().String(), 0) // 向注册中心发送心跳
	wg.Done()                                                     // 通知等待组
	server.Accept(l)                                              // 接受 RPC 请求
}

func foo(xc *client.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = xc.Broadcast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

// call 发送 RPC 请求到注册中心发现的服务器。
func call(registry string) {
	d := client.NewRegistryDiscovery(registry, 0)        // 创建服务发现实例
	xc := client.NewXClient(d, client.RandomSelect, nil) // 创建 XClient 实例
	defer func() { _ = xc.Close() }()                    // 确保关闭 XClient
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i}) // 发送 RPC 请求
		}(i)
	}
	wg.Wait()
}

// broadcast 广播 RPC 请求到所有注册的服务器。
func broadcast(registry string) {
	d := client.NewRegistryDiscovery(registry, 0)        // 创建服务发现实例
	xc := client.NewXClient(d, client.RandomSelect, nil) // 创建 XClient 实例
	defer func() { _ = xc.Close() }()                    // 确保关闭 XClient
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcast", "Foo.Sum", &Args{Num1: i, Num2: i * i}) // 发送广播调用
			// 预期 2-5 秒超时
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i}) // 发送带超时的 RPC 请求
		}(i)
	}
	wg.Wait()
}

// main 是程序的入口。
func main() {
	log.SetFlags(0)                                         // 设置日志格式
	registryAddr := "http://localhost:9999/_krpc_/registry" // 注册中心地址
	var wg sync.WaitGroup
	wg.Add(1)
	go startRegistry(&wg) // 启动注册中心
	wg.Wait()

	time.Sleep(time.Second) // 等待注册中心启动
	wg.Add(2)
	go startServer(registryAddr, &wg) // 启动两个 RPC 服务器
	go startServer(registryAddr, &wg)
	wg.Wait()

	time.Sleep(time.Second) // 等待服务器启动
	call(registryAddr)      // 发送 RPC 请求
	broadcast(registryAddr) // 发送广播调用
}
