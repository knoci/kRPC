package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kRPC/codec"
	"kRPC/server"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	connected        = "200 Connected to Gee RPC" // HTTP 连接成功的响应消息
	defaultRPCPath   = "/_kprc_"                  // 默认的 RPC 请求路径
	defaultDebugPath = "/debug/krpc"              // 默认的调试路径
	MagicNumber      = 0x3bef5c
)

// Call 代表一个活跃的RPC调用
type Call struct {
	Seq           uint64      // 调用唯一序号
	ServiceMethod string      // 格式 "<服务名>.<方法名>"
	Args          interface{} // 函数参数
	Reply         interface{} // 函数返回结果
	Error         error       // 调用错误信息
	Done          chan *Call  // 调用完成通知通道
}

// 把当前Call放入Done管道
func (call *Call) done() {
	call.Done <- call
}

// Client 表示一个RPC客户端
// 单个客户端可以关联多个未完成调用，且支持多协程并发使用
type Client struct {
	cc       codec.Codec      // 编解码器
	opt      *server.Option   // 协议选项
	sending  sync.Mutex       // 发送锁，保证请求原子性
	header   codec.Header     // 请求头（复用减少内存分配）
	mu       sync.Mutex       // 全局锁（保护以下字段）
	seq      uint64           // 请求序号生成器（原子递增）
	pending  map[uint64]*Call // 未完成调用映射表，键是编号，值是 Call 实例。
	closing  bool             // 用户主动关闭标记
	shutdown bool             // 服务端要求关闭标记
}

var _ io.Closer = (*Client)(nil)

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *server.Option) (client *Client, err error)

var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 客户端是否可用
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 注册RPC调用到待处理队列
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 从待处理队列移除调用（性能优化点：快速删除）
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 终止所有未完成调用（异常处理）
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending { // 线性遍历优化点：大表可分批处理
		call.Error = err
		call.done()
	}
}

// 接收响应协程（核心事件循环）
func (client *Client) receive() {
	var err error
	for err == nil { // 持续处理响应
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil: // 写部分失败后的无效响应
			err = client.cc.ReadBody(nil)
		case h.Error != "": // 服务端返回错误
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default: // 正常处理响应体
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// 发生错误时终止所有调用
	client.terminateCalls(err)
}

// 创建客户端（连接协商阶段）
func NewClient(conn net.Conn, opt *server.Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType] // 对应类型的CodecFunc接收conn返回特定类型Codec实例
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	// 发送协议选项（优化点：缓冲写入）
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// 初始化客户端编解码器
func newClientCodec(cc codec.Codec, opt *server.Option) *Client {
	client := &Client{
		seq:     1, // 序号从1开始，0表示无效调用
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive() // 启动接收协程
	return client
}

// 解析选项（默认值处理）
func parseOptions(opts ...*server.Option) (*server.Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return server.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("too many args")
	}
	opt := opts[0]
	opt.MagicNumber = server.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = server.DefaultOption.CodecType
	}
	return opt, nil
}

// 发送请求（核心发送逻辑）
func (client *Client) send(call *Call) {
	client.sending.Lock()         // 保证请求原子性
	defer client.sending.Unlock() // 确保锁释放

	// 注册调用（含锁操作）
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备回复的头部
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码并发送请求（核心IO操作）
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil { // 部分写入失败处理
			call.Error = err
			call.done()
		}
	}
}

// Go 异步调用入口（非阻塞）
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10) // 缓冲通道优化
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel need cache")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// dialTimeout 尝试连接到指定的网络地址，并创建一个 RPC 客户端。
// 如果连接超时，则返回错误。
func dialTimeout(f newClientFunc, network, address string, opts ...*server.Option) (client *Client, err error) {
	// 解析选项
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	// 尝试建立连接，带超时
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	// 如果客户端创建失败，关闭连接
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	// 创建一个通道用于接收客户端创建的结果
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt) // 调用传入的客户端创建函数
		ch <- clientResult{client: client, err: err}
	}()

	// 如果没有设置连接超时，则直接等待结果
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}

	// 使用 select 等待超时或客户端创建完成
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial 连接到指定的 RPC 服务器地址。
// Dial 是 dialTimeout 的一个封装，使用默认的 NewClient 函数。
func Dial(network, address string, opts ...*server.Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// Call 调用指定的服务方法，并等待调用完成。
// 如果调用失败，返回错误。
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// 发起调用，并等待完成
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done(): // 如果上下文完成或超时
		client.removeCall(call.Seq) // 移除调用
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done: // 等待调用完成
		return call.Error
	}
}

// NewHTTPClient 通过 HTTP 协议创建一个新的 RPC 客户端实例。
func NewHTTPClient(conn net.Conn, opt *server.Option) (*Client, error) {
	// 向服务器发送 CONNECT 请求，尝试建立 RPC 连接
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// 读取 HTTP 响应，确保连接成功
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt) // 如果连接成功，创建 RPC 客户端
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status) // 如果响应状态不匹配，返回错误
	}
	return nil, err
}

// DialHTTP 连接到一个监听在默认 HTTP RPC 路径上的 HTTP RPC 服务器。
func DialHTTP(network, address string, opts ...*server.Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...) // 使用 NewHTTPClient 创建客户端
}

// XDial 根据 rpcAddr 的协议部分调用不同的函数来连接到 RPC 服务器。
// rpcAddr 是一个通用格式（protocol@addr），用于表示 RPC 服务器的地址。
// 示例：
//
//	http@10.0.0.1:7001
//	tcp@10.0.0.1:9999
//	unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*server.Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1] // 分离协议和地址
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...) // 对于 HTTP 协议，调用 DialHTTP
	default:
		// 其他协议（如 tcp、unix 等）
		return Dial(protocol, addr, opts...) // 调用 Dial
	}
}
