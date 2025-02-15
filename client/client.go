package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kRPC/codec"
	"kRPC/server"
	"log"
	"net"
	"sync"
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

func (call *Call) done() {
	call.Done <- call
}

// Client 表示一个RPC客户端
// 单个客户端可以关联多个未完成调用，且支持多协程并发使用
type Client struct {
	cc       codec.Codec      // 编解码器
	opt      *server.Option   // 协议选项
	sending  sync.Mutex       // 发送锁，保证请求原子性（保护header）
	header   codec.Header     // 请求头（复用减少内存分配）
	mu       sync.Mutex       // 全局锁（保护以下字段）
	seq      uint64           // 请求序号生成器（原子递增）
	pending  map[uint64]*Call // 未完成调用映射表
	closing  bool             // 用户主动关闭标记
	shutdown bool             // 服务端要求关闭标记
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close 实现io.Closer接口
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
	f := codec.NewCodecFuncMap[opt.CodecType]
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

// Dial 连接到指定RPC服务器（网络层建立）
func Dial(network, address string, opts ...*server.Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	// 安全关闭处理（defer优化）
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
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

	// 复用header减少内存分配
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

// Call 同步调用入口（阻塞直到完成）
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
