package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kRPC/codec"
	"kRPC/service"
	"log"
	"net"
	"net/http"
	"reflect"
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

type Option struct {
	MagicNumber    int // MagicNumber 用于标识这是一个RPC请求
	CodecType      codec.Type
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// request stores all information of a call
type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and replyv of request
	mtype        *service.MethodType
	svc          *service.Service
}

// 一个简单的RPC服务器结构体
type Server struct {
	serviceMap sync.Map
}

// 创建一个新的RPC服务器实例
func NewServer() *Server {
	return &Server{}
}

// 默认的RPC服务器实例，方便直接使用
var DefaultServer = NewServer()

// Accept 监听网络连接，并为每个新连接启动一个协程来处理请求
func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// ServeConn 处理单个客户端连接。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serveCodec(f(conn))
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec 处理来自客户端的RPC请求
func (server *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)  // wait until all request are handled
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, DefaultOption.ConnectTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// 从连接中读取RPC请求的头部和参数
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.NewArgv()
	req.replyv = req.mtype.NewReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

// 将响应写入连接
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 处理RPC请求
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.Call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

// Register 将接收者的公共方法注册到服务器中。
func (server *Server) Register(rcvr interface{}) error {
	s := service.NewService(rcvr) // 创建一个服务实例
	// 使用 sync.Map 的 LoadOrStore 方法将服务注册到服务映射中
	// 如果服务已存在，LoadOrStore 返回已存在的服务实例，并将 dup 设置为 true
	if _, dup := server.serviceMap.LoadOrStore(s.Name, s); dup {
		return errors.New("rpc: service already defined: " + s.Name) // 如果服务已注册，返回错误
	}
	return nil // 注册成功返回 nil
}

// Register 将接收者的公共方法注册到默认服务器（DefaultServer）中。
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr) // 调用 DefaultServer 的 Register 方法
}

// 通过 ServiceMethod 从 serviceMap 中找到对应的 service
func (server *Server) findService(serviceMethod string) (svc *service.Service, mtype *service.MethodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service.Service)
	mtype = svc.Method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// ServeHTTP 实现了 http.Handler 接口，用于处理 HTTP 请求。
// 它只支持 HTTP 的 CONNECT 方法，用于建立 RPC 连接。
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		// 如果请求方法不是 CONNECT，返回 405 Method Not Allowed
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	// 使用 Hijacker 接管连接
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}

	// 向客户端发送连接成功的 HTTP 响应
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")

	// 调用 ServeConn 方法处理 RPC 请求
	server.ServeConn(conn)
}

// HandleHTTP 注册一个 HTTP 处理器，用于处理默认路径上的 RPC 请求。
// 它将 RPC 请求转发到 `ServeHTTP` 方法。
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server) // 注册默认的 RPC 路径
}

// HandleHTTP 是一个便捷方法，用于默认服务器注册 HTTP 处理器。
func HandleHTTP() {
	DefaultServer.HandleHTTP() // 调用默认服务器的 HandleHTTP 方法
}
