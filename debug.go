package main

import (
	"fmt"
	"html/template"
	"kRPC/server"
	"log"
	"net/http"
)

const (
	connected        = "200 Connected to Gee RPC" // HTTP 连接成功的响应消息
	defaultRPCPath   = "/_kprc_"                  // 默认的 RPC 请求路径
	defaultDebugPath = "/debug/krpc"              // 默认的调试路径
	MagicNumber      = 0x3bef5c
)

// debugText 是一个 HTML 模板，用于展示 RPC 服务的调试信息。
const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

// debug 是一个解析好的 HTML 模板，用于渲染调试页面。
var debug = template.Must(template.New("RPC debug").Parse(debugText))

// debugHTTP 是一个 HTTP 处理器，用于展示 RPC 服务的调试信息。
type debugHTTP struct {
	*sever.Server // 包含 Server 的指针
}

// debugService 是一个结构体，用于存储服务的调试信息。
type debugService struct {
	Name   string                      // 服务名称
	Method map[string]*krpc.MethodType // 方法及其调用统计
}

// ServeHTTP 是 debugHTTP 的 HTTP 处理方法，用于处理调试请求。
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 构建一个排序后的服务数据列表
	var services []debugService
	server.serviceMap.Range(func(namei, svci interface{}) bool {
		svc := svci.(*service)
		services = append(services, debugService{
			Name:   namei.(string),
			Method: svc.method,
		})
		return true
	})
	// 使用模板渲染调试页面
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}

// HandleHTTP 注册 HTTP 处理器，用于处理 RPC 请求和调试请求。
func (server *server.Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)              // 注册 RPC 请求处理器
	http.Handle(defaultDebugPath, debugHTTP{server}) // 注册调试请求处理器
	log.Println("rpc server debug path:", defaultDebugPath)
}
