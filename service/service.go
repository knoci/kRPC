package service

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// MethodType 用于描述一个方法的元信息，包括方法本身、参数类型、返回类型以及调用次数。
type MethodType struct {
	method    reflect.Method // 方法的反射信息
	ArgType   reflect.Type   // 方法的参数类型
	ReplyType reflect.Type   // 方法的返回类型
	numCalls  uint64         // 方法被调用的次数
}

// NumCalls 返回方法被调用的次数。
func (m *MethodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls) // 使用原子操作确保线程安全
}

// newArgv 创建一个新的参数值，支持指针类型和值类型。
func (m *MethodType) NewArgv() reflect.Value {
	if m.ArgType.Kind() == reflect.Ptr {
		return reflect.New(m.ArgType.Elem()) // 如果是引用类型，创建一个指向底层类型的指针
	}
	return reflect.New(m.ArgType).Elem() // 如果是值类型，直接创建值
}

// newReplyv 创建一个新的返回值，支持初始化复杂类型（如 map 和 slice）。
func (m *MethodType) NewReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem())) // 初始化空 map
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0)) // 初始化空 slice
	}
	return replyv
}

// Service 表示一个 RPC 服务，包含服务名称、类型、接收者以及方法映射。
type Service struct {
	Name   string                 // 服务名称
	typ    reflect.Type           // 服务类型
	Rcvr   reflect.Value          // 服务的接收者（对象实例）
	Method map[string]*MethodType // 方法映射，键为方法名
}

// newService 创建一个新的 RPC 服务实例，并注册其中的方法。
func NewService(rcvr interface{}) *Service {
	s := &Service{
		Rcvr: reflect.ValueOf(rcvr),
		Name: reflect.Indirect(reflect.ValueOf(rcvr)).Type().Name(),
		typ:  reflect.TypeOf(rcvr),
	}
	if !isExportedOrBuiltinType(s.typ) {
		log.Fatalf("rpc server: %s is not a valid Service name", s.Name)
	}
	s.registerMethods() // 注册服务中的方法
	return s
}

// registerMethods 遍历服务类型的所有方法，并注册符合条件的方法。
func (s *Service) registerMethods() {
	s.Method = make(map[string]*MethodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 检查方法签名是否符合 RPC 方法的要求：2 个参数（接收者除外）和 1 个返回值，且返回值为 error 类型
		if mType.NumIn() != 3 || mType.NumOut() != 1 || mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue // 参数和返回类型必须是导出的或内置类型
		}
		s.Method[method.Name] = &MethodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.Name, method.Name)
	}
}

// isExportedOrBuiltinType 检查一个类型是否是导出的或内置类型。
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// call 调用一个注册的方法，并返回错误（如果有）。
func (s *Service) Call(m *MethodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1) // 增加方法调用次数
	returnValues := m.method.Func.Call([]reflect.Value{s.Rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error) // 如果返回值是非 nil 的 error，返回错误
	}
	return nil
}
