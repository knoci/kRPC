package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// conn 是由构建函数传入，通常是通过 TCP 或者 Unix 建立 socket 时得到的链接实例
// dec 和 enc 对应 gob 的 Decoder 和 Encoder
// buf 是为了防止阻塞而创建的带缓冲的 Writer，一般这么做能提升性能
type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

// 类型断言，使得 GobCodec 实现了 Codec 接口
var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

// ReadHeader 从连接中读取 RPC 消息头。
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h) // 使用 gob 解码器解码消息头
}

// ReadBody 从连接中读取 RPC 消息体。
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body) // 使用 gob 解码器解码消息体
}

// Write 将 RPC 消息头和消息体写入连接。
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush() // 确保缓冲区数据被写入
		if err != nil {
			_ = c.Close() // 如果发生错误，关闭连接
		}
	}()
	if err := c.enc.Encode(h); err != nil { // 编码消息头
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil { // 编码消息体
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

// Close 关闭底层的连接。
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
