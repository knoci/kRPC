package codec

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
)

// JsonCodec 结构体
type JsonCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *json.Decoder
	enc  *json.Encoder
}

// 类型断言，确保 JsonCodec 实现了 Codec 接口
var _ Codec = (*JsonCodec)(nil)

// NewJsonCodec 创建一个新的 JsonCodec 实例
func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &JsonCodec{
		conn: conn,
		buf:  buf,
		dec:  json.NewDecoder(conn), // 连接conn的 gob 解码器
		enc:  json.NewEncoder(buf),  // 缓冲buf的 gob 编码器
	}
}

// ReadHeader 从连接中读取头部信息
func (c *JsonCodec) ReadHeader(h *Header) error {

	return c.dec.Decode(h)
}

// ReadBody 从连接中读取正文内容
func (c *JsonCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// Write 将头部和正文写入连接
func (c *JsonCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: json error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: json error encoding body:", err)
		return err
	}
	return nil
}

// Close 关闭连接
func (c *JsonCodec) Close() error {
	return c.conn.Close()
}
