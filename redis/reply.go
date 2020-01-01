package redis

import (
	"bytes"
	"encoding/json"
)

//返回类型，实现了Bytes接口
type Reply interface {
	Bytes() []byte
}

/**
redis的返回：
	1 状态
	2 error
	3 int
	4 bulk
	5 multiBulk

**/
// Redis Replies
type StatusReply string
type ErrorReply string
type IntegerReply int
type BulkReply []byte
type MultiBulkReply []interface{} // interface{} can be int/string/[]byte

//状态回复
func (r StatusReply) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString("+")
	buf.WriteString(string(r))
	buf.WriteString(CRLF)
	return buf.Bytes()
}

//error回复
func (r ErrorReply) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString("-")
	buf.WriteString(string(r))
	buf.WriteString(CRLF)
	return buf.Bytes()
}
//整数
func (r IntegerReply) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString(":")
	buf.WriteString(itoa(int(r)))
	buf.WriteString(CRLF)
	return buf.Bytes()
}

//多个字符
func (r BulkReply) Bytes() []byte {
	if r == nil {
		return []byte("$-1\r\n") // NULL Bulk Reply
	}
	buf := bytes.Buffer{}
	buf.WriteString("$")
	buf.WriteString(itoa(len(r)))
	buf.WriteString(CRLF)
	buf.Write(r)
	buf.WriteString(CRLF)
	return buf.Bytes()
}

//多个命令回复
func (r MultiBulkReply) Bytes() []byte {
	//空
	if r == nil {
		return []byte("*-1\r\n") // Null Multi Bulk Reply
	}
	if len(r) == 0 {
		return []byte("*0\r\n") // Empty Multi Bulk Reply
	}
	//写入多条命令
	buf := bytes.Buffer{}
	buf.WriteString("*")
	buf.WriteString(itoa(len(r)))
	buf.WriteString(CRLF)
	for _, bulk := range r {
		switch bulk.(type) {
		case string:
			buf.WriteString("$")
			b := []byte(bulk.(string))
			buf.WriteString(itoa(len(b)))
			buf.WriteString(CRLF)
			buf.Write(b)
		case []byte:
			b := bulk.([]byte)
			if b == nil {
				buf.WriteString("$-1")
			} else {
				buf.WriteString("$")
				buf.WriteString(itoa(len(b)))
				buf.WriteString(CRLF)
				buf.Write(b)
			}
		case int:
			buf.WriteString(":")
			buf.WriteString(itoa(bulk.(int)))
		case nil:
			// nil element
			buf.WriteString("$-1")
		default:
			// as json json压缩
			b, err := json.Marshal(bulk)
			if err != nil {
				buf.WriteString("$")
				b = []byte(err.Error())
				buf.WriteString(itoa(len(b)))
				buf.WriteString(CRLF)
				buf.Write(b)
			} else {
				buf.WriteString("$")
				buf.WriteString(itoa(len(b)))
				buf.WriteString(CRLF)
				buf.Write(b)
			}
		}
		buf.WriteString(CRLF)
	}
	return buf.Bytes()
}
