package redis

import (
	"bytes"
	"encoding/json"
)
//字节数组数组
type Command [][]byte

/**
“*”号作为第一个字符，跟着一个数字代表元素个数，后面一个CRLF。
每个元素是一个RESP类型

字符串数组

**/
func (c Command) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteByte('*')
	//命令的数量
	argCount := len(c)
	buf.WriteString(itoa(argCount)) //<number of arguments>
	buf.WriteString(CRLF)
	for i := 0; i < argCount; i++ {
		buf.WriteByte('$')//字符串块
		buf.WriteString(itoa(len(c[i]))) //<number of bytes of argument i>
		buf.WriteString(CRLF)
		buf.Write(c[i]) //<argument data>
		buf.WriteString(CRLF)
	}
	return buf.Bytes()
}

//返回命令的string
func (c Command) String() string {
	//获取command的长度的数组
	arr := make([]string, len(c))
	for i := range c {
		arr[i] = string(c[i])
	}
	//加密
	b, _ := json.Marshal(arr)
	return string(b)
}
