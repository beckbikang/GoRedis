package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

//定义session
// cmd, err := session.ReadCommand()
// session.WriteReply(reply)
// session.Write(reply.Bytes())
type Session struct {
	net.Conn
	rd *bufio.Reader
}

//新建一个session
func NewSession(conn net.Conn) *Session {
	return &Session{
		Conn: conn,
		rd:   bufio.NewReader(conn),
	}
}

//"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
//*3$3SET$3eat$13I want to eat\r\n 标准协议
func (s *Session) ReadCommand() (Command, error) {
	//读取*
	// Read ( *<number of arguments> CR LF )
	if err := s.skipByte('*'); err != nil { // io.EOF
		return nil, err
	}
	//参数的个数
	// number of arguments
	argCount, err := s.readInt()
	if err != nil {
		return nil, err
	}
	//定义参数的存储数据
	args := make([][]byte, argCount)
	for i := 0; i < argCount; i++ {
		// Read ( $<number of bytes of argument 1> CR LF )
		if err := s.skipByte('$'); err != nil {
			return nil, err
		}
		//读取数据的长度
		var argSize int
		argSize, err = s.readInt()
		if err != nil {
			return nil, err
		}
		//读取命令
		// Read ( <argument data> CR LF )
		args[i] = make([]byte, argSize)
		_, err = io.ReadFull(s, args[i])
		if err != nil {
			return nil, err
		}
		//跳过一些命令
		err = s.skipBytes([]byte{CR, LF})
		if err != nil {
			return nil, err
		}
	}
	return Command(args), nil
}

//读取字节
func (s *Session) Read(p []byte) (int, error) {
	return s.rd.Read(p)
}

//写入字节
func (s *Session) WriteReply(r Reply) (int, error) {
	return s.Write(r.Bytes())
}

//读取一个byte，判断是否是需要的协议信息
func (s *Session) skipByte(c byte) (err error) {
	var tmp byte
	tmp, err = s.rd.ReadByte()
	if err != nil {
		return
	}
	if tmp != c {
		err = errors.New(fmt.Sprintf("Illegal Byte [%d] != [%d]", tmp, c))
	}
	return
}
//跳过多个byte
func (s *Session) skipBytes(bs []byte) error {
	for _, c := range bs {
		if err := s.skipByte(c); err != nil {
			return err
		}
	}
	return nil
}

//获取一行的命令
func (s *Session) readLine() (line []byte, err error) {
	line, err = s.rd.ReadSlice(LF)
	if err == bufio.ErrBufferFull {
		return nil, errors.New("line too long")
	}
	if err != nil {
		return
	}
	i := len(line) - 2
	if i < 0 || line[i] != CR {
		err = errors.New("bad line terminator:" + string(line))
	}
	return line[:i], nil
}

//读取int
func (s *Session) readInt() (int, error) {
	if line, err := s.readLine(); err == nil {
		return strconv.Atoi(string(line))
	} else {
		return 0, err
	}
}
