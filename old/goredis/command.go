// Copyright 2013 Latermoon. All rights reserved.

package goredis

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// Command表示一个客户端指令
type Command struct {
	args  [][]byte
	attrs map[string]interface{}
}

func NewCommand(args ...[]byte) (cmd *Command) {
	cmd = &Command{
		args:  args,
		attrs: make(map[string]interface{}),
	}
	return
}

func (cmd *Command) SetAttribute(name string, v interface{}) {
	cmd.attrs[name] = v
}

func (cmd *Command) GetAttribute(name string) interface{} {
	return cmd.attrs[name]
}

// 大写的指令名称
func (cmd *Command) Name() string {
	return string(bytes.ToUpper(cmd.args[0]))
}

// 原始数据
func (cmd *Command) Args() [][]byte {
	return cmd.args
}

func (cmd *Command) StringAtIndex(i int) string {
	if i >= cmd.Len() {
		return ""
	}
	return string(cmd.args[i])
}

func (cmd *Command) ArgAtIndex(i int) (arg []byte, err error) {
	if i >= cmd.Len() {
		err = errors.New(fmt.Sprintf("out of range %d/%d", i, cmd.Len()))
		return
	}
	arg = cmd.args[i]
	return
}

func (cmd *Command) IntAtIndex(i int) (n int, err error) {
	var f float64
	if f, err = cmd.FloatAtIndex(i); err == nil {
		n = int(f)
	}
	return
}

func (cmd *Command) Int64AtIndex(i int) (n int64, err error) {
	var f float64
	if f, err = cmd.FloatAtIndex(i); err == nil {
		n = int64(f)
	}
	return
}

func (cmd *Command) FloatAtIndex(i int) (n float64, err error) {
	if i >= cmd.Len() {
		err = errors.New(fmt.Sprintf("out of range %d/%d", i, cmd.Len()))
		return
	}
	n, err = strconv.ParseFloat(string(cmd.args[i]), 64)
	return
}

func (cmd *Command) Len() int {
	return len(cmd.args)
}

// Redis协议的Command数据
/*
*<number of arguments> CR LF
$<number of bytes of argument 1> CR LF
<argument data> CR LF
...
$<number of bytes of argument N> CR LF
<argument data> CR LF
*/
func (cmd *Command) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteByte('*')
	argCount := cmd.Len()
	buf.WriteString(itoa(argCount)) //<number of arguments>
	buf.WriteString(CRLF)
	for i := 0; i < argCount; i++ {
		buf.WriteByte('$')
		argSize := len(cmd.args[i])
		buf.WriteString(itoa(argSize)) //<number of bytes of argument i>
		buf.WriteString(CRLF)
		buf.Write(cmd.args[i]) //<argument data>
		buf.WriteString(CRLF)
	}
	return buf.Bytes()
}

func ParseCommand(buf *bytes.Buffer) (*Command, error) {
	// Read ( *<number of arguments> CR LF )
	if c, err := buf.ReadByte(); c != '*' { // io.EOF
		return nil, err
	}
	// number of arguments
	line, err := buf.ReadBytes(LF)
	if err != nil {
		return nil, err
	}
	argCount, _ := strconv.Atoi(string(line[:len(line)-2]))
	args := make([][]byte, argCount)
	for i := 0; i < argCount; i++ {
		// Read ( $<number of bytes of argument 1> CR LF )
		if c, err := buf.ReadByte(); c != '$' {
			return nil, err
		}

		line, err := buf.ReadBytes(LF)
		if err != nil {
			return nil, err
		}
		argSize, _ := strconv.Atoi(string(line[:len(line)-2]))
		// Read ( <argument data> CR LF )
		args[i] = make([]byte, argSize)
		n, e2 := buf.Read(args[i])
		if n != argSize {
			return nil, errors.New("argSize too short")
		}
		if e2 != nil {
			return nil, e2
		}

		_, err = buf.ReadBytes(LF)
		if err != nil {
			return nil, err
		}
	}
	cmd := NewCommand(args...)
	return cmd, nil
}

func (cmd *Command) String() string {
	buf := bytes.Buffer{}
	for i, count := 0, cmd.Len(); i < count; i++ {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.Write(cmd.args[i])
	}
	return buf.String()
}
