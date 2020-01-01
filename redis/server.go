package redis

import (
	"errors"
	"fmt"
	"log"
	"net"
)

// handler = &server.GoRedisServer{}
// lis, err := net.Listen("tcp", "localhost:6380")
// if err != nil {
//     panic(err)
// }
// redis.Serve(lis, handler)

func Register(handler ServerHandler) { DefaultServer.Register(handler) }

func Serve(lis net.Listener) error { return DefaultServer.Serve(lis) }

//默认的服务器
var DefaultServer = NewServer()

//定义一个接口
type ServerHandler interface {
	SessionOpened(*Session)//打开session
	SessoinClosed(*Session, error)//关闭session
	RecvCommand(*Session, Command)//接收命令
}

//定义服务
type Server struct {
	handler ServerHandler
}

//空服务
func NewServer() *Server {
	return &Server{}
}

//注册服务
func (s *Server) Register(handler ServerHandler) {
	s.handler = handler
}

//监听服务
func (s *Server) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("redis.Serve: accept:", err.Error())
			return err
		}
		//处理每个请求
		go s.ServeSession(NewSession(conn))
	}

	return nil
}

//针对session的处理
func (s *Server) ServeSession(session *Session) {

	//
	defer func() {
		//关闭服务
		session.Close()
		if v := recover(); v != nil {
			//定义一个error
			err, ok := v.(error)
			if !ok {
				err = errors.New(fmt.Sprint(v))
			}
			//关闭session
			s.handler.SessoinClosed(session, err)
		}
		//关闭session
		s.handler.SessoinClosed(session, nil)
	}()

	//打开session
	s.handler.SessionOpened(session)

	for {
		//读取命令
		cmd, err := session.ReadCommand()
		if err != nil {
			break
		}
		//接收命令
		s.handler.RecvCommand(session, cmd)
	}
}
