package server

import (
	. "github.com/latermoon/GoRedis/redis"
	"github.com/latermoon/GoRedis/rocks"
	"log"
	"reflect"
	"strings"
)

//redis的server
type GoRedisServer struct {
	ServerHandler//处理接口
	db      *rocks.DB//rocks.db
	cmdfunc map[string]HandlerFunc//处理器
}

func New(db *rocks.DB) *GoRedisServer {
	s := &GoRedisServer{db: db}
	s.registerCmdFunc()
	return s
}

//打开
func (s *GoRedisServer) SessionOpened(sess *Session) {
	log.Println("connection accepted from", sess.RemoteAddr())
}

//关闭
func (s *GoRedisServer) SessoinClosed(sess *Session, err error) {
	log.Println("end connection", sess.RemoteAddr(), err)
}

//接收
func (s *GoRedisServer) RecvCommand(sess *Session, c Command) {
	log.Println("command:", c)

	// invoke On[Command] functions
	cmdname := strings.ToUpper(string(c[0]))
	//获取函数
	cmdFunc, ok := s.cmdfunc[cmdname]
	if !ok {
		sess.WriteReply(ErrorReply("Handler Not Found"))
	}
	//调用写入
	cmdFunc(sess, c)
}

//注册处理函数
// register all On[Comamd Name] functions,
// such as OnPING/OnGET/OnSET, into HandlerFunc map
func (s *GoRedisServer) registerCmdFunc() {
	s.cmdfunc = make(map[string]HandlerFunc)
	//注册val，type
	objval := reflect.ValueOf(s)
	objtyp := reflect.TypeOf(s)

	//注册调用函数
	for i := 0; i < objtyp.NumMethod(); i++ {
		name := objtyp.Method(i).Name
		if len(name) > 2 && strings.HasPrefix(name, "On") {
			//
			// tricks
			func(name string, method reflect.Value) {
				//注册函数
				s.cmdfunc[name] = HandlerFunc(func(r ReplyWriter, c Command) {
					//接收的值是，value，命令行
					in := []reflect.Value{reflect.ValueOf(r), reflect.ValueOf(c)}
					method.Call(in)
				})
			}(strings.ToUpper(name[2:]), objval.Method(i))
		}
	}
}

//回复接口
type ReplyWriter interface {
	WriteReply(Reply) (int, error)
}

//处理函数
type HandlerFunc func(ReplyWriter, Command)

func (f HandlerFunc) Serve(r ReplyWriter, c Command) {
	f(r, c)
}
