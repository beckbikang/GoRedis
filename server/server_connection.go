package server

import (
	. "github.com/latermoon/GoRedis/redis"
)

// http://redis.io/commands#connection
//连接处理
func (s *GoRedisServer) OnPING(r ReplyWriter, c Command) {
	r.WriteReply(StatusReply("PONG"))
}
