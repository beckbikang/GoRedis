package server

import (
	. "github.com/latermoon/GoRedis/redis"
)

// http://redis.io/commands#string
//获取数据
func (s *GoRedisServer) OnGET(r ReplyWriter, c Command) {
	val, err := s.db.Get(c[1])
	if err != nil {
		r.WriteReply(ErrorReply(err.Error()))
	} else {
		r.WriteReply(BulkReply(val))
	}
}

//设置数据
func (s *GoRedisServer) OnSET(r ReplyWriter, c Command) {
	err := s.db.Set(c[1], c[2])
	if err != nil {
		r.WriteReply(ErrorReply(err.Error()))
	} else {
		r.WriteReply(StatusReply("OK"))
	}
}
