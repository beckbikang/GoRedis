package goredis_server

import (
	. "GoRedis/goredis"
)

// SADD key member [member ...]
// Add one or more members to a set
func (server *GoRedisServer) OnSADD(cmd *Command) (reply *Reply) {
	key := cmd.StringAtIndex(1)
	members := cmd.Args()[2:]
	hash := server.levelRedis.GetSet(key)
	// 使用leveldb的mset，无法得知新增key数量
	fieldVals := make([][]byte, 0, len(members)*2)
	for _, member := range members {
		fieldVals = append(fieldVals, member, []byte(""))
	}
	n := hash.Set(fieldVals...)
	return IntegerReply(n)
}

func (server *GoRedisServer) OnSCARD(cmd *Command) (reply *Reply) {
	key := cmd.StringAtIndex(1)
	hash := server.levelRedis.GetSet(key)
	n := hash.Count()
	return IntegerReply(n)
}

func (server *GoRedisServer) OnSISMEMBER(cmd *Command) (reply *Reply) {
	key := cmd.StringAtIndex(1)
	member, _ := cmd.ArgAtIndex(2)
	hash := server.levelRedis.GetSet(key)
	if hash.Exist(member) {
		reply = IntegerReply(1)
	} else {
		reply = IntegerReply(0)
	}
	return
}

func (server *GoRedisServer) OnSMEMBERS(cmd *Command) (reply *Reply) {
	key := cmd.StringAtIndex(1)
	hash := server.levelRedis.GetSet(key)
	elems := hash.GetAll(1000)
	keys := make([]interface{}, 0, len(elems))
	for _, elem := range elems {
		keys = append(keys, elem.Key)
	}
	reply = MultiBulksReply(keys)
	return
}

func (server *GoRedisServer) OnSREM(cmd *Command) (reply *Reply) {
	key := cmd.StringAtIndex(1)
	members := cmd.Args()[2:]
	hash := server.levelRedis.GetSet(key)
	n := hash.Remove(members...)
	return IntegerReply(n)
}
