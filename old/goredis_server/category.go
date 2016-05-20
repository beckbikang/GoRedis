package goredis_server

// 指令集信息
import (
	"strings"
)

// 指令集分类
type CCate string

const (
	CCateKey         CCate = "key"
	CCateString      CCate = "string"
	CCateHash        CCate = "hash"
	CCateList        CCate = "list"
	CCateSet         CCate = "set"
	CCateSortedSet   CCate = "zset"
	CCatePubSub      CCate = "pubsub" // nouse
	CCateTransaction CCate = "trans"  // nouse
	CCateScript      CCate = "script" // nouse
	CCateConnection  CCate = "conn"
	CCateServer      CCate = "server"
	CCateUnknown     CCate = "unknown"
)

var CommandCategoryList = []CCate{CCateKey, CCateString, CCateHash, CCateList, CCateSet, CCateSortedSet, CCateConnection, CCateServer, CCateUnknown}

// 指令集命令列表
var ccatemaplist = map[CCate]string{
	CCateKey:         "DEL,DUMP,EXISTS,EXPIRE,EXPIREAT,KEYS,MIGRATE,MOVE,OBJECT,PERSIST,PEXPIRE,PEXPIREAT,PTTL,RANDOMKEY,RENAME,RENAMENX,RESTORE,SORT,TTL,TYPE",
	CCateString:      "APPEND,BITCOUNT,BITOP,DECR,DECRBY,GET,GETBIT,GETRANGE,GETSET,INCR,INCRBY,INCRBYFLOAT,MGET,MSET,MSETNX,PSETEX,SET,SETBIT,SETEX,SETNX,SETRANGE,STRLEN",
	CCateHash:        "HDEL,HEXISTS,HGET,HGETALL,HINCRBY,HINCRBYFLOAT,HKEYS,HLEN,HMGET,HMSET,HSET,HSETNX,HVALS",
	CCateList:        "BLPOP,BRPOP,BRPOPLPUSH,LINDEX,LINSERT,LLEN,LPOP,LPUSH,LPUSHX,LRANGE,LREM,LSET,LTRIM,RPOP,RPOPLRUSH,RPUSH,RPUSHX",
	CCateSet:         "SADD,SCARD,SDIFF,SDIFFSTORE,SINTER,SINTERSTORE,SISMEMBER,SMEMBERS,SMOVE,SPOP,SRANDMEMBER,SREM,SUNION,SUNIONSTORE",
	CCateSortedSet:   "ZADD,ZCARD,ZCOUNT,ZINCRBY,ZINTERSTORE,ZRANGE,ZRANGEBYSCORE,ZRANK,ZREM,ZREMRANGEBYRANK,ZREMRANGEBYSCORE,ZREVRANGE,ZREVRANGEBYSCORE,ZREVRANK,ZSCORE,ZUNIONSTORE",
	CCatePubSub:      "PSUBSCRIBE,PUBSUB,PUBLISH,PUNSUBSCRIBE,SUBSCRIBE,UNSUBSCRIBE",
	CCateTransaction: "DISCARD,EXEC,MULTI,UNWATCH,WATCH",
	CCateScript:      "EVAL,EVALSHA,SCRIPT",
	CCateConnection:  "AUTH,ECHO,PING,QUIT,SELECT",
	CCateServer:      "BGREWRITEAOF,BGSAVE,CLIENT,CONFIG,DBSIZE,DEBUG,FLUSHALL,FLUSHDB,INFO,LASTSAVE,MONITOR,SAVE,SHUTDOWM,SLAVEOF,SLOWLOG,SYNC,TIME",
}

// 需要同步到从库的命令 RAW_SET,RAW_SET_NOREPLY
var syncCmdlist = "DEL,EXPIRE,PERSIST,PEXPIRE,PEXPIREAT,RENAME,RENAMENX,SORT,APPEND,DECR,DECRBY,INCR,INCRBY,INCRBYFLOAT,MSET,MSETNX,PSETEX,SET,SETBIT,SETEX,SETNX,SETRANGE,HDEL,HINCRBY,HINCRBYFLOAT,HMSET,HSET,HSETNX,BLPOP,BRPOP,BRPOPLPUSH,LINSERT,LPOP,LPUSH,LPUSHX,LREM,LSET,LTRIM,RPOP,RPOPLRUSH,RPUSH,RPUSHX,SADD,SDIFFSTORE,SINTERSTORE,SMOVE,SPOP,SREM,SUNIONSTORE,ZADD,ZINCRBY,ZINTERSTORE,ZREM,ZREMRANGEBYRANK,ZREMRANGEBYSCORE,ZREVRANK,ZUNIONSTORE"

// 存放指令类别
var ccatemap map[string]CCate
var synccmds map[string]bool

func init() {
	// ccatemap["GET"] = "string"
	ccatemap = make(map[string]CCate)
	for cate, cmdlist := range ccatemaplist {
		cmds := strings.Split(cmdlist, ",")
		for _, cmd := range cmds {
			ccatemap[cmd] = cate
		}
	}
	// synccmds
	synccmds = make(map[string]bool)
	cmds := strings.Split(syncCmdlist, ",")
	for _, cmd := range cmds {
		synccmds[cmd] = true
	}
}

// 获取指令类别，传入大写cmd
func commandCategory(cmd string) CCate {
	if cate, ok := ccatemap[cmd]; ok {
		return cate
	} else {
		return CCateUnknown
	}
}

// 判断指令是否需要同步
// 要求cmd大写
func needSync(cmd string) bool {
	return synccmds[cmd]
}

func NeedSync(cmd string) bool {
	return needSync(cmd)
}
