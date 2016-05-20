package goredis_server

import (
	. "GoRedis/goredis"
	"GoRedis/libs/levelredis"
	"GoRedis/libs/stat"
	"GoRedis/libs/stdlog"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (server *GoRedisServer) Init() (err error) {

	server.initSignalNotify()

	err = server.initLevelDB()
	if err != nil {
		return
	}
	err = server.initSyncLog()
	if err != nil {
		return
	}
	server.config = NewConfig(server.levelRedis, PREFIX+"config:")
	// monitor
	server.initCommandMonitor(server.opt.LogPath() + "/cmd.log")
	server.initCommandCounterLog("string", []string{"GET", "SET", "MGET", "MSET", "INCR", "DECR", "INCRBY", "DECRBY"})
	server.initCommandCounterLog("hash", []string{"HGETALL", "HGET", "HSET", "HDEL", "HMGET", "HMSET", "HINCRBY", "HLEN"})
	server.initCommandCounterLog("set", []string{"SADD", "SCARD", "SISMEMBER", "SMEMBERS", "SREM"})
	server.initCommandCounterLog("list", []string{"LPUSH", "RPUSH", "LPOP", "RPOP", "LINDEX", "LLEN", "LRANGE", "LTRIM"})
	server.initCommandCounterLog("zset", []string{"ZADD", "ZCARD", "ZSCORE", "ZINCRBY", "ZRANGE", "ZRANGEBYSCORE", "ZRANK", "ZREM", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZREVRANK"})
	server.initSeqLog(server.opt.LogPath() + "/seq.log")
	server.initLeveldbIOLog(server.opt.LogPath() + "/leveldb.io.log")
	server.initLeveldbStatsLog(server.opt.LogPath() + "/leveldb.stats.log")
	server.initExecLog(server.opt.LogPath() + "/exec.time.log")
	server.initSlowlog(server.opt.LogPath() + "/slow.log")
	stdlog.Printf("init uid %s\n", server.UID())
	server.initSlaveOf()
	return
}

// 关闭服务器前执行
func (server *GoRedisServer) DeferClosing(fn func()) {
	server.rwlock.Lock()
	defer server.rwlock.Unlock()
	server.closingFunc.PushBack(fn)
}

// 处理退出事件
func (server *GoRedisServer) initSignalNotify() {
	server.sigs = make(chan os.Signal, 1)
	signal.Notify(server.sigs, syscall.SIGTERM)
	go func() {
		sig := <-server.sigs
		stdlog.Println("recv signal:", sig)
		server.Close()
	}()
}

// 关闭服务
func (server *GoRedisServer) Close() {
	server.closing = true               // 标记退出
	server.Suspend()                    // 挂起全部传入数据
	time.Sleep(time.Millisecond * 2000) // 休息一下，Suspend瞬间可能还有数据库写入
	server.levelRedis.Close()
	server.levelRedis = nil // 防止调用
	server.synclog.Close()
	server.synclog = nil // 防止调用
	for e := server.closingFunc.Front(); e != nil; e = e.Next() {
		fn, ok := e.Value.(func())
		if ok {
			fn()
		} else {
			stdlog.Println("closing func err", e.Value)
		}
	}
	time.Sleep(time.Millisecond * 2000) // 休息一下
	stdlog.Println("bye")
	os.Exit(0)
}

// 发起主从同步请求
func (server *GoRedisServer) initSlaveOf() {
	host, port := server.opt.SlaveOf()
	if len(host) > 0 && port != 0 {
		stdlog.Printf("init slaveof %s:%d\n", host, port)
		// 模拟外部, session=nil
		simulatedCmd := NewCommand(formatByteSlice("SLAVEOF", host, port)...)
		reply := server.OnSLAVEOF(nil, simulatedCmd)
		stdlog.Printf("slaveof: %s:%d, %s\n", host, port, reply)
	}
}

// 初始化leveldb
func (server *GoRedisServer) initLevelDB() (err error) {
	opts := levelredis.NewOptions()
	cache := levelredis.NewLRUCache(128 * 1024 * 1024)
	opts.SetCache(cache)
	opts.SetCompression(levelredis.SnappyCompression)
	opts.SetBlockSize(8 * 1024)
	opts.SetMaxBackgroundCompactions(6)
	opts.SetWriteBufferSize(32 * 1024 * 1024)
	opts.SetMaxOpenFiles(100000)
	opts.SetCreateIfMissing(true)
	env := levelredis.NewDefaultEnv()
	env.SetBackgroundThreads(6)
	env.SetHighPriorityBackgroundThreads(2)
	opts.SetEnv(env)
	db, e1 := levelredis.Open(server.opt.DBPath()+"/db0", opts)
	if e1 != nil {
		return e1
	}
	server.levelRedis = levelredis.NewLevelRedis(db, false)
	server.DeferClosing(func() {
		opts.Close()
		cache.Close()
		env.Close()
		stdlog.Println("db closed")
	})
	return
}

// 初始化主从日志
func (server *GoRedisServer) initSyncLog() error {
	opts := levelredis.NewOptions()
	cache := levelredis.NewLRUCache(32 * 1024 * 1024)
	opts.SetCache(cache)
	opts.SetCompression(levelredis.SnappyCompression)
	opts.SetBlockSize(4 * 1024)
	opts.SetMaxBackgroundCompactions(2)
	opts.SetWriteBufferSize(32 * 1024 * 1024)
	opts.SetMaxOpenFiles(100000)
	opts.SetCreateIfMissing(true)
	env := levelredis.NewDefaultEnv()
	env.SetBackgroundThreads(2)
	env.SetHighPriorityBackgroundThreads(1)
	opts.SetEnv(env)
	db, e1 := levelredis.Open(server.opt.LogPath()+"/synclog", opts)
	if e1 != nil {
		return e1
	}
	ldb := levelredis.NewLevelRedis(db, false)
	server.synclog = NewSyncLog(ldb, "sync")
	server.DeferClosing(func() {
		opts.Close()
		cache.Close()
		env.Close()
		stdlog.Println("synclog closed")
	})
	return nil
}

// 命令执行监控
func (server *GoRedisServer) initCommandMonitor(path string) {
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}

	st := stat.New(file)
	st.Add(stat.TextItem("time", 8, func() interface{} { return stat.TimeString() }))
	st.Add(stat.IncrItem("total", 7, func() int64 { return server.cmdCateCounters.Get("total").Count() }))
	// key, string, hash, list, ...
	for _, cate := range CommandCategoryList {
		func(name string) {
			var padding int
			if padding = len(name) + 1; padding < 7 {
				padding = 7
			}
			st.Add(stat.IncrItem(name, padding, func() int64 { return server.cmdCateCounters.Get(name).Count() }))
		}(string(cate))
	}

	st.Add(stat.TextItem("connection", 11, func() interface{} { return server.counters.Get("connection").Count() }))

	go st.Start()
	server.DeferClosing(func() {
		st.Close()
	})
}

func (server *GoRedisServer) initExecLog(path string) {
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}
	st := stat.New(file)
	st.Add(stat.TextItem("time", 8, func() interface{} { return stat.TimeString() }))
	for _, name := range []string{"<1ms", "1-5ms", "6-10ms", "11-30ms", ">30ms"} {
		func(n string) {
			st.Add(stat.IncrItem(n, 8, func() int64 { return server.execCounters.Get(n).Count() }))
		}(name)
	}
	go st.Start()
	server.DeferClosing(func() {
		st.Close()
	})
}

func (server *GoRedisServer) initSeqLog(path string) {
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}
	st := stat.New(file)
	st.Add(stat.TextItem("time", 8, func() interface{} { return stat.TimeString() }))
	st.Add(stat.TextItem("minseq", 16, func() interface{} {
		if server.synclog.IsEnabled() {
			return server.synclog.MinSeq()
		} else {
			return "-"
		}
	}))
	st.Add(stat.TextItem("maxseq", 16, func() interface{} {
		if server.synclog.IsEnabled() {
			return server.synclog.MaxSeq()
		} else {
			return "-"
		}
	}))
	st.Add(stat.TextItem("size", 16, func() interface{} {
		if server.synclog.IsEnabled() {
			return server.synclog.MaxSeq() - server.synclog.MinSeq()
		} else {
			return "-"
		}
	}))
	go st.Start()
	server.DeferClosing(func() {
		st.Close()
	})
}

func (server *GoRedisServer) initSlowlog(path string) {
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}
	slowlog.SetOutput(file)
}

func (server *GoRedisServer) initLeveldbIOLog(path string) {
	// leveldb.io
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}
	st := stat.New(file)
	st.Add(stat.TextItem("time", 8, func() interface{} { return stat.TimeString() }))

	// leveldb io 操作数
	ldbkeys := []string{"get", "set", "batch", "enum", "del", "lru_hit", "lru_miss"}
	for _, k := range ldbkeys {
		// pass local var to inner func()
		func(name string) {
			st.Add(stat.IncrItem(name, 10, func() int64 { return server.levelRedis.Counter(name) }))
		}(k)
	}
	go st.Start()
	server.DeferClosing(func() {
		st.Close()
	})
}

func (server *GoRedisServer) initLeveldbStatsLog(path string) {
	// leveldb.stats
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for _ = range ticker.C {
			t := time.Now()
			tm := fmt.Sprintf("# %04d-%02d-%02d %02d:%02d:%02d\n", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
			file.WriteString(tm)
			file.WriteString(server.levelRedis.Stats())
			file.WriteString("\n")
		}
	}()
	server.DeferClosing(func() {
		ticker.Stop()
	})
}

func (server *GoRedisServer) initCommandCounterLog(cate string, cmds []string) {
	path := fmt.Sprintf("%s/cmd.%s.log", server.opt.LogPath(), cate)
	file, err := openfile(path)
	if err != nil {
		panic(err)
	}

	st := stat.New(file)
	st.Add(stat.TextItem("time", 8, func() interface{} { return stat.TimeString() }))
	for _, k := range cmds {
		func(cmd string) {
			var padding int
			if padding = len(cmd) + 1; padding < 8 {
				padding = 8
			}
			st.Add(stat.IncrItem(cmd, padding, func() int64 { return server.cmdCounters.Get(cmd).Count() }))
		}(k)
	}
	go st.Start()
	server.DeferClosing(func() {
		st.Close()
	})
}
