package main

import (
	"flag"
	"github.com/latermoon/GoRedis/redis"
	"github.com/latermoon/GoRedis/rocks"
	"github.com/latermoon/GoRedis/server"
	"github.com/tecbot/gorocksdb"
	"log"
	"net"
)

//接收端口
func init() {
	flag.StringVar(&address, "bind address", ":6380", "Bind address")
}

func main() {
	flag.Parse()
	log.Println("server start ...")

	// new rocksdb
	db := newRocksDB("/tmp/rocks_6380")

	// new GoRedisServer handler
	handler := server.New(db)

	// register command handler
	redis.Register(handler)

	//监听地址
	// Serve
	lis, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	//监听服务
	redis.Serve(lis)
}

//创建一个redis的db
func newRocksDB(dir string) *rocks.DB {
	//基本选项
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	//打开一个db
	rdb, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		panic(err)
	}
	//创建一个db
	return rocks.New(rdb)
}

var (
	address string
)
