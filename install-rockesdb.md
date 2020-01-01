CGO_CFLAGS="-I/usr/local/Cellar/rocksdb/6.5.2/include" \
CGO_LDFLAGS="-L/usr/local/Cellar/rocksdb/6.5.2/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
  go get github.com/tecbot/gorocksdb