package levelredis

// 基于leveldb实现的list，主要用于海量存储，比如aof、日志
// 本页面命名注意，idx都表示大于l.start的那个索引序号，而不是0开始的数组序号

import (
	"GoRedis/libs/gorocks"
	"bytes"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Element struct {
	Value interface{}
}

// LevelList的特点
// 类似双向链表，右进左出，可以通过索引查找
// 海量存储，占用内存小
type LevelList struct {
	LevelElem
	redis    *LevelRedis
	entryKey string
	// 游标控制
	start int64
	end   int64
	mu    sync.RWMutex
}

func NewLevelList(redis *LevelRedis, entryKey string) (l *LevelList) {
	l = &LevelList{}
	l.redis = redis
	l.entryKey = entryKey
	l.start = 0
	l.end = -1
	l.initCount()
	return
}

func (l *LevelList) initCount() {
	val, _ := l.redis.RawGet(l.infoKey())
	if val == nil || len(val) == 0 {
		return
	}
	pairs := strings.Split(string(val), ",")
	if len(pairs) != 2 {
		return
	}
	l.start, _ = strconv.ParseInt(pairs[0], 10, 64)
	l.end, _ = strconv.ParseInt(pairs[1], 10, 64)
	if !(l.end == -1 && l.start == 0) && l.end < l.start {
		os.Stderr.WriteString("bad list: " + l.entryKey)
		l.start, l.end = 0, -1
	}
}

func (l *LevelList) Key() string {
	return l.entryKey
}

func (l *LevelList) Size() int {
	return 1
}

// __key:[entry key]:list =
func (l *LevelList) infoKey() []byte {
	return joinStringBytes(KEY_PREFIX, SEP_LEFT, l.entryKey, SEP_RIGHT, LIST_SUFFIX)
}

func (l *LevelList) infoValue() []byte {
	return []byte(strconv.FormatInt(l.start, 10) + "," + strconv.FormatInt(l.end, 10))
}

func (l *LevelList) keyPrefix() []byte {
	return joinStringBytes(LIST_PREFIX, SEP_LEFT, l.entryKey, SEP_RIGHT)
}

// _l[key]#11005 = hello
func (l *LevelList) idxKey(idx int64) []byte {
	// 正负符号, 因为经过uint64转换后，负数的字典顺序比整数大，所以需要前置一个0、1保障顺序
	var sign string
	if idx < 0 {
		sign = "0"
	} else {
		sign = "1"
	}
	idxStr := string(Int64ToBytes(idx))
	return joinStringBytes(LIST_PREFIX, SEP_LEFT, l.entryKey, SEP_RIGHT, SEP, sign, idxStr)
}

func (l *LevelList) splitIndexKey(idxkey []byte) (idx int64) {
	pos := bytes.LastIndex(idxkey, []byte(SEP))
	idx = BytesToInt64(idxkey[pos+1+1:]) // +1 skip sign "0/1"
	return
}

func (l *LevelList) LPush(values ...[]byte) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 左游标
	oldstart := l.start
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	for _, value := range values {
		l.start--
		batch.Put(l.idxKey(l.start), value)
	}
	batch.Put(l.infoKey(), l.infoValue())
	err = l.redis.WriteBatch(batch)
	if err != nil {
		// 回退
		l.start = oldstart
	}
	return
}

func (l *LevelList) RPush(values ...[]byte) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 右游标
	oldend := l.end
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	for _, value := range values {
		l.end++
		batch.Put(l.idxKey(l.end), value)
	}
	batch.Put(l.infoKey(), l.infoValue())
	err = l.redis.WriteBatch(batch)
	if err != nil {
		// 回退
		l.end = oldend
	}
	return
}

func (l *LevelList) RPop() (e *Element, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.len() == 0 {
		return nil, nil
	}
	// backup
	oldstart, oldend := l.start, l.end

	// get
	idx := l.end
	e = &Element{}
	e.Value, err = l.redis.RawGet(l.idxKey(idx))
	if err != nil || e.Value == nil {
		return
	}

	// 只剩下一个元素时，删除infoKey(0)
	shouldReset := l.len() == 1
	// 删除数据, 更新左游标
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	batch.Delete(l.idxKey(idx))
	if shouldReset {
		l.start = 0
		l.end = -1
		batch.Delete(l.infoKey())
	} else {
		l.end--
		batch.Put(l.infoKey(), l.infoValue())
	}
	err = l.redis.WriteBatch(batch)
	if err != nil {
		// 回退
		l.start, l.end = oldstart, oldend
	}
	return
}

func (l *LevelList) LPop() (e *Element, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.len() == 0 {
		return nil, nil
	}
	// backup
	oldstart, oldend := l.start, l.end

	// get
	idx := l.start
	e = &Element{}
	e.Value, err = l.redis.RawGet(l.idxKey(idx))
	if err != nil || e.Value == nil {
		return
	}
	// 只剩下一个元素时，删除infoKey(0)
	shouldReset := l.len() == 1
	// 删除数据, 更新左游标
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	batch.Delete(l.idxKey(idx))
	if shouldReset {
		l.start = 0
		l.end = -1
		batch.Delete(l.infoKey())
	} else {
		l.start++
		batch.Put(l.infoKey(), l.infoValue())
	}
	err = l.redis.WriteBatch(batch)
	if err != nil {
		// 回退
		l.start, l.end = oldstart, oldend
	}
	return
}

// 保留左边
func (l *LevelList) TrimLeft(count uint) (n int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	oldlen := l.len()
	if oldlen == 0 || oldlen <= int64(count) {
		return
	}
	oldstart, oldend := l.start, l.end
	batch := gorocks.NewWriteBatch()
	defer batch.Close()

	for i := int64(count); i < oldlen; i++ {
		idx := oldstart + i
		// fmt.Println("LTRIM", l.entryKey, "i=", i, ", idx=", idx)
		batch.Delete(l.idxKey(idx))
		l.end--
	}
	shouldReset := l.len() == 0
	if shouldReset {
		l.start = 0
		l.end = -1
		batch.Delete(l.infoKey())
	} else {
		batch.Put(l.infoKey(), l.infoValue())
	}

	err := l.redis.WriteBatch(batch)
	if err != nil {
		// 回退
		l.start, l.end = oldstart, oldend
	}
	return
}

func (l *LevelList) Range(start, stop int64) (elems []*Element, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if start < 0 || (stop != -1 && start > stop) {
		err = errors.New("bad start/stop")
		return
	}

	min := l.idxKey(l.start + start)
	var max []byte
	if stop == -1 {
		max = l.idxKey(l.end)
		elems = make([]*Element, 0, 100)
	} else {
		max = l.idxKey(l.start + stop)
		buflen := stop - start // 预分配
		if buflen > 1000 {
			buflen = 1000
		}
		elems = make([]*Element, 0, buflen)
	}

	keyPrefix := l.keyPrefix()
	l.redis.RangeEnumerate(min, max, IterForward, func(i int, key, value []byte, quit *bool) {
		if !bytes.HasPrefix(key, keyPrefix) {
			*quit = true
			return
		}
		e := &Element{Value: value}
		elems = append(elems, e)
	})

	return
}

func (l *LevelList) Index(i int64) (e *Element, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if i < 0 || i >= l.len() {
		return nil, nil
	}
	idx := l.start + i
	e = &Element{}
	e.Value, err = l.redis.RawGet(l.idxKey(idx))
	if err != nil || e.Value == nil {
		return
	}
	return
}

func (l *LevelList) Enumerate(fn func(i int, value []byte, quit *bool)) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	l.redis.PrefixEnumerate(l.keyPrefix(), IterForward, func(i int, key, value []byte, quit *bool) {
		fn(i, value, quit)
	})
}

func (l *LevelList) len() int64 {
	if l.end < l.start {
		return 0
	}
	return l.end - l.start + 1
}

func (l *LevelList) Len() int64 {
	return l.len()
}

func (l *LevelList) Type() string {
	return LIST_SUFFIX
}

func (l *LevelList) Drop() (ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	l.redis.PrefixEnumerate(l.keyPrefix(), IterForward, func(i int, key, value []byte, quit *bool) {
		batch.Delete(key)
	})
	batch.Delete(l.infoKey())
	l.redis.WriteBatch(batch)
	ok = true
	l.start = 0
	l.end = -1
	return
}
