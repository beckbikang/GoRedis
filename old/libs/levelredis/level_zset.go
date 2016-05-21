package levelredis

// 基于leveldb实现的zset，用于海量存储，节约内存

import (
	"GoRedis/libs/gorocks"
	"bytes"
	"strconv"
	"sync"
)

type LevelZSet struct {
	LevelElem
	redis      *LevelRedis
	key        string
	mu         sync.RWMutex
	totalCount int
}

func NewLevelZSet(redis *LevelRedis, key string) (l *LevelZSet) {
	l = &LevelZSet{}
	l.redis = redis
	l.key = key
	l.totalCount = -1
	l.initOnce()
	return
}

func (l *LevelZSet) Key() string {
	return l.key
}

func (l *LevelZSet) Size() int {
	return 1
}

func (l *LevelZSet) initOnce() {
	if l.totalCount == -1 {
		value, _ := l.redis.RawGet(l.zsetKey())
		if value != nil {
			l.totalCount, _ = strconv.Atoi(string(value))
		} else {
			l.totalCount = 0
		}
	}
}

func (l *LevelZSet) zsetKey() []byte {
	return joinStringBytes(KEY_PREFIX, SEP_LEFT, l.key, SEP_RIGHT, ZSET_SUFFIX)
}

func (l *LevelZSet) zsetValue() []byte {
	s := strconv.Itoa(l.totalCount)
	return []byte(s)
}

func zmemberKey(key, member []byte) []byte {
	return joinStringBytes(ZSET_PREFIX, SEP_LEFT, string(key), SEP_RIGHT, "m", SEP, string(member))
}

func (l *LevelZSet) memberKey(member []byte) []byte {
	return joinStringBytes(ZSET_PREFIX, SEP_LEFT, l.key, SEP_RIGHT, "m", SEP, string(member))
}

func (l *LevelZSet) scoreKey(member []byte, score []byte) []byte {
	scoreint := BytesToInt64(score)
	var sign string // 正负数
	if scoreint < 0 {
		sign = "0"
	} else {
		sign = "1"
	}
	return joinStringBytes(ZSET_PREFIX, SEP_LEFT, l.key, SEP_RIGHT, "s", SEP, sign, string(score), SEP, string(member))
}

func (l *LevelZSet) scoreKeyPrefix() []byte {
	return joinStringBytes(ZSET_PREFIX, SEP_LEFT, l.key, SEP_RIGHT, "s", SEP)
}

func (l *LevelZSet) scoreKeyPrefixWith(scoreint int64) []byte {
	var sign string // 正负数
	if scoreint < 0 {
		sign = "0"
	} else {
		sign = "1"
	}
	return joinStringBytes(ZSET_PREFIX, SEP_LEFT, l.key, SEP_RIGHT, "s", SEP, sign, string(Int64ToBytes(scoreint)))
}

// _z[user_rank]s#1378000907596#100428 = ""
func (l *LevelZSet) splitScoreKey(scorekey []byte) (score, member []byte) {
	pos2 := bytes.LastIndex(scorekey, []byte(SEP))
	sepr := bytes.Index(scorekey, []byte(SEP_RIGHT))
	pos1 := bytes.Index(scorekey[sepr:], []byte(SEP)) + sepr
	member = copyBytes(scorekey[pos2+1:])
	score = copyBytes(scorekey[pos1+1+1 : pos2]) // +1 skip sign "0/1"
	return
}

func (l *LevelZSet) Add(scoreMembers ...[]byte) (n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	count := len(scoreMembers)
	for i := 0; i < count; i += 2 {
		score := scoreMembers[i]
		member, memberkey := scoreMembers[i+1], l.memberKey(scoreMembers[i+1])
		// remove old score
		oldscore, _ := l.redis.RawGet(memberkey)
		if oldscore != nil {
			batch.Delete(l.scoreKey(member, oldscore))
		} else {
			l.totalCount++
			// The number of elements added to the sorted sets, not including elements already existing for which the score was updated.
			n++
		}
		// set member
		batch.Put(memberkey, score)
		// new score
		batch.Put(l.scoreKey(member, score), nil)
	}
	batch.Put(l.zsetKey(), l.zsetValue())
	err := l.redis.WriteBatch(batch)
	if err != nil {
		panic(err)
	}
	return
}

func (l *LevelZSet) Score(member []byte) (score []byte) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.score(member)
}

func (l *LevelZSet) score(member []byte) (score []byte) {
	score, _ = l.redis.RawGet(l.memberKey(member))
	return
}

func (l *LevelZSet) IncrBy(member []byte, incr int64) (newscore []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	score := l.score(member)
	batch := gorocks.NewWriteBatch()
	defer batch.Close()

	oldcount := l.totalCount
	if score == nil {
		newscore = Int64ToBytes(incr)
		l.totalCount++
	} else {
		batch.Delete(l.scoreKey(member, score))
		scoreInt := BytesToInt64(score)
		newscore = Int64ToBytes(scoreInt + incr)
	}
	batch.Put(l.memberKey(member), newscore)
	batch.Put(l.scoreKey(member, newscore), nil)
	if l.totalCount != oldcount {
		batch.Put(l.zsetKey(), l.zsetValue())
	}
	err := l.redis.WriteBatch(batch)
	if err != nil {
		l.totalCount = oldcount
		panic(err) // need refect
	}
	return
}

// 返回-1表示member不存在
func (l *LevelZSet) Rank(high2low bool, member []byte) (idx int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// 对于不存在的key，先检查一次，减少扫描成本
	if l.score(member) == nil {
		return -1
	}
	direction := IterForward
	if high2low {
		direction = IterBackward
	}
	idx = -1
	l.redis.PrefixEnumerate(l.scoreKeyPrefix(), direction, func(i int, key, value []byte, quit *bool) {
		_, curmember := l.splitScoreKey(key)
		// fmt.Println(i, string(key), string(curmember), string(member))
		rtn := bytes.Compare(member, curmember)
		if rtn == 0 {
			idx = i
			*quit = true
		} else if high2low && rtn > 0 {
			*quit = true
			return
		} else if !high2low && rtn < 0 {
			*quit = true
			return
		}
	})
	return
}

func (l *LevelZSet) RangeByIndex(high2low bool, start, stop int) (scoreMembers [][]byte) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	direction := IterForward
	if high2low {
		direction = IterBackward
	}
	scoreMembers = make([][]byte, 0, 2)
	l.redis.PrefixEnumerate(l.scoreKeyPrefix(), direction, func(i int, key, value []byte, quit *bool) {
		if i < start {
			return
		} else if i >= start && (stop == -1 || i <= stop) {
			score, member := l.splitScoreKey(key)
			scoreMembers = append(scoreMembers, score)
			scoreMembers = append(scoreMembers, member)
		} else {
			*quit = true
		}
	})
	return
}

func (l *LevelZSet) Enumerate(fn func(i int, member, score []byte, quit *bool)) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.redis.PrefixEnumerate(l.scoreKeyPrefix(), IterForward, func(i int, key, value []byte, quit *bool) {
		score, member := l.splitScoreKey(key)
		fn(i, member, score, quit)
	})
}

func (l *LevelZSet) RangeByScore(high2low bool, min, max int64, offset, count int) (scoreMembers [][]byte) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	direction := IterForward
	if high2low {
		direction = IterBackward
	}
	min2 := l.scoreKeyPrefixWith(min)
	max2 := joinBytes(l.scoreKeyPrefixWith(max), []byte{MAXBYTE})
	scoreMembers = make([][]byte, 0, 2)
	l.redis.RangeEnumerate(min2, max2, direction, func(i int, key, value []byte, quit *bool) {
		if i < offset { // skip
			return
		}
		if count != -1 && i >= offset+count {
			*quit = true
			return
		}
		score, member := l.splitScoreKey(key)
		scoreMembers = append(scoreMembers, score)
		scoreMembers = append(scoreMembers, member)
	})
	return
}

func (l *LevelZSet) Remove(members ...[]byte) (n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	for _, member := range members {
		score, _ := l.redis.RawGet(l.memberKey(member))
		if score == nil {
			continue
		}
		batch.Delete(l.memberKey(member))
		batch.Delete(l.scoreKey(member, score))
		n++
	}
	l.totalCount -= n
	if l.totalCount == 0 {
		batch.Delete(l.zsetKey())
	} else {
		batch.Put(l.zsetKey(), l.zsetValue())
	}
	err := l.redis.WriteBatch(batch)
	if err != nil {
		panic(err)
	}
	return
}

func (l *LevelZSet) RemoveByIndex(start, stop int) (n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	l.redis.PrefixEnumerate(l.scoreKeyPrefix(), IterForward, func(i int, key, value []byte, quit *bool) {
		if i < start {
			return
		} else if i >= start && (stop == -1 || i <= stop) {
			score, member := l.splitScoreKey(key)
			batch.Delete(l.memberKey(member))
			batch.Delete(l.scoreKey(member, score))
			n++
		} else {
			*quit = true
		}
	})
	l.totalCount -= n
	if l.totalCount == 0 {
		batch.Delete(l.zsetKey())
	} else {
		batch.Put(l.zsetKey(), l.zsetValue())
	}
	err := l.redis.WriteBatch(batch)
	if err != nil {
		panic(err)
	}
	return
}

func (l *LevelZSet) RemoveByScore(min, max int64) (n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	min2 := l.scoreKeyPrefixWith(min)
	max2 := joinBytes(l.scoreKeyPrefixWith(max), []byte{MAXBYTE})
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	l.redis.RangeEnumerate(min2, max2, IterForward, func(i int, key, value []byte, quit *bool) {
		score, member := l.splitScoreKey(key)
		batch.Delete(l.memberKey(member))
		batch.Delete(l.scoreKey(member, score))
		n++
	})
	l.totalCount -= n
	if l.totalCount == 0 {
		batch.Delete(l.zsetKey())
	} else {
		batch.Put(l.zsetKey(), l.zsetValue())
	}
	err := l.redis.WriteBatch(batch)
	if err != nil {
		panic(err)
	}
	return
}

func (l *LevelZSet) len() (n int) {
	return l.totalCount
}

func (l *LevelZSet) Len() (n int) {
	return l.len()
}

func (l *LevelZSet) Type() string {
	return ZSET_SUFFIX
}

func (l *LevelZSet) Drop() (ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.totalCount == 0 {
		return true
	}
	batch := gorocks.NewWriteBatch()
	defer batch.Close()
	prefix := joinStringBytes(ZSET_PREFIX, SEP_LEFT, l.key, SEP_RIGHT)
	l.redis.PrefixEnumerate(prefix, IterForward, func(i int, key, value []byte, quit *bool) {
		batch.Delete(key)
	})
	batch.Delete(l.zsetKey())
	err := l.redis.WriteBatch(batch)
	if err != nil {
		panic(err)
	}
	l.totalCount = 0
	ok = true
	return
}
