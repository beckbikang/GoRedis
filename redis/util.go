package redis

import (
	"strconv"
)

//定义常量
const (
	CR   = '\r'
	LF   = '\n'
	CRLF = "\r\n"
)

//整数转字符串
// itoa speed up the strconv.Itoa in small numbers
var itoa func(int) string

func init() {
	size := 1000
	itoaCache := make([]string, size)
	for i := 0; i < size; i++ {
		itoaCache[i] = strconv.Itoa(i)
	}

	//对itoa进行定义和处理
	itoa = func(i int) string {
		if i >= 0 && i < size {
			return itoaCache[i]
		} else {
			return strconv.Itoa(i)
		}
	}
}
//[]string转[]interface{}
func stringSliceInterfaces(vals []string) []interface{} {
	out := make([]interface{}, len(vals))
	for i, val := range vals {
		out[i] = val
	}
	return out
}
