package kvraft

import (
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) clearCache(Id int64) {
	if Id < 0 {
		return
	}
	delete(kv.cache, Id-1)
}
