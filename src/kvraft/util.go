package kvraft

import (
	"log"
)

const Debug = false
const Output = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (kv *KVServer) checkDuplicate(clientId int64, seq int) bool {
	lastOperation, ok := kv.lastOperation[clientId]
	if ok && lastOperation.Seq >= seq {
		return true
	}
	return false
}
