package shardkv

import "log"

const Debug = true
const Output = true

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
func (kv *ShardKV) checkDuplicate(clientId int64, seq int) bool {
	lastOperation, ok := kv.lastOperation[clientId]
	if ok && lastOperation.Seq >= seq {
		return true
	}
	return false
}
