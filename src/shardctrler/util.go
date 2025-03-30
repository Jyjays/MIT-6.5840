package shardctrler

import "log"

const timeout = 1000
const Debug = true
const Output = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) checkDuplicate(clientId int64, seq int) bool {
	lastOperation, ok := sc.lastOperation[clientId]
	if ok && lastOperation.Seq >= seq {
		return true
	}
	return false
}
