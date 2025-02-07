package shardctrler

import "log"

const timeout = 1000
const debug = false

func DPrintf(format string, a ...interface{}) {
	if debug {
		log.Printf(format, a...)
	}
}

func (sc *ShardCtrler) checkDuplicate(clientId int64, seq int) bool {
	lastOperation, ok := sc.lastOperation[clientId]
	if ok && lastOperation.Seq >= seq {
		return true
	}
	return false
}
