package shardkv

import "log"

const Debug = false
const Output = false
const GC = false

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

func (kv *ShardKV) getGidToShards(state ShardState) map[int][]int {
	shards := kv.stateMachine.getShardsByState(state)
	gidToShards := make(map[int][]int)
	for _, sid := range shards {
		gid := kv.lastConfig.Shards[sid]
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
		if gid != 0 {
			gidToShards[gid] = append(gidToShards[gid], sid)
		}
	}
	return gidToShards
}
