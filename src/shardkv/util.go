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
		if lastOperation.Err == OK {
			// 上次操作成功，可以直接返回true
			return true
		}
	}
	// 上次操作失败，可能是因为配置过时或其他原因
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
