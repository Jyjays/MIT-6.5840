package shardkv

type StateMachine struct {
	Shards map[int]*Shard
}

func (kv *StateMachine) insertShard(sid int, shard *Shard) {
	//REVIEW - 应该深拷贝
	kv.Shards[sid] = shard
}

func (kv *StateMachine) insertShards(shards map[int]*Shard) {
	for sid, shard := range shards {
		kv.insertShard(sid, shard)
	}
}

func (kv *StateMachine) getShard(sid int) *Shard {
	return kv.Shards[sid]
}

func (kv *StateMachine) deleteShard(sid int) {
	delete(kv.Shards, sid)
}

func (kv *StateMachine) deleteShards(sids []int) {
	for _, sid := range sids {
		kv.deleteShard(sid)
	}
}

func (kv *StateMachine) apply(op Op) (ShardState, string) {
	sid := key2shard(op.Key)
	shard := kv.getShard(sid)

	if shard == nil {
		return Unknown, ""
	}
	if shard.State == Serving {
		value, flag := shard.apply(op)
		if flag {
			return Serving, value
		}
	}
	return shard.State, ""
}

func newStateMachine() *StateMachine {
	return &StateMachine{
		Shards: make(map[int]*Shard),
	}
}

func (kv *StateMachine) setShardState(sid int, state ShardState) {
	shard := kv.getShard(sid)
	if shard == nil {
		shard = newShard()
		kv.insertShard(sid, shard)
	}
	shard.setShardState(state)
}
