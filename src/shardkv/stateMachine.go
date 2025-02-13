package shardkv

type StateMachine struct {
	Shards map[int]*Shard
}

func (kv *StateMachine) insertShard(sid int, shard *Shard, states ...ShardState) {
	state := Serving // 默认值
	if len(states) > 0 {
		state = states[0]
	}
	kv.Shards[sid] = copyShard(shard)
	kv.Shards[sid].State = state
}

func (kv *StateMachine) insertShards(shards map[int]*Shard, states ...ShardState) {
	state := Serving // 默认值
	if len(states) > 0 {
		state = states[0]
	}
	for sid, shard := range shards {
		kv.insertShard(sid, shard, state)
	}
}

func (kv *StateMachine) getShard(sid int) *Shard {
	return kv.Shards[sid]
}

func (kv *StateMachine) getShardState(sid int) ShardState {
	shard := kv.getShard(sid)
	if shard == nil {
		return Unknown
	}
	return shard.State
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
		DPrintf("Didn't find shard %v", sid)
		return Unknown, ""
	}
	if shard.State == Serving || shard.State == GCing {
		value, flag := shard.apply(op)
		if flag {
			// DPrintf("{Server %d} apply %v, value %v", sid, op, value)
			return shard.State, value
		} else {
			return Unknown, ""
		}
	} else {
		return shard.State, ""
	}
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
		kv.insertShard(sid, shard, state)
	} else {
		shard.State = state
	}
	//FIXME - 由于深拷贝，这里的shard是一个新的对象，所以这里的setShardState不会影响到kv.Shards
	//shard.setShardState(state)
}

func (kv *StateMachine) getShardsByState(state ShardState) []int {
	shards := make([]int, 0)
	for sid, shard := range kv.Shards {
		if shard.getShardState() == state {
			shards = append(shards, sid)
		}
	}
	return shards
}
