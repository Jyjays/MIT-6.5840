package shardkv

type StateMachine struct {
	Shards map[int]*Shard
}

func (kv *StateMachine) insertShard(sid int, shard *Shard) {
	kv.Shards[sid] = shard
}

func (kv *StateMachine) getShard(sid int) *Shard {
	return kv.Shards[sid]
}

func (kv *StateMachine) deleteShard(sid int) {
	delete(kv.Shards, sid)
}

func (kv *StateMachine) apply(sid int, op Op) (ShardState, string) {
	shard := kv.getShard(sid)
	if shard == nil {
		shard = newShard()
		kv.insertShard(sid, shard)
	}
	if shard.State == Serving {
		value, flag := shard.apply(op)
		if flag {
			return Serving, value
		}
	}
	return shard.State, ""
}

func newKVStateMachine() *StateMachine {
	return &StateMachine{
		Shards: make(map[int]*Shard),
	}
}
