package shardkv

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var stateMachine StateMachine
	var lastOperation map[int64]ReplyContext

	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperation) != nil {
		panic("decode persist state fail")
	}

	kv.stateMachine = &stateMachine
	kv.lastOperation = lastOperation
}

// kvSnapshot has lock
func (kv *ShardKV) kvSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate != -1 && kv.persist.RaftStateSize() > kv.maxraftstate {
		DPrintf("server {%d} get snapshot index = %d maxraftstate = %d raftStateSize = %d\n", kv.me, kv.lastApplied, kv.maxraftstate, kv.persist.RaftStateSize())
		kv.rf.Snapshot(kv.lastApplied, kv.kvEncodeState())
	}
}

func (kv *ShardKV) kvEncodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperation)
	data := w.Bytes()
	return data
}
