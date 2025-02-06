package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var stateMachine kvStateMachine
	var lastOperation map[int64]ReplyContext

	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperation) != nil {
		panic("decode persist state fail")
	}

	kv.StateMachine = &stateMachine
	kv.LastOperation = lastOperation
}

// kvSnapshot has lock
func (kv *KVServer) kvSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate != -1 && kv.persist.RaftStateSize() > kv.maxraftstate {
		DPrintf("server {%d} get snapshot index = %d maxraftstate = %d raftStateSize = %d\n", kv.me, kv.LastApplied, kv.maxraftstate, kv.persist.RaftStateSize())
		kv.rf.Snapshot(kv.LastApplied, kv.kvEncodeState())
	}
}

func (kv *KVServer) kvEncodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.LastOperation)
	data := w.Bytes()
	return data
}
