package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
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

	kv.stateMachine = &stateMachine
	kv.lastOperation = lastOperation
}
