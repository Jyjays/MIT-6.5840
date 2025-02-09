package shardkv

func (kv *ShardKV) applier() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf("{Server %d} drop command index = %d lastApplied = %d", kv.me, applyMsg.CommandIndex, kv.lastApplied)
					continue
				}
				reply := kv.apply(applyMsg.Command)
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CurrentTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}
				kv.kvSnapshot()
				kv.lastApplied = applyMsg.CommandIndex
			}
			if applyMsg.SnapshotValid {
				kv.mu.Lock()
				if applyMsg.SnapshotIndex > kv.lastApplied {
					kv.restoreSnapshot(applyMsg.Snapshot)
					kv.lastApplied = applyMsg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}
func (kv *ShardKV) apply(cmd interface{}) *NotifychMsg {
	reply := &NotifychMsg{}
	op := cmd.(Op)
	DPrintf("{Server %d} apply command = %v\n", kv.me, op)
	// 有可能出现这边刚执行到这里 然后另一边重试 进来了重复命令 这边还没来得及更新 那边判断重复指令不重复
	// 因此需要在应用日志之前再过滤一遍日志 如果发现有重复日志的话 那么就直接返回OK
	if op.Type != "Get" && kv.checkDuplicate(op.ClientID, op.Seq) {
		reply.Err = OK
	} else {
		reply = kv.applyLogToStateMachine(&op)
		if op.Type != "Get" {
			kv.updateLastOperation(&op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (kv *ShardKV) applyLogToStateMachine(op *Op) *NotifychMsg {
	var reply = &NotifychMsg{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Type {
	case "Get":
		if kv.stateMachine.hasKey(op.Key) {
			reply.Value = kv.stateMachine.get(op.Key)
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	case "Put":
		kv.stateMachine.put(op.Key, op.Value)
		reply.Err = OK
	case "Append":
		kv.stateMachine.append(op.Key, op.Value)
		reply.Err = OK
	}

	return reply
}

func (kv *ShardKV) notify(index int, reply *NotifychMsg) {
	ch := kv.getNotifyChMsg(index)

	if ch != nil {
		ch <- reply
	}
}

func (kv *ShardKV) updateLastOperation(op *Op, reply *NotifychMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ctx := ReplyContext{
		Seq:  op.Seq,
		Type: op.Type,
		Err:  reply.Err,
	}

	last, ok := kv.lastOperation[op.ClientID]
	if !ok || last.Seq < op.Seq {
		kv.lastOperation[op.ClientID] = ctx
	}
}
