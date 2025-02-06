package kvraft

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.LastApplied {
					DPrintf("{Server %d} drop command index = %d LastApplied = %d", kv.me, applyMsg.CommandIndex, kv.LastApplied)
					continue
				}
				reply := kv.apply(applyMsg.Command)
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CurrentTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}
				kv.kvSnapshot()
				kv.LastApplied = applyMsg.CommandIndex
			} else if applyMsg.SnapshotValid {
				kv.restoreSnapshot(applyMsg.Snapshot)
			}
		}
	}
}
func (kv *KVServer) apply(cmd interface{}) *NotifychMsg {
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

func (kv *KVServer) applyLogToStateMachine(op *Op) *NotifychMsg {
	var reply = &NotifychMsg{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Type {
	case "Get":
		if kv.StateMachine.hasKey(op.Key) {
			reply.Value = kv.StateMachine.get(op.Key)
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	case "Put":
		kv.StateMachine.put(op.Key, op.Value)
		reply.Err = OK
	case "Append":
		kv.StateMachine.append(op.Key, op.Value)
		reply.Err = OK
	}

	return reply
}

func (kv *KVServer) notify(index int, reply *NotifychMsg) {
	ch := kv.getNotifyChMsg(index)

	if ch != nil {
		ch <- reply
	}
}

func (kv *KVServer) updateLastOperation(op *Op, reply *NotifychMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ctx := ReplyContext{
		Seq:   op.Seq,
		Type:  op.Type,
		Err:   reply.Err,
		Value: op.Value,
	}

	last, ok := kv.LastOperation[op.ClientID]
	if !ok || last.Seq < op.Seq {
		kv.LastOperation[op.ClientID] = ctx
	}
}
