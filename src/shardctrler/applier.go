package shardctrler

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= sc.LastApplied {
					DPrintf("{Server %d} drop command index = %d LastApplied = %d", sc.me, applyMsg.CommandIndex, sc.LastApplied)
					continue
				}
				reply := sc.apply(applyMsg.Command)
				currentTerm, isLeader := sc.rf.GetState()
				if isLeader && applyMsg.CurrentTerm == currentTerm {
					sc.notify(applyMsg.CommandIndex, reply)
				}
				sc.kvSnapshot()
				sc.LastApplied = applyMsg.CommandIndex
			}
			if applyMsg.SnapshotValid {
				sc.mu.Lock()
				if applyMsg.SnapshotIndex > sc.LastApplied {
					sc.restoreSnapshot(applyMsg.Snapshot)
					sc.LastApplied = applyMsg.SnapshotIndex
				}
				sc.mu.Unlock()
			}
		}
	}
}
func (sc *ShardCtrler) apply(cmd interface{}) *NotifychMsg {
	reply := &NotifychMsg{}
	op := cmd.(Op)
	DPrintf("{Server %d} apply command = %v\n", sc.me, op)
	// 有可能出现这边刚执行到这里 然后另一边重试 进来了重复命令 这边还没来得及更新 那边判断重复指令不重复
	// 因此需要在应用日志之前再过滤一遍日志 如果发现有重复日志的话 那么就直接返回OK
	if op.Type != "Get" && sc.checkDuplicate(op.ClientID, op.Seq) {
		reply.Err = OK
	} else {
		reply = sc.applyLogToStateMachine(&op)
		if op.Type != "Get" {
			sc.updateLastOperation(&op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) applyLogToStateMachine(op *Op) *NotifychMsg {
	var reply = &NotifychMsg{}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	switch op.Type {
	case "Get":
		if sc.StateMachine.hasKey(op.Key) {
			reply.Value = sc.StateMachine.get(op.Key)
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	case "Put":
		sc.StateMachine.put(op.Key, op.Value)
		reply.Err = OK
	case "Append":
		sc.StateMachine.append(op.Key, op.Value)
		reply.Err = OK
	}

	return reply
}

func (sc *ShardCtrler) notify(index int, reply *NotifychMsg) {
	ch := sc.getNotifyChMsg(index)

	if ch != nil {
		ch <- reply
	}
}

func (sc *ShardCtrler) updateLastOperation(op *Op, reply *NotifychMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ctx := ReplyContext{
		Seq:   op.Seq,
		Type:  op.Type,
		Err:   reply.Err,
		Value: op.Value,
	}

	last, ok := sc.LastOperation[op.ClientID]
	if !ok || last.Seq < op.Seq {
		sc.LastOperation[op.ClientID] = ctx
	}
}
