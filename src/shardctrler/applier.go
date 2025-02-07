package shardctrler

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				reply := sc.apply(applyMsg.Command)
				currentTerm, isLeader := sc.rf.GetState()
				if isLeader && applyMsg.CurrentTerm == currentTerm {
					sc.notify(applyMsg.CommandIndex, reply)
				}
			}
		}
	}
}
func (sc *ShardCtrler) apply(cmd interface{}) *NotifychMsg {
	reply := &NotifychMsg{}
	op := cmd.(Op)
	DPrintf("{Server %d} apply command = %v\n", sc.me, op)
	if op.Type != "Query" && sc.checkDuplicate(op.ClientID, op.Seq) {
		reply.Err = OK
	} else {
		reply = sc.applyLogToStateMachine(&op)
		if op.Type != "Query" {
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
	case "Join":
		reply = sc.joinHandler(op)
	case "Leave":
		reply = sc.leaveHandler(op)
	case "Move":
		reply = sc.moveHandler(op)
	case "Query":
		reply = sc.queryHandler(op)

	default:
		reply.Err = Err("Unknown operation")
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
		Seq:  op.Seq,
		Type: op.Type,
		Err:  reply.Err,
	}

	last, ok := sc.lastOperation[op.ClientID]
	if !ok || last.Seq < op.Seq {
		sc.lastOperation[op.ClientID] = ctx
	}
}

func (sc *ShardCtrler) joinHandler(op *Op) *NotifychMsg {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := addMap(lastConfig.Groups, op.Servers)
	newShards := greedyRebalance(lastConfig.Shards, newGroups)
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	return &NotifychMsg{
		Err:    OK,
		Config: newConfig,
	}
}

func (sc *ShardCtrler) leaveHandler(op *Op) *NotifychMsg {
	return nil
}

func (sc *ShardCtrler) moveHandler(op *Op) *NotifychMsg {
	return nil
}

func (sc *ShardCtrler) queryHandler(op *Op) *NotifychMsg {
	return nil
}
