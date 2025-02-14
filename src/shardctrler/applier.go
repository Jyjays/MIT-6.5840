package shardctrler

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				sc.mu.Lock()
				reply := sc.apply(applyMsg.Command)
				currentTerm, isLeader := sc.rf.GetState()

				if isLeader && applyMsg.CurrentTerm == currentTerm {
					sc.notify(applyMsg.CommandIndex, reply)
				}
				sc.mu.Unlock()
			}
		}
	}
}
func (sc *ShardCtrler) apply(cmd interface{}) *NotifychMsg {
	reply := &NotifychMsg{}
	op := cmd.(Op)
	if op.Type != "Query" && sc.checkDuplicate(op.ClientID, op.Seq) {
		reply.Err = OK
	} else {
		reply = sc.applyLogToStateMachine(&op)
		DPrintf("{Server %d} apply op = %v, reply = %v\n", sc.me, op, reply)
		if op.Type != "Query" {
			sc.updateLastOperation(&op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) applyLogToStateMachine(op *Op) *NotifychMsg {
	var reply = &NotifychMsg{}
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
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
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
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
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := addMap(lastConfig.Groups, op.Servers)
	newShards := greedyRebalance(lastConfig.Shards, newGroups, true)
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newConfig)
	return &NotifychMsg{
		Err:    OK,
		Config: newConfig,
	}
}

func (sc *ShardCtrler) leaveHandler(op *Op) *NotifychMsg {
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := removeMap(lastConfig.Groups, op.GIDs)
	newShards := greedyRebalance(lastConfig.Shards, newGroups, false)
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newConfig)
	return &NotifychMsg{
		Err:    OK,
		Config: newConfig,
	}

}

func (sc *ShardCtrler) moveHandler(op *Op) *NotifychMsg {
	lastConfig := sc.configs[len(sc.configs)-1]
	newShards := moveShard(lastConfig.Shards, op.Shard, op.GID)
	// move the original shard to the new shard
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: newShards,
		Groups: lastConfig.Groups,
	}
	sc.configs = append(sc.configs, newConfig)
	return &NotifychMsg{
		Err:    OK,
		Config: newConfig,
	}
}

func (sc *ShardCtrler) queryHandler(op *Op) *NotifychMsg {
	// 使用具有该编号的配置进行回复
	// Query（-1） 的结果应反映分片控制器在收到 Query（-1） RPC 之前完成处理的每个 Join、Leave 或 Move RPC。
	// 大于已知最大配置编号，则 shardctrler 应回复最新配置
	if op.Num == -1 || op.Num >= len(sc.configs) {
		return &NotifychMsg{
			Err:    OK,
			Config: sc.configs[len(sc.configs)-1],
		}
	} else {
		return &NotifychMsg{
			Err:    OK,
			Config: sc.configs[op.Num],
		}
	}
}
