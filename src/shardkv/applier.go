package shardkv

import "6.5840/shardctrler"

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
				//kv.kvSnapshot()
				kv.lastApplied = applyMsg.CommandIndex
			}
			// if applyMsg.SnapshotValid {
			// 	kv.mu.Lock()
			// 	if applyMsg.SnapshotIndex > kv.lastApplied {
			// 		kv.restoreSnapshot(applyMsg.Snapshot)
			// 		kv.lastApplied = applyMsg.SnapshotIndex
			// 	}
			// 	kv.mu.Unlock()
			// }
		}
	}
}
func (kv *ShardKV) apply(cmd interface{}) *NotifychMsg {
	reply := &NotifychMsg{}
	command := cmd.(Command)
	//DPrintf("{Server %d} apply command %v", kv.me, command)
	switch command.Type {
	case Operation:
		op := command.Data.(Op)
		reply = kv.applyLogToStateMachine(&op)
	case AddConfig:
		cfg := command.Data.(shardctrler.Config)
		reply = kv.applyConfig(cfg)
	case InsertShard:
		reply = kv.applyInsertShard(command.Data.(GetShardReply))
	case DeleteShard:
		reply = kv.applyDeleteShard(command.Data.(DeleteShardArgs))
	}
	return reply
}

func (kv *ShardKV) applyLogToStateMachine(op *Op) *NotifychMsg {
	var reply = &NotifychMsg{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sid := key2shard(op.Key)
	if !kv.shardCanServe(sid) {
		reply.Err = ErrWrongGroup
		return reply
	}
	if op.Type != "Get" && kv.checkDuplicate(op.ClientID, op.Seq) {
		reply.Err = OK
		return reply
	}
	state, value := kv.stateMachine.apply(*op)
	if op.Type != "Get" {
		kv.updateLastOperation(op, reply)
	}
	if state == Serving {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrWrongGroup
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

func (kv *ShardKV) applyConfig(config shardctrler.Config) *NotifychMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &NotifychMsg{}
	DPrintf("{Server %d} apply config %v, currentNum %v", kv.me, config, kv.currentConfig.Num)
	if config.Num <= kv.currentConfig.Num {
		reply.Err = OK
		return reply
	}
	// 更新状态机
	kv.processNewConfig(config)
	reply.Err = OK
	return reply
}
func (kv *ShardKV) applyInsertShard(re GetShardReply) *NotifychMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &NotifychMsg{}
	if re.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		return reply
	}
	kv.stateMachine.insertShards(re.Shards)
	kv.lastOperation = re.LastRequestMap
	reply.Err = OK
	return reply
}

func (kv *ShardKV) applyDeleteShard(args DeleteShardArgs) *NotifychMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &NotifychMsg{}
	if args.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		return reply
	}
	kv.stateMachine.deleteShards(args.ShardIDs)
	reply.Err = OK
	return reply
}
