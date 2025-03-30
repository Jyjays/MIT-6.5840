package shardkv

import "6.5840/shardctrler"

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.mu.Lock()
				if applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf("{Server %d} drop command index = %d lastApplied = %d", kv.me, applyMsg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				reply := kv.apply(applyMsg.Command)
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CurrentTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}
				kv.kvSnapshot()
				kv.lastApplied = applyMsg.CommandIndex
				kv.mu.Unlock()
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
	case EmptyLog:
		reply.Err = OK
	}
	return reply
}

func (kv *ShardKV) applyLogToStateMachine(op *Op) *NotifychMsg {
	var reply = &NotifychMsg{}
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	sid := key2shard(op.Key)
	if !kv.shardCanServe(sid) {
		DPrintf("P1 {Group %v Server %v} Shard %v cannot serve\n", kv.gid, kv.me, sid)
		reply.Err = ErrWrongGroup
		return reply
	}
	if op.Type != "Get" && kv.checkDuplicate(op.ClientID, op.Seq) {
		//FIXME - duplicate request
		reply.Err = OK
		return reply
	}
	state, value := kv.stateMachine.apply(*op)
	// FIXME - Wrong order to update lastOperation
	// if op.Type != "Get" {
	// 	kv.updateLastOperation(op, reply)
	// }
	if state == Unknown {
		reply.Err = ErrNoKey
	} else if state == Serving || state == GCing {
		reply.Err = OK
		reply.Value = value
	} else {
		DPrintf("P2 {Group %v Server %v} Shard %v State %v, cannot serve\n", kv.gid, kv.me, sid, state)
		reply.Err = ErrWrongGroup
	}
	if op.Type != "Get" {
		kv.updateLastOperation(op, reply)
	}
	//DPrintf("{Group %v Server %v} apply op %v, reply %v sid %v \n", kv.gid, kv.me, op, reply, sid)
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

	if kv.lastOperation == nil {
		kv.lastOperation = make(map[int64]ReplyContext)
	}

	last, ok := kv.lastOperation[op.ClientID]

	if !ok || last.Seq < op.Seq {
		kv.lastOperation[op.ClientID] = ctx
	}
}

func (kv *ShardKV) applyConfig(config shardctrler.Config) *NotifychMsg {
	//kv.mu.Lock()
	reply := &NotifychMsg{}
	if config.Num <= kv.currentConfig.Num {
		reply.Err = OK
		return reply
	}
	// kv.mu.Unlock()
	kv.processNewConfig(config.DeepCopy())

	reply.Err = OK
	return reply
}
func (kv *ShardKV) applyInsertShard(re GetShardReply) *NotifychMsg {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	reply := &NotifychMsg{}
	if re.ConfigNum < kv.currentConfig.Num {
		DPrintf("{Group %v Server %v applyInsertShard: config num %v is less than current %v, ignoring\n",
			kv.gid, kv.me, re.ConfigNum, kv.currentConfig.Num)
		reply.Err = OK
		return reply
	}
	if re.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrWrongConfigNum
		return reply
	}
	//DPrintf("{Group %v Server %v} applyInsertShard %v\n", kv.gid, kv.me, re)
	for sid, shardData := range re.Shards {
		currentState := kv.stateMachine.getShardState(sid)
		if currentState == Pulling {
			// NOTE - 如果当前状态是 Pulling，说明这个 shard 还没有被处理过
			kv.stateMachine.insertShard(sid, shardData, GCing)
		} else {
			//DPrintf("{Group %v Server %v} Shard %v State %v already processed, skipping\n", kv.gid, kv.me, sid, currentState)
		}
	}
	for cid, op := range re.LastRequestMap {
		if last, ok := kv.lastOperation[cid]; !ok || last.Seq < op.Seq {
			kv.lastOperation[cid] = op
		}
	}
	reply.Err = OK
	return reply
}

func (kv *ShardKV) applyUpdateShard(args UpdateShardState) *NotifychMsg {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	reply := &NotifychMsg{}
	// 如果传入的 config 版本低于当前版本，则认为这条命令已经过时，不必重复处理
	if args.ConfigNum < kv.currentConfig.Num {
		DPrintf("{Group %v Server %v} applyUpdateShard: config num %v is less than current %v, ignoring\n",
			kv.gid, kv.me, args.ConfigNum, kv.currentConfig.Num)
		reply.Err = OK
		return reply
	}
	if args.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		return reply
	}

	for _, sid := range args.ShardIDs {
		kv.stateMachine.setShardState(sid, args.ShardState)
	}
	reply.Err = OK
	DPrintf("{Group %v Server %v} applyUpdateShard success  %v\n", kv.gid, kv.me, args)
	return reply
}

func (kv *ShardKV) applyDeleteShard(args DeleteShardArgs) *NotifychMsg {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	reply := &NotifychMsg{}
	// 如果命令的配置版本比当前配置旧，则认为该命令已经过时，可以直接返回 OK
	if args.ConfigNum < kv.currentConfig.Num {
		DPrintf("{Group %v Server %v} applyDeleteShard: config num %v is less than current %v, ignoring\n",
			kv.gid, kv.me, args.ConfigNum, kv.currentConfig.Num)
		reply.Err = OK
		return reply
	}
	if args.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		return reply
	}
	for _, sid := range args.ShardIDs {
		shard := kv.stateMachine.getShard(sid)
		shardstate := shard.getShardState()
		// 根据当前状态执行更新
		if shardstate == Sending {
			// 原shard状态是Sending，收到Delete的时候，说明这个shard已经被删除了
			kv.stateMachine.insertShard(sid, newShard(), Serving)
		} else if shardstate == GCing {
			shard.setShardState(Serving)
		} else {
			DPrintf("{Group %v Server %v} applyDeleteShard: shard %v state %v, skipping\n",
				kv.gid, kv.me, sid, shard.getShardState())
		}
	}
	reply.Err = OK
	return reply
}

func (kv *ShardKV) processNewConfig(config shardctrler.Config) {
	//三种情况：1.旧配置中有，新配置中没有，删除；2.旧配置中没有，新配置中有，增加；3.旧配置中有，新配置中有，不变
	// toBeInsertedShards := make(map[int]*Shard)
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	if config.Num != kv.currentConfig.Num+1 {
		return // 拒绝跳跃式配置更新
	} else {
		kv.lastConfig = kv.currentConfig.DeepCopy()
		kv.currentConfig = config
	}
	// if !kv.isLeader() {
	// 	return
	// }

	oldShards := kv.lastConfig.Shards
	newShards := kv.currentConfig.Shards
	// 持有者 -> 要拉取的shards
	//pullMap := make(map[int][]int)
	//sendMap := make(map[int][]int)
	DPrintf("{Group %v Server %v} newconfig %v\n", kv.gid, kv.me, config)
	for sid := 0; sid < shardctrler.NShards; sid++ {
		newgid := newShards[sid]
		oldgid := oldShards[sid]
		if newgid == kv.gid {
			if oldgid != kv.gid {
				// 仅当旧配置中的分片不属于当前组时设为 Pulling
				if oldgid != 0 && oldgid != kv.gid {
					kv.stateMachine.setShardState(sid, Pulling)
					//pullArray = append(pullArray, sid)
					//DPrintf("{Group %v Server %v} set shard %v state Pulling\n", kv.gid, kv.me, sid)
				} else {
					// 旧配置中分片已属于当前组，直接设为 Serving
					kv.stateMachine.setShardState(sid, Serving)
					//serveArray = append(serveArray, sid)
				}
			}
		} else if newgid != kv.gid {
			if oldgid == kv.gid {
				if oldgid != 0 {
					kv.stateMachine.setShardState(sid, Sending)
					//sendArray = append(sendArray, sid)
				}

			}
		}
	}

}
