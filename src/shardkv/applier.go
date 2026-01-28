package shardkv

import (
	"fmt"

	"6.5840/shardctrler"
)

func (kv *ShardKV) applier() {
	monitor := GetMonitor()

	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				// 记录Raft日志应用事件
				monitor.LogEvent("RAFT", kv.gid, kv.me, fmt.Sprintf("Applying command at index %d", applyMsg.CommandIndex), map[string]interface{}{
					"commandIndex": applyMsg.CommandIndex,
					"currentTerm":  applyMsg.CurrentTerm,
				})

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

				// 记录状态机更新事件
				monitor.LogEvent("RAFT", kv.gid, kv.me, fmt.Sprintf("Command applied successfully at index %d", applyMsg.CommandIndex), map[string]interface{}{
					"lastApplied": kv.lastApplied,
					"isLeader":    isLeader,
					"term":        currentTerm,
				})

				kv.mu.Unlock()
			}
			if applyMsg.SnapshotValid {
				// 记录快照应用事件
				monitor.LogEvent("RAFT", kv.gid, kv.me, fmt.Sprintf("Applying snapshot at index %d", applyMsg.SnapshotIndex), map[string]interface{}{
					"snapshotIndex": applyMsg.SnapshotIndex,
				})

				kv.mu.Lock()
				if applyMsg.SnapshotIndex > kv.lastApplied {
					kv.restoreSnapshot(applyMsg.Snapshot)
					kv.lastApplied = applyMsg.SnapshotIndex

					monitor.LogEvent("RAFT", kv.gid, kv.me, "Snapshot restored successfully", map[string]interface{}{
						"lastApplied": kv.lastApplied,
					})
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
	// 调用此函数时已经持有 kv.mu 锁

	ch, ok := kv.notifyMap[index]

	if ok {
		select {
		case ch <- reply:
			// 成功发送
		default:
			// channel 满了（虽然 buffer=1 且逻辑正确时不应该满），
			// 但为了健壮性，防止阻塞 applier 导致死锁，这里必须非阻塞
		}
	} else {
		// ok 为 false，说明 StartCmd 已经超时并调用了 delete，
		// 或者这条命令是重复的。
		// 直接忽略即可，什么都不用做。
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
	monitor := GetMonitor()

	oldShards := kv.currentConfig.Shards
	newShards := config.Shards
	kv.lastConfig = kv.currentConfig.DeepCopy()
	kv.currentConfig = config.DeepCopy()

	monitor.LogEvent("CONFIG", kv.gid, kv.me, fmt.Sprintf("Processing config change: %d -> %d", kv.lastConfig.Num, config.Num), map[string]interface{}{
		"oldConfigNum": kv.lastConfig.Num,
		"newConfigNum": config.Num,
		"oldShards":    oldShards,
		"newShards":    newShards,
	})

	DPrintf("{Group %v Server %v} newconfig %v\n", kv.gid, kv.me, config)
	for sid := 0; sid < shardctrler.NShards; sid++ {
		newgid := newShards[sid]
		oldgid := oldShards[sid]
		if newgid == kv.gid {
			if oldgid != kv.gid {
				// 仅当旧配置中的分片不属于当前组时设为 Pulling
				if oldgid != 0 && oldgid != kv.gid {
					kv.stateMachine.setShardState(sid, Pulling)
					monitor.LogEvent("SHARD", kv.gid, kv.me, fmt.Sprintf("Shard %d state changed to Pulling", sid), map[string]interface{}{
						"shardId": sid,
						"oldGid":  oldgid,
						"newGid":  newgid,
						"state":   "Pulling",
					})
					//pullArray = append(pullArray, sid)
					//DPrintf("{Group %v Server %v} set shard %v state Pulling\n", kv.gid, kv.me, sid)
				} else {
					// 旧配置中分片已属于当前组，直接设为 Serving
					kv.stateMachine.setShardState(sid, Serving)
					monitor.LogEvent("SHARD", kv.gid, kv.me, fmt.Sprintf("Shard %d state changed to Serving", sid), map[string]interface{}{
						"shardId": sid,
						"oldGid":  oldgid,
						"newGid":  newgid,
						"state":   "Serving",
					})
					//serveArray = append(serveArray, sid)
				}
			}
		} else if newgid != kv.gid {
			if oldgid == kv.gid {
				if oldgid != 0 {
					kv.stateMachine.setShardState(sid, Sending)
					monitor.LogEvent("SHARD", kv.gid, kv.me, fmt.Sprintf("Shard %d state changed to Sending", sid), map[string]interface{}{
						"shardId": sid,
						"oldGid":  oldgid,
						"newGid":  newgid,
						"state":   "Sending",
					})
					//sendArray = append(sendArray, sid)
				}

			}
		}
	}

}
