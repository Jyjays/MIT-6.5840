package kvraft

// func (kv *KVServer) applyOp(op Op) bool {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	if op.Type == "Get" || kv.checkDuplicate(op.ClientID, op.Seq) {
// 		return true
// 	}

// 	// 执行操作
// 	switch op.Type {
// 	case "Put":
// 		kv.kvData[op.Key] = op.Value
// 	case "Append":
// 		kv.kvData[op.Key] += op.Value
// 	}

// 	// 更新客户端序列号
// 	kv.clientSeq[op.ClientID] = op.Seq

// 	return true
// }

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("server {%d} receive applyMsg = %v", kv.me, applyMsg)
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf("server {%d} drop command index = %d lastApplied = %d", kv.me, applyMsg.CommandIndex, kv.lastApplied)
					continue
				}
				// 假设本节点因为网络原因落后于其他的节点 其他节点到日志条目数为n1时做了一个快照 同时这个节点是leader节点
				// 本节点请求日志的时候发现leader节点没有需要的日志（变成快照后删除了） 因此leader节点发送快照给本节点

				// 如果有快照的时候会读取快照 这个时候如果leader中没有保存快照直接把日志发送过来的话 直接返回即可不需要应用
				reply := kv.apply(applyMsg.Command)
				// 这个时候他可能不是leader 或者是发生了分区的旧leader 这两种情况下都不需要通知回复客户端
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CurrentTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}

				// if kv.maxraftstate != -1 && kv.persist.RaftStateSize() > kv.maxraftstate {
				// 	DPrintf("server {%d} get snapshot index = %d maxraftstate = %d raftStateSize = %d\n", kv.me, applyMsg.CommandIndex, kv.maxraftstate, kv.persist.RaftStateSize())
				// 	kv.rf.Snapshot(applyMsg.CommandIndex, kv.getSnapshot())
				// }

				kv.lastApplied = applyMsg.CommandIndex
			}
		}
	}
}
func (kv *KVServer) apply(cmd interface{}) *NotifychMsg {
	reply := &NotifychMsg{}
	op := cmd.(Op)
	DPrintf("server {%d} apply command = %v\n", kv.me, op)
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

	last, ok := kv.lastOperation[op.ClientID]
	if !ok || last.Seq < op.Seq {
		kv.lastOperation[op.ClientID] = ctx
	}
}
