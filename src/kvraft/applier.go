package kvraft

func (kv *KVServer) applyOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op.Type == "Get" || kv.checkDuplicate(op.ClientID, op.Seq) {
		return
	}

	// 执行操作
	switch op.Type {
	case "Put":
		kv.kvData[op.Key] = op.Value
	case "Append":
		kv.kvData[op.Key] += op.Value
	}

	// 更新客户端序列号
	kv.clientSeq[op.ClientID] = op.Seq
}

func (kv *KVServer) applier() {

	for kv.killed() == false {

		for msg := range kv.applyCh {

			if msg.CommandIndex <= kv.lastApplied {
				continue
			}
			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)

			kv.applyOp(op)

			ch := kv.getNotifyCh(msg.CommandIndex)
			select {
			case ch <- msg.CommandIndex:
			default:
			}

		}
	}

}
