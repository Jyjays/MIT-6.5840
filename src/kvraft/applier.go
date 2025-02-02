package kvraft

func (kv *KVServer) applyOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 执行操作
	switch op.Type {
	case "Put":
		kv.kvData[op.Key] = op.Value
	case "Append":
		currentValue, exists := kv.kvData[op.Key]
		if !exists {
			currentValue = ""
		}
		kv.kvData[op.Key] = currentValue + op.Value
	}

	// 更新客户端序列号
	kv.clientSeq[op.ClientID] = op.Seq
	kv.clearCache(op.ClientID)
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.applyOp(op)

			kv.mu.Lock()
			if ch, ok := kv.notifyMap[msg.CommandIndex]; ok {
				// 发送操作已完成的信号（例如日志索引）
				ch <- &msg
				delete(kv.notifyMap, msg.CommandIndex)
			}
			kv.mu.Unlock()
		}
	}
}
