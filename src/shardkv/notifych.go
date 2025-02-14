package shardkv

type NotifychMsg struct {
	Err   Err
	Value string
}

func (kv *ShardKV) getNotifyChMsg(index int) chan *NotifychMsg {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	ch, ok := kv.notifyMap[index]
	if !ok {
		ch = make(chan *NotifychMsg, 1)
		kv.notifyMap[index] = ch
	}
	return ch
}

func (kv *ShardKV) closeNotifyChMsg(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.notifyMap[index]
	if ok {
		close(ch)
		delete(kv.notifyMap, index)
	}
}
