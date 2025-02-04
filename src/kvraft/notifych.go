package kvraft

type NotifychMsg struct {
	Err   Err
	Value string
}

func (kv *KVServer) getNotifyChMsg(index int) chan *NotifychMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.notifyMap[index]
	if !ok {
		ch = make(chan *NotifychMsg, 1)
		kv.notifyMap[index] = ch
	}
	return ch
}

func (kv *KVServer) closeNotifyChMsg(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.notifyMap[index]
	if ok {
		close(ch)
		delete(kv.notifyMap, index)
	}
}

func checkMsg(index int, flag bool, msg *NotifychMsg, reply Reply) bool {
	if !flag {
		reply.SetErr(ErrWrongLeader)
		return false
	}
	if msg == nil {
		reply.SetErr(ErrTimeout)
		return false
	}
	if msg.Err != OK {
		reply.SetErr(msg.Err)
		return false
	}
	reply.SetErr(OK)
	return true
}
