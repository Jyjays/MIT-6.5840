package shardctrler

type NotifychMsg struct {
	Err    Err
	Config Config
}

func (sc *ShardCtrler) getNotifyChMsg(index int) chan *NotifychMsg {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()

	ch, ok := sc.notifyMap[index]
	if !ok {
		ch = make(chan *NotifychMsg, 2)
		sc.notifyMap[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) closeNotifyChMsg(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	ch, ok := sc.notifyMap[index]
	if ok {
		close(ch)
		delete(sc.notifyMap, index)
	}
}
