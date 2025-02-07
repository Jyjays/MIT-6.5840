package shardctrler

type NotifychMsg struct {
	Err   Err
	Value string
}

func (sc *ShardCtrler) getNotifyChMsg(index int) chan *NotifychMsg {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.notifyMap[index]
}

func (sc *ShardCtrler) closeNotifyChMsg(index int) {

}

func checkMsg(index int, flag bool, msg *NotifychMsg, reply Reply) bool {
	return true
}
