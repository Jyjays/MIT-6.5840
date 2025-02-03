package kvraft

import (
	"log"
	"os"
)

func (kv *KVServer) applyOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Server %v applyOp %v\n", kv.me, op)
	if kv.checkDuplicate(op.ClientID, op.Seq) {
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
	logfile, _ := os.OpenFile("test.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	defer logfile.Close()

	// 将日志输出重定向到日志文件
	log.SetOutput(logfile)
	for kv.killed() == false {

		for msg := range kv.applyCh {
			DPrintf("Server %v apply msg %v\n", kv.me, msg)
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
