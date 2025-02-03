package kvraft

import (
	"log"
	"os"
)

func (kv *KVServer) applyOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeq, exists := kv.clientSeq[op.ClientID]
	if exists && op.Seq <= lastSeq {
		return
	}
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
	logfile, _ := os.OpenFile("test.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	defer logfile.Close()

	// 将日志输出重定向到日志文件
	log.SetOutput(logfile)
	for kv.killed() == false {

		for msg := range kv.applyCh {
			if msg.CommandValid {
				DPrintf("Server %v apply msg %v\n", kv.me, msg)
				op := msg.Command.(Op)
				kv.applyOp(op)

				kv.mu.Lock()
				if ch, ok := kv.notifyMap[msg.CommandIndex]; ok {
					// 发送操作已完成的信号（例如日志索引）
					DPrintf("Send msg %v to ch %v\n", msg, ch)
					ch <- msg.CommandIndex
					delete(kv.notifyMap, msg.CommandIndex)
				}
				kv.mu.Unlock()
			}
		}
	}
}
