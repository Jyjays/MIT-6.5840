package kvraft

import (
	"log"
)

const Debug = true
const Output = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// func (kv *KVServer) clearCache(Id int64) {
// 	if Id < 0 {
// 		return
// 	}
// 	delete(kv.cache, Id-1)
// }

func (kv *KVServer) getNotifyCh(index int) chan int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.notifyMap[index]
	if !ok {
		kv.notifyMap[index] = make(chan int, 1)
		ch = kv.notifyMap[index]
	}
	return ch
}

func (kv *KVServer) closeNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.notifyMap[index]
	if ok {
		close(ch)
		delete(kv.notifyMap, index)
	}
}
