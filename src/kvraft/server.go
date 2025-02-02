package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     string // "Get", "Put", "Append"
	Key      string
	Value    string
	ClientID int64 // 唯一标识客户端
	Seq      int   // 客户端分配的递增序列号
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int               // snapshot if log grows this big
	kvData       map[string]string // 键值存储
	cache        map[int64]interface{}
	clientSeq    map[int64]int               // 记录客户端最大序列号
	notifyMap    map[int]chan *raft.ApplyMsg // 通知通道
	persist      *raft.Persister             // 持久化存储（Part B 使用）
}

func (kv *KVServer) IsLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	if value, ok := kv.kvData[args.Key]; ok {
		reply.Value = value
		oper := Op{
			Type:     "Get",
			Key:      args.Key,
			Value:    value,
			ClientID: args.ClientID,
			Seq:      args.Seq,
		}
		index, flag, msg := kv.startOp(oper)
		checkMsg(index, flag, msg, reply)
		DPrintf("Get:Reply:%v\n", reply)
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	lastSeq, exists := kv.clientSeq[args.ClientID]
	if exists && args.Seq <= lastSeq {
		reply.Value = kv.cache[args.ClientID].(string)
		reply.Err = OK
		return
	}
	oper := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	index, flag, msg := kv.startOp(oper)
	if !checkMsg(index, flag, msg, reply) {
		return
	} else {
		kv.cache[args.ClientID] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeq, exists := kv.clientSeq[args.ClientID]
	if exists && args.Seq <= lastSeq {
		reply.Value = kv.cache[args.ClientID].(string)
		reply.Err = OK
		return
	}
	oper := Op{
		Type:     "Append",
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	index, flag, msg := kv.startOp(oper)
	if !checkMsg(index, flag, msg, reply) {
		return
	} else {
		kv.cache[args.ClientID] = args.Value
	}
}

func (kv *KVServer) startOp(op Op) (int, bool, *raft.ApplyMsg) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return index, false, nil
	}
	kv.mu.Lock()
	kv.notifyMap[index] = make(chan *raft.ApplyMsg, 1)
	kv.mu.Unlock()
	// 等待通知并加上超时处理
	select {
	case msg := <-kv.notifyMap[index]:
		return index, true, msg
	case <-time.After(500 * time.Millisecond):
		return index, false, nil
	}
}

func checkDuplication(clientID int64, seq int, clientSeq map[int64]int) bool {
	lastSeq, exists := clientSeq[clientID]
	if exists && seq <= lastSeq {
		return true
	}
	return false
}

func checkMsg(index int, flag bool, msg *raft.ApplyMsg, reply Reply) bool {
	if !flag {
		reply.SetErr(ErrWrongLeader)
		return false
	}
	if !msg.CommandValid || msg.CommandIndex != index {
		reply.SetErr(ErrWrongLeader)
		return false
	}
	reply.SetErr(OK)
	reply.SetValue(msg.Command.(Op).Value)
	return true
}

//NOTE - go 1.18 and above, we can use generics
// func checkMsg[T Reply](index int, flag bool, msg *raft.ApplyMsg, reply T) bool {
// 	if !flag {
// 		reply.SetErr(ErrWrongLeader)
// 		return false
// 	} else {
// 		if !msg.CommandValid || msg.CommandIndex != index {
// 			reply.SetErr(ErrWrongLeader)
// 			return false
// 		} else {
// 			reply.SetErr(OK)
// 		}
// 	}
// 	return true
// }

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.kvData = make(map[string]string)
	kv.clientSeq = make(map[int64]int)
	kv.notifyMap = make(map[int]chan *raft.ApplyMsg)
	kv.persist = persister

	go kv.applier()

	return kv
}
