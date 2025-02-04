package kvraft

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

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

	maxraftstate int // snapshot if log grows this big
	//REVIEW - lastApplied shouldn't be in the server
	lastApplied  int // 最后一个已经应用到状态机的日志索引
	stateMachine *kvStateMachine
	//cache       map[int64]string
	lastOperation map[int64]ReplyContext
	notifyMap     map[int]chan *NotifychMsg
	persist       *raft.Persister // 持久化存储（Part B 使用）
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
	if !kv.stateMachine.hasKey(args.Key) {
		reply.Err = ErrNoKey
		return
	} else {
		op := Op{
			Type:     "Get",
			Key:      args.Key,
			ClientID: args.ClientID,
			Seq:      args.Seq,
		}
		kv.mu.Unlock()
		index, flag, msg := kv.startOp(op)
		kv.mu.Lock()
		if !checkMsg(index, flag, msg, reply) {
			return
		}
		reply.Value = msg.Value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.checkDuplicate(args.ClientID, args.Seq) {
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
	kv.mu.Unlock()
	index, flag, msg := kv.startOp(oper)
	kv.mu.Lock()
	if !checkMsg(index, flag, msg, reply) {
		return
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.checkDuplicate(args.ClientID, args.Seq) {
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
	kv.mu.Unlock()
	index, flag, msg := kv.startOp(oper)
	kv.mu.Lock()
	if !checkMsg(index, flag, msg, reply) {
		return
	}
}

func (kv *KVServer) startOp(op Op) (int, bool, *NotifychMsg) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return index, false, nil
	}
	ch := kv.getNotifyChMsg(index)

	// 等待结果或超时
	select {
	case msg := <-ch:
		kv.closeNotifyChMsg(index)
		return index, true, msg
	case <-time.After(100 * time.Millisecond): // 添加超时处理
		DPrintf("Server %v startOp timeout index:%v\n", kv.me, index)
		kv.closeNotifyChMsg(index)
		return index, true, nil
	}
}

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

	kv.stateMachine = newKVStateMachine()
	kv.lastOperation = make(map[int64]ReplyContext)
	kv.notifyMap = make(map[int]chan *NotifychMsg)
	kv.persist = persister

	go kv.applier()
	if Output {
		logfile, _ := os.OpenFile("test.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		// 将日志输出重定向到日志文件
		log.SetOutput(logfile)
	}
	return kv
}
