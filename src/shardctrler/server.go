package shardctrler

import (
	"log"
	"os"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs       []Config
	notifyMap     map[int]chan *NotifychMsg
	lastOperation map[int64]ReplyContext
}

type Op struct {
	ClientID int64
	Seq      int
	Type     string
	GIDs     []int            // Leave
	Servers  map[int][]string // Join
	Shard    int              // Move
	GID      int              // Move
	Num      int              // Query
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Type:     "Join",
		Servers:  args.Servers,
	}
	ok, msg := sc.startOp(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = msg.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Type:     "Leave",
		GIDs:     args.GIDs,
	}
	ok, msg := sc.startOp(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = msg.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Type:     "Move",
		Shard:    args.Shard,
		GID:      args.GID,
	}
	ok, msg := sc.startOp(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = msg.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Type:     "Query",
		Num:      args.Num,
	}
	ok, msg := sc.startOp(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = msg.Err
	reply.Config = msg.Config
}

func (sc *ShardCtrler) startOp(op Op) (bool, *NotifychMsg) {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return false, nil
	}
	ch := sc.getNotifyChMsg(index)
	var msg *NotifychMsg = nil
	select {
	case msg = <-ch:
		sc.closeNotifyChMsg(index)

	case <-time.After(timeout * time.Millisecond): // 添加超时处理
		DPrintf("Server %v startOp timeout index:%v\n", sc.me, index)
		sc.closeNotifyChMsg(index)
	}
	return true, msg
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	// close all notify channel and applier goroutine
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	// for index, ch := range sc.notifyMap {
	// 	close(ch)
	// 	delete(sc.notifyMap, index)
	// }

}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	if Output {
		logfile, _ := os.OpenFile("test.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		// 将日志输出重定向到日志文件
		log.SetOutput(logfile)
	}
	DPrintf("Server %v start\n", me)
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = newConfigs()
	sc.notifyMap = make(map[int]chan *NotifychMsg)
	sc.lastOperation = make(map[int64]ReplyContext)

	go sc.applier()

	return sc
}
