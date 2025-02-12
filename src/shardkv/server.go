package shardkv

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	Type     string // "Get", "Put", "Append"
	Key      string
	Value    string
	ClientID int64 // 唯一标识客户端
	Seq      int   // 客户端分配的递增序列号
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk
	// Your definitions here.
	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config
	lastApplied   int
	stateMachine  *StateMachine
	lastOperation map[int64]ReplyContext
	notifyMap     map[int]chan *NotifychMsg
	persist       *raft.Persister // 持久化存储
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	cmd := NewOperationCommand(&op)

	msg := kv.startCmd(cmd)
	DPrintf("{Group %v Server %v} Get %v\n", kv.gid, kv.me, msg)
	if msg.Err == OK {
		reply.Err = OK
		reply.Value = msg.Value
	} else {
		reply.Err = msg.Err
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	cmd := NewOperationCommand(&op)
	msg := kv.startCmd(cmd)
	if msg.Err == OK {
		reply.Err = OK
	} else {
		reply.Err = msg.Err
	}
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	// DPrintf("Server %v GetShard %v\n", kv.me, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum != kv.lastConfig.Num || args.Gid != kv.gid {
		DPrintf("GetShard err: {Group %v server %v} args.configNum %v, lastconfigNum %v, gid %v\n", kv.gid, kv.me, args.ConfigNum, kv.lastConfig.Num, args.Gid)
		reply.Err = ErrWrongConfigNum
	}
	shards := make(map[int]*Shard)
	for _, sid := range args.ShardIDs {
		shard := kv.stateMachine.getShard(sid)
		if shard != nil && shard.getShardState() == Sending {
			// 这里深拷贝后传递，防止后续修改
			shards[sid] = copyShard(shard)
			//FIXME - 不应该直接设置为GCing，应该等到applyInsertShard后再设置
			//shard.setShardState(GCing)
		}
	}
	if len(shards) == 0 {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
		reply.ConfigNum = kv.lastConfig.Num
		reply.Shards = shards
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) listenConfig() {
	for !kv.killed() {

		kv.mu.Lock()
		currentNum := kv.currentConfig.Num
		kv.mu.Unlock()
		config := kv.mck.Query(currentNum + 1)
		if config.Num == currentNum+1 {
			cmd := NewConfigCommand(&config)
			kv.startCmd(cmd)
		} else {
			//DPrintf("Server %v listenConfig config.Num:%v currentNum:%v\n", kv.me, config.Num, currentNum)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) listenPullingShard() {
	for !kv.killed() {
		kv.mu.Lock()
		pullShards := kv.stateMachine.getShardsByState(Pulling)

		if len(pullShards) > 0 {
			//TODO - pull shards
			DPrintf("{Group %v Server %v} listenPullingShard %v\n", kv.gid, kv.me, pullShards)
			for _, sid := range pullShards {
				get_gid := kv.lastConfig.Shards[sid]
				if get_gid == kv.gid {
					kv.stateMachine.setShardState(sid, Serving)
					continue
				}
				args := GetShardArgs{
					ConfigNum: kv.lastConfig.Num, //NOTE - 应该从旧配置中获取，因为shard数据是在旧配置中的
					ShardIDs:  pullShards,
					Gid:       get_gid,
				}
				for _, server := range kv.lastConfig.Groups[get_gid] {
					srv := kv.make_end(server)

					reply := GetShardReply{}
					ok := srv.Call("ShardKV.GetShard", &args, &reply)

					if ok && reply.Err == OK {
						//TODO - apply shard
						DPrintf("Server %v GetShard %v %v\n", kv.me, args, reply)
						kv.mu.Unlock()
						cmd := NewInsertShardsCommand(&reply)
						kv.startCmd(cmd)
						kv.mu.Lock()
						break
					}
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) processNewConfig(config shardctrler.Config) {
	//三种情况：1.旧配置中有，新配置中没有，删除；2.旧配置中没有，新配置中有，增加；3.旧配置中有，新配置中有，不变
	// toBeInsertedShards := make(map[int]*Shard)
	DPrintf("{Group%v Server %v} processNewConfig %v currentNum %v \n", kv.gid, kv.me, config, kv.currentConfig.Num)
	oldShards := kv.currentConfig.Shards
	newShards := config.Shards
	// 持有者 -> 要拉取的shards
	// pullMap := make(map[int][]int)
	// // 接收端 -> 要发送的shards
	// sendMap := make(map[int][]int)
	DPrintf("{Group %v Server %v} currentConfig %v\n", kv.gid, kv.me, kv.currentConfig)
	for sid := 0; sid < shardctrler.NShards; sid++ {
		newgid := newShards[sid]
		oldgid := oldShards[sid]
		if newgid == kv.gid {
			if oldgid != kv.gid {
				if oldgid != 0 {
					//pullMap[oldgid] = append(pullMap[oldgid], sid)
					kv.stateMachine.setShardState(sid, Pulling)
					// DPrintf("{Group %v Server %v} pull shard %v from %v\n", kv.gid, kv.me, sid, oldgid)
					// DPrintf("{Group %v Server %v} set shard %v state Pulling\n", kv.gid, kv.me, *(kv.stateMachine.getShard(sid)))
				} else {
					kv.stateMachine.insertShard(sid, newShard())
				}

			}
		} else if newgid != kv.gid {
			if oldgid == kv.gid {
				if oldgid != 0 {
					//sendMap[newgid] = append(sendMap[newgid], sid)

					kv.stateMachine.setShardState(sid, Sending)
					// DPrintf("{Group %v Server %v} send shard %v to %v\n", kv.gid, kv.me, sid, newgid)
					// DPrintf("{Group %v Server %v} set shard %v state Sending\n", kv.gid, kv.me, *(kv.stateMachine.getShard(sid)))
				}

			}
		}
	}
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = config

}

func (kv *ShardKV) shardCanServe(sid int) bool {
	if shard := kv.stateMachine.getShard(sid); shard != nil {
		if kv.currentConfig.Shards[sid] == kv.gid && shard.State == Serving {
			return true
		}
	}
	return false
}

func (kv *ShardKV) startCmd(cmd interface{}) *NotifychMsg {
	//DPrintf("Server %v StartCmd %v ", kv.me, cmd)
	var msg *NotifychMsg = nil
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		msg := &NotifychMsg{}
		msg.Err = ErrWrongLeader
		//DPrintf("{Group %v Server %v} StartCmd %v ErrWrongLeader", kv.gid, kv.me, cmd)
		return msg
	}
	ch := kv.getNotifyChMsg(index)
	select {
	case msg = <-ch:
		kv.closeNotifyChMsg(index)
		//DPrintf("{Group %v Server %v} msg:%v\n", kv.gid, kv.me, msg)
	case <-time.After(timeout * time.Millisecond): // 添加超时处理
		//DPrintf("{Group %v Server %v} startOp timeout index:%v\n", kv.gid, kv.me, index)
		kv.closeNotifyChMsg(index)
	}
	return msg
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(Shard{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(GetShardReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.dead = 0
	kv.lastApplied = 0

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.currentConfig = shardctrler.Config{}
	kv.lastConfig = shardctrler.Config{}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stateMachine = newStateMachine()
	kv.notifyMap = make(map[int]chan *NotifychMsg)
	kv.lastOperation = make(map[int64]ReplyContext)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persist = persister

	go kv.applier()
	go kv.listenConfig()
	go kv.listenPullingShard()
	if Output {
		file, _ := os.OpenFile("log.txt", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		log.SetOutput(file)
	}
	return kv
}
