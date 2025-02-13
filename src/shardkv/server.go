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
	mu           sync.RWMutex
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
	//DPrintf("{Group %v Server %v} Get:Op %v %v Server ConfigNum: %v\n", kv.gid, kv.me, op, msg, kv.currentConfig.Num)
	if msg.Err == OK {
		reply.Err = OK
		reply.Value = msg.Value
	} else {
		reply.Err = msg.Err
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// kv.mu.RLock()
	// if kv.checkDuplicate(args.ClientID, args.Seq) {
	// 	lastOp := kv.lastOperation[args.ClientID]
	// 	kv.mu.RUnlock()
	// 	reply.Err = lastOp.Err
	// 	return
	// }
	// kv.mu.RUnlock()
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

func (kv *ShardKV) isLeader() bool {
	if _, isLeader := kv.rf.GetState(); isLeader {
		return true
	}
	return false
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 要求只有 Leader 响应
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	// 如果请求的配置编号与当前配置不匹配，则返回错误
	// 注意：如果你希望处理拉取旧配置的数据，可以在这里进行适当调整，比如允许 args.ConfigNum 等于 kv.lastConfig.Num
	if args.ConfigNum != kv.currentConfig.Num {
		DPrintf("GetShard err: {Group %v server %v} args.ConfigNum %v, currentConfig.Num %v, gid %v\n",
			kv.gid, kv.me, args.ConfigNum, kv.currentConfig.Num, args.Gid)
		reply.Err = ErrWrongConfigNum
		return
	}
	// 检查 gid 是否匹配
	if args.Gid != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	shards := make(map[int]*Shard)
	for _, sid := range args.ShardIDs {
		shard := kv.stateMachine.getShard(sid)
		if shard != nil {
			// 这里进行深拷贝
			shards[sid] = copyShard(shard)
		}
	}

	if len(shards) == 0 {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
		reply.ConfigNum = kv.currentConfig.Num
		reply.Shards = shards
		// deepcopy lastop
		reply.LastRequestMap = make(map[int64]ReplyContext)
		for k, v := range kv.lastOperation {
			reply.LastRequestMap[k] = v
		}
	}
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	if args.ConfigNum < kv.currentConfig.Num {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	cmd := NewDeleteShardsCommand(args)
	msg := kv.startCmd(cmd)
	reply.Err = msg.Err

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
		if !kv.isLeader() {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		flag := true
		//FIXME - If there's any shard's state is not Serving, then don't listen new config
		for _, shard := range kv.stateMachine.Shards {
			if shard.getShardState() == Pulling {
				DPrintf("{Group %v Server %v} listenConfig shard %v state Pulling\n", kv.gid, kv.me, shard)
				flag = false
				break
			}
		}
		currentNum := kv.currentConfig.Num
		kv.mu.Unlock()
		if flag {
			config := kv.mck.Query(currentNum + 1)
			if config.Num == currentNum+1 {
				cmd := NewConfigCommand(&config)
				kv.startCmd(cmd)
			} else {
				//DPrintf("Server %v listenConfig config.Num:%v currentNum:%v\n", kv.me, config.Num, currentNum)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// func (kv *ShardKV) listenPullingShard() {
// 	for !kv.killed() {
// 		if !kv.isLeader() {
// 			time.Sleep(100 * time.Millisecond)
// 			continue
// 		}
// 		kv.mu.Lock()
// 		pullShards := kv.stateMachine.getShardsByState(Pulling)
// 		//DPrintf("{Group %v Server %v} listenPullingShard %v\n", kv.gid, kv.me, pullShards)
// 		pullMeShardIDs := make([]int, 0) // gid为当前组的shard
// 		if len(pullShards) > 0 {
// 			//DPrintf("{Group %v Server %v} listenPullingShard %v\n", kv.gid, kv.me, pullShards)
// 			for _, sid := range pullShards {
// 				get_gid := kv.lastConfig.Shards[sid]
// 				if get_gid == kv.gid {
// 					pullMeShardIDs = append(pullMeShardIDs, sid)
// 					continue
// 				}
// 				args := GetShardArgs{
// 					ConfigNum: kv.currentConfig.Num,
// 					ShardIDs:  pullShards,
// 					Gid:       get_gid,
// 				}

// 				for _, server := range kv.lastConfig.Groups[get_gid] {
// 					srv := kv.make_end(server)
// 					reply := GetShardReply{}
// 					ok := srv.Call("ShardKV.GetShard", &args, &reply)
// 					DPrintf("{Group %v Server %v} GetShard %v reply %v\n", kv.gid, kv.me, args, reply)
// 					if ok && reply.Err == OK {
// 						kv.mu.Unlock()
// 						cmd := NewInsertShardsCommand(&reply)
// 						kv.startCmd(cmd)
// 						kv.mu.Lock()
// 						break
// 					}
// 				}
// 			}
// 		}
// 		if len(pullMeShardIDs) > 1 {
// 			updateShardState := &UpdateShardState{
// 				ConfigNum:  kv.currentConfig.Num,
// 				ShardIDs:   pullMeShardIDs,
// 				ShardState: Serving,
// 			}
// 			cmd := NewUpdateShardsCommand(updateShardState)
// 			kv.mu.Unlock()
// 			kv.startCmd(cmd)
// 			kv.mu.Lock()
// 		}
// 		kv.mu.Unlock()
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }
func (kv *ShardKV) listenPullingShard() {
	for !kv.killed() {
		// 只有 leader 才执行迁移操作
		if !kv.isLeader() {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// 先获取当前需要拉取的 shard 列表以及相关配置信息，快速复制后释放锁
		kv.mu.RLock()
		pullingShards := kv.stateMachine.getShardsByState(Pulling)
		currentConfigNum := kv.currentConfig.Num
		// 对 lastConfig 做个深拷贝，避免后续变化影响使用
		lastConfig := kv.lastConfig.DeepCopy()
		kv.mu.RUnlock()

		// 如果没有需要拉取的 shard，则等待一会再重试
		if len(pullingShards) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 将 pullShards 按照原持有者（即 lastConfig.Shards[sid]）分组
		// 对于那些在旧配置中已经属于本组的 shard，我们不需要拉取数据，只需直接将状态更新为 Serving
		groupToShardIDs := make(map[int][]int)
		var selfShardIDs []int
		for _, sid := range pullingShards {
			origGid := lastConfig.Shards[sid]
			if origGid == kv.gid {
				selfShardIDs = append(selfShardIDs, sid)
			} else {
				groupToShardIDs[origGid] = append(groupToShardIDs[origGid], sid)
			}
		}

		// 对于旧配置中本组已拥有的 shard，只更新状态为 Serving
		if len(selfShardIDs) > 0 {
			updateArgs := &UpdateShardState{
				ConfigNum:  currentConfigNum,
				ShardIDs:   selfShardIDs,
				ShardState: Serving,
			}
			// 提交状态更新命令（此处可以直接调用 startCmd，因为整个过程也通过 Raft 复制）
			kv.startCmd(NewUpdateShardsCommand(updateArgs))
		}

		// 对于需要从其他组拉取数据的 shard，按 group 并发发送 RPC
		var wg sync.WaitGroup
		for gid, shardIDs := range groupToShardIDs {
			servers, ok := lastConfig.Groups[gid]
			if !ok || len(servers) == 0 {
				continue
			}
			wg.Add(1)
			go func(gid int, shardIDs []int, servers []string, configNum int) {
				defer wg.Done()
				// 构造 GetShard RPC 参数，这里传递需要拉取的 shardIDs 和 configNum（使用当前配置编号）
				args := GetShardArgs{
					ConfigNum: configNum,
					ShardIDs:  shardIDs,
					Gid:       gid,
				}
				// 轮流尝试组内的服务器
				for _, server := range servers {
					srv := kv.make_end(server)
					var reply GetShardReply
					if ok := srv.Call("ShardKV.GetShard", &args, &reply); ok && reply.Err == OK {
						// 成功拉取 shard 数据后，通过 Raft 命令插入这些 shard
						kv.startCmd(NewInsertShardsCommand(&reply))
						return
					}
				}
			}(gid, shardIDs, servers, currentConfigNum)
		}
		wg.Wait()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) processNewConfig(config shardctrler.Config) {
	//三种情况：1.旧配置中有，新配置中没有，删除；2.旧配置中没有，新配置中有，增加；3.旧配置中有，新配置中有，不变
	// toBeInsertedShards := make(map[int]*Shard)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num != kv.currentConfig.Num+1 {
		return // 拒绝跳跃式配置更新
	} else {
		kv.lastConfig = kv.currentConfig.DeepCopy()
		kv.currentConfig = config
	}
	// if !kv.isLeader() {
	// 	return
	// }

	oldShards := kv.lastConfig.Shards
	newShards := kv.currentConfig.Shards
	// 持有者 -> 要拉取的shards
	//pullMap := make(map[int][]int)
	pullArray := make([]int, 0)
	// 接收端 -> 要发送的shards
	serveArray := make([]int, 0)
	//sendMap := make(map[int][]int)
	sendArray := make([]int, 0)
	DPrintf("{Group %v Server %v} newconfig %v\n", kv.gid, kv.me, config)
	for sid := 0; sid < shardctrler.NShards; sid++ {
		newgid := newShards[sid]
		oldgid := oldShards[sid]
		if newgid == kv.gid {
			if oldgid != kv.gid {
				// 仅当旧配置中的分片不属于当前组时设为 Pulling
				if oldgid != 0 && oldgid != kv.gid {
					kv.stateMachine.setShardState(sid, Pulling)
					//pullArray = append(pullArray, sid)
					//DPrintf("{Group %v Server %v} set shard %v state Pulling\n", kv.gid, kv.me, sid)
				} else {
					// 旧配置中分片已属于当前组，直接设为 Serving
					kv.stateMachine.setShardState(sid, Serving)
					//serveArray = append(serveArray, sid)
				}
			}
		} else if newgid != kv.gid {
			if oldgid == kv.gid {
				if oldgid != 0 {
					kv.stateMachine.setShardState(sid, Sending)
					//sendArray = append(sendArray, sid)
				}

			}
		}
	}
	if len(pullArray) != 0 {
		uss := &UpdateShardState{
			ConfigNum:  config.Num,
			ShardIDs:   pullArray,
			ShardState: Pulling,
		}
		updateShardStateCmd := NewUpdateShardsCommand(uss)
		kv.mu.Unlock()
		msg := kv.startCmd(updateShardStateCmd)
		kv.mu.Lock()
		DPrintf("{Group %v Server %v} processNewConfig %v\n", kv.gid, kv.me, msg)
		if GC {
			if msg.Err == OK {
				dss := &DeleteShardArgs{
					ShardIDs:  sendArray,
					ConfigNum: config.Num,
				}
				deleteShardCmd := NewDeleteShardsCommand(dss)
				kv.mu.Unlock()
				kv.startCmd(deleteShardCmd)
				kv.mu.Lock()
			}
		}
	}
	if len(serveArray) != 0 {
		uss := &UpdateShardState{
			ConfigNum:  config.Num,
			ShardIDs:   serveArray,
			ShardState: Serving,
		}
		updateShardStateCmd := NewUpdateShardsCommand(uss)
		kv.mu.Unlock()
		kv.startCmd(updateShardStateCmd)
		kv.mu.Lock()
	}

}
func (kv *ShardKV) shardCanServe(sid int) bool {
	shard := kv.stateMachine.getShard(sid)
	if shard != nil {
		if kv.currentConfig.Shards[sid] == kv.gid && shard.getShardState() == Serving {
			return true
		}
	}
	//DPrintf("{Group %v Server %v} shardCanServe %v false state %v\n", kv.gid, kv.me, sid, shard.getShardState())
	return false
}

func (kv *ShardKV) startCmd(cmd interface{}) *NotifychMsg {
	//DPrintf("Server %v StartCmd %v ", kv.me, cmd)
	var msg *NotifychMsg = nil
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		msg := &NotifychMsg{}
		msg.Err = ErrWrongLeader
		DPrintf("{Group %v Server %v} StartCmd %v ErrWrongLeader", kv.gid, kv.me, cmd)
		return msg
	}
	ch := kv.getNotifyChMsg(index)
	select {
	case msg = <-ch:
		kv.closeNotifyChMsg(index)
		DPrintf("{Group %v Server %v} StartCmd %v msg:%v\n", kv.gid, kv.me, cmd, msg)
	case <-time.After(timeout * time.Millisecond): // 添加超时处理
		DPrintf("{Group %v Server %v} startOp %v timeout index:%v\n", kv.gid, kv.me, cmd, index)
		kv.closeNotifyChMsg(index)
		msg = &NotifychMsg{}
		msg.Err = ErrTimeout
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
	labgob.Register(UpdateShardState{})
	labgob.Register(DeleteShardArgs{})
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

	kv.restoreSnapshot(kv.persist.ReadSnapshot())
	go kv.applier()
	go kv.listenConfig()
	go kv.listenPullingShard()
	if Output {
		file, _ := os.OpenFile("log.txt", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		log.SetOutput(file)
	}
	return kv
}
