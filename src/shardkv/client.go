package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	seq       int
	clientID  int64
	leaderIDs map[int]int // gid -> leaderID
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.seq = 0
	ck.clientID = nrand()
	ck.config = ck.sm.Query(-1)
	ck.leaderIDs = make(map[int]int)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.seq++
	args := GetArgs{
		Key:      key,
		Seq:      ck.seq,
		ClientID: ck.clientID,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		leaderId, ok := ck.leaderIDs[gid]
		if !ok {
			leaderId = 0
		}
		if servers, ok := ck.config.Groups[gid]; ok {
			n := len(servers)
			// 使用循环队列方式遍历所有服务器
			for i := 0; i < n; i++ {
				si := (leaderId + i) % n // 以 leaderId 为起点，循环遍历
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				// 如果调用成功且错误为 OK 或 ErrNoKey，说明找到了正确的 leader
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// 更新该组的 leaderId 为成功响应的服务器索引
					ck.leaderIDs[gid] = si
					return reply.Value
				}
				// 如果返回 ErrWrongGroup，则说明配置可能已经过时，退出当前循环
				if ok && reply.Err == ErrWrongGroup {
					ck.leaderIDs[gid] = si
					break
				}
				// 对于 ErrWrongLeader 或调用失败，则继续尝试下一个服务器
			}
		}
		// 没有找到正确的服务器或请求失败，等待一段时间后更新最新配置重试
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Seq:      ck.seq,
		ClientID: ck.clientID,
		Op:       op,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		leaderId, ok := ck.leaderIDs[gid]
		if !ok {
			leaderId = 0
		}
		if servers, ok := ck.config.Groups[gid]; ok {
			n := len(servers)
			// 使用循环队列方式遍历所有服务器
			for i := 0; i < n; i++ {
				si := (leaderId + i) % n // 以 leaderId 为起点，循环遍历
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				// 如果调用成功且错误为 OK，则说明找到了正确的 leader
				if ok && reply.Err == OK {
					// 更新该组的 leaderId 为成功响应的服务器索引
					ck.leaderIDs[gid] = si
					return
				}
				// 如果返回 ErrWrongGroup，则说明配置可能已经过时，退出当前循环
				if ok && reply.Err == ErrWrongGroup {
					ck.leaderIDs[gid] = si
					break
				}
				// 对于 ErrWrongLeader 或调用失败，则继续尝试下一个服务器
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
