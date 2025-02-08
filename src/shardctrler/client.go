package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seq      int
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seq++
	args := &QueryArgs{
		Num:      num,
		ClientID: ck.clientId,
		Seq:      ck.seq,
	}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seq++
	args := &JoinArgs{
		Servers:  servers,
		ClientID: ck.clientId,
		Seq:      ck.seq,
	}
	args.Servers = servers

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seq++
	args := &LeaveArgs{
		GIDs:     gids,
		ClientID: ck.clientId,
		Seq:      ck.seq,
	}
	args.GIDs = gids

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seq++
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientID: ck.clientId,
		Seq:      ck.seq,
	}
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
