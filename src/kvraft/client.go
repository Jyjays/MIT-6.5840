package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID int64
	seq      int
	leaderId int
	//lock   sync.Mutex
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
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) getLeader() int {
	return ck.leaderId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("C calling the GetRPC key is %s", key)
	// You will have to modify this function.
	ck.seq++
	getArgs := GetArgs{
		Key:      key,
		ClientID: ck.ClientID,
		Seq:      ck.seq,
	}
	for i := ck.getLeader(); ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &getArgs, &reply)
		if !ok {
			continue
		}
		if reply.Err == OK {
			// DPrintf("Get finish key:%v value:%v\n", key, reply.Value)
			ck.leaderId = i
			return reply.Value
		} else if reply.Err == ErrWrongLeader {
			continue
		} else if reply.Err == ErrNoKey {
			ck.leaderId = i
			DPrintf("Get no key key:%v\n", key)
			return ""

		} else if reply.Err == ErrTimeout {
			DPrintf("Get timeout key:%v\n", key)
			i = (i + len(ck.servers) - 1) % len(ck.servers)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq++
	args := PutAppendArgs{Key: key, Value: value, ClientID: ck.ClientID, Seq: ck.seq}

	len := len(ck.servers)
	for i := ck.getLeader(); ; i = (i + 1) % len {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == OK {
			DPrintf("PutAppend finish key:%v value:%v\n", key, value)
			ck.leaderId = i
			break
		} else if reply.Err == ErrWrongLeader {
			continue
		} else if reply.Err == ErrNoKey {
			ck.leaderId = i
			break
		} else if reply.Err == ErrTimeout {
			time.Sleep(100 * time.Millisecond)
			i = (i + len - 1) % len
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
