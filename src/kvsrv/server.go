package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	kvmap map[string]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res, ok := kv.kvmap[args.Key]
	if !ok {
		// Key does not exist, set res to empty string
		res = ""
	}
	reply.Value = res
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvmap[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.k
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.kvmap[args.Key]
	if !ok {
		kv.kvmap[args.Key] = args.Value
		reply.Value = ""
		return
	}
	kv.kvmap[args.Key] = v + args.Value
	reply.Value = v
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvmap = make(map[string]string)
	// You may need initialization code here.

	return kv
}
