package kvsrv

import (
	"log"
	"sync"
)
// forbiden the debug
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// KVServer is a key-value server.
type KVServer struct {
	mu    sync.Mutex
	kvmap map[string]string
	cache map[int64]string
}
// Get a value
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res, ok := kv.kvmap[args.Key]
	if !ok {
		// Key does not exist, set res to empty string
		res = ""
	}
	reply.Value = res
}

// Put func return the operation value
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.cache[args.Id]; ok {
		reply.Value = args.Value
		return 
	}
	kv.kvmap[args.Key] = args.Value
	kv.cache[args.Id] = ""
	reply.Value = args.Value
	kv.clearCache(args.Id)
}

// Append func return the origin string
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value,ok := kv.cache[args.Id]; ok{
		reply.Value = value
		return 
	}
	v, ok := kv.kvmap[args.Key]
	if !ok {
		kv.kvmap[args.Key] = args.Value
		reply.Value = ""
		kv.cache[args.Id] = ""
		return
	}
	kv.cache[args.Id] = v
	kv.kvmap[args.Key] = v + args.Value
	reply.Value = v
	kv.clearCache(args.Id)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvmap = make(map[string]string)
	kv.cache = make(map[int64]string)
	return kv
}
func (kv *KVServer)clearCache(Id int64){
	if Id < 0 {
		return
	}
	delete(kv.cache, Id - 1)
}
