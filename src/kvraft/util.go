package kvraft

func (kv *KVServer) clearCache(Id int64) {
	if Id < 0 {
		return
	}
	delete(kv.cache, Id-1)
}
