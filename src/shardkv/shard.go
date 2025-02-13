package shardkv

type ShardState int

const (
	Serving ShardState = iota // 当前分片正常服务中
	Pulling                   // 当前分片正在从其他复制组中拉取
	Sending                   // 当前分片正在复制给其他复制组
	GCing                     // 当前分片正在等待清除（监视器检测到之后需要从拥有这个分片的复制组中删除分片）
	Unknown
)

func (s *ShardState) String() string {
	switch *s {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case Sending:
		return "Sending"
	case GCing:
		return "GCing"
	}
	return "Unknown"
}

type Shard struct {
	KvData map[string]string
	State  ShardState
}

func newShard() *Shard {
	return &Shard{
		KvData: make(map[string]string),
		State:  Serving,
	}
}

func (s *Shard) put(key string, value string) {
	s.KvData[key] = value
}

func (s *Shard) get(key string) string {
	return s.KvData[key]
}

func (s *Shard) append(key string, value string) {
	s.KvData[key] += value
}

func (s *Shard) apply(op Op) (string, bool) {
	switch op.Type {
	case "Get":
		if s.hasKey(op.Key) {
			return s.get(op.Key), true
		}
		DPrintf("Didn't find key %v", op.Key)
		return "", false
	case "Put":
		s.put(op.Key, op.Value)
		return "", true
	case "Append":
		s.append(op.Key, op.Value)
		return "", true
	}
	return "", false
}

func (s *Shard) hasKey(key string) bool {
	_, ok := s.KvData[key]
	return ok
}

func (s *Shard) getShardState() ShardState {
	return s.State
}

func (s *Shard) setShardState(state ShardState) {
	s.State = state
}

func copyShard(s *Shard) *Shard {
	if s == nil {
		return nil
	}
	newShard := new(Shard)
	// 深拷贝 map
	newShard.KvData = make(map[string]string)
	for k, v := range s.KvData {
		newShard.KvData[k] = v
	}
	newShard.State = s.State
	return newShard
}
