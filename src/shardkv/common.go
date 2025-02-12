package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
const timeout = 1000

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrWrongConfigNum = "ErrWrongConfigNum"
	ErrTimeout        = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq      int
	ClientID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	Seq      int
	ClientID int64
}

type GetReply struct {
	Err   Err
	Value string
}
type ReplyContext struct {
	Seq  int
	Type string
	Err  Err
}

type GetShardArgs struct {
	Gid       int   // 复制组ID
	ShardIDs  []int // 需要获取的分片数组ID
	ConfigNum int   // 复制组的配置号
}

type GetShardReply struct {
	Err            Err
	ConfigNum      int                    // 复制组配置号
	Shards         map[int]*Shard         // 获取的分片数据
	LastRequestMap map[int64]ReplyContext // 请求Map 发送分片的时候也发送这个过去 更新对面复制组的map 用于去重
}

type DeleteShardArgs struct {
	ShardIDs  []int // 需要删除的分片数组ID
	ConfigNum int   // 当前配置号
}

type DeleteShardReply struct {
	Err Err
}
