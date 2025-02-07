package shardctrler

import "sort"

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// The very first configuration should be numbered zero,
// and contains no groups, and all shards are assigned to GID 0.
func newConfigs() []Config {
	configs := make([]Config, 1)
	configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	return configs
}

func copyMap(oldMap map[int][]string) map[int][]string {
	newMap := make(map[int][]string)
	for k, v := range oldMap {
		newslice := make([]string, len(v))
		copy(newslice, v)
		newMap[k] = newslice
	}
	return newMap
}

func addMap(original map[int][]string, servers map[int][]string) map[int][]string {
	newMap := copyMap(original)
	for k, v := range servers {
		newMap[k] = v
	}
	return newMap
}

// 贪心策略： 每次都将最多的shard分配给最少的group，直到所有group的shard数目平均或相差1
// 1. 计算每个group的shard数目
// 2. 计算每个group的shard数目的平均值
// 3. 计算每个group的shard数目与平均值的差值
// 4. 将差值最大的shard分配给差值最小的group
// 5. 重复3-4直到所有group的shard数目平均或相差1
func greedyRebalance(shards [NShards]int, groups map[int][]string) [NShards]int {
	// 如果没有可用组，则所有分片设为 0
	if len(groups) == 0 {
		var newShards [NShards]int
		for i := 0; i < NShards; i++ {
			newShards[i] = 0
		}
		return newShards
	}

	// 构造映射：gid -> 当前分配给该组的分片索引列表
	g2shards := make(map[int][]int)
	// 获取 groups 的所有 gid，并初始化映射（为了保证确定性，对 gid 排序）
	var gids []int
	for gid := range groups {
		gids = append(gids, gid)
		g2shards[gid] = []int{}
	}
	sort.Ints(gids)

	// 将当前 shards 中有效的分片（gid 不为 0 且存在于 groups 中）加入 g2shards，
	// 其余视为未分配。
	var unassigned []int
	for i, gid := range shards {
		if gid != 0 {
			if _, ok := groups[gid]; ok {
				g2shards[gid] = append(g2shards[gid], i)
				continue
			}
		}
		unassigned = append(unassigned, i)
	}

	// 将未分配的分片轮询分配给所有组
	for i, shardIdx := range unassigned {
		gid := gids[i%len(gids)]
		g2shards[gid] = append(g2shards[gid], shardIdx)
	}

	// 贪心平衡：不断将 max 组中一个分片转移到 min 组，直到两者差距不超过 1
	for {
		maxGid, maxCount := 0, -1
		minGid, minCount := 0, 1<<31-1 // 一个很大的初始值

		for _, gid := range gids {
			count := len(g2shards[gid])
			if count > maxCount {
				maxCount = count
				maxGid = gid
			}
			if count < minCount {
				minCount = count
				minGid = gid
			}
		}

		if maxCount-minCount <= 1 {
			break
		}

		// 从分片数最多的组（maxGid）中选择一个分片（这里选择列表中的第一个）
		shardToMove := g2shards[maxGid][0]
		// 从 maxGid 的分片列表中移除该分片
		g2shards[maxGid] = g2shards[maxGid][1:]
		// 将该分片加入到分片数最少的组（minGid）的列表中
		g2shards[minGid] = append(g2shards[minGid], shardToMove)
	}

	// 构造新的 shards 分配
	var newShards [NShards]int
	for _, gid := range gids {
		for _, shardIdx := range g2shards[gid] {
			newShards[shardIdx] = gid
		}
	}

	return newShards
}

//--------------------------------------------
// RPC arguments and replies.
//--------------------------------------------

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	Seq      int
	ClientID int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	Seq      int
	ClientID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	Seq      int
	ClientID int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	Seq      int
	ClientID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ReplyContext struct {
	Seq  int
	Type string
	Err  Err
}
