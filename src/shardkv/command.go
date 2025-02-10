package shardkv

import "6.5840/shardctrler"

type cmd string

type Command struct {
	Type cmd
	Data interface{}
}

const (
	Operation   cmd = "Operation"
	AddConfig       = "AddConfig"
	InsertShard     = "InsertShard"
	DeleteShard     = "DeleteShard"
)

// NewConfigCommand 添加新配置的命令
func NewConfigCommand(cfg *shardctrler.Config) Command {
	return Command{Type: AddConfig, Data: *cfg}
}

// NewOperationCommand 执行操作的命令Put、Append、Get
func NewOperationCommand(args *Op) Command {
	return Command{Type: Operation, Data: *args}
}

func NewInsertShardsCommand(reply *GetShardReply) Command {
	return Command{Type: InsertShard, Data: *reply}
}

func NewDeleteShardsCommand(args *DeleteShardArgs) Command {
	return Command{Type: DeleteShard, Data: *args}
}
