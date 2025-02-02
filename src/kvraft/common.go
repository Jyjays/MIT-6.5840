package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type Reply interface {
	SetErr(err Err)
	SetValue(value string)
	GetErr() Err
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int
}

type PutAppendReply struct {
	Err   Err
	Value string
}

func (reply *PutAppendReply) SetErr(err Err) {
	reply.Err = err
}

func (reply *PutAppendReply) SetValue(value string) {
	reply.Value = value
}

func (reply *PutAppendReply) GetErr() Err {
	return reply.Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply *GetReply) SetErr(err Err) {
	reply.Err = err
}

func (reply *GetReply) SetValue(value string) {
	reply.Value = value
}

func (reply *GetReply) GetErr() Err {
	return reply.Err
}
