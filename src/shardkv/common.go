package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
import "time"
import "cpsc416/shardctrler"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut 	   = "ErrTimeOut"
)

const WaitTimeOut = 2000 * time.Millisecond

const Debug = false

type Err string

type CommonReply struct {
	Err 		Err
	Value		string
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId	int64
	Seq int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId	int64
	Seq int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ReceiveShardArgs struct {
	Db	map[string]string
	Config shardctrler.Config
	GID		int
}

type ReceiveShardReply struct {
	Err	Err
}