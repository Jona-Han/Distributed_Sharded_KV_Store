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
	ErrOutdated    = "ErrOutdated"
	ErrConfigOutdated = "ErrConfigOutdated"
)

const WaitTimeOut = 2000 * time.Millisecond

const Debug = true

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

	ClerkId	int64
	Seq int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClerkId	int64
	Seq int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestShardArgs struct {
	Config 			shardctrler.Config
	ShardsRequested map[int]bool
}

type RequestShardReply struct {
	Data 	map[string]string
	Err	Err
}