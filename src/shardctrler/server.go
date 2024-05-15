package shardctrler

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead int32

	logger *Logger

	configs []Config // indexed by config num
}


type Op struct {

}


// Creates a new config that includes new replica groups
// Shards divided as evenly as possible, moving as few shards as possible
// Allows reuse of GID
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

}


func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	sc := new(ShardCtrler)
	sc.me = me
	sc.logger = logger

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	return sc
}



