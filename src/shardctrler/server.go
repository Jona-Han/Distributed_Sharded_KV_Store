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

	clerkLastSeq   map[int64]int64 // To check for duplicate requests
	notifyChans    map[int64]chan Op
	lastApplied int

	configs []Config // indexed by config num
}


type Op struct {
	Operation 	string
	ClerkId		int64
	Seq 		int64
	Servers map[int][]string
	GIDs 		[]int
	Shard int
	GID   int
	Num int
}


// Creates a new config that includes new replica groups
// Shards divided as evenly as possible, moving as few shards as possible
// Allows reuse of GID
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op {
		Operation:	"Join",
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Servers:	args.Servers,
	}

	result := sc.checkIfLeaderAndSendOp(op, args.ClerkId, args.Seq)
	reply.WrongLeader = result.WrongLeader
	reply.Err = result.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op {
		Operation:	"Leave",
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		GIDs:		args.GIDs,
	}

	result := sc.checkIfLeaderAndSendOp(op, args.ClerkId, args.Seq)
	reply.WrongLeader = result.WrongLeader
	reply.Err = result.Err
}


func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op {
		Operation:	"Move",
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Shard:		args.Shard,
		GID:		args.GID,
	}

	result := sc.checkIfLeaderAndSendOp(op, args.ClerkId, args.Seq)
	reply.WrongLeader = result.WrongLeader
	reply.Err = result.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op {
		Operation:	"Query",
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Num:		args.Num,
	}
	
	result := sc.checkIfLeaderAndSendOp(op, args.ClerkId, args.Seq)
	reply.WrongLeader = result.WrongLeader
	reply.Err = result.Err
	reply.Config = result.Config
}

func (sc *ShardCtrler) checkIfLeaderAndSendOp(op Op, clerkId int64, seq int64) CommonReply {
	reply := CommonReply{}

	sc.mu.Lock()
	// first check if this server is the leader
	if _, isLeader := sc.rf.GetState(); !isLeader {
		sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d is not the leader for %s request on servers", sc.me, op.Operation))
		reply.WrongLeader = true
		sc.mu.Unlock()
		return reply
	}

	ch := make(chan Op, 1)
	sc.notifyChans[clerkId] = ch
	sc.rf.Start(op)
	sc.mu.Unlock()

	// set up a timer to avoid waiting indefinitely.
	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // timer expires
		sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out waiting for %s request", sc.me, op.Operation))
		reply.Err = ErrTimeOut
	case resultOp := <-ch: // wait for the operation to be applied
		// check if the operation corresponds to the request
		if resultOp.ClerkId != clerkId || resultOp.Seq != seq {
			sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d received a non-matching %s result", sc.me, op.Operation))
			reply.WrongLeader = true
		} else {
			if (op.Operation == "Query") {
				configNum := resultOp.Num
				if configNum == -1 || configNum >= len(sc.configs) {
					reply.Config = sc.configs[len(sc.configs) - 1]
				} else {
					reply.Config = sc.configs[configNum]
				}
			}
			sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed %s request", sc.me, op.Operation))
		}
	}
	return reply
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

	// Your code here.
	sc.clerkLastSeq = make(map[int64]int64)
	sc.notifyChans = make(map[int64]chan Op)

	go sc.applier()

	return sc
}


func (sc *ShardCtrler) applier() {
	// continuously process messages from the applyCh channel
	for !sc.killed() {
		msg := <-sc.applyCh // wait for a message from Raft
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			// check if the operation is the latest from the clerk
			lastSeq, found := sc.clerkLastSeq[op.ClerkId]
			if !found || lastSeq < op.Seq {
				// apply the operation to the state machine
				sc.applyOperation(op)
				sc.clerkLastSeq[op.ClerkId] = op.Seq
			}

			// notify the waiting goroutine if it's present
			if ch, ok := sc.notifyChans[op.ClerkId]; ok {
				select {
				case ch <- op: // notify the waiting goroutine
				    sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for ClerkId %d", sc.me, op.ClerkId))
				default:
					// if the channel is already full, skip to prevent blocking.
				}
			}
			sc.lastApplied = msg.CommandIndex
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) applyOperation(op Op) {
	switch op.Operation {
	case "Join":
		sc.applyJoin(op)
	case "Leave":
		sc.applyLeave(op)
	case "Move":
		sc.applyMove(op)
	}
}



