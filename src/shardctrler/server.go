/*
Package shardctrler provides mechanisms to manage shard configurations in a distributed system.
It allows joining new groups, leaving groups, and moving shards between groups.
*/
package shardctrler

import (
	"cpsc416/raft"
	"cpsc416/kvsRPC"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

// ShardCtrler is a controller for managing shard configurations in a distributed system.
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead int32

	logger *Logger

	clerkLastSeq map[int64]int64       // To check for duplicate requests
	notifyChans  map[int64]chan Op     // Notification channels for each clerk
	lastApplied  int                   // Index of the last applied log entry
	configs      []Config              // Indexed by config number
}

// Op represents an operation to be applied by the ShardCtrler.
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


// Join handles a request to add new replica groups to the shard configuration.
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

// Leave handles a request to remove replica groups from the shard configuration.
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

// Move handles a request to move a shard to a different replica group.
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

// Query handles a request to retrieve the current or specified shard configuration.
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

// checkIfLeaderAndSendOp checks if the current server is the leader and sends the operation to Raft.
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
		sc.mu.Lock()
		if resultOp.ClerkId != clerkId || resultOp.Seq != seq {
			sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d received a non-matching %s result", sc.me, op.Operation))
			reply.WrongLeader = true
			sc.mu.Unlock()
		} else {
			if (op.Operation == "Query") {
				configNum := resultOp.Num
				if configNum == -1 || configNum >= len(sc.configs) {
					reply.Config = sc.configs[len(sc.configs) - 1]
				} else {
					reply.Config = sc.configs[configNum]
				}
			}
			sc.mu.Unlock()
			sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed %s request", sc.me, op.Operation))
		}
	}
	return reply
} 


// Kill stops the ShardCtrler instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

// killed checks if the ShardCtrler instance is stopped.
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft returns the Raft instance associated with the ShardCtrler.
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer initializes a new ShardCtrler server.
func StartServer(servers []kvsRPC.RPCClient, me int, persister *raft.Persister) *ShardCtrler {
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	sc := new(ShardCtrler)
	sc.me = me
	sc.logger = logger

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

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

// applyOperation applies the given operation to the state machine.
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



