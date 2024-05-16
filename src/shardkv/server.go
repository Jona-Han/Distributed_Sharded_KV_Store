package shardkv


import "cpsc416/labrpc"
import "cpsc416/raft"
import "sync"
import "cpsc416/labgob"
import "cpsc416/shardctrler"
import "time"
import "fmt"
import "sync/atomic"


type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation 	string
	ClerkId		int64
	Seq 		int64
	Key			string
	Value		string

	//Migration
	NewConfig	shardctrler.Config
	DB 			map[string]string
	GID			int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead 		 int32

	logger *Logger


	sm			*shardctrler.Clerk
	config   	shardctrler.Config

	clerkLastSeq   map[int64]int64 		// To check for duplicate requests
	notifyChans    map[int64]chan Op
	lastApplied    int

	db             		map[string]string
	MIP 				bool 				// Migration in progress
	commandQueue 		[]Op				// Queue for operations during a migration

	cache 				map[int64]string
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op {
		Operation:	"Get",
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Key:		args.Key,
	}
	res := kv.checkAndSendOp(op, args.ConfigNum)
	reply.Err = res.Err
	reply.Value = res.Value
}


func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op {
		Operation:	args.Op,
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Key:		args.Key,
		Value:		args.Value,
	}
	res := kv.checkAndSendOp(op, args.ConfigNum)
	reply.Err = res.Err
}

func (kv *ShardKV) checkAndSendOp(op Op, clerkConfigNum int) CommonReply {
	reply := CommonReply{}

	kv.mu.Lock()
	if (kv.config.Shards[key2shard(args.Key)] != kv.gid) {
		kv.mu.Unlock()
		kv.logger.Log(LogTopicOperation, fmt.Sprintf("S%d received wrong group for %s request for seq %d", kv.me, args.Op, op.Seq))
		reply.Err = ErrWrongGroup
		return reply
	}

	if clerkConfigNum > kv.config.Num {
		kv.logger.Log(LogTopicOperation, fmt.Sprintf("S%d config is outdated %d compared to %d for seq %d", kv.me, kv.config.Num, clerkConfigNum, op.Seq))
		reply.Err = ErrConfigOutdated
		kv.mu.Unlock()
		return
	}

	// first check if this server is the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d is not the leader for %s request for seq %d", kv.me, op.Operation, op.Seq))
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return reply
	}

	// If leader, then check if a migration is in progress
	// if (kv.MIP) {
	// 	kv.logger.Log(LogTopicOperation, fmt.Sprintf("S%d received %s request for seq %d but MIP with current config num: %d", kv.me, op.Operation, op.Seq, kv.config.Num))
	// 	reply.Err = ErrWrongGroup
	// 	kv.mu.Unlock()
	// 	return
	// }

	// Check if the operation is a duplicate
	if op.Operation != "Get" {
		lastSeq, found := kv.clerkLastSeq[op.ClerkId]
		if found && lastSeq >= op.Seq {
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received a duplicate %s request from ClerkID %d, Seq %d", kv.me, op.Operation, op.ClerkId, op.Seq))
			reply.Err = OK
			kv.mu.Unlock()
			return reply
		}
	}

	kv.logger.Log(LogTopicOperation, fmt.Sprintf("S%d submits %s operation for seq %d", kv.me, op.Operation, op.Seq))

	ch := make(chan Op, 1)
	kv.notifyChans[op.ClerkId] = ch
	kv.rf.Start(op)
	kv.mu.Unlock()

	// set up a timer to avoid waiting indefinitely.
	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select{
	case <-timer.C: // timer expires
		kv.logger.Log(LogTopicOperation, fmt.Sprintf("S%d timed out waiting for request seq %d", kv.me, op.Seq))
		reply.Err = ErrTimeOut
	case resultOp := <-ch: // wait for the operation to be applied
		// check if the operation corresponds to the request
		if resultOp.ClerkId != op.ClerkId || resultOp.Seq != op.Seq {
			kv.logger.Log(LogTopicOperation, fmt.Sprintf("S%d received a non-matching %s result", kv.me, op.Operation))
			reply.Err = ErrWrongLeader
		} else {
			if (resultOp.Operation == "Get") {
				kv.mu.Lock()
				reply.Value = kv.db[op.Key]
				kv.mu.Unlock()
			}
			reply.Err = OK
		}
	}
	return reply
} 

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


/*
Detect config change:
Send config change to raft

APPLIER:
Gets/Puts prior to committed config change respond.
All gets/puts after config change should only apply after migration is done

ConfigChange -> Get/Put (shard 1) -> Get/Put (shard 2)
 1. I still service shard 1 so I can respond immediately.
 2. I no longer service shard 1 so I relay that info back to client.
 3. I didn't used to service shard 1 but now I do. I need to wait for an up to date shard.

OPTIONS:
1. Have a queue for all messages after configChange. Respond to messages after configChange process is done
2. Reject all messages after configChange. Client needs to retry.
3. Accept some operations after reject/queue other
*/
func (kv *ShardKV) ConfigChecker() {
	lastNoticed := 0
	for !kv.killed() {
		kv.mu.Lock()

		newConfig := kv.sm.Query(-1)
		_, isLeader := kv.rf.GetState()
		if isLeader && newConfig.Num > kv.config.Num && !kv.MIP && newConfig.Num > lastNoticed {
			kv.startConfigChange(newConfig)
			lastNoticed = newConfig.Num
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}


func (kv *ShardKV) startConfigChange(newConfig shardctrler.Config) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("S%d noticed config change from %d to %d", kv.me, kv.config.Num, newConfig.Num))

	op := Op{
		Operation: "ConfigChange",
		NewConfig: newConfig,
	}

	kv.rf.Start(op)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}
	kv.logger = logger

	// To talk to the shardctrler:
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)

	kv.clerkLastSeq = make(map[int64]int64)
	kv.notifyChans = make(map[int64]chan Op)
	kv.commandQueue = []Op{}
	kv.cache = make(map[int64]string)

	go kv.Applier()
	go kv.ConfigChecker()

	return kv
}