/*
Package shardkv implements a sharded, fault-tolerant key/value store
built on top of a Raft-based replication system. It handles client key-value operations
(Put, Append, Get)
*/
package shardkv


import (
	"fmt"
	"sync"
	"sync/atomic"

	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"cpsc416/shardctrler"
)


// Op represents an operation to be applied by the ShardKV server.

type Op struct {
	Op 	string
	ClerkId		int64
	Seq 		int64
	Key			string
	Value		string

	//Migration
	NewConfig			shardctrler.Config
	PrevConfig   		shardctrler.Config
	ShardData           map[string]string
	NewCache    		map[int64]CacheResponse

	ShardsToDelete		map[int]bool
	ShardsReceived		map[int]map[int]bool
}

// CacheResponse represents a response to be cached for handling duplicate requests.
type CacheResponse struct {
	Seq 		int64
	Value		string
	Op			string
	OK			bool
}

// ShardKV represents a sharded key/value server.
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
	persister	 *raft.Persister

	logger *Logger

	sm			*shardctrler.Clerk
	config   	shardctrler.Config
	prevConfig  shardctrler.Config

	cachedResponses   	map[int64]CacheResponse 		// To check for duplicate requests
	notifyChans    		map[string]chan CacheResponse
	lastApplied   		int

	shardsPendingDeletion map[int]bool

	db             		map[string]string
	MIP 				bool 				// Migration in progress
	sl       sync.RWMutex
}

// Get handles the Get RPC request.
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Check if the Op is a duplicate
	op := Op {
		Op:	"Get",
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Key:		args.Key,
	}
	res := kv.checkAndSendOp(op, args.ConfigNum)
	reply.Err = res.Err
	reply.Value = res.Value
}

// PutAppend handles the Put and Append RPC requests.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Check if the Op is a duplicate
	op := Op {
		Op:	args.Op,
		ClerkId:	args.ClerkId,
		Seq:		args.Seq,
		Key:		args.Key,
		Value:		args.Value,
	}
	res := kv.checkAndSendOp(op, args.ConfigNum)
	reply.Err = res.Err
}

// acceptingKeyInShard checks if the current server is responsible for the given key.
func (kv *ShardKV) acceptingKeyInShard(key string) bool {
	if !kv.MIP {
		return kv.config.Shards[key2shard(key)] == kv.gid
	} else {
		inCurrShard := kv.config.Shards[key2shard(key)] == kv.gid
		inPrevShard := kv.prevConfig.Shards[key2shard(key)] == kv.gid
		return inPrevShard && inCurrShard
	}
}

// checkAndSendOp checks for duplicates and sends the operation to Raft if needed.
func (kv *ShardKV) checkAndSendOp(op Op, clerkConfigNum int) CommonReply {
	reply := CommonReply{}

	kv.sl.RLock()
	lastResponse, found := kv.cachedResponses[op.ClerkId]
	if found && lastResponse.Seq == op.Seq {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("%d - S%d received a duplicate %s request from ClerkID %d, Seq %d", kv.gid, kv.me, op.Op, op.ClerkId, op.Seq))
		reply.Err = OK
		reply.Value = lastResponse.Value
		kv.sl.RUnlock()
		return reply
	}

	if !kv.acceptingKeyInShard(op.Key) {
		kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d not accepting %s request for seq %d", kv.gid, kv.me, op.Op, op.Seq))
		reply.Err = ErrWrongGroup
		kv.sl.RUnlock()
		return reply
	}

	if clerkConfigNum > kv.config.Num {
		kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d config is outdated %d compared to %d for seq %d", kv.gid, kv.me, kv.config.Num, clerkConfigNum, op.Seq))
		reply.Err = ErrOutdated
		kv.sl.RUnlock()
		return reply
	}
	kv.sl.RUnlock()

	kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d submits %s Op for seq %d, key=%d", kv.gid, kv.me, op.Op, op.Seq, key2shard(op.Key)))

	kv.mu.Lock()
	idx, term, leader := kv.rf.Start(op)

	if !leader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return reply
	}


	ch := make(chan CacheResponse, 1)
	kv.notifyChans[termPlusIndexToStr(term, idx)] = ch
	kv.mu.Unlock()
	resp := <-ch

	kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d received %s Op for seq %d, key=%d", kv.gid, kv.me, op.Op, op.Seq, key2shard(op.Key)))
	if !resp.OK {
		reply.Err = ErrWrongGroup
	} else if resp.Seq != op.Seq {
		reply.Err = ErrWrongLeader
	} else {
		reply.Value = resp.Value
		reply.Err = OK
	}
	return reply
} 

// Kill stops the ShardKV instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

// killed checks if the ShardKV instance is stopped.
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer initializes a new ShardKV server.
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1 doesn't snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(map[string]string{})
	labgob.Register(map[int64]CacheResponse{})
	labgob.Register(map[int]bool{})
	labgob.Register(CacheResponse{})
	labgob.Register([]int{})
	labgob.Register(map[int][]string{})
	

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	
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
	kv.cachedResponses = make(map[int64]CacheResponse)
	kv.notifyChans = make(map[string]chan CacheResponse)
	kv.shardsPendingDeletion = make(map[int]bool)

	kv.config = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}

	kv.prevConfig = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}

	for i := 0; i < shardctrler.NShards; i++ {
		kv.config.Shards[i] = 0
		kv.prevConfig.Shards[i] = 0
	}
	kv.MIP = false
	kv.lastApplied = 0

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	go kv.configChecker()
	go kv.snapshotChecker()

	return kv
}