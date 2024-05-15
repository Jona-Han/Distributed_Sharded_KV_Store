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

	clerkLastSeq   map[int64]int64 // To check for duplicate requests
	notifyChans    map[int64]chan Op
	lastApplied    int

	db             map[string]string
	waitingToBeReceived map[int]bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if (len(kv.waitingToBeReceived) != 0) {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received get operation for key %s but MIP", kv.me, args.Key))
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received get operation for key %s", kv.me, args.Key))
	if (kv.config.Shards[key2shard(args.Key)] == kv.gid) {
		op := Op {
			Operation:	"Get",
			ClerkId:	args.ClerkId,
			Seq:		args.Seq,
			Key:		args.Key,
		}
		kv.mu.Unlock()
		res := kv.checkIfLeaderAndSendOp(op)
		reply.Err = res.Err
		reply.Value = res.Value
	} else {
		kv.mu.Unlock()
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received wrong group for get operation for key %s", kv.me, args.Key))
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if (len(kv.waitingToBeReceived) != 0) {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received get operation for key %s but MIP", kv.me, args.Key))
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if (kv.config.Shards[key2shard(args.Key)] == kv.gid) {
		op := Op {
			Operation:	args.Op,
			ClerkId:	args.ClerkId,
			Seq:		args.Seq,
			Key:		args.Key,
			Value:		args.Value,
		}
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received %s operation for key %s", kv.me, op.Operation, args.Key))
		kv.mu.Unlock()
		res := kv.checkIfLeaderAndSendOp(op)
		reply.Err = res.Err
	} else {
		kv.mu.Unlock()
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received wrong group for put/append operation for key %s", kv.me, args.Key))
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) checkIfLeaderAndSendOp(op Op) CommonReply {
	reply := CommonReply{}

	kv.mu.Lock()
	// first check if this server is the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d is not the leader for %s request on servers", kv.me, op.Operation))
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return reply
	}

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

	ch := make(chan Op, 1)
	kv.notifyChans[op.ClerkId] = ch
	kv.rf.Start(op)
	kv.mu.Unlock()

	// set up a timer to avoid waiting indefinitely.
	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select{
	case <-timer.C: // timer expires
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out waiting for %s request", kv.me, op.Operation))
		reply.Err = ErrTimeOut
	case resultOp := <-ch: // wait for the operation to be applied
		// check if the operation corresponds to the request
		if resultOp.ClerkId != op.ClerkId || resultOp.Seq != op.Seq {
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received a non-matching %s result", kv.me, op.Operation))
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
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) startConfigChange(newConfig shardctrler.Config) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d is the leader for migration from to %v", kv.me, newConfig.Num))

	op := Op{
		Operation: "ConfigChange",
		NewConfig: newConfig,
	}

	kv.rf.Start(op)
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
	for !kv.killed() {
		kv.mu.Lock()

		newConfig := kv.sm.Query(-1)

		if newConfig.Num > kv.config.Num && len(kv.waitingToBeReceived) == 0 {
			kv.startConfigChange(newConfig)
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
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

	kv.waitingToBeReceived = make(map[int]bool)

	go kv.Applier()
	go kv.ConfigChecker()

	return kv
}

func (kv *ShardKV) Applier() {
	// continuously process messages from the applyCh channel
	for !kv.killed() {
		msg := <-kv.applyCh // wait for a message from Raft
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			//Check if message is migration
			if op.Operation == "ConfigChange" {
				if kv.config.Num >= op.NewConfig.Num {
					kv.mu.Unlock()
					continue
				}

				oldConfig := kv.config
				kv.config = op.NewConfig

				// Check if the current group is now responsible for the shard
				for shard, oldGID := range oldConfig.Shards {
					if kv.config.Shards[shard] == kv.gid && oldGID != kv.gid && oldGID != 0 {
						kv.waitingToBeReceived[oldGID] = true
					}
				}

				shardsToTransfer := make(map[int]map[string]string)
				for key, value := range kv.db {
					newGid := kv.config.Shards[key2shard(key)]
					if newGid != kv.gid && newGid != 0 {
						if shardsToTransfer[newGid] == nil{
							shardsToTransfer[newGid] = make(map[string]string)	
						}
						shardsToTransfer[newGid][key] = value
					}
				}

				for gid, db := range shardsToTransfer {
					go kv.SendShard(gid, db, kv.config)
				}
			} else if op.Operation == "InstallMigration" {
				oldConfig := kv.config
				kv.config = op.NewConfig

				if oldConfig.Num < kv.config.Num {
					for shard, oldGID := range oldConfig.Shards {
						if kv.config.Shards[shard] == kv.gid && oldGID != kv.gid && oldGID != 0 {
							kv.waitingToBeReceived[oldGID] = true
						}
					}
	
					shardsToTransfer := make(map[int]map[string]string)
					for key, value := range kv.db {
						newGid := kv.config.Shards[key2shard(key)]
						if newGid != kv.gid && newGid != 0 {
							if shardsToTransfer[newGid] == nil{
								shardsToTransfer[newGid] = make(map[string]string)	
							}
							shardsToTransfer[newGid][key] = value
						}
					}
	
					for gid, db := range shardsToTransfer {
						go kv.SendShard(gid, db, kv.config)
					}
				}

				for key,value := range op.DB {
					kv.db[key] = value
				}
				delete(kv.waitingToBeReceived, op.GID)
			} else if (op.Operation == "CompleteMigration") {
			
			} else if (kv.config.Shards[key2shard(op.Key)] == kv.gid) {
				// check if the operation is the latest from the clerk
				lastSeq, found := kv.clerkLastSeq[op.ClerkId]
				if !found || lastSeq < op.Seq {
					// apply the operation to the state machine
					kv.applyOperation(op)
					kv.clerkLastSeq[op.ClerkId] = op.Seq
				}

				// notify the waiting goroutine if it's present
				if ch, ok := kv.notifyChans[op.ClerkId]; ok {
					select {
					case ch <- op: // notify the waiting goroutine
						kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for ClerkId %d", kv.me, op.ClerkId))
					default:
						// if the channel is already full, skip to prevent blocking.
					}
				}
			}
			kv.lastApplied = msg.CommandIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyOperation(op Op) {
	switch op.Operation {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	case "Get":
		// No state change for Get
	}
}