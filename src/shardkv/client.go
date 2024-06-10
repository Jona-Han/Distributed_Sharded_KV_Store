/*
Package shardkv implements a sharded, fault-tolerant key/value store
built on top of a Raft-based replication system. It handles client key-value operations
(Put, Append, Get)
*/
package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"cpsc416/labrpc"
	"cpsc416/shardctrler"
)


// key2shard determines which shard a given key belongs to.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// nrand generates a random int64 number.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Clerk represents a client that communicates with shard controller and shard key/value servers.
type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	clerkId        int64
	seq 		   int64

	logger *Logger
}


// MakeClerk creates a new Clerk instance.
//
// ctrlers is used to create a shard controller clerk.
// make_end is a function that converts a server name to a labrpc.ClientEnd for RPC communication.
//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}
	ck.logger = logger

	ck.seq = 0
	ck.clerkId = nrand()
	ck.config = ck.sm.Query(-1)

	return ck
}

// Get fetches the current value for a key.
// Returns "" if the key does not exist.
// Keeps trying indefinitely in the face of errors.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:		key,
		ClerkId: 	ck.clerkId,
		Seq:	 	atomic.AddInt64(&ck.seq, 1),
		ConfigNum:	ck.config.Num,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)

				if ok && (reply.Err == OK) {
					ck.logger.Log(LogTopicClerk, fmt.Sprintf("C%d Get operation success for seq %d", ck.clerkId, ck.seq))
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// PutAppend is shared by Put and Append operations.
// Keeps trying indefinitely in the face of errors.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:		key,
		Value:		value,
		ClerkId: 	ck.clerkId,
		Op:			op,
		Seq :	 	atomic.AddInt64(&ck.seq, 1),
		ConfigNum:	ck.config.Num,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				// ck.logger.Log(LogTopicClerk, fmt.Sprintf("C%d sent %s operation for key %s to server %v", ck.clerkId, args.Op, key, si))
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.logger.Log(LogTopicClerk, fmt.Sprintf("C%d - %s operation success for seq %d", ck.clerkId, args.Op, ck.seq))
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// Put stores a key-value pair in the key/value store.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append appends a value to an existing key in the key/value store.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
