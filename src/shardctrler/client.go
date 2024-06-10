/*
Package shardctrler provides mechanisms to manage shard configurations in a distributed system.
It allows joining new groups, leaving groups, and moving shards between groups.
*/
package shardctrler

import (
	"cpsc416/labrpc"
	"time"
	"crypto/rand"
	"math/big"
)

// Clerk represents a client that communicates with shard controller servers.
type Clerk struct {
	servers []*labrpc.ClientEnd		// List of servers to communicate with
	clerkId        int64			// Unique ID for this Clerk
	seq 		   int64			// Sequence number for requests
}

// nrand generates a random 64-bit integer.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk creates a new Clerk instance with the given servers.
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk {
		servers: servers,
		clerkId:    nrand(),
		seq: 		0,
	}

	return ck
}

// Query requests the configuration number `num` from the shard controller.
// It returns the requested configuration.
func (ck *Clerk) Query(num int) Config {
	ck.seq++
	args := &QueryArgs{
		ClerkId:	ck.clerkId,
		Seq: 		ck.seq,
		Num:		num,
	}
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Join requests the shard controller to join the specified servers to the shard configuration.
func (ck *Clerk) Join(servers map[int][]string) {
	ck.seq++
	args := &JoinArgs{
		ClerkId:	ck.clerkId,
		Seq: 		ck.seq,
		Servers:	servers,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave requests the shard controller to remove the specified groups from the shard configuration.
func (ck *Clerk) Leave(gids []int) {
	ck.seq++
	args := &LeaveArgs{
		ClerkId:	ck.clerkId,
		Seq: 		ck.seq,
		GIDs:		gids,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Move requests the shard controller to move the specified shard to the specified group.
func (ck *Clerk) Move(shard int, gid int) {
	ck.seq++
	args := &MoveArgs{
		ClerkId:	ck.clerkId,
		Seq: 		ck.seq,
		Shard:		shard,
		GID:		gid,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
