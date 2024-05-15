package shardctrler

//
// Shardctrler clerk.
//

import "cpsc416/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	clerkId        int64
	seq 		   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk {
		servers: servers,
		clerkId:    nrand(),
		seq: 		0,
	}

	return ck
}

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
