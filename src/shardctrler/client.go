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

}

func (ck *Clerk) Join(servers map[int][]string) {

}

func (ck *Clerk) Leave(gids []int) {

}

func (ck *Clerk) Move(shard int, gid int) {

}
