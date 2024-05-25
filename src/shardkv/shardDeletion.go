package shardkv
import (
	"cpsc416/labrpc"
	"cpsc416/shardctrler"
	"time"
	"sync"
)

type ConfirmShardReceiptArgs struct {
	ShardsConfirmed map[int]bool
}

type ConfirmShardReceiptReply struct {
	Err Err
}

func (kv *ShardKV) ConfirmShardReceipt(args *ConfirmShardReceiptArgs, reply *ConfirmShardReceiptReply) {
	kv.mu.Lock()

	opToSend := Op{
		Op: "DeleteShards",
		ShardsToDelete:	make(map[int]bool),
	}

	for k, _ := range args.ShardsConfirmed {
		opToSend.ShardsToDelete[k] = true
	}

	idx, term, leader := kv.rf.Start(opToSend)

	if !leader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan CacheResponse, 1)
	kv.notifyChans[termPlusIndexToStr(term, idx)] = ch
	kv.mu.Unlock()
	resp := <-ch

	if !resp.OK {
		reply.Err = ErrWrongLeader
	}
	reply.Err = OK
}

func (kv *ShardKV) sendReceiptConfirmations(shardsByGroup map[int]map[int]bool, prevConfig shardctrler.Config) {
	var wg sync.WaitGroup

	// Request shards from each group
	for gid, shardsMap := range shardsByGroup {
		wg.Add(1)

		go func(gid int, shards map[int]bool) {
			defer wg.Done()
			group, exists := prevConfig.Groups[gid]
			if !exists {
				return
			}

			req := ConfirmShardReceiptArgs{
				ShardsConfirmed:    shards,
			}
			kv.sendShardReceiptConfirmation(&req, group)
		}(gid, shardsMap)
	}
	
	wg.Wait()
}

func (kv *ShardKV) sendShardReceiptConfirmation(args *ConfirmShardReceiptArgs, group []string) {
	servers := make([]*labrpc.ClientEnd, len(group))
	for i, serverName := range group {
		servers[i] = kv.make_end(serverName)
	}

	replyCh := make(chan *ConfirmShardReceiptReply, 10)

	for {
		for si := 0; si < len(servers); si++ {
			go func() {
				rep := ConfirmShardReceiptReply{}
				ok := servers[si].Call("ShardKV.ConfirmShardReceipt", args, &rep)
				if ok {
					replyCh <- &rep
				}
			}()

			select {
			case rep := <-replyCh:
				if rep.Err == ErrOutdated {
					time.Sleep(200 * time.Millisecond )
				} else if rep.Err == OK {
					return
				}
				// Else try next server
			case <-time.After(200 * time.Millisecond):
				// Timeout - try next server
			}
		}
	}
}