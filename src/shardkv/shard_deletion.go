/*
Package shardkv implements a sharded, fault-tolerant key/value store
built on top of a Raft-based replication system. It handles client key-value operations
(Put, Append, Get)
*/
package shardkv

import (
	"cpsc416/labrpc"
	"cpsc416/shardctrler"
	"time"
	"sync"
)

// ConfirmShardReceiptArgs represents the arguments for confirming shard receipt.
type ConfirmShardReceiptArgs struct {
	ShardsConfirmed map[int]bool 		// The shards that have been confirmed as received
}

// ConfirmShardReceiptReply represents the reply to a shard receipt confirmation.
type ConfirmShardReceiptReply struct {
	Err Err 							// Error information
}

// ConfirmShardReceipt handles the confirmation of shard receipt and starts the process to delete the shards.
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

// sendReceiptConfirmations sends confirmations of shard receipt to the relevant groups.
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

// sendShardReceiptConfirmation sends a shard receipt confirmation request to a specified group.
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