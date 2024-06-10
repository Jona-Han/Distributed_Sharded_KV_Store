/*
Package shardkv implements a sharded, fault-tolerant key/value store
built on top of a Raft-based replication system. It handles client key-value operations
(Put, Append, Get)
*/
package shardkv

import (
	"fmt"
	"sync"
	"time"

	"cpsc416/labrpc"
	"cpsc416/shardctrler"
)

// configChecker periodically checks for new configurations and initiates
// the configuration change process if necessary.
func (kv *ShardKV) configChecker() {
	for !kv.killed() {
		kv.sl.RLock()
		prevConfig := shardctrler.Config{}
		copyConfig(&prevConfig, &kv.config)
		queryNum := prevConfig.Num + 1
		migrationInProgress := kv.MIP
		kv.sl.RUnlock()

		newConfig := kv.sm.Query(queryNum)

		if newConfig.Num > prevConfig.Num || migrationInProgress {
			go kv.startConfigChange(migrationInProgress, prevConfig, newConfig)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// startConfigChange starts the process of changing the configuration, including
// requesting and applying new shards if needed.
func (kv *ShardKV) startConfigChange(migrationInProgress bool, prevConfig shardctrler.Config, newConfig shardctrler.Config) {
	if !migrationInProgress {
		opToSend := Op{
			Op:        "StartConfigChange",
			PrevConfig: prevConfig,
			NewConfig:  newConfig,
		}
	
		kv.mu.Lock()
		idx, term, leader := kv.rf.Start(opToSend)
		if !leader {
			kv.mu.Unlock()
			return
		}
	
		ch := make(chan CacheResponse, 1)
		kv.notifyChans[termPlusIndexToStr(term, idx)] = ch
		kv.mu.Unlock()
	
		reply := <-ch
		if !reply.OK {
			return
		}
	}
	
	kv.sl.RLock()
	if kv.MIP {
		prevConfig := shardctrler.Config{}
		newConfig := shardctrler.Config{}
		copyConfig(&newConfig, &kv.config)
		copyConfig(&prevConfig, &kv.prevConfig)
	
		// Find and get shards I need
		requiredShards := []int{}
		for shard, newGid := range newConfig.Shards {
			prevGid := prevConfig.Shards[shard]
			if prevGid != 0 && newGid == kv.gid && prevGid != newGid {
				requiredShards = append(requiredShards, shard)
			}
		}
		kv.sl.RUnlock()
	
		newData, newCachedResponses, shardsReceived := kv.requestNewShards(requiredShards, prevConfig)
	
		opToSend := Op{
			Op:          "CompleteConfigChange",
			PrevConfig:  prevConfig,
			NewConfig:   newConfig,
			ShardData:   newData,
			NewCache:    newCachedResponses,
			ShardsReceived:	shardsReceived,
		}
	
		ch := make(chan CacheResponse, 1)
		kv.mu.Lock()
		idx, term, leader := kv.rf.Start(opToSend)
		if !leader {
			kv.mu.Unlock()
			return
		}
		kv.notifyChans[termPlusIndexToStr(term, idx)] = ch
		kv.mu.Unlock()
	
		reply := <-ch
		if !reply.OK {
			return
		}
	} else {
		kv.sl.RUnlock()
	}
}

// requestNewShards requests the necessary shards from other groups and returns
// the new data and cached responses.
func (kv *ShardKV) requestNewShards(requiredShards []int, 
	prevConfig shardctrler.Config) (map[string]string, map[int64]CacheResponse, map[int]map[int]bool) {
	newKv := make(map[string]string)
	newClientReplyMap := make(map[int64]CacheResponse)
	shardsByGroup := make(map[int]map[int]bool)

	// Group required shards by group ID
	for _, shard := range requiredShards {
		gid := prevConfig.Shards[shard]
		if _, ok := shardsByGroup[gid]; !ok {
			shardsByGroup[gid] = make(map[int]bool)
		}
		shardsByGroup[gid][shard] = true
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	// Request shards from each group
	for gid, shardsMap := range shardsByGroup {
		wg.Add(1)

		go func(gid int, shards map[int]bool) {
			defer wg.Done()
			group, exists := prevConfig.Groups[gid]
			if !exists {
				return
			}

			req := RequestShardArgs{
				ShardsRequested:    shards,
				ConfigNum:			prevConfig.Num,
			}
			reply := RequestShardReply{}
			kv.sendShardRequest(&req, &reply, group)

			mu.Lock()

			// Merge received data
			for k, v := range reply.ShardData {
				newKv[k] = v
			}

			for cid, newReply := range reply.PrevCache {
				oldReply, ok := newClientReplyMap[cid]
				if !ok || newReply.Seq > oldReply.Seq {
					newClientReplyMap[cid] = newReply
				}
			}
			mu.Unlock()

		}(gid, shardsMap)
	}
	
	wg.Wait()
	return newKv, newClientReplyMap, shardsByGroup
}


// RequestShardArgs represents the arguments for a shard request.
type RequestShardArgs struct {
	ConfigNum       int            		// The configuration number at the time of the request
	ShardsRequested map[int]bool   		// The shards being requested
}

// RequestShardReply represents the reply to a shard request.
type RequestShardReply struct {
	ShardData   map[string]string      	// The data for the requested shards
	PrevCache   map[int64]CacheResponse // Cached responses for the requested shards
	Err         Err                    	// Error information
}

// sendShardRequest sends a shard request to the specified group and handles the response.
func (kv *ShardKV) sendShardRequest(args *RequestShardArgs, reply *RequestShardReply, group []string) {
	servers := make([]*labrpc.ClientEnd, len(group))
	for i, serverName := range group {
		servers[i] = kv.make_end(serverName)
	}

	replyCh := make(chan *RequestShardReply, 10)

	for {
		for si := 0; si < len(servers); si++ {
			go func() {
				rep := RequestShardReply{}
				ok := servers[si].Call("ShardKV.RequestShard", args, &rep)
				if ok {
					replyCh <- &rep
				}
			}()

			select {
			case rep := <-replyCh:
				if rep.Err == ErrOutdated {
					time.Sleep(200 * time.Millisecond )
				} else if rep.Err == OK {
					*reply = *rep
					return
				}
				// Else try next server
			case <-time.After(200 * time.Millisecond):
				// Timeout - try next server
			}
		}
	}
}

// RequestShard handles an incoming shard request from another group.
func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	kv.sl.RLock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.logger.Log(LogTopicRequestShard, fmt.Sprintf("%d - S%d requestShard but not the leader", kv.gid, kv.me))
		kv.sl.RUnlock()
		reply.Err = ErrWrongLeader
		return
	}

	if args.ConfigNum >= kv.config.Num {
		kv.logger.Log(LogTopicRequestShard, fmt.Sprintf("%d - S%d requestShard outdated %d < %d", kv.gid, kv.me, kv.config.Num, args.ConfigNum))
		kv.sl.RUnlock()
		reply.Err = ErrOutdated
		return
	}

	reply.ShardData = make(map[string]string)
	reply.PrevCache = make(map[int64]CacheResponse)
	for k, v := range kv.db {
		if val, found := args.ShardsRequested[key2shard(k)]; val && found {
			reply.ShardData[k] = v
		}
	}

	for k, v := range kv.cachedResponses {
		reply.PrevCache[k] = v
	}
	kv.sl.RUnlock()

	kv.logger.Log(LogTopicRequestShard, fmt.Sprintf("%d - S%d sending shards %v", kv.gid, kv.me, reply.ShardData))
	reply.Err = OK
}

