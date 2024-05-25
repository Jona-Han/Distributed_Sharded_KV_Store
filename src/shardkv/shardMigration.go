package shardkv

import "cpsc416/shardctrler"
import "time"
import "fmt"
import "cpsc416/labrpc"

func (kv *ShardKV) ConfigChecker() {
	for !kv.killed() {
		kv.shardLock.RLock()
		prevConfig := shardctrler.Config{}
		copyConfig(&prevConfig, &kv.config)
		queryNum := prevConfig.Num + 1
		migrationInProgress := kv.MIP
		kv.shardLock.RUnlock()

		newConfig := kv.sm.Query(queryNum)

		if newConfig.Num > prevConfig.Num || migrationInProgress {
			go func() {
				if !migrationInProgress {
					opToSend := Op{
						Op:			 "StartConfigChange",
						PrevConfig:          prevConfig,
						NewConfig:           newConfig,
					}
					kv.mu.Lock()
		
					idx, term, leader := kv.rf.Start(opToSend)

					if !leader {
						kv.mu.Unlock()
						return
					} else {
						ch := make(chan CacheResponse, 1)
						kv.notifyChans[termPlusIndexToStr(term, idx)] = ch
						kv.mu.Unlock()
						reply := <-ch
						if !reply.OK {
							return
						}
					}
				}

				kv.shardLock.RLock()
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

					kv.shardLock.RUnlock()
					newData, newCachedResponses := kv.getNewShards(requiredShards, prevConfig)

					opToSend := Op{
						Op:			 "CompleteConfigChange",
						PrevConfig:          prevConfig,
						NewConfig:           newConfig,
						ShardData:           newData,
						NewCache:    		 newCachedResponses,
					}

					ch := make(chan CacheResponse, 1)
					kv.mu.Lock()
					idx, term, leader := kv.rf.Start(opToSend)
					kv.notifyChans[termPlusIndexToStr(term, idx)] = ch

					if !leader {
						kv.mu.Unlock()
						return
					} else {
						ch := make(chan CacheResponse, 1)
						kv.notifyChans[termPlusIndexToStr(term, idx)] = ch
						kv.mu.Unlock()
						reply := <-ch
						if !reply.OK {
							return
						}
					}
				} else {
					kv.shardLock.RUnlock()
				}
			}()
		}

		time.Sleep(200 * time.Millisecond)
	}
}

type RequestShardArgs struct {
	ConfigNum 		int
	ShardsRequested map[int]bool
}

type RequestShardReply struct {
	ShardData 	map[string]string
	PrevCache	map[int64]CacheResponse
	Err	Err
}

func (kv *ShardKV) requestNewShards(RequiredShards []int,
	prevConfig shardctrler.Config) (map[string]string, map[int64]CacheResponse) {

	newDb := make(map[string]string)
	newCache := make(map[int64]CacheResponse)

	// For each group, create a set of shards to request
	shardsFromGroup := make(map[int]map[int]bool)
	for _, shard := range RequiredShards {
		gid := prevConfig.Shards[shard]
		if _, ok := shardsFromGroup[gid]; !ok {
			shardsFromGroup[gid] = make(map[int]bool)
		}
		shardsFromGroup[gid][shard] = true
	}

	for gid, shards := range shardsFromGroup {
		args := RequestShardArgs{
			ShardsRequested:    shards,
			ConfigNum: prevConfig.Num,
		}

		for {
			if servers, ok := kv.prevConfig.Groups[gid]; ok {
				shardReceived := false
				// try each server for the shard.
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					var reply RequestShardReply
	
					ok = srv.Call("ShardKV.RequestShard", &args, &reply)
					if ok && (reply.Err == OK) {
						for k, v := range reply.ShardData {
							newDb[k] = v
						}
	
						for clerkId, reply := range reply.PrevCache {
							oldResp, ok := newCache[clerkId]
							if !ok || reply.Seq > oldResp.Seq {
								newCache[clerkId] = reply
							}
						}
						shardReceived = true
						break
					}
				}
				if shardReceived {
					break
				}
			}
		}
	}
	return newDb, newCache
}

func (kv *ShardKV) getNewShards(RequiredShards []int,
	oldConfig shardctrler.Config) (map[string]string, map[int64]CacheResponse) {
	// for now sequentially req shards

	newKv := make(map[string]string)
	newClientReplyMap := make(map[int64]CacheResponse)
	shardsFromGroup := make(map[int]map[int]bool)

	for _, shard := range RequiredShards {
		gid := oldConfig.Shards[shard]
		if _, ok := shardsFromGroup[gid]; !ok {
			shardsFromGroup[gid] = make(map[int]bool)
		}
		shardsFromGroup[gid][shard] = true
	}

	for gid, shards := range shardsFromGroup {
		group, _ := oldConfig.Groups[gid]

		req := RequestShardArgs{
			ShardsRequested:    shards,
			ConfigNum: oldConfig.Num,
		}
		reply := RequestShardReply{}
		kv.sendShardRequest(&req, &reply, group)
		for k, v := range reply.ShardData {
			newKv[k] = v
		}
		for cid, newReply := range reply.PrevCache {
			oldReply, ok := newClientReplyMap[cid]
			if !ok || newReply.Seq > oldReply.Seq {
				newClientReplyMap[cid] = newReply
			}
		}
	}

	return newKv, newClientReplyMap
}

func (kv *ShardKV) sendShardRequest(args *RequestShardArgs, reply *RequestShardReply, group []string) {

	servers := make([]*labrpc.ClientEnd, len(group))
	for i, serverName := range group {
		servers[i] = kv.make_end(serverName)
	}

	doneChan := make(chan *RequestShardReply, 10)
	leader := 0
	sendReq := func(to int) {
		// log.Printf("group %d, sending req %d to %d", kv.gid, args, to)
		sendShardReply := RequestShardReply{}
		ok := servers[to].Call("ShardKV.RequestShard", args, &sendShardReply)
		if ok {
			doneChan <- &sendShardReply
		}
	}

	go sendReq(leader)

	for {
		select {
		case sendShardReply := <-doneChan:
			if sendShardReply.Err == ErrOutdated {
				time.Sleep(time.Millisecond * 200)
			} else if sendShardReply.Err != OK {
				leader = (leader + 1) % len(servers)
				go sendReq(leader)
			} else {
				*reply = *sendShardReply
				return
			}
		case <-time.After(time.Millisecond * 200):
			leader = (leader + 1) % len(servers)
			go sendReq(leader)
		}

	}
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.logger.Log(LogTopicRequestShard, fmt.Sprintf("%d - S%d requestShard but not the leader", kv.gid, kv.me))
		reply.Err = ErrWrongLeader
		return
	}

	kv.shardLock.RLock()
	defer kv.shardLock.RUnlock()

	if args.ConfigNum >= kv.config.Num {
		kv.logger.Log(LogTopicRequestShard, fmt.Sprintf("%d - S%d requestShard outdated %d < %d", kv.gid, kv.me, kv.config.Num, args.ConfigNum))
		reply.Err = ErrOutdated
		return
	}

	reply.ShardData = make(map[string]string)
	for k, v := range kv.db {
		if val, found := args.ShardsRequested[key2shard(k)]; val && found {
			reply.ShardData[k] = v
		}
	}

	reply.PrevCache = make(map[int64]CacheResponse)
	for k, v := range kv.cachedResponses {
		reply.PrevCache[k] = v
	}
	kv.logger.Log(LogTopicRequestShard, fmt.Sprintf("%d - S%d sending shards %v", kv.gid, kv.me, reply.ShardData))

	reply.Err = OK
}
