package shardkv

import "cpsc416/shardctrler"
import "time"

func (kv *ShardKV) ConfigChecker() {
	for !kv.killed() {
		kv.mu.Lock()
		migrationInProgress := kv.MIP
		prevConfig := shardctrler.Config{}
		copyConfig(&prevConfig, &kv.config)
		queryNum := prevConfig.Num + 1

		newConfig := kv.sm.Query(queryNum)

		if newConfig.Num > prevConfig.Num && !migrationInProgress {
			opToSend := Op{
				Op:			 "StartConfigChange",
				PrevConfig:          prevConfig,
				NewConfig:           newConfig,
			}

			kv.rf.Start(opToSend)
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) ConfigMigrator() {
	for !kv.killed() {
		prevConfig := shardctrler.Config{}
		newConfig := shardctrler.Config{}
		kv.mu.Lock()
		copyConfig(&newConfig, &kv.config)
		copyConfig(&prevConfig, &kv.prevConfig)

		if kv.MIP {
			// Find and get shards I need
			requiredShards := []int{}
			for shard, newGid := range newConfig.Shards {
				prevGid := prevConfig.Shards[shard]
				if prevGid != 0 && newGid == kv.gid && prevGid != newGid {
					requiredShards = append(requiredShards, shard)
				}
			}
			kv.mu.Unlock()
			newData, newCachedResponses := kv.requestNewShards(requiredShards, prevConfig)
			kv.mu.Lock()

			opToSend := Op{
				Op:			 "CompleteConfigChange",
				PrevConfig:          prevConfig,
				NewConfig:           newConfig,
				ShardData:           newData,
				NewCache:    		 newCachedResponses,
			}
			kv.rf.Start(opToSend)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
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
		reply := RequestShardReply{}

		if servers, ok := kv.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.RequestShard", &args, &reply)
				if ok && (reply.Err == OK) {
					break
				}
			}
		}

		for k, v := range reply.ShardData {
			newDb[k] = v
		}
		for clerkId, reply := range reply.PrevCache {
			oldResp, ok := newCache[clerkId]
			if !ok || reply.Seq > oldResp.Seq {
				newCache[clerkId] = reply
			}
		}
	}

	return newDb, newCache
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrOutdated
		return
	}

	reply.ShardData = make(map[string]string)
	for k, v := range kv.db {
		if args.ShardsRequested[key2shard(k)] {
			reply.ShardData[k] = v
		}
	}

	reply.PrevCache = make(map[int64]CacheResponse)
	for k, v := range kv.cachedResponses {
		reply.PrevCache[k] = v
	}

	reply.Err = OK
}
