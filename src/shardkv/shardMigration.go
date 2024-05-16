package shardkv

import "cpsc416/shardctrler"


func (kv *ShardKV) GetShard(gid int, shardsToRequest map[int]bool, newConfig shardctrler.Config, responseChan chan RequestShardReply) {
	args := RequestShardArgs {
		Config: newConfig,
		ShardsRequested: shardsToRequest,
	}
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader || kv.config.Num > newConfig.Num {
			defer kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if servers, ok := kv.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply RequestShardReply
		
				ok := srv.Call("ShardKV.RequestShard", &args, &reply)
				if ok && reply.Err == OK {
					responseChan <- reply
					return
				}
			}
		} else {
			return
		}
	}
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.config.Num < args.Config.Num {
		reply.Err = ErrOutdated
		return
	}

	reply.Data = make(map[string]string)
	for key, value := range kv.db {
		if args.ShardsRequested[key2shard(key)] {
			reply.Data[key] = value
		}
	}

	reply.Err = OK
}
