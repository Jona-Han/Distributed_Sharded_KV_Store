package shardkv

import "cpsc416/shardctrler"

// Sends shard to shards in gid
func (kv *ShardKV) SendShard(gid int, db map[string]string, config shardctrler.Config) {
	args := ReceiveShardArgs {
		Db: db,
		Config: config,
		GID: kv.gid,
	}
	
	for {
		if servers, ok := config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ReceiveShardReply
		
				ok := srv.Call("ShardKV.ReceiveShard", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
			}
		} else {
			return
		}
	}
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) {
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	op := Op {
		Operation: "InstallMigration",
		DB: make(map[string]string),
		NewConfig: args.Config,
		GID:	args.GID,
	}

	for key,value := range args.Db {
		op.DB[key] = value
	}

	kv.rf.Start(op)

	reply.Err = OK

	kv.mu.Unlock()
}
