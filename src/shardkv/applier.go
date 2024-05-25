package shardkv

import "fmt"

func (kv *ShardKV) applier() {
	// continuously process messages from the applyCh channel
	for !kv.killed() {
		msg := <-kv.applyCh // wait for a message from Raft
		if msg.CommandValid {
			op, _ := msg.Command.(Op)
			kv.sl.Lock()
			kv.lastApplied = msg.CommandIndex

			var response CacheResponse

			switch op.Op {
			case "StartConfigChange":
				response = kv.handleConfigChange(op)
			case "CompleteConfigChange":
				response = kv.handleCompleteConfigChange(op)
			case "DeleteShards":
				response = kv.handleDeleteShards(op)
			default:
				response = kv.handleClientOperation(op, msg.CommandTerm)
			}
			kv.sl.Unlock()

			// Send message to channel
			key := termPlusIndexToStr(msg.CommandTerm, msg.CommandIndex)
			kv.mu.Lock()
			if ch, ok := kv.notifyChans[key]; ok {
				delete(kv.notifyChans, key)
				kv.mu.Unlock()
				ch <-response
			} else {
				kv.mu.Unlock()
			}
		} else {
			kv.sl.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.sl.Unlock()
			kv.logger.Log(LogTopicServer, fmt.Sprintf("%d - S%d processed a snapshot message", kv.gid, kv.me))
		}
	}
}

func (kv *ShardKV) handleClientOperation(op Op, term int) CacheResponse {
	// check if the operation is the latest from the clerk

	if !kv.acceptingKeyInShard(op.Key) {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejecting op from raft ClerkId %d, seq %d", kv.me, op.ClerkId, op.Seq))
		return CacheResponse{OK: false}
	}

	lastReply, found := kv.cachedResponses[op.ClerkId]
	if !found || lastReply.Seq < op.Seq {
		// apply the operation to the state machine
		response := CacheResponse{
			Seq:		op.Seq,
			Op: 		op.Op,
			OK:			 true,
		}

		kv.applyOperation(op)
		if op.Op == "Get" {
			if val, ok := kv.db[op.Key]; ok {
				response.Value = val
			} else {
				response.Value = ""
			}
		}
		
		kv.cachedResponses[op.ClerkId] = response
		return response
	} else if op.Seq == lastReply.Seq {
		kv.logger.Log(LogTopicOp, fmt.Sprintf("S%d sees already applied %s op with seq %d", kv.me, op.Op, op.Seq))
		return lastReply
	} else {
		return CacheResponse{OK: false}
	}
}

func (kv *ShardKV) applyOperation(op Op) {
	switch op.Op {
		case "Put":
			kv.db[op.Key] = op.Value
			// kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, op.Value))
		case "Append":
			kv.db[op.Key] += op.Value
			// kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, kv.db[op.Key]))
		case "Get":
			// kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, kv.db[op.Key]))
			// No state change for Get
	}
}

func (kv *ShardKV) handleConfigChange(op Op) CacheResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.MIP || op.NewConfig.Num <= kv.config.Num {
		return CacheResponse{OK: false}
	}

	for shard, gid := range kv.config.Shards {
		if gid == kv.gid && op.NewConfig.Shards[shard] != kv.gid {
			kv.shardsPendingDeletion[shard] = true
		}
	}
	
	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d started config change from %d to %d", kv.gid, kv.me, kv.config.Num, op.NewConfig.Num))
	kv.MIP = true
	copyConfig(&kv.prevConfig, &kv.config)
	copyConfig(&kv.config, &op.NewConfig)

	return CacheResponse{OK: true}
}

func (kv *ShardKV) handleCompleteConfigChange(op Op) CacheResponse {
	if !kv.MIP || op.NewConfig.Num != kv.config.Num {
		return CacheResponse{OK: false}
	}
	kv.MIP = false

	go kv.sendReceiptConfirmations(op.ShardsReceived, kv.prevConfig)

	copyConfig(&kv.prevConfig, &kv.config)

	// Update shard data into k/v
	for k, v := range op.ShardData {
		kv.db[k] = v
	}

	// Update cached responses
	for clerkId, newResponse := range op.NewCache {
		oldResponse, ok := kv.cachedResponses[clerkId]
		if !ok || newResponse.Seq > oldResponse.Seq {
			kv.cachedResponses[clerkId] = newResponse
		}
	}
	// kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d completed config change from %d to %d - %v", kv.gid, kv.me, op.PrevConfig.Num, op.NewConfig.Num, kv.config.Shards))
	return CacheResponse{OK: true}
}

func (kv *ShardKV) handleDeleteShards(op Op) CacheResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for shard := range op.ShardsToDelete {
		if _, exists := kv.shardsPendingDeletion[shard]; exists {
			delete(kv.shardsPendingDeletion, shard)
			for k := range kv.db {
				if key2shard(k) == shard {
					delete(kv.db, k)
				}
			}
		}
	}
	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d deleted shards %v", kv.gid, kv.me, op.ShardsToDelete))
	return CacheResponse{OK: true}
}