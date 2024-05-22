package shardkv

import "fmt"

func (kv *ShardKV) Applier() {
	// continuously process messages from the applyCh channel
	for !kv.killed() {
		msg := <-kv.applyCh // wait for a message from Raft
		kv.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)
			kv.lastApplied = msg.CommandIndex

			if op.Op == "StartConfigChange" {
				kv.handleConfigChange(op)
			} else if op.Op == "CompleteConfigChange" {
				kv.handleCompleteConfigChange(op)
			} else {
				kv.handleClientOperation(op)
			}
		} else if msg.SnapshotValid {
			kv.readSnapshot(msg.Snapshot)
			kv.logger.Log(LogTopicServer, fmt.Sprintf("%d - S%d processed a snapshot message", kv.gid, kv.me))
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) handleClientOperation(op Op) {
	// check if the operation is the latest from the clerk

	if !kv.acceptingKeyInShard(op.Key) {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejecting op from raft ClerkId %d, seq %d", kv.me, op.ClerkId, op.Seq))
		return 
	}

	response := CacheResponse{
		Seq:		op.Seq,
		Op: 		op.Op,
	}

	shouldSendResponse := false

	lastReply, found := kv.cachedResponses[op.ClerkId]
	if !found || lastReply.Seq < op.Seq {
		// apply the operation to the state machine
		kv.applyOperation(op)
		if op.Op == "Get" {
			val, ok := kv.db[op.Key]
			if !ok {
				val = ""
			}
			response.Value = val
		}
		kv.cachedResponses[op.ClerkId] = response
		shouldSendResponse = true
	} else if op.Seq == lastReply.Seq {
		kv.logger.Log(LogTopicOp, fmt.Sprintf("S%d sees already applied %s op with seq %d", kv.me, op.Op, op.Seq))
		response = lastReply
		shouldSendResponse = true
	}

	// notify the waiting goroutine if it's present
	if ch, ok := kv.notifyChans[op.ClerkId]; ok && shouldSendResponse {
		select {
			case ch <-response:
				kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for ClerkId %d, seq %d", kv.me, op.ClerkId, op.Seq))
			default:
				// if the channel is already full, skip to prevent blocking.
		}
	}
}

func (kv *ShardKV) applyOperation(op Op) {
	switch op.Op {
		case "Put":
			kv.db[op.Key] = op.Value
			kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, op.Value))
		case "Append":
			kv.db[op.Key] += op.Value
			kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, kv.db[op.Key]))
		case "Get":
			kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, kv.db[op.Key]))
			// No state change for Get
	}
}

func (kv *ShardKV) handleConfigChange(op Op) {
	if kv.MIP || op.NewConfig.Num <= kv.config.Num {
		return
	}
	
	kv.MIP = true

	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d started config change from %d to %d", kv.gid, kv.me, kv.config.Num, op.NewConfig.Num))

	copyConfig(&kv.prevConfig, &kv.config)
	copyConfig(&kv.config, &op.NewConfig)
}

func (kv *ShardKV) handleCompleteConfigChange(op Op) {
	if !kv.MIP || op.NewConfig.Num != kv.config.Num {
		return
	}
	kv.MIP = false
	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d completed config change from %d to %d - %v", kv.gid, kv.me, op.PrevConfig.Num, op.NewConfig.Num, kv.config.Shards))


	copyConfig(&kv.prevConfig, &kv.config)

	// Update shard data into k/v
	for k, v := range op.ShardData {
		kv.db[k] = v
	}

	// Update cached responses
	for clerkId, newResponse := range op.NewCache {
		oldResponse, ok := kv.cachedResponses[clerkId]
		if !ok || newResponse.Seq > oldResponse.Seq {
			kv.cachedResponses[clerkId] = oldResponse
		}
	}
}
