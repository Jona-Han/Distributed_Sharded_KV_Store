package shardkv

import "fmt"
import "strconv"

func (kv *ShardKV) Applier() {
	// continuously process messages from the applyCh channel
	for !kv.killed() {
		msg := <-kv.applyCh // wait for a message from Raft
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if msg.CommandIndex == 0 {
				continue
			}

			if ok && (op.Op == "StartConfigChange" || op.Op == "CompleteConfigChange") {
				valid := false
				kv.shardLock.Lock()
				if op.Op == "StartConfigChange" {
					valid = kv.handleConfigChange(op)
				} else {
					valid = kv.handleCompleteConfigChange(op)
				}
				kv.shardLock.Unlock()
				kv.mu.Lock()
				if ch, ok := kv.notifyChans[termPlusIndexToStr(msg.CommandTerm, msg.CommandIndex)]; ok {
					delete(kv.notifyChans, termPlusIndexToStr(msg.CommandTerm, msg.CommandIndex))
					kv.mu.Unlock()
					select {
						case ch <- CacheResponse{OK: valid,}:
							kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for config change term %d, idx %d", kv.me, msg.CommandTerm, msg.CommandIndex))
						default:
							// if the channel is already full, skip to prevent blocking.
					}
				} else {
					kv.mu.Unlock()
				}
			} else if ok {
				kv.shardLock.Lock()
				kv.lastApplied = msg.CommandIndex
				response := kv.handleClientOperation(op, msg.CommandTerm)
				kv.shardLock.Unlock()

				kv.mu.Lock()
				if ch, ok := kv.notifyChans[strconv.FormatInt(op.ClerkId, 10)]; ok {
					kv.mu.Unlock()
					select {
						case ch <-response:
							kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for op ClerkId %d, seq %d", kv.me, op.ClerkId, op.Seq))
						default:
							// if the channel is already full, skip to prevent blocking.
					}
				} else {
					kv.mu.Unlock()
				}
			}
		} else if msg.SnapshotValid {
			kv.shardLock.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.shardLock.Unlock()
			kv.logger.Log(LogTopicServer, fmt.Sprintf("%d - S%d processed a snapshot message", kv.gid, kv.me))
		}
	}
}

func (kv *ShardKV) handleClientOperation(op Op, term int) CacheResponse {
	// check if the operation is the latest from the clerk

	if !kv.acceptingKeyInShard(op.Key) {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejecting op from raft ClerkId %d, seq %d", kv.me, op.ClerkId, op.Seq))
		return CacheResponse{OK: false,}
	}

	response := CacheResponse{
		Seq:		op.Seq,
		Op: 		op.Op,
		Term:		term,
	}

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
		response.OK = true
		kv.cachedResponses[op.ClerkId] = response
	} else if op.Seq == lastReply.Seq {
		kv.logger.Log(LogTopicOp, fmt.Sprintf("S%d sees already applied %s op with seq %d", kv.me, op.Op, op.Seq))
		response = lastReply
		response.OK = true
	} else {
		return CacheResponse{OK: false,}
	}
	return response
}

func (kv *ShardKV) applyOperation(op Op) {
	switch op.Op {
		case "Put":
			kv.db[op.Key] = op.Value
			// kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, op.Value))
		case "Append":
			oldVal, ok := kv.db[op.Key]
			if !ok {
				oldVal = ""
			}
			kv.db[op.Key] = oldVal + op.Value
			// kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, kv.db[op.Key]))
		case "Get":
			// kv.logger.Log(LogTopicOp, fmt.Sprintf("%d - S%d applying %s op with seq %d: %s || %s", kv.gid, kv.me, op.Op, op.Seq, op.Key, kv.db[op.Key]))
			// No state change for Get
	}
}

func (kv *ShardKV) handleConfigChange(op Op) bool {
	if kv.MIP || op.NewConfig.Num <= kv.config.Num {
		return false
	}
	
	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d started config change from %d to %d", kv.gid, kv.me, kv.config.Num, op.NewConfig.Num))
	kv.MIP = true
	copyConfig(&kv.prevConfig, &kv.config)
	copyConfig(&kv.config, &op.NewConfig)

	return true
}

func (kv *ShardKV) handleCompleteConfigChange(op Op) bool {
	if !kv.MIP || op.NewConfig.Num != kv.config.Num {
		return false
	}
	kv.MIP = false

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
	kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("%d - S%d completed config change from %d to %d - %v", kv.gid, kv.me, op.PrevConfig.Num, op.NewConfig.Num, kv.config.Shards))
	return true
}
