package shardkv

import "time"
import "fmt"

func (kv *ShardKV) Applier() {
	// continuously process messages from the applyCh channel
	for !kv.killed() {
		msg := <-kv.applyCh // wait for a message from Raft
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			if _, isLeader := kv.rf.GetState(); isLeader && op.Operation == "ConfigChange" {
				kv.handleLeaderConfigChange(op)
			} else if op.Operation == "ConfigChange" {
				fmt.Println("follower")
				go kv.handleFollowerConfigChange(op)
			} else if op.Operation == "CompleteConfigChange" {
				kv.logger.Log(LogTopicConfigChange, fmt.Sprintf("S%d completed config change from %d to %d ", kv.me, kv.config.Num, op.NewConfig.Num))
				kv.handleCompleteConfigChange(op)
			} else if kv.MIP {
				// Ad op to command queue
				kv.logger.Log(LogTopicMIP, fmt.Sprintf("S%d queued %s op with key %s ", kv.me, op.Operation, op.Key))
				kv.commandQueue = append(kv.commandQueue, op)
			} else if (kv.config.Shards[key2shard(op.Key)] == kv.gid) {
				kv.logger.log(LogTopicOperation, fmt.Sprintf("S%d applying "))
				kv.handleKvOperation(op)
			}
			kv.lastApplied = msg.CommandIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) handleKvOperation(op Op) {
	// check if the operation is the latest from the clerk
	lastSeq, found := kv.clerkLastSeq[op.ClerkId]
	if !found || lastSeq < op.Seq {
		// apply the operation to the state machine
		kv.applyOperation(op)
		kv.clerkLastSeq[op.ClerkId] = op.Seq
	}

	// notify the waiting goroutine if it's present
	if ch, ok := kv.notifyChans[op.ClerkId]; ok {
		select {
		case ch <- op: // notify the waiting goroutine
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for ClerkId %d, seq %d", kv.me, op.ClerkId, op.Seq))
		default:
			// if the channel is already full, skip to prevent blocking.
		}
	}
}

func (kv *ShardKV) applyOperation(op Op) {
	switch op.Operation {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	case "Get":
		// No state change for Get
	}
}

func (kv *ShardKV) handleLeaderConfigChange(op Op) {
	oldConfig := kv.config
	kv.config = op.NewConfig
	kv.MIP = true

	migrationRequests := make(map[int]map[int]bool)
	// Set which shards I need to request data from before proceeding
	for shard, oldGID := range oldConfig.Shards {
		if kv.config.Shards[shard] == kv.gid && oldGID != kv.gid && oldGID != 0 {
			if _, exists := migrationRequests[oldGID]; !exists {
				migrationRequests[oldGID] = make(map[int]bool)
			}
			migrationRequests[oldGID][shard] = true
		}
	}

	// Send RequestShard RPCs
	responses := []RequestShardReply{}
	responseChan := make(chan RequestShardReply)
	for gid, shardsToRequest := range migrationRequests {
		go kv.GetShard(gid, shardsToRequest, kv.config, responseChan)
	}

	// Collect responses to aggregate and signal completion of migration
	for i := 0; i < len(migrationRequests); i++ {
		select {
		case result := <-responseChan:
			responses = append(responses, result)
		case <-time.After(3 * time.Second):  // Adjust the timeout duration as needed
			fmt.Println("Timeout occurred while waiting for a response")
			return
		}
    }

	completionOp := Op{
		NewConfig: op.NewConfig,
		Operation: "CompleteConfigChange",
		DB:  make(map[string]string),
	}

	for _, response := range responses {
		for key,value := range response.Data {
			completionOp.DB[key] = value
		}
	}

	kv.rf.Start(completionOp)
}

func (kv *ShardKV) handleFollowerConfigChange(op Op) {
	kv.MIP = true
	for {
        _, isLeader := kv.rf.GetState() // Check leadership status outside the blocking loop
        if !isLeader { // Only enter if not leader
            kv.mu.Lock()
            if kv.config.Num >= op.NewConfig.Num {
                kv.mu.Unlock() // Unlock before return
				fmt.Println("follower return")
                return
            }
            kv.mu.Unlock()
            time.Sleep(50 * time.Millisecond) // Sleep to prevent tight loop on leadership check
        } else {
			break
		}
	}
	fmt.Println("leader failed")
	// If didn't return through the loop due to a higher config number seen
	// then the previous leader must have failed to complete migration
	kv.handleFollowerConfigChange(op)
}

func (kv *ShardKV) handleCompleteConfigChange(op Op) {
	kv.mu.Lock()
	fmt.Println("Complete config change")
	kv.MIP = false
	// Set the db
	for key,value := range op.DB {
		kv.db[key] = value
	}
	// Update the config to new config
	kv.config = op.NewConfig
	// Force queued commands to be executed sequentially
	for _, op := range kv.commandQueue  {
		if (kv.config.Shards[key2shard(op.Key)] == kv.gid) {
			kv.handleKvOperation(op)
		}
	}
	kv.commandQueue = kv.commandQueue[:0]
	kv.mu.Unlock()
}

