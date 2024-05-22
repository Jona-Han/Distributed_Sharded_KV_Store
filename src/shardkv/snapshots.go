package shardkv

import "cpsc416/shardctrler"
import "log"
import "fmt"
import "cpsc416/labgob"
import "bytes"
import "time"

func (kv *ShardKV) snapshotChecker() {
	if kv.maxraftstate == -1  {
		return
	}
	for !kv.killed() {
		if kv.persister.RaftStateSize() >= kv.maxraftstate*2/3 {
			kv.logger.Log(LogTopicServer, fmt.Sprintf("%d - S%d triggered a snapshot creation", kv.gid, kv.me))
			kv.createSnapshot()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) createSnapshot() {
	kv.mu.Lock()
	// check if snapshot is necessary based on the maxraftstate size
	if kv.persister.RaftStateSize() <= kv.maxraftstate || kv.maxraftstate == -1 {
		kv.mu.Unlock()
		return
	}

	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d starts taking a snapshot", kv.me))

	dbCpy := make(map[string]string)
	cachedResponsesCpy := make(map[int64]CacheResponse)
	configCpy := shardctrler.Config{}
	prevConfigCpy := shardctrler.Config{}
	copyConfig(&configCpy, &kv.config)
	copyConfig(&prevConfigCpy, &kv.prevConfig)

	for k, v := range kv.db {
		dbCpy[k] = v
	}
	for k, v := range kv.cachedResponses {
		cachedResponsesCpy[k] = v
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	index := kv.lastApplied
	kv.mu.Unlock()

	e.Encode(index)
	e.Encode(dbCpy)
	e.Encode(cachedResponsesCpy)
	e.Encode(kv.MIP)
	e.Encode(configCpy)
	e.Encode(prevConfigCpy)
	data := w.Bytes()

	kv.rf.Snapshot(index, data)
	
	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d completed taking a snapshot", kv.me))
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.config = shardctrler.Config{}
	kv.prevConfig = shardctrler.Config{}
	kv.cachedResponses = make(map[int64]CacheResponse)
	kv.db = make(map[string]string)

	if d.Decode(&kv.lastApplied) != nil ||
		d.Decode(&kv.db) != nil ||
		d.Decode(&kv.cachedResponses) != nil ||
		d.Decode(&kv.MIP) != nil ||
		d.Decode(&kv.config) != nil||
		d.Decode(&kv.prevConfig) != nil {
		log.Fatal("readSnapshot: error decoding data")
	}
}