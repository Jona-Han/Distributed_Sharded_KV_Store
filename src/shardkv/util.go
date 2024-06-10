/*
Package shardkv implements a sharded, fault-tolerant key/value store
built on top of a Raft-based replication system. It handles client key-value operations
(Put, Append, Get)
*/
package shardkv

import (
	"cpsc416/shardctrler"
	"fmt"
)

// copyConfig copies the configuration from the 'from' config to the 'to' config.
func copyConfig(to *shardctrler.Config, from *shardctrler.Config) {
	to.Num = from.Num
	to.Shards = from.Shards

	to.Groups = make(map[int][]string, len(from.Groups))
	for k, v := range from.Groups {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		to.Groups[k] = newSlice
	}
}

// termPlusIndexToStr converts a term and index into a string in the format "term+index".
func termPlusIndexToStr(term int, index int) string {
	return fmt.Sprintf("%d+%d", term, index)
}
