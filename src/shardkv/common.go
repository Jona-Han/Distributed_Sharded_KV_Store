/*
Package shardkv implements a sharded, fault-tolerant key/value store
built on top of a Raft-based replication system. It handles client key-value operations
(Put, Append, Get)
*/
package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
import "time"

const (
	OK             = "OK"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut 	   = "ErrTimeOut"
	ErrOutdated    = "ErrOutdated"
)

// Constants for timeout duration and debug mode.
const (
	WaitTimeOut = 2000 * time.Millisecond
	Debug       = false
)

// Err represents an error message.
type Err string

// CommonReply represents a common structure for replies from the shardkv server.
type CommonReply struct {
	Err   Err    // Error message, if any
	Value string // Returned value, if any
}

// PutAppendArgs represents the arguments for a Put or Append request.
type PutAppendArgs struct {
	Key       string // The key to be put or appended to
	Value     string // The value to be put or appended
	Op        string // Operation type: "Put" or "Append"
	ClerkId   int64  // Unique identifier for the Clerk
	Seq       int64  // Sequence number for the request
	ConfigNum int    // Configuration number
}

// PutAppendReply represents the reply to a Put or Append request.
type PutAppendReply struct {
	Err Err // Error message, if any
}

// GetArgs represents the arguments for a Get request.
type GetArgs struct {
	Key       string // The key to be retrieved
	ClerkId   int64  // Unique identifier for the Clerk
	Seq       int64  // Sequence number for the request
	ConfigNum int    // Configuration number
}

// GetReply represents the reply to a Get request.
type GetReply struct {
	Err   Err    // Error message, if any
	Value string 
}