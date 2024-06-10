/*
Package shardctrler provides mechanisms to manage shard configurations in a distributed system.
It allows joining new groups, leaving groups, and moving shards between groups.
*/
package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//

// The number of shards.
const NShards = 10

const Debug = false

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
	ErrTimeOut = "ErrTimeOut"
)

// WaitTimeOut specifies the timeout duration for operations.
const WaitTimeOut = 2000 * time.Millisecond

// Err represents an error message.
type Err string

// CommonReply is a common structure for replies from the shard controller.
type CommonReply struct {
	WrongLeader bool // indicates if the reply is from a wrong leader
	Err         Err  // error message, if any
	Config      Config // returned configuration, if applicable
}

// JoinArgs represents the arguments for a Join request.
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClerkId int64            // unique identifier for the Clerk
	Seq     int64            // sequence number for the request
}

// JoinReply represents the reply for a Join request.
type JoinReply struct {
	WrongLeader bool 	// indicates if the reply is from a wrong leader
	Err         Err  	// error message, if any
}

// LeaveArgs represents the arguments for a Leave request.
type LeaveArgs struct {
	GIDs    []int 		// list of GIDs to leave
	ClerkId int64 		// unique identifier for the Clerk
	Seq     int64 		// sequence number for the request
}

// LeaveReply represents the reply for a Leave request.
type LeaveReply struct {
	WrongLeader bool 	// indicates if the reply is from a wrong leader
	Err         Err  	// error message, if any
}

// MoveArgs represents the arguments for a Move request.
type MoveArgs struct {
	Shard   int   		// shard number to move
	GID     int   		// target group ID
	ClerkId int64 		// unique identifier for the Clerk
	Seq     int64 		// sequence number for the request
}

// MoveReply represents the reply for a Move request.
type MoveReply struct {
	WrongLeader bool 	// indicates if the reply is from a wrong leader
	Err         Err  	// error message, if any
}

// QueryArgs represents the arguments for a Query request.
type QueryArgs struct {
	Num     int   		// desired config number
	ClerkId int64 		// unique identifier for the Clerk
	Seq     int64 		// sequence number for the request
}

// QueryReply represents the reply for a Query request.
type QueryReply struct {
	WrongLeader bool  	// indicates if the reply is from a wrong leader
	Err         Err    	// error message, if any
	Config      Config 	// returned configuration
}
