package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"cpsc416/labgob"
	"cpsc416/labgob"
	"cpsc416/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type done struct{}
type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

const (
	APPEND_ENTRIES_RPC   = "Raft.AppendEntries"
	REQUEST_VOTE_RPC     = "Raft.RequestVote"
	INSTALL_SNAPSHOT_RPC = "Raft.InstallSnapshot"
)

// LogEntry represents each log entry in the rf.logs array
type LogEntry struct {
	Index   int         // for 2D: we should keep track of the log indexs because we truncate the log often; helps the log index calculation easy
	Term    int         // the term of the log entry for Index
	Command interface{} // the command
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu   sync.Mutex // Lock to protect shared access to this peer's state
	lock sync.Mutex // Lock to protect the shared accessed during log replication
	cond *sync.Cond // Cond variable to synchronize the log replication go routines

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logger *Logger

	// for 2D
	XIndex   int       // shows the index we have a snapshot for it
	XTerm    int       // shows the term associated with X
	snapshot []byte    // latest snapshot
	appEntCh chan done // woke up a goroutine to commit
	quit     chan done // ask anyone waiting to quit

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState   RaftState  // the current state of this Raft (Follower, Candidate, Leader)
	currTerm    int        // current term at this Raft
	votedFor    int        // the peer this Raft voted for during the last election
	heartbeat   bool       // keeps track of the heartbeats
	logs        []LogEntry // keeps the logs of the current Raft
	commitIndex int        // index of highest log entry known to be commited (initalized to be 0)
	lastApplied int        // index of highes log entry known to be applied to the SM (initalized to be 0)
	leaderId    int        // index of the leader for Followers to redirect the requests to

	// specific to the leader; must be re-initialized after election
	nextIndex  []int // for each server, the index of the next log entry to send to that server (initialiazed to last log index + 1)
	matchIndex []int // for each server, the index of the next log entry to be _replicated_ on other servers (initialized to be 0)

	// channel to pass results to the server
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return int(rf.currTerm), rf.raftState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.XIndex)
	e.Encode(rf.XTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)

	rf.logger.Log(-1, fmt.Sprintf("(persist) PERSISTED! len(snapshot)=%d", len(rf.snapshot)))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currTerm, votedFor, XIndex, XTerm int
	var logs []LogEntry

	if d.Decode(&currTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&XIndex) != nil && d.Decode(&XTerm) != nil {
		fmt.Println("COULDN'T DECODE THE STATE")
	}
	rf.currTerm = currTerm
	rf.votedFor = votedFor
	rf.logs = append([]LogEntry{}, logs...)
	rf.XIndex = XIndex
	rf.XTerm = XTerm
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Success bool // if the snapshot installed successfully; for the leader
	XIndex  int  // the LastIncludedIndex used for snapsthot
	XTerm   int  // the LastIncludedTerm used for snapshot
	Term    int  // current term of the follower
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.logger.Log(LogTopicInstallSnapshotRPC, fmt.Sprintf("LEADER ASKED TO INSTALL A SNAPSHOT; args=(Term=%d, LeaderId=%d, LastIncludedIndex=%d, LastIncludedTerm=%d, Offset=%d, len(data)=%d)", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, args.Offset, len(args.Data)))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeat = true

	if args.Term < rf.currTerm {
		rf.logger.Log(LogTopicInstallSnapshotRPC, fmt.Sprintf("REJECT TO INSTALL THE SNAPSHOT - LEADER (S%d) LOWER TERM; args.Term=%d, rf.currTerm=%d", args.LeaderId, args.Term, rf.currTerm))
		reply.Term = rf.currTerm
		return
	}

	reply.Term = rf.currTerm
	reply.XIndex = args.LastIncludedIndex
	reply.XTerm = args.LastIncludedTerm

	if rf.XIndex > args.LastIncludedIndex {
		rf.logger.Log(LogTopicInstallSnapshotRPC, fmt.Sprintf("REJECT INSTALL_SNAPSHOT_RPC; STALE INDEX; rf.XIndex=%d args.LastIncludedIndex=%d args.LastIncludedTerm=%d", rf.XIndex, args.LastIncludedIndex, args.LastIncludedTerm))

		reply.Success = false

		return
	} else if rf.XIndex == args.LastIncludedIndex && rf.XTerm == args.LastIncludedTerm {
		rf.logger.Log(LogTopicInstallSnapshotRPC, fmt.Sprintf("SAME INDEX AND TERM AS MY SNAPSHOT; SKIPPED THE INSTALL; (rf.XTerm=%d, args.LastIncludedIndex=%d) (rf.XTerm=%d, args.LastIncludedTerm=%d) (len(rf.snapshot)=%d len(args.Data)=%d)", rf.XIndex, args.LastIncludedIndex, rf.XTerm, args.LastIncludedTerm, len(rf.snapshot), len(args.Data)))
		reply.Success = true
		return
	}
	defer rf.persist()

	prevSnapLen := len(rf.snapshot)
	rf.snapshot = args.Data

	i := args.LastIncludedIndex - rf.XIndex - 1
	if i >= 0 && i < len(rf.logs) && rf.logs[i].Index == args.LastIncludedIndex && rf.logs[i].Term == args.LastIncludedTerm {
		// retain entries after i
		rf.logs = rf.logs[i+1:]

		rf.XIndex = args.LastIncludedIndex
		rf.XTerm = args.LastIncludedTerm

		rf.logger.Log(LogTopicInstallSnapshotRPC, fmt.Sprintf("SNAP INDEX AND TERM MATCHED; RTAIN EVERYTHING AFTER THAT! (rf.XIndex=%d rf.XTerm=%d) (snap_len_before=%d snap_len_after=%d)", rf.XIndex, rf.XTerm, prevSnapLen, len(rf.snapshot)))
	} else {
		// discard the log
		rf.logs = []LogEntry{}

		rf.XIndex = args.LastIncludedIndex
		rf.XTerm = args.LastIncludedTerm

		rf.logger.Log(LogTopicInstallSnapshotRPC, fmt.Sprintf("MISMATCH SNAP INDEX AND TERM; DISCARD MY LOG! (rf.XIndex=%d rf.XTerm=%d) (snap_len_before=%d snap_len_after=%d)", rf.XIndex, rf.XTerm, prevSnapLen, len(rf.snapshot)))
	}

	reply.Success = true

	go func() {
		select {
		case <-rf.quit:
		case rf.appEntCh <- done{}:
		}
	}()
}

// saveSnapshot saves the given snapshot and trim the log accordingly
// func (rf *Raft) saveSnapshot(xIndex, xTerm int, snapshot []byte) bool {
// 	// check if the snapshot is a new one
// 	rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) ASKED TO SAVE A SNAPSHOT (x=%d, len(snapshot)=%d)", xIndex, len(snapshot)))

// 	if xIndex < rf.XIndex {
// 		rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 1. REJECTED TO SAVE A SNAPSHOT; (OLD INDEX) (x=%d < rf.XIndex=%d, len(logs)=%d, len(snapshot)=%d)", xIndex, rf.XIndex, len(rf.logs), len(snapshot)))
// 		return false
// 	} else if xIndex == rf.XIndex && xTerm == rf.XTerm {
// 		rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 2. SKIPPING TO SAVE THE SNAPSHOT; (THE SAME X_INDEX & X_TERM) (xIndex=%d == rf.XIndex=%d, xTerm=%d, rf.XTerm=%d len(logs)=%d, len(snapshot)=%d)", xIndex, rf.XIndex, xTerm, rf.XTerm, len(rf.logs), len(snapshot)))
// 		return true
// 	}
// 	defer rf.persist()

// 	rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 3. TRUNCATING MY LOG FOR THIS SNAPSHOT .... xIndex=%d, rf.lastApplied=%d, rf.commitIndex=%d, len(logs)=%d logs=%v", xIndex, rf.lastApplied, rf.commitIndex, rf.XIndex+len(rf.logs), rf.logs))

// 	// set the snapshot
// 	rf.snapshot = snapshot

// 	if xIndex > rf.XIndex+len(rf.logs) {
// 		// this follower is way behind the leader
// 		// discard the log
// 		rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 4. DICARDING MY LOG; MY LOG IS THE LEADER (rf.XIndex=%d, rf.XTerm=%d) (len(logs)=%d, xIndex=%d)", rf.XIndex, rf.XTerm, rf.XIndex+len(rf.logs), xIndex))

// 		rf.logs = []LogEntry{}

// 		rf.XIndex = xIndex
// 		rf.XTerm = xTerm

// 	} else {
// 		xIdx := xIndex - rf.XIndex - 1

// 		// check if the xIdx has the same index and term as xTerm and xTerm
// 		if xTerm == -1 || (xIdx >= 0 && rf.logs[xIdx].Index == xIndex && rf.logs[xIdx].Term == xTerm) {
// 			if xTerm != -1 {
// 				rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 5. RETAINING THE TAIL; INDEX AND TERM MATCHES AT THE SNAPSHOT INDEX; (xIndex=%d, xTerm=%d, rf.logs[%d]=%+v) logs=%v", xIndex, xTerm, xIdx, rf.logs[xIdx], rf.logs))
// 			} else {
// 				rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 6. CLIENT ASKED FOR A SNAPSHOT; RETAINING MY TAIL (rf.XIndex=%d -> %d, rf.XTerm=%d -> %d, rf.logs[%d]=%+v) logs=%v", rf.XIndex, xIndex, rf.XTerm, rf.logs[xIdx].Term, xIdx, rf.logs[xIdx], rf.logs))
// 			}

// 			rf.XIndex = xIndex
// 			rf.XTerm = rf.logs[xIdx].Term
// 			rf.logs = rf.logs[xIdx+1:]
// 		} else {
// 			rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 7. DICARDING MY ENTIRE LOG; MISMATCH TERM OR INDEX @ THE SNAPSHOT INDEX (x=%d, rf.XIndex=%d) logs=%v", xIndex, rf.XIndex, rf.logs))

// 			rf.logs = []LogEntry{}

// 			rf.XIndex = xIndex
// 			rf.XTerm = Max(0, xTerm)

// 		}
// 	}

// 	rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(saveSnapshot) 8. SNAPSHOT CREATED (rf.commitIndex=%d, rf.lastApplied=%d, rf.XIndex=%d rf.XTerm=%d) new_log=%v", rf.commitIndex, rf.lastApplied, rf.XIndex, rf.XTerm, rf.logs))

// 	go func() {
// 		select {
// 		case <-rf.quit:
// 		case rf.appEntCh <- done{}:
// 		}
// 	}()

// 	return true
// }

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(snapIndex int, snapshot []byte) {
	// Your code here (2D).
	rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(Snapshot) 1. CLIENT SENT A SNAPSHOT snapIndex=%d, len(snapshot)=%d", snapIndex, len(snapshot)))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !(snapIndex <= rf.XIndex+len(rf.logs)) {
		rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(Snapshot) WARNNING: CLIENT CREATED A SNAPSHOT UP TO AN INDEX WHICH DOES NOT EXIST IN MY LOG; snapIndex=%d, last_index=%d", snapIndex, rf.XIndex+len(rf.logs)))
	}

	rf.snapshot = snapshot

	prevSnapLen := len(rf.snapshot)

	// remove all log entries up to the snapIndex and save the snapsot
	if xIdx := snapIndex - rf.XIndex - 1; xIdx >= 0 && xIdx < len(rf.logs) {
		// I have the log entry; trim my log

		rf.XIndex = rf.logs[xIdx].Index
		rf.XTerm = rf.logs[xIdx].Term

		rf.logs = rf.logs[xIdx+1:]

		rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(Snapshot) 1.2 TRIMMED MY LOG DUE TO THE SNAPSHOT; currTerm=%d (prev_snap_len=%d, curr_snap_len=%d, tail_len_after_trim=%d)", rf.currTerm, prevSnapLen, len(rf.snapshot), len(rf.logs)))
	} else {
		// I dont have the log entry; discard the log
		rf.XIndex = snapIndex
		rf.XTerm = rf.currTerm

		preLogLen := len(rf.logs)
		rf.logs = []LogEntry{}

		rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(Snapshot) 1.3 NOT ENOUGH ENTRIES; EMPTY THE LOG; currTerm=%d (rf.XIndex=%d, preLogLen=%d, xIdx=%d, prev_snap_len=%d, curr_snap_len=%d, tail_len=%d)", rf.currTerm, rf.XIndex, preLogLen, xIdx, prevSnapLen, len(snapshot), len(rf.logs)))
	}

	rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(Snapshot) 2. CLIENT's SANPSHOT SAVED snapIndex=%d, len(snapshot)=%d rf.lastApplied=%d rf.commitIndex=%d", snapIndex, len(snapshot), rf.lastApplied, rf.commitIndex))

	go func() {
		select {
		case <-rf.quit:
		case rf.appEntCh <- done{}:
		}
	}()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandId      int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote called by Candidates to ask for the peer's vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the candidate's term is smaller than mine, I reject the vote
	if args.Term < rf.currTerm {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE REJECTED: S%d asked for a vote but has a lower term! (args.Term=%d rf.currTerm=%d)", args.CandId, args.Term, rf.currTerm))
		reply.VoteGranted = false
		reply.Term = rf.currTerm
		return
	}

	// if the candidate's term is higher than mine:
	//		1- update my term to the candidate's term
	// 		2- turn into a follower
	//		3- reset my vote
	if args.Term > rf.currTerm {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTING: turnning to a follower because S%d has a higher than me! (args.Term=%d, rf.currTerm=%d)", args.CandId, args.Term, rf.currTerm))
		rf.raftState = Follower
		rf.currTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// the candidate's get the vote if:
	//	1- I haven't voted so far
	//	2- The candidate's log is more up-to-date than mine
	if rf.votedFor >= 0 && rf.votedFor != args.CandId {
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	}

	// the candidate has a more up-to-date log than mine if:
	//	1- my last log entry has a lower term than the candidate's last log term; or
	//	2- if the last log terms matches betweem mine and the candidate, then the candidate gets the vote if the it has a longer log
	lastLogIdx := rf.XIndex
	lastLogTerm := rf.XTerm
	if lenLogs := len(rf.logs); lenLogs > 0 {
		lastLogIdx += lenLogs
		lastLogTerm = rf.logs[lenLogs-1].Term
	}

	if lastLogTerm < args.LastLogTerm {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE GRANTED! S%d's last entry has a higher term than me! (args.Term=%d, rf.currTerm=%d) (rf.logs[lastLogIdx].Term=%d, args.LastLogTerm=%d)\n\tlogs=%v", args.CandId, args.Term, rf.currTerm, lastLogTerm, args.LastLogTerm, rf.logs))

		rf.votedFor = args.CandId
		rf.persist()

		reply.VoteGranted = true
		reply.Term = rf.currTerm

		return
	} else if lastLogTerm == args.LastLogTerm && args.LastLogIdx >= lastLogIdx {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE GRANTED! S%d's log is >= than me; (args.Term=%d, rf.currTerm=%d) (args.LastLogIdx=%d, lastLogIdx=%d)\n\tlogs=%v", args.CandId, args.Term, rf.currTerm, args.LastLogIdx, lastLogIdx, rf.logs))

		rf.votedFor = args.CandId
		rf.persist()

		reply.VoteGranted = true
		reply.Term = rf.currTerm

		return
	}

	rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE REJECTED! my log is more up-to-date than S%d! (args.Term=%d, rf.currTerm=%d) (rf.votedFor=%d) (args.LastLogIndex=%d, args.LastLogTerm=%d, lastLogIdx=%d, lastLogTerm=%d)\n\tlog=%v", args.CandId, args.Term, rf.currTerm, rf.votedFor, args.LastLogIdx, args.LastLogTerm, lastLogIdx, lastLogTerm, rf.logs))

	reply.Term = rf.currTerm
	reply.VoteGranted = false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.XIndex + len(rf.logs) + 1 // the command will be appeared at this index on the leader's log

	term := int(rf.currTerm)           // the leader current term for this command
	isLeader := rf.raftState == Leader // if the server believes it's a leader

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	// append the command to my log
	newEntry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.logs = append(rf.logs, newEntry)
	rf.persist()

	rf.logger.Log(LogTopicStartCmd, fmt.Sprintf("Added the new command to my log!\n\tcommand=%v\n\tlog=%v", command, rf.logs))

	// notify go routines to sync the logs
	rf.cond.Broadcast()

	return index, term, isLeader
}

type AppendEntriesArg struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id so that followers can redirect them
	PrevLogIndex int        // index of previous log entry preceeding the new one
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for HB, more than one for logs)
	LeaderCommit int        // leader's commitIndex
}
type AppendEntriesReply struct {
	NeedsSnapshot bool // if the follower needs a snapshot
	XIsShort      bool // if the follower's log is shorter
	XLen          int  // the length of the follower's log
	XTerm         int  // the conflicting term in the follower's log
	XIndex        int  // the index of the first entry of XTerm
	Term          int  // currTerm, for leader to update itself
	Success       bool // true if the follower contined the prevLogIndex and preLogTerm
}

// AppendEntries called by the leader either to send a HB or log entries
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the leader's term lower than mine, I reject the HB
	if args.Term < rf.currTerm {
		rf.logger.Log(LogTopicAppendEntryRpc, fmt.Sprintf("APE REJECTED: S%d WITH LOWR TERM! (args.Term=%d rf.currTerm=%d)", args.LeaderId, args.Term, rf.currTerm))
		reply.Term = rf.currTerm
		reply.Success = false
		return
	}

	// if the leader has a higher term, I update my term and turn into a follower
	if args.Term > rf.currTerm {
		rf.logger.Log(LogTopicAppendEntryRpc, fmt.Sprintf("APE: TURNING TO A FOLLOWER; S%d HAS A HIGHER TERM. (args.Term=%d rf.currTerm=%d) (leader=S%d)", args.LeaderId, args.Term, rf.currTerm, args.LeaderId))
		rf.raftState = Follower
		rf.votedFor = -1
		rf.currTerm = args.Term
		rf.persist()
	}

	// reset my HB variable and set the leader id for this term
	rf.heartbeat = true
	rf.leaderId = args.LeaderId

	rf.logger.Log(-1, fmt.Sprintf("CHECK_LOG_CONST: args=%+v (len(logs)=%d, rf.XIndex=%d, rf.XTerm=%d, prevLogIdx=%d) logs=%v", args, rf.XIndex+len(rf.logs), rf.XIndex, rf.XTerm, args.PrevLogIndex-rf.XIndex-1, rf.logs))

	//  These are possible cases:
	//  case 0) if !(0 <= args.PrevIndex <= rf.XIndex + len(rf.logs)), the given args.PrevIndex is out our search space. Then, we tell the leader that our log is short, and ask for a prevLogIndex set to reply.XLen+1. In the next round, we decide about the APE.
	//	case 1) if the range is in our search space, there is a match if any of these happens:
	// 		1. if prevLogIdx == -1, i.e. args.PrevIndex is at rf.XIndex, it is a match if args.PrevIndex == rf.XIndex && args.PrevTerm == rf.XTerm
	//		2. if prevLogIdx >=  0, i.e. args.PrevIndex is in the tail, it is a match if args.PrevIndex == rf.logs[prevLogIdx].Index && args.PrevTerm == rf.logs[prevLogIdx].Term
	// 	case 2) if we couldn't find a match, then we have to reject the APE. But, we have to fill reply properly
	//      - find the largest index j such that Term(j) != Term(prevLogIdx), where j<prevLogIdx. Therefore, the j's range is [-2,-1,...,prevLogIdx-1]:
	//			1. if prevLogIdx == -1 we need a new snapshot because we cannot find such a j, meaning there is no search space for that (see 1.1)
	// 			2. if j==-2: we need a new snapshot because we couldn't find such a j, i.e. we searched but such j is before our last snapshot
	// 			3. if j>=-1: set the reply.XIndex=rf.logs[j+1].Index, i.e. we could find the j that satisfies case 2.
	prevLogIdx := args.PrevLogIndex - rf.XIndex - 1
	if !(args.PrevLogIndex >= rf.XIndex && args.PrevLogIndex <= rf.XIndex+len(rf.logs)) {
		// case 0) claim that the log is shorter
		reply.Success = false
		reply.Term = rf.currTerm

		reply.XIsShort = true
		reply.XLen = rf.XIndex + len(rf.logs)

		rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: SHORTER LOG LENGTH! leader = S%d (currTerm=%d) (args=%+v)(reply=%v) (not in range [rf.XIndex=%d len(logs)=%d) log=%v", args.LeaderId, rf.currTerm, args, reply, rf.XIndex, rf.XIndex+len(rf.logs), rf.logs))

		return
	} else if !(prevLogIdx == -1 && args.PrevLogIndex == rf.XIndex && args.PrevLogTerm == rf.XTerm) && !(prevLogIdx >= 0 && args.PrevLogIndex == rf.logs[prevLogIdx].Index && args.PrevLogTerm == rf.logs[prevLogIdx].Term) {
		// if it is not case 1), so we are in case 2)
		reply.Success = false
		reply.Term = rf.currTerm
		reply.XLen = rf.XIndex + len(rf.logs)

		if prevLogIdx == -1 {
			// case 2.1
			reply.NeedsSnapshot = true
			rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: CANNOT FIND THE J INDEX (BEFORE SNAPSHOT INDEX); NEEDS A SNAPSHOT; (leader=S%d, currTerm=%d) (rf.XIndex=%d, rf.XTerm=%d) (args=%v, reply=%v) logs=%v", args.LeaderId, rf.currTerm, rf.XIndex, rf.XTerm, args, reply, rf.logs))
			return
		}

		j := prevLogIdx - 1
		for j >= -1 {
			if (j >= 0 && rf.logs[j].Term != rf.logs[prevLogIdx].Term) || (j == -1 && rf.XTerm != args.PrevLogTerm) {
				break
			}
			j -= 1
		}

		if j == -2 {
			// case 2.2
			reply.NeedsSnapshot = true
			rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: COULDN'T FIND THE J INDEX; NEEDS A SNAPSHOT; (leader=S%d, currTerm=%d) (rf.XIndex=%d, rf.XTerm=%d) (args=%v, reply=%v) logs=%v", args.LeaderId, rf.currTerm, rf.XIndex, rf.XTerm, args, reply, rf.logs))
			return
		}

		// case 2.3
		reply.XIndex = rf.logs[j+1].Index
		reply.XTerm = rf.logs[j+1].Term

		rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: ASK FOR A NEW INDEX; (leader=S%d, currTerm=%d) (rf.XIndex=%d, rf.XTerm=%d) (args=%v, reply=%v) logs=%v", args.LeaderId, rf.currTerm, rf.XIndex, rf.XTerm, args, reply, rf.logs))
		return
	}

	// case 1)
	rf.logger.Log(LogTopicMatchPrevApe, fmt.Sprintf("MATCHED PREV_INDEX+PREV_TERM from S%d; args.PrevLogIndex=%d, args.PrevLogTerm=%d, args.Entries=%v logs=%v", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.logs))

	// prevLogIdx is the index where we match the args.PrevXX. So, prevLogIdx + 1 is the immediate index to start matching with args.Entries. Now, we find how much overlap is between the args.Entries and entries after prevLogIdx.
	// 		1. Retain all the overlapped entries and discard the rest in the rf.logs
	// 		2. Append the new portion from args.Entries, i.e. non-overlapped part.

	// for two indices i in [prevLogIdx+1 ... len(rf.logs)-1] and j in [0 ... len(args.Entries)], find the maximum value j such that, Term_i == Args_Entries_Term_k  Index_i == Args_Entries_Term_k, for k < j
	j := 0
	for j < len(args.Entries) {
		nextIdx := (prevLogIdx + 1) + j
		if nextIdx >= len(rf.logs) {
			// my log is ended
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(LOG_CONFLICT, Leader S%d) @ INDEX=%d! conflictIdx=%d log=%v", args.LeaderId, nextIdx, j, rf.logs))
			break
		} else if rf.logs[nextIdx].Term != args.Entries[j].Term {
			// truncate the log
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(LOG CONFLICT, Leader S%d) DIFF TERM @ INDEX=%d AND args.Entries[%d]! SHARED UP TO INDEX %d WITH args.Entries; TRUNCATING MY LOG ... (rf.logs[%d].Term=%d, args.Entries[%d].Term=%d) new_log=%v log=%v arg.Entries=%v", args.LeaderId, nextIdx, j, nextIdx, nextIdx, rf.logs[nextIdx].Term, j, args.Entries[j].Term, rf.logs[:nextIdx], rf.logs, args.Entries))

			rf.logs = rf.logs[:nextIdx]
			rf.persist()
			break
		} else if rf.logs[nextIdx].Index != args.Entries[j].Index {
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(ASSERTION ERROR) INDEX MISMATCH AFTER PRE_LOG MATCH; Leader = S%d rf.logs[%d:]=%+v args.Entries[%d:]=%+v", args.LeaderId, nextIdx, rf.logs[nextIdx:], j, args.Entries[j:]))
		}
		j += 1
	}

	// find which part of the entries have to be added
	if len(args.Entries[j:]) > 0 {
		rf.logger.Log(LogTopicAppendingEntryApe, fmt.Sprintf("NEW PORTION of args.Entries! Leader = S%d new_portion=%v args.Entries=%v", args.LeaderId, args.Entries[j:], args.Entries))
	}
	args.Entries = args.Entries[j:] // the portion of the entries we should add to the log; could be empty

	if len(args.Entries) > 0 {
		// append the new entries
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()

		rf.logger.Log(LogTopicAppendingEntryApe, fmt.Sprintf("NEW PORTION APPENED! (Leader = S%d, currTerm=%d) args.Entries=%v new_log=%v", args.LeaderId, rf.currTerm, args.Entries, rf.logs))
	}

	// if the leader is ahead of me; commit all the entries we haven't commited yet
	if args.LeaderCommit > rf.commitIndex {
		rf.logger.Log(LogTopicUpdateCommitIdxApe, fmt.Sprintf("UPDATE COMMIT_INDEX; leaderCommit(%d) > rf.commitIndex(%d)! Min(leaderCommit, my_last_log_idx=%d), term=%d", args.LeaderCommit, rf.commitIndex, rf.XIndex+len(rf.logs), rf.currTerm))

		rf.commitIndex = Min(args.LeaderCommit, rf.XIndex+len(rf.logs))

	}
	go func() {
		select {
		case <-rf.quit:
		case rf.appEntCh <- done{}:
		}
	}()

	reply.Term = rf.currTerm
	reply.Success = true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.logger.Log(LogTopicElection, fmt.Sprintln("I got killed :("))

	// quit everyone is waiting
	close(rf.quit)

	// make sure there is no pending goroutines
	rf.cond.Broadcast()

	// just wait until all goroutines exit
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	var ms int64
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// avoids the cold start vote split situation
		ms = 250 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// check if we got a heartbeat from the leader
		// if we haven't recieved any hearts; start an election
		rf.mu.Lock()
		if !rf.heartbeat && !rf.killed() {
			go rf.startElection()
		}
		// reset the heartbeat
		rf.heartbeat = false
		rf.mu.Unlock()

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	logger, err := NewLogger(me)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:          sync.Mutex{},
		lock:        sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		logger:      logger,
		raftState:   Follower,
		currTerm:    0,
		votedFor:    -1,
		heartbeat:   false,
		logs:        []LogEntry{},
		XIndex:      0,
		snapshot:    []byte{},
		commitIndex: 0,
		lastApplied: 0,
		leaderId:    -1,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
		appEntCh:    make(chan done),
		quit:        make(chan done),
	}

	rf.cond = sync.NewCond(&rf.lock)

	// initialize nextIndex for each server;
	// it has to be 1 at the beginning as it is the next log entry to be sent to them
	for pid := range peers {
		rf.nextIndex[pid] = 1
	}

	// read the last persisted data; snapshot and state variables
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()

	// ticker goroutine checks the HB
	go rf.ticker()

	// applyEntries goroutine checks if there are entries to be applied to the state-machine
	go rf.applyEntries()

	return rf
}
