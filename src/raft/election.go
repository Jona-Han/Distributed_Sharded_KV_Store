package raft

import (
	"fmt"
	"time"
)

// startSendingHB sends HB to each server every TIMEOUT milliseconds
func (rf *Raft) startSendingHB() {
	var raftState RaftState
	var nextIndex int

	for !rf.killed() {
		rf.mu.Lock()
		raftState = rf.raftState
		nextIndex = rf.XIndex + len(rf.logs) + 1 // lastLogIndex + 1
		rf.mu.Unlock()

		if raftState != Leader || rf.killed() {
			rf.logger.Log(-1, "(startSendingHB) NOT A LEADER OR KILLED ... ")
			return
		}

		for pId := range rf.peers {
			if pId != rf.me {
				go rf.sendEntry(pId, nextIndex)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	rf.logger.Log(LogTopicElection, fmt.Sprintf("(startSendingHB) KILLED, currTerm=%d", rf.currTerm))
}

// startEelction starts an election
func (rf *Raft) startElection() {
	// starting a new election
	rf.mu.Lock()

	// only Followers and Candidates can start elections
	// skip if I'm a leader - might happen when there was a timeout from previous elections or another leader is selected
	if rf.raftState == Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	// 0. transition to the Candidate state; reset the timer
	rf.raftState = Candidate
	rf.heartbeat = true // reset my timer

	// 1. increment my term
	rf.currTerm += 1

	// 2. vote for myself
	rf.votedFor = rf.me
	rf.persist()

	// 3. ask others to vote for me as well
	args := &RequestVoteArgs{
		Term:   rf.currTerm,
		CandId: rf.me,
	}
	args.LastLogIdx = rf.XIndex
	args.LastLogTerm = rf.XTerm
	if lastIdx := len(rf.logs) - 1; lastIdx >= 0 {
		args.LastLogIdx += len(rf.logs)
		args.LastLogTerm = rf.logs[lastIdx].Term
	}

	args.Term = rf.currTerm
	args.CandId = rf.me

	rf.logger.Log(LogTopicElection, fmt.Sprintf("starting an election; currTerm=%d", rf.currTerm))

	rf.mu.Unlock()

	// should ask the peers in parallel for their vote;
	// so we'll wait on this channel after sending the requests in parallel
	voteCh := make(chan bool)

	gotVotes := 1                   // gotVotes counts granted votes for me in this round of election; counted my vote already
	majority := len(rf.peers)/2 + 1 // majority is the threshold for winning the current election
	recVotes := 1                   // recVotes counts all peers voted (mine counted); in case we haven't reached a majority of votes

	// asking peers to vote until
	// 1. I win!
	// 2. someone else wins!
	// 3. another timeout happens
	for i := 0; i < len(rf.peers); i += 1 {
		// skip asking myself - already voted
		if i != rf.me {
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.peers[i].Call(REQUEST_VOTE_RPC, args, reply)

				// make sure we don't get old responses
				voteCh <- ok && reply.VoteGranted && reply.Term == args.Term
			}(i)
		}
	}

	// let's count the votes
	for gotVotes < majority && recVotes < len(rf.peers) {
		select {
		case <-rf.quit:
			rf.logger.Log(-1, "CHANN CLOSED (START_ELECTION GR)")
			return
		case gotIt := <-voteCh:
			if gotIt {
				gotVotes += 1
			}
		}
		recVotes += 1
	}

	// 1. let's check if there's another server who has already been elected
	rf.mu.Lock()
	if rf.killed() || rf.raftState != Candidate {
		// I'm not a Candidate anymore; we're done with counting
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// Did I get the majority of votes?
	if gotVotes >= majority {

		// change the state to Leader
		rf.mu.Lock()
		rf.logger.Log(LogTopicElection, fmt.Sprintf("Got the majority. I'm the leader now; currTerm=%d, my_log=%+v", rf.currTerm, rf.logs))

		rf.raftState = Leader
		rf.leaderId = rf.me

		// reset my nextIndex and matchIndex after election
		// nextIndex intialiazed to the last entry's index in the leader's logs
		logLen := rf.XIndex // this is going to be the nextIndx
		if currLogLen := len(rf.logs); currLogLen > 0 {
			logLen += currLogLen
		}

		// rf.cond.L.Lock()
		for i := range rf.peers {
			rf.nextIndex[i] = logLen + 1
			rf.matchIndex[i] = 0 // default values
		}
		// rf.cond.L.Unlock()

		// start sending HBs
		go rf.startSendingHB()

		// start syncing the entries in the log
		for pId := range rf.peers {
			go rf.syncLogEntries(pId, rf.currTerm)
		}

		rf.mu.Unlock()
	}
}
