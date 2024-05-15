package raft

import (
	"fmt"
	"time"
)

// syncLogEntries is run by the leader and sends AppendEntries to all followers
func (rf *Raft) syncLogEntries(pId int, term int) {
	// skip myself
	if pId == rf.me {
		return
	}
	rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(syncLogEntries) START SYNCING WITH S%d, term=%d ...", pId, term))

	var raftState RaftState
	var CurrTerm, XIndex, NextIndex, LogLen int

	for !rf.killed() {
		rf.mu.Lock()
		raftState = rf.raftState
		CurrTerm = rf.currTerm

		XIndex = rf.XIndex

		LogLen = XIndex + len(rf.logs)
		NextIndex = rf.nextIndex[pId]

		rf.mu.Unlock()

		if raftState != Leader || CurrTerm > term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(syncLogEntries) NOT A LEADER or OLD TERM RETURN ..., state=%v, termSent=%d, currTerm=%d", raftState, term, CurrTerm))
			return
		}

		rf.cond.L.Lock()
		if NextIndex > LogLen {
			// there is no new logs to be added; just wait for someone to wake me up
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(syncLogEntries) WAITING FOR BROADCAST... rf.nextIndex[S%d]=%d, logLen=%d", pId, NextIndex, LogLen))
			rf.cond.Wait()

			// someone asked to check if we need to sync entries
			rf.cond.L.Unlock()
			continue
		}
		rf.cond.L.Unlock()

		rf.sendEntry(pId, NextIndex)

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendEntry(pId, nextIndex int) {
	rf.mu.Lock()
	if rf.raftState != Leader || rf.killed() {
		rf.logger.Log(-1, "(sendEntry) NOT A LEADER OR KILLED RETURN ....")
		rf.mu.Unlock()
		return
	}

	CurrTerm := rf.currTerm
	LogLen := rf.XIndex + len(rf.logs)

	LeaderId := rf.me
	LeaderCommit := rf.commitIndex

	XIndex := rf.XIndex
	XTerm := rf.XTerm

	Entries := []LogEntry{}

	var PrevLogIndex, PrevLogTerm int

	if nIdxLog := nextIndex - XIndex - 1; nIdxLog == 0 {
		PrevLogIndex = XIndex
		PrevLogTerm = XTerm

		Entries = make([]LogEntry, len(rf.logs))
		copy(Entries, rf.logs)

		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) SEND ENTRIES TO S%d; WHOLE TAIL leaderCommit=%d (prevLogIndex=%d, prevLogTerm=%d) entries=%v", pId, LeaderCommit, PrevLogIndex, PrevLogTerm, Entries))

	} else if nIdxLog > 0 {
		PrevLogIndex = rf.logs[nIdxLog-1].Index
		PrevLogTerm = rf.logs[nIdxLog-1].Term

		Entries = append(Entries, rf.logs[nIdxLog:]...)

		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) SEND ENTRIES TO S%d; PARTIAL TAIL leaderCommit=%d (prevLogIndex=%d, prevLogTerm=%d) entries=%v", pId, LeaderCommit, PrevLogIndex, PrevLogTerm, Entries))

	} else {
		// nIdxLog < 0, i.e. nextIndex - XIndex < 1; either
		//   1) nextIndex == XIndex != 0; which is fine!
		//   2) nextIndex == XIndex == 0; which is really bad!
		if rf.XIndex == 0 {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) ASSERTION ERROR: X_INDEX AND NEXT_INDEX BOTH ARE ZERO! Leader = S%d, rf.nextIndex=%v, rf.matchIndex=%v, status=%v", rf.leaderId, rf.nextIndex, rf.matchIndex, rf.raftState))
		} else {
			rf.logger.Log(-1, fmt.Sprintf("(sendEntry) NEGATIVE NEXT INDEX - SEND SNAPSHOT TO S%d  nextIdx=%d, rf.XIndex=%d, rf.XTerm=%d, len(logs)=%d logs=%v", pId, nextIndex, XIndex, XTerm, LogLen, rf.logs))

			rf.sendInstallSnapshot(pId)
			rf.mu.Unlock()
			return
		}
	}

	if len(Entries) == 0 {
		rf.logger.Log(-1, fmt.Sprintf("(sendEntry) SEND HB TO S%d, (nextIdx=%d, rf.XIndex=%d, len(logs)=%d)", pId, nextIndex, XIndex, LogLen))
	}

	rf.mu.Unlock()

	args := AppendEntriesArg{
		Term:         CurrTerm,
		LeaderId:     LeaderId,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  PrevLogTerm,
		Entries:      Entries,
		LeaderCommit: LeaderCommit,
	}
	reply := AppendEntriesReply{}

	// ok := false
	go func() {
		resCh := make(chan bool)

		ok := false
		go func() {
			resCh <- rf.peers[pId].Call(APPEND_ENTRIES_RPC, &args, &reply)
		}()

		select {
		case <-rf.quit:
			rf.logger.Log(-1, "CHANL CLOSED ... (SEND_ENTRY GR)")
			return
		case ok = <-resCh:
			rf.logger.Log(-1, "GOT A REPLY FROM A FOLLOWER")
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) .... S%d DID NOT RESPOND; RETURN (termSent=%d, rf.currTerm=%d) state=%v;", pId, args.Term, rf.currTerm, rf.raftState))
			return
		}
		defer rf.cond.Broadcast()

		if rf.killed() || rf.raftState != Leader {
			rf.logger.Log(-1, "(sendEntry) KILLED OR NOT A LEADER .... AFTER GOT A RESPONSE")
			return
		}

		if rf.currTerm > args.Term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) STALE RESPONSE FROM S%d; RETURN! (sentTerm=%d, rf.currTerm=%d)", pId, args.Term, rf.currTerm))
			return
		}

		if reply.Term > rf.currTerm {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) CHANGE TO A FOLLOWER! (leader=S%d reply.Term=%d, rf.currTerm=%d)", pId, reply.Term, rf.currTerm))

			rf.raftState = Follower
			rf.votedFor = -1
			rf.currTerm = reply.Term
			rf.persist()

			return
		}

		if args.Term != rf.currTerm {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) ARGS.TERM != RF.CURR_TERM; RETURN ... args.Term=%d != rf.currTerm=%d! Comming from S%d", args.Term, rf.currTerm, pId))
			return
		}

		if reply.Success {
			rf.matchIndex[pId] = Max(rf.matchIndex[pId], args.PrevLogIndex+len(args.Entries))
			rf.nextIndex[pId] = rf.matchIndex[pId] + 1

			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) APPEND SUCCESSFULL BY S%d  (sentTerm=%d, rf.currTerm=%d, rf.raftState=%d) (nextIndex[S%d]=%d, matchIndex[S%d]=%d) myLastLogIndex=%d log=%v", pId, args.Term, rf.currTerm, rf.raftState, pId, rf.nextIndex[pId], pId, rf.matchIndex[pId], len(rf.logs)-1, rf.logs))

			rf.commitEntries(args.Term)
			go func() {
				select {
				case <-rf.quit:
					return
				case rf.appEntCh <- done{}:
				}
			}()
		} else {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) FOLLOWER S%d REJECTED APE ...", pId))

			// Optimization for 2D:
			// Case 0: follower asked the leader for a snapshot
			// Optimization for 2C
			//  by the leader
			// 	Case 1: leader doesn't have XTerm:
			// 		nextIndex = XIndex
			//  Case 2: leader has XTerm:
			// 		nextIndex = leader's last entry for XTerm
			//  Case 3: follower's log is too short:
			// 		nextIndex = XLen
			if reply.NeedsSnapshot {
				// case 0; set the nextIndex to XIndex; so the next time, we will send a snapshot
				rf.nextIndex[pId] = rf.XIndex

				rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) FOLLOWER (S%d) ASKED FOR A SNAPSHOT! SET TO X_INDEX FOR NEXT CALL nextIndex[S%d]=%d len(logs)=%d reply=%+v", pId, pId, rf.nextIndex[pId], len(rf.logs), reply))
			} else if reply.XIsShort {
				// case 3; we might get rejected again which we will ended up in case 1 or 2
				rf.nextIndex[pId] = reply.XLen + 1

				rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) FOLLOWER (S%d) HAS A SHORTER LOG! nextIndex[S%d]=%d len(logs)=%d reply=%+v", pId, pId, rf.nextIndex[pId], len(rf.logs), reply))
			} else {

				nextIdx := -1
				for idx := len(rf.logs) - 1; idx > 0; idx-- {
					if logEntry := rf.logs[idx]; logEntry.Term == reply.XTerm {
						// case 2: leader has the term; idx is the index of the last entry
						nextIdx = rf.logs[idx].Index
						break
					}
				}

				if nextIdx != -1 {
					// case 2: the leader has the term
					rf.nextIndex[pId] = nextIdx
					rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) FOLLOWER (S%d) MISMATCH PREV TERM! (FOUNDED)! nextIndex[S%d]=%d\n\tlen(logs)=%d\n\treply=%+v", pId, pId, nextIdx, len(rf.logs), reply))
				} else {
					// case 1: the leader doesn't have the term
					rf.nextIndex[pId] = reply.XIndex
					rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) FOLLOWER (S%d) MISMATCH PREV TERM! (NOT_FOUNDED)! nextIndex[S%d]=%d len(logs)=%d, reply=%+v", pId, pId, reply.XIndex, rf.XIndex+len(rf.logs), reply))
				}

			}
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(sendEntry) NEW INDICES FOR S%d ; (nextIndex[S%d]=%d, matchIndex[S%d]=%d) myLastLogIndex=%d", pId, pId, rf.nextIndex[pId], pId, rf.matchIndex[pId], len(rf.logs)-1))
		}
	}()
}
