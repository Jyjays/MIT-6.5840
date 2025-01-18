package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
		//fmt.Printf(format, a...)
	}
}

// Make function for RequestVote and AppendEntries
func MakeRequestVoteArgs(term int, candidateId int, lastLogIndex int, lastLogTerm int) RequestVoteArgs {
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = candidateId
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm
	return args
}
func MakeRequestVoteReply(term int, voteGranted bool) RequestVoteReply {
	reply := RequestVoteReply{}
	reply.Term = term
	reply.VoteGranted = voteGranted
	return reply
}
func MakeAppendEntriesArgs(term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = term
	args.LeaderId = leaderId
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = entries
	args.LeaderCommit = leaderCommit
	return args
}

func (rf *Raft) MakeHeartbeatArgs(peer int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	prevEntry := rf.getPrevEntry(peer)
	args.PrevLogIndex = prevEntry.Index
	args.PrevLogTerm = prevEntry.Term
	args.LeaderCommit = rf.commitIndex
	return args
}

func MakeAppendEntriesReply(term int, success bool) AppendEntriesReply {
	reply := AppendEntriesReply{}
	reply.Term = term
	reply.Success = success
	return reply
}

func isMatched(lastLog LogEntry, prevLog LogEntry) bool {
	if lastLog.Index == -1 {
		return true
	}
	return lastLog.Term == prevLog.Term && lastLog.Index == prevLog.Index
}
func (rf *Raft) becomeFollower(term int, candidateID int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = candidateID
	rf.resetElectionTimer()
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = time.Now().Add(time.Duration(300+rand.Intn(1000)) * time.Millisecond)
}

func (rf *Raft) checkElectionTimeout() bool {
	return time.Now().After(rf.electionTimeout)
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.Log[len(rf.Log)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.Log[0]
}

func (rf *Raft) getPrevEntry(peer int) LogEntry {
	if len(rf.Log) == 0 || rf.nextIndex[peer] == 0 {
		return LogEntry{}
	}
	return rf.Log[rf.nextIndex[peer]-1]
}
func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.Log[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.Log[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.Log[prevLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}
