package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

func currentTime() string {
	return time.Now().Format("2025-01-19 15:37:05")
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
		//fmt.Printf(format, a...)
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Make function for RequestVote and AppendEntries
//
//	func MakeRequestVoteArgs(term int, candidateId int, lastLogIndex int, lastLogTerm int) RequestVoteArgs {
//		args := RequestVoteArgs{}
//		args.Term = term
//		args.CandidateId = candidateId
//		args.LastLogIndex = lastLogIndex
//		args.LastLogTerm = lastLogTerm
//		return args
//	}
func MakeRequestVoteReply(term int, voteGranted bool) RequestVoteReply {
	reply := RequestVoteReply{}
	reply.Term = term
	reply.VoteGranted = voteGranted
	return reply
}

func (rf *Raft) MakeRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLog().Index
	args.LastLogTerm = rf.getLastLog().Term
	return args
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

func (rf *Raft) isMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.Log[index-rf.getFirstLog().Index].Term
}

func (rf *Raft) isUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) becomeFollower(term int, candidateID int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	//DPrintf("[Server %d become follower, term %d, vote for %d", rf.me, term, candidateID)
	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = candidateID
	rf.heartbeatTimer.Stop()
	rf.resetElectionTimer()
}

func (rf *Raft) becomeCandidate() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	//DPrintf("[Server %d become candidate, term %d", rf.me, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.electionTimer.Stop() // stop election
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
}

func (rf *Raft) becomeLeader() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	//DPrintf("[Server %d become leader, term %d", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLog().Index + 1
	}
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
}

const ElectionTimeout = 1000
const HeartbeatTimeout = 125

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var GlobalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func (rf *Raft) resetElectionTimer() {
	// rf.electionTimeout = time.Now().Add(time.Duration(rand.Intn(ElectionTimeout)+ElectionTimeout) * time.Millisecond)
	rf.electionTimer.Reset(RandomElectionTimeout())
}

// func (rf *Raft) checkElectionTimeout() bool {
// 	return time.Now().After(rf.electionTimeout)
// }

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

// TODO - add a parameter to specify the heartbeat and appendEntries
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

func shrinkEntries(entries []LogEntry) []LogEntry {
	const lenMultiple = 2
	if cap(entries) > len(entries)*lenMultiple {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

func (rf *Raft) findLastLogByTerm(term int) int {
	firstLogIndex := rf.getFirstLog().Index
	for i := len(rf.Log) - 1; i >= 0; i-- {
		if rf.Log[i].Term == term {
			return i + firstLogIndex
		}
	}
	return -1
}
