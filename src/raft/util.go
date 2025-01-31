package raft

import (
	"math/rand"
	"sync"
	"time"
	"log"
	//"fmt"
)

// Debugging
const Debug = true

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

// TODO - add a parameter to specify the heartbeat and appendEntries
func (rf *Raft) MakeAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	if prevLogIndex < firstLogIndex {
		DPrintf("[Server %d] prevLogIndex %d < firstLogIndex %d", rf.me, prevLogIndex, firstLogIndex)
		return nil
	}
	entries := make([]LogEntry, len(rf.log[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}

func (rf *Raft) MakeInstallSnapshotArgs() *InstallSnapshotArgs {
	firstlog := rf.getFirstLog()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstlog.Index,
		LastIncludedTerm:  firstlog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
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
	return index >= rf.getFirstLog().Index && index <= rf.getLastLog().Index && term == rf.log[index-rf.getFirstLog().Index].Term
}

func (rf *Raft) isUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) becomeFollower(term int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	//DPrintf("[Server %d become follower, term %d, vote for %d", rf.me, term, candidateID)
	rf.state = Follower
	rf.currentTerm = term
	// rf.voteFor = candidateID
	rf.heartbeatTimer.Stop()
	rf.resetElectionTimer()
}

func (rf *Raft) becomeCandidate() {
	if rf.state == Leader {
		DPrintf("[Server %d] is already leader, term %d\n", rf.me, rf.currentTerm)
        return
    }
    rf.state = Candidate
    rf.currentTerm += 1
    rf.voteFor = rf.me  // 必须为自己投票
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
	rf.matchIndex[rf.me] = rf.getLastLog().Index
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
}

const ElectionTimeout = 500
const HeartbeatTimeout = 50

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
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}

func (rf *Raft) getPrevEntry(peer int) LogEntry {
	if len(rf.log) == 0 || rf.nextIndex[peer] == 0 {
		return LogEntry{}
	}
	return rf.log[rf.nextIndex[peer]-1]
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

func (rf *Raft) findLastLogIndexByTerm(term int) (bool ,int) {
	// 如果找到了，返回true和index
	// 如果没找到，返回false和-1

	firstLogIndex := rf.getFirstLog().Index
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return true, i + firstLogIndex
		}
	}
	return false, -1
}
