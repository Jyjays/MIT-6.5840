package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		//log.Printf(format, a...)
		fmt.Printf(format, a...)
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
func MakeAppendEntriesReply(term int, success bool) AppendEntriesReply {
	reply := AppendEntriesReply{}
	reply.Term = term
	reply.Success = success
	return reply
}

func getStandrad(peers int) int {
	if peers%2 == 0 {
		return peers / 2
	} else {
		return peers/2 + 1
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = time.Now().Add(time.Duration(50+rand.Intn(300)) * time.Millisecond)
}

func (rf *Raft) checkElectionTimeout() bool {
	return time.Now().After(rf.electionTimeout)
}
