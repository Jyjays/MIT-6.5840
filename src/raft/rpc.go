package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int // 冲突的term
	XIndex  int // 冲突的Index
	XLen    int // 需要的长度
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
	// Reply false if term < currentTerm(§5.1)
	// if the term is same as currentTerm, and the votedFor is not null and not the candidateId, then reject the vote(§5.2)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.voteFor = args.CandidateId
		rf.persist()
	}

	// if candidate's log is not up-to-date, reject the vote(§5.4)
	if !rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.persist()
	rf.resetElectionTimer()
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.persist()
	}
	rf.becomeFollower(args.Term)
	rf.persist()
	rf.resetElectionTimer()
	// check the log is matched, if not, return the conflict index and term
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it(§5.3)
	firstIndex := rf.getFirstLog().Index
	if !rf.isMatched(args.PrevLogIndex, args.PrevLogTerm) {
		if BackupQuick {
			reply.Term, reply.Success = rf.currentTerm, false

			lastIndex := rf.getLastLog().Index
			if args.PrevLogIndex > lastIndex {
				// Follower 日志长度太短，通知 Leader 从lastIndex+1开始
				reply.XTerm, reply.XIndex, reply.XLen = -1, -1, lastIndex+1
				// TODO - Add the situation that the leader's log is shorter than the follower's log
			} else if args.PrevLogIndex >= firstIndex {
				// PrevLogIndex 在 Follower 的日志范围内，则冲突可能是Term不同
				reply.XTerm = rf.log[args.PrevLogIndex-firstIndex].Term
				reply.XIndex = firstIndex
				// 找到冲突XTerm中的第一个Index
				for i := args.PrevLogIndex; i >= firstIndex; i-- {
					if rf.log[i-firstIndex].Term != reply.XTerm {
						reply.XIndex = i + 1
						break
					}
				}
			} else {
				//NOTE - prevLogIndex is smaller than the firstIndex, which means the follower's log is longer than the leader's log
				reply.XTerm, reply.XIndex, reply.XLen = -1, -1, firstIndex
			}
		} else {
			reply.Term, reply.Success = rf.currentTerm, false
		}
		return
	}

	for index, entry := range args.Entries {
		if entry.Index < firstIndex {
			fmt.Println("entry.Index < firstIndex", entry.Index, firstIndex)
			continue
		}
		if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			if entry.Index-firstIndex >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[index:]...)
			} else {
				rf.log = append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...)
			}
			//NOTE - slice is based on array, in the memory, we need to check whether the size of array is too large to store the data
			rf.log = shrinkEntries(rf.log)
			rf.persist()
			break
		}
		// fmt.Println("entry.Index", entry.Index)
	}

	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex && newCommitIndex >= rf.getFirstLog().Index {
		DPrintf("{Node %v} commitIndex from %v to %v with leaderCommit %v in term %v \n", rf.me, rf.commitIndex, newCommitIndex, args.LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer DPrintf("InstallSnapshot: {Node %v}'s state is {state %v, term %v}} after processing InstallSnapshot,  InstallSnapshotArgs %v and InstallSnapshotReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if args.LastIncludedIndex > rf.getLastLog().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = shrinkEntries(rf.log[args.LastIncludedIndex-rf.getFirstLog().Index:])
		rf.log[0].Command = nil
	}
	rf.log[0].Term, rf.log[0].Index = args.LastIncludedTerm, args.LastIncludedIndex
	rf.commitIndex, rf.lastApplied = args.LastIncludedIndex, args.LastIncludedIndex
	DPrintf("{Node %v} Lastapplied changed to %v in InstallSnapshot\n", rf.me, rf.lastApplied)
	rf.persister.Save(rf.encodeState(), args.Data)
	//FIXME - Shouldn's use goroutine here
	snapmsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- snapmsg
	rf.mu.Lock()
}

// REVIEW - The return value is never used
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("{Node %v} sendInstallSnapshot to {Node %v} with args %v\n", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
