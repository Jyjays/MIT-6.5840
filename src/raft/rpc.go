package raft

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
	XTerm   int
	XIndex  int
	XLen    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term 			int
	LeaderId 		int
	LastIncludedIndex int
	LastIncludedTerm int
	Data 			[]byte
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
		rf.becomeFollower(args.Term, args.CandidateId)
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
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	// Reply false if term < currentTerm(§5.1)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// indicate the peer is the leader
	if args.Term > rf.currentTerm {
		//TODO - to be optimized
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.persist()
	}
	rf.becomeFollower(args.Term, -1)
	rf.persist()
	rf.resetElectionTimer()

	// check the log is matched, if not, return the conflict index and term
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it(§5.3)
	if !rf.isMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		firstIndex := rf.getFirstLog().Index
		lastIndex := rf.getLastLog().Index
		if args.PrevLogIndex > rf.getLastLog().Index {
			reply.XTerm, reply.XIndex, reply.XLen = -1, -1, lastIndex+1
			// TODO - Add the situation that the leader's log is shorter than the follower's log
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			for i := args.PrevLogIndex; i >= firstIndex; i-- {
				if rf.log[i-firstIndex].Term != reply.XTerm {
					reply.XIndex = i + 1
					break
				}
			}
		}

		return
	}
	// append any new entries not already in the log
	firstLogIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		// find the junction of the existing log and the appended log.
		if entry.Index-firstLogIndex >= len(rf.log) || rf.log[entry.Index-firstLogIndex].Term != entry.Term {
			//NOTE - slice is based on array, in the memory, we need to check whether the size of array is too large to store the data
			rf.log = shrinkEntries(append(rf.log[:entry.Index-firstLogIndex], args.Entries[index:]...))
			rf.persist()
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (paper)
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %v} advances commitIndex from %v to %v with leaderCommit %v in term %v", rf.me, rf.commitIndex, newCommitIndex, args.LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing InstallSnapshot,  InstallSnapshotArgs %v and InstallSnapshotReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, -1)
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
	rf.persister.Save(rf.encodeState(), args.Data)

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
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
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
