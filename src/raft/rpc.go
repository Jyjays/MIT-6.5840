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

// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	reply.Term = rf.currentTerm

// 	if args.Term < rf.currentTerm {
// 		reply.VoteGranted = false
// 		return
// 	}

// 	//NOTE - RequestVote 3B
// 	// case 1 - args.LastLogTerm < rf.getLastLog().Term
// 	// case 2 - args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex < rf.getLastLog().Index
// 	// case 3 - args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex == rf.getLastLog().Index
// 	lastLog := rf.getLastLog()
// 	if args.LastLogTerm < lastLog.Term {
// 		reply.VoteGranted = false
// 		return
// 	} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index {
// 		reply.VoteGranted = false
// 		return
// 	}

// 	if args.Term > rf.currentTerm {
// 		//DPrintf("1. Node %d: Become follower for term %d from state: %d\n", rf.me, args.Term, rf.state)
// 		rf.becomeFollower(args.Term, args.CandidateId)
// 		reply.VoteGranted = true
// 		return
// 	}
// 	if rf.state != Follower {
// 		reply.VoteGranted = false
// 		return
// 	}

// 	if rf.voteFor >= 0 && rf.voteFor != args.CandidateId {
// 		reply.VoteGranted = false
// 		return
// 	}

// 	rf.voteFor = args.CandidateId
// 	reply.VoteGranted = true
// }

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

// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	//DPrintf("Node %d: AppendEntries start", rf.me)
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	//DPrintf("Node %d: Recive AppendEntries for term %d\n", rf.me, args.Term)
// 	reply.Term = rf.currentTerm
// 	if args.Term < rf.currentTerm {
// 		reply.Success = false
// 		DPrintf("Node %d: args is mismatch %d\n", rf.me, args.Term)
// 		return
// 	}
// 	//
// 	//NOTE - AppendEntries 3A
// 	//FIXME - If this rf server is follower, and recive the heartbeat
// 	//1. if the rf's voteFor is not -1, then it should be reset to -1
// 	//DPrintf("Node %d: media of AppendEntries", rf.me)
// 	//NOTE - if this rf is candidate, but recive heartbeat from another leader
// 	// and its term is not smaller than this rf's term , then this rf should become follower

// 	//NOTE - if this rf is candidate, but recive heartbeat from another leader
// 	// and its term is not smaller than this rf's term , then this rf should become follower
// 	if args.Term > rf.currentTerm {
// 		rf.currentTerm, rf.voteFor = args.Term, -1
// 		rf.persist()
// 	}
// 	if rf.state != Follower {
// 		DPrintf("2. Node %d: Become follower for term %d from state: %d\n", rf.me, args.Term, rf.state)
// 		rf.becomeFollower(args.Term, -1)
// 	} else {
// 		rf.resetElectionTimer()
// 	}
// 	//DPrintf("<<AppendEntries:Node %d: Recive heartbeat, args.leaderCommit %d, rf.commitIndex %d\n", rf.me, args.LeaderCommit, rf.commitIndex)

// 	//DPrintf("Node %d: end of AppendEntries", rf.me)
// 	if args.Entries == nil || len(args.Entries) == 0 {
// 		reply.Success = true
// 		return
// 	}
// 	//NOTE - AppendEntries 3B
// 	//REVIEW - Using the slowest but most reliable way to append logs
// 	// compare the logs between the leader and the follower one by one
// 	// if there's a conflict, then delete the conflict logs and append the new logs
// 	lastLog := rf.getLastLog()

// 	//NOTE - case 1 args.prevLogIndex is larger than the lastLog.Index
// 	// case 2 term is mismatch

// 	// Case 1: prevLogIndex超出Follower的日志长度
// 	if args.PrevLogIndex > lastLog.Index {
// 		reply.Success = false
// 		reply.ConflictIndex = lastLog.Index + 1
// 		reply.ConflictTerm = -1
// 		return
// 	}

// 	// Case 2: Term不匹配
// 	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
// 		reply.Success = false
// 		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term

// 		// 找到该Term的第一个索引
// 		reply.ConflictIndex = 0
// 		for i := args.PrevLogIndex; i >= 0; i-- {
// 			if rf.Log[i].Term != reply.ConflictTerm {
// 				reply.ConflictIndex = i + 1
// 				break
// 			}
// 		}
// 		return
// 	}
// 	//NOTE - case 3
// 	//NOTE - if the term is matched, then delete the conflict logs and append the new logs
// 	rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
// 	DPrintf("Node %d: AppendEntries for term %d, log %v\n", rf.me, args.Term, rf.Log)
// 	//DPrintf("args %v\n", args)
// 	// if isMatched(lastLog, rf.Log[args.PrevLogIndex]) {

// 	// 	DPrintf("Node %d receive AppendEntries for term %d, log %v\n", rf.me, args.Term, rf.Log)
// 	// }
// 	//DPrintf("Node %d: end of AppendEntries", rf.me)\
// 	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
// 	//DPrintf("AppendEntries: Node %d commitIndex %d, newCommitIndex %d\n", rf.me, rf.commitIndex, newCommitIndex)
// 	if newCommitIndex > rf.commitIndex {
// 		rf.commitIndex = newCommitIndex
// 		rf.applyCond.Signal()
// 	}
// 	reply.Success = true
// 	return

// }

// REVIEW - The return value is never used
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
