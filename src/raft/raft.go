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
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Log string
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	Log         []*LogEntry
	// heartbeat: decide whether to start the election
	//heartbeat   bool
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state           State
	electionTimeout time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

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
	Success bool
}

//TODO - Reimplement with GetState function
// example RequestVote RPC handler.
// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	//SECTION - RequestVote
// 	log.Printf("---------RequestVote-------------------\n")
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	//NOTE - debug in request vote
// 	//log.Printf("Candidate %d receive request vote from %d, and term is %d\n", rf.me, args.CandidateId, args.Term)
// 	//log.Printf("Candidate %d current term is %d\n", rf.me, rf.currentTerm)
// 	//REVIEW -
// 	if args.Term > rf.currentTerm {
// 		if rf.state != Follower {
// 			rf.state = Follower
// 		}
// 		rf.voteFor = args.CandidateId
// 		rf.currentTerm = args.Term
// 		reply.VoteGranted = true
// 		return
// 	}
// 	reply.Term = rf.currentTerm
// 	if args.Term < rf.currentTerm || rf.state != Follower || (rf.voteFor >= 0 && rf.voteFor != args.CandidateId) {
// 		//	log.Printf("refused here,because term is %d, state is %d, voteFor is %d\n", rf.currentTerm, rf.state, rf.voteFor)
// 		reply.VoteGranted = false
// 		return
// 	}

// 	rf.voteFor = args.CandidateId
// 	reply.VoteGranted = true
// 	//!SECTION
// }
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 设置回复的 Term，先返回自己的 Term
	reply.Term = rf.currentTerm
	// 1. 如果请求的 Term 小于当前的 Term，拒绝投票请求
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// 2. 如果请求的 Term 大于当前的 Term，转换为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, args.CandidateId)
		reply.VoteGranted = true
		return
	}
	// 3. 如果当前节点不是 Follower，拒绝投票请求
	if rf.state != Follower {
		reply.VoteGranted = false
		return
	}
	// 4. 检查是否已经投票给了其他候选人
	if rf.voteFor >= 0 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	// 5. 允许投票并更新状态
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
}

// 转换为 Follower 状态的函数
func (rf *Raft) becomeFollower(term int, candidateID int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = candidateID
	rf.resetElectionTimer() // 重置选举超时
	log.Printf("Node %d: Became follower for term %d due to leader %d\n", rf.me, term, candidateID)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	//
	//FIXME - If this rf server is follower, and recive the heartbeat
	//1. if the rf's voteFor is not -1, then it should be reset to -1
	log.Printf("---------AppendEntries rf:%d----------------\n", rf.me)

	if len(args.Entries) == 0 {
		//NOTE - if this rf is candidate, but recive heartbeat from another leader
		// and its term is not smaller than this rf's term , then this rf should become follower
		rf.voteFor = -1
		log.Printf("args: %v\n", args)
		if rf.state == Candidate {
			rf.becomeFollower(args.Term, args.LeaderId)
		}
		//rf.heartbeat = true
		reply.Success = true
		return
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	//SECTION - enable log file
	logFile, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	// 设置日志输出到文件
	log.SetOutput(logFile)
	log.Printf("-----------A new lab start--------------\n")
	//!SECTION
	//FIXME - Error: if start election is called before the ticker, then the ticker will not work
	// this means every raft will start election at the same time
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		//REVIEW - start election
		// Start a new term
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Follower && rf.checkElectionTimeout() {
			rf.mu.Unlock()
			rf.StartElection()
		}

		//rf.mu.Unlock()
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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.Init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// ------------------utils-------------------

// Init function
func (rf *Raft) Init() {
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.Log = append(rf.Log, &LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
}

// Leader send heartbeat functon
// func (rf *Raft) SendHeartbeats() {
// 	//FIXME - the loop should be a two-level loop

// 	for {
// 		for i := range rf.peers {
// 			//NOTE - debug
// 			//log.Printf("Leader send heartbeat:%d\n", rf.me)
// 			aargs := MakeAppendEntriesArgs(rf.currentTerm, rf.me, len(rf.Log)-1, rf.currentTerm, nil, rf.commitIndex)
// 			aargs.Term = rf.currentTerm
// 			areply := AppendEntriesReply{}
// 			rf.sendAppendEntries(i, &aargs, &areply)
// 			//log.Printf("reply: %v\n", areply)
// 			// if !areply.Success {
// 			// 	break
// 			// }
// 		}
// 		if rf.state != Leader {
// 			break
// 		}
// 	}
// }
func (rf *Raft) SendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		go func(server int) {
			args := MakeAppendEntriesArgs(currentTerm, rf.me, len(rf.Log)-1, 0, nil, rf.commitIndex)
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
				}
			}
		}(i)
	}
}

// func (rf *Raft) StartElection() {
// 	log.Printf("------------------------------\n")
// 	rf.mu.Lock()
// 	rf.state = Candidate
// 	rf.currentTerm += 1
// 	rf.voteFor = rf.me
// 	rargs := MakeRequestVoteArgs(rf.currentTerm, rf.me, len(rf.Log), rf.currentTerm)
// 	rrpley := RequestVoteReply{}
// 	votesize := 1
// 	//REVIEW - standrad should be len(rf.peers) / 2
// 	//standrad := len(rf.peers) / 2
// 	standrad := getStandrad(len(rf.peers))
// 	rf.mu.Unlock()
// 	//TODO : Timeout detection
// 	for i := range rf.peers {
// 		//NOTE - debug
// 		log.Printf("Candidate %d send request vote to %d\n", rf.me, i)
// 		rf.sendRequestVote(i, &rargs, &rrpley)
// 		log.Printf("reply: %v\n", rrpley)
// 		if rrpley.Term > rf.currentTerm {
// 			rf.mu.Lock()
// 			rf.currentTerm = rrpley.Term
// 			rf.state = Follower
// 			rf.mu.Unlock()
// 			break
// 		}
// 		if rrpley.VoteGranted == true {
// 			votesize += 1
// 			if votesize > standrad {
// 				rf.mu.Lock()
// 				log.Printf("Leader anncounced %d\n", rf.me)
// 				rf.state = Leader
// 				//REVIEW - send heartbeat
// 				rf.SendHeartbeats()
// 				rf.mu.Unlock()
// 				break
// 			}
// 		}
// 	}

// }
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.resetElectionTimer()
	//TODO - MakeRequestVoteArgs's parameter should be the last log's term
	rargs := MakeRequestVoteArgs(rf.currentTerm, rf.me, len(rf.Log), rf.currentTerm)
	rf.mu.Unlock()

	votes := 1
	standard := getStandrad(len(rf.peers))
	for i := range rf.peers {
		go func(server int) {
			rrply := RequestVoteReply{}
			if rf.sendRequestVote(server, &rargs, &rrply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 更新 term 如果发现更高的 term
				if rrply.Term > rf.currentTerm {
					rf.currentTerm = rrply.Term
					rf.state = Follower
					return
				}

				if rrply.VoteGranted && rf.state == Candidate {
					votes += 1
					if votes > standard {
						rf.state = Leader
						rf.SendHeartbeats()
						//REVIEW -
						log.Printf("Node %d: Became leader for term %d\n", rf.me, rf.currentTerm)
					}
				}
			}
		}(i)
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
	rf.electionTimeout = time.Now().Add(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
}

func (rf *Raft) checkElectionTimeout() bool {
	return time.Now().After(rf.electionTimeout)
}
