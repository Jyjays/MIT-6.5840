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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 设置回复的 Term，先返回自己的 Term
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		//NOTE - follower 1
		DPrintf("1. Node %d: Become follower for term %d from state: %d\n", rf.me, args.Term, rf.state)
		rf.becomeFollower(args.Term, args.CandidateId)
		reply.VoteGranted = true
		return
	}
	if rf.state != Follower {
		reply.VoteGranted = false
		return
	}

	if rf.voteFor >= 0 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
}

func (rf *Raft) becomeFollower(term int, candidateID int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = candidateID
	rf.resetElectionTimer()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//NOTE - debug in AppendEntries
	DPrintf("Node %d: Recive AppendEntries for term %d\n", rf.me, args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	//
	//FIXME - If this rf server is follower, and recive the heartbeat
	//1. if the rf's voteFor is not -1, then it should be reset to -1

	if len(args.Entries) == 0 {
		//NOTE - if this rf is candidate, but recive heartbeat from another leader
		// and its term is not smaller than this rf's term , then this rf should become follower
		rf.voteFor = -1
		if rf.state == Candidate {
			//NOTE - follower 2
			DPrintf("2. Node %d: Become follower for term %d from state: %d\n", rf.me, args.Term, rf.state)
			rf.becomeFollower(args.Term, -1)
		} else {
			rf.resetElectionTimer()
		}
		//rf.heartbeat = true
		reply.Success = true
		return
	}
}
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
	DPrintf("-----------A new lab start--------------\n")
	//!SECTION
	//FIXME - Error: if start election is called before the ticker, then the ticker will not work
	// this means every raft will start election at the same time
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 //+ (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//SECTION - debug in ticker
		DPrintf("<<<<Node %d details: currentTerm: %d, voteFor: %d, state: %d>>>>\n", rf.me, rf.currentTerm, rf.voteFor, rf.state)
		//!SECTION
		//REVIEW - start election
		// Start a new term
		rf.mu.Lock()
		flag := rf.state == Follower && rf.checkElectionTimeout()
		rf.mu.Unlock()
		if flag {
			//NOTE - debug in ticker
			DPrintf("Node %d: Start election for term %d\n", rf.me, rf.currentTerm+1)
			go rf.StartElection()
		}

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
	rf.resetElectionTimer()
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

func (rf *Raft) SendHeartbeats() {
	// 启动心跳循环
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond) // 心跳周期
		defer ticker.Stop()
		for {
			rf.mu.Lock()
			// 如果不再是 Leader，退出心跳循环
			// NOTE - leader 1

			if rf.state != Leader {
				DPrintf("Node %d: Stop sending heartbeat for term %d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			}
			// 获取当前 Term 和其他状态
			currentTerm := rf.currentTerm
			leaderCommit := rf.commitIndex
			rf.mu.Unlock()
			// 向所有 Follower 发送心跳
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int) {
					args := MakeAppendEntriesArgs(currentTerm, rf.me, len(rf.Log)-1, 0, nil, leaderCommit)
					reply := AppendEntriesReply{}
					DPrintf("Node %d: Send heartbeat to Node %d for term %d\n", rf.me, server, currentTerm)
					if rf.sendAppendEntries(server, &args, &reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm {
							//NOTE - follower 4
							DPrintf("4.Node %d: Become follower for term %d from state: %d\n", rf.me, args.Term, rf.state)
							rf.becomeFollower(reply.Term, -1)
						}
					}
				}(i)
			}
			<-ticker.C // 等待下一个心跳周期
		}
	}()
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.resetElectionTimer() // 重置选举超时时间
	//rf.persist()            // 持久化状态
	currentTerm := rf.currentTerm
	rargs := MakeRequestVoteArgs(currentTerm, rf.me, len(rf.Log), currentTerm)
	rf.mu.Unlock()

	var votes int32 = 1
	var wg sync.WaitGroup
	voteThreshold := len(rf.peers) / 2 // 过半数阈值

	for i := range rf.peers {
		if i == rf.me {
			continue // 跳过自己
		}

		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &rargs, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 更新 term，如果发现更高的 term
				if reply.Term > rf.currentTerm {
					//NOTE - follower 5
					DPrintf("5. Node %d: Become follower for term %d from state: %d\n", rf.me, reply.Term, rf.state)
					rf.becomeFollower(reply.Term, -1)
					return
				}

				// 检查投票结果
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&votes, 1)
					if atomic.LoadInt32(&votes) > int32(voteThreshold) {
						rf.state = Leader
						rf.persist()
						rf.SendHeartbeats()
						DPrintf("Node %d: Became leader for term %d\n", rf.me, rf.currentTerm)
					}
				}
			}
		}(i)
	}

	// 等待投票完成或超时
	//REVIEW - 为什么要用 goroutine
	go func() {
		voteTimeout := time.After(time.Millisecond * 150)
		done := make(chan bool)

		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-voteTimeout:
			rf.mu.Lock()
			if rf.state == Candidate {
				//NOTE - follower 6
				DPrintf("6. Node %d: Election timed out in term %d\n", rf.me, rf.currentTerm)
				rf.becomeFollower(rf.currentTerm, -1)
				//rf.persist()
			}
			rf.mu.Unlock()
		case <-done:
			// 正常完成所有投票请求
			rf.mu.Lock()
			if rf.state == Candidate && atomic.LoadInt32(&votes) <= int32(voteThreshold) {
				//NOTE - follower 7
				DPrintf("7. Node %d: Failed to become leader for term %d\n", rf.me, rf.currentTerm)
				//rf.persist()
			}
			rf.mu.Unlock()
		}
	}()
}
