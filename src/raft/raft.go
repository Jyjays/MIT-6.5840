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
	"sort"
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
	Command interface{}
	Index   int
	Term    int
}

// NOTE - 预定义区
// Time define
// const (
// 	HeartbeatInterval = 125
// 	ElectionTimeout   = 1000
// )

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	Log         []LogEntry
	// heartbeat: decide whether to start the election
	//heartbeat   bool
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh       chan ApplyMsg
	applyCond     *sync.Cond
	replicateCond []*sync.Cond
	state         State
	//REVIEW - time.Time is not recommended by the student manual
	electionTimer  *time.Timer // timer for election timeout
	heartbeatTimer *time.Timer // timer for heartbeat
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (rf.state == Leader)
	if !isLeader {
		return index, term, isLeader
	}
	index = rf.getLastLog().Index + 1
	DPrintf("Node %d: Start agreement for command %v at index %d\n", rf.me, command, index)
	term = rf.currentTerm
	entry := LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.Log = append(rf.Log, entry)
	DPrintf("Node %d: Log %v Index %d Term %d\n", rf.me, rf.Log, index, term)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	go func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.replicateCond[i].Signal()
		}
	}()
	return index, term, isLeader
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// check the commitIndex is advanced
		for rf.commitIndex <= rf.lastApplied {
			// need to wait for the commitIndex to be advanced
			rf.applyCond.Wait()
		}

		// apply log entries to state machine
		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.Log[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		//DPrintf("applier:Node %d: commitIndex %d lastApplied %d\n", rf.me, rf.commitIndex, rf.lastApplied)
		// send the apply message to applyCh for service/State Machine Replica
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies log entries from index %v to %v in term %v", rf.me, lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.resetElectionTimer()
		rf.mu.Unlock()
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// check the logs of peer is behind the leader
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Unlock()
	for rf.killed() == false {
		for rf.needReplicating(peer) == false {
			rf.replicateCond[peer].Wait()
		}
		rf.SendHeartbeatOrLogs(peer)
	}
}

func (rf *Raft) checkNeedCommit() bool {
	// According the matchIndex, check if there is a log entry that is replicated by a majority of servers
	// and has not been committed yet
	// if there is such a log entry, commit it
	// if there is no such a log entry, return false
	// if there is such a log entry, return true
	// [10, 11, 12, 12, 13]
	// After sort , we can find which index was replicated by majority of servers
	// sort the matchIndex
	length := len(rf.matchIndex)
	matchIndex := make([]int, length)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	// find the index that is replicated by a majority of servers

	commitIndex := matchIndex[length-length/2-1]
	//DPrintf("Leader:Node %d: matchIndex %v, commitIndex %d\n", rf.me, matchIndex, commitIndex)
	//DPrintf("Node %d: commitIndex %d, rf.commitIndes %d\n", rf.me, commitIndex, rf.commitIndex)
	if commitIndex > rf.commitIndex {
		// check the term of the log entry
		if rf.Log[commitIndex].Term == rf.currentTerm {
			//DPrintf("Node %d commitIndex %d\n", rf.me, commitIndex)
			rf.commitIndex = commitIndex
			DPrintf("Leader's commitIndex %d\n", rf.commitIndex)
			return true
		}
	} else {
		return false
	}
	return false

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
	logFile, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	// 设置日志输出到文件
	log.SetOutput(logFile)
	for rf.killed() == false {

		select {
		case <-rf.electionTimer.C:
			// rf.ChangeState(Candidate)
			// rf.currentTerm += 1
			rf.persist()
			// start election
			rf.StartElection()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				// should send heartbeat
				rf.SendHeartbeats()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// func (rf *Raft) ticker() {
// 	//SECTION - enable log file
// 	logFile, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatalf("failed to open log file: %v", err)
// 	}
// 	defer logFile.Close()
// 	// 设置日志输出到文件
// 	log.SetOutput(logFile)
// 	//!SECTION
// 	//FIXME - Error: if start election is called before the ticker, then the ticker will not work
// 	// this means every raft will start election at the same time
// 	for rf.killed() == false {

// 		// Your code here (3A)
// 		// Check if a leader election should be started.

// 		// pause for a random amount of time between 50 and 350
// 		// milliseconds.
// 		ms := ElectionTimeout + (rand.Int63() % 300)
// 		time.Sleep(time.Duration(ms) * time.Millisecond)

// 		//REVIEW - start election
// 		// Start a new term
// 		rf.mu.Lock()
// 		flag := rf.state == Follower && rf.checkElectionTimeout()
// 		rf.mu.Unlock()
// 		if flag {
// 			//NOTE - debug in ticker
// 			//DPrintf("Node %d: Start election for term %d\n", rf.me, rf.currentTerm+1)
// 			go rf.StartElection()
// 		}

// 	}

// }

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
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCond = make([]*sync.Cond, len(rf.peers))

	// Your initialization code here (3A, 3B, 3C).
	rf.Init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimer()
	// start ticker goroutine to start elections

	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
		if peer != rf.me {
			rf.replicateCond[peer] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to send log entries to peer
			go rf.replicator(peer)
		}
	}

	go rf.applier()
	go rf.ticker()

	return rf
}

// ------------------actions-------------------

// Init function
func (rf *Raft) Init() {
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.Log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.electionTimer = time.NewTimer(RandomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
}

func (rf *Raft) SendHeartbeatOrLogs(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1

	args := rf.genAppendEntriesArgs(prevLogIndex)
	rf.mu.RUnlock()
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, args, &reply) {
		rf.mu.Lock()
		if args.Term == rf.currentTerm && rf.state == Leader {
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term, -1)
				} else if reply.Term == rf.currentTerm {
					// decrease nextIndex and retry
					// rf.nextIndex[peer] = reply.ConflictIndex
					// DPrintf("Node %d: Decrease nextIndex for peer %d to %d\n", rf.me, peer, rf.nextIndex[peer])
					// if reply.ConflictTerm != -1 {
					// 	firstLogIndex := rf.getFirstLog().Index
					// 	for index := rf.getLastLog().Index; index >= firstLogIndex; index-- {
					// 		if rf.Log[index-firstLogIndex].Term == reply.ConflictTerm {
					// 			rf.nextIndex[peer] = index
					// 			DPrintf("Node %d: Decrease nextIndex for peer %d to %d\n", rf.me, peer, rf.nextIndex[peer])
					// 			break
					// 		}
					// 	}
					// }
					if reply.ConflictTerm == -1 {
						rf.nextIndex[peer] = reply.ConflictIndex
						// } else if reply.ConflictTerm != -1 {
						// 	if reply.ConflictIndex == 0 {
						// 		firstIndex := rf.getFirstLog().Index
						// 		for index := rf.getLastLog().Index; index >= firstIndex; index-- {
						// 			if rf.Log[index-firstIndex].Term == reply.ConflictTerm {
						// 				rf.nextIndex[peer] = index + 1
						// 				break
						// 			}
						// 		}
						// 	} else {
						// 		rf.nextIndex[peer] = reply.ConflictIndex
						// 	}
					} else {
						rf.nextIndex[peer]--
					}
				}
			} else {
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				// advance commitIndex if possible
				if rf.checkNeedCommit() {
					rf.applyCond.Signal()
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) SendHeartbeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.SendHeartbeatOrLogs(peer)
	}
}

// func (rf *Raft) SendHeartbeats() {
// 	//DPrintf("Node %d: SendHeartbeats\n", rf.me)
// 	//ticker := time.NewTicker(time.Duration(HeartbeatInterval) * time.Millisecond) // 心跳周期
// 	rf.mu.RLock()
// 	// 如果不再是 Leader，退出心跳循环

// 	if rf.state != Leader {
// 		//DPrintf("Node %d: Stop sending heartbeat for term %d\n", rf.me, rf.currentTerm)
// 		rf.mu.RUnlock()
// 		return
// 	}
// 	// 获取当前 Term 和其他状态

// 	rf.mu.RUnlock()
// 	// 向所有 Follower 发送心跳
// 	for i := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}
// 		go func(server int) {
// 			rf.mu.RLock()
// 			args := rf.MakeHeartbeatArgs(server)

// 			reply := AppendEntriesReply{}
// 			if rf.state != Leader {
// 				return
// 			}
// 			rf.mu.RUnlock()
// 			//DPrintf("Heartbeat send: Node %d: Send heartbeat to Node %d for term %d\n", rf.me, server, rf.currentTerm)
// 			if rf.sendAppendEntries(server, &args, &reply) {
// 				rf.mu.Lock()
// 				defer rf.mu.Unlock()
// 				if reply.Term > rf.currentTerm {
// 					//DPrintf("4.Node %d: Become follower for term %d from state: %d\n", rf.me, args.Term, rf.state)
// 					rf.becomeFollower(reply.Term, -1)
// 				}
// 			}
// 			//DPrintf("HeartBeat accept reply from Node:%d\n", server)
// 		}(i)
// 	}

// }

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	DPrintf("Node %d: Start election for term %d\n", rf.me, rf.currentTerm+1)
	// rf.state = Candidate
	// rf.currentTerm += 1
	// rf.voteFor = rf.me

	// rf.resetElectionTimer() // 重置选举超时时间
	rf.becomeCandidate()
	//rf.persist()            // 持久化状态
	rargs := rf.MakeRequestVoteArgs()
	rf.mu.Unlock()

	//REVIEW - wg的作用（gpt写的-_-）
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
					//DPrintf("5. Node %d: Become follower for term %d from state: %d\n", rf.me, reply.Term, rf.state)
					rf.becomeFollower(reply.Term, -1)
					return
				}

				// 检查投票结果
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&votes, 1)
					if atomic.LoadInt32(&votes) > int32(voteThreshold) {
						rf.becomeLeader()
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
		voteTimeout := time.After(time.Millisecond * ElectionTimeout)
		done := make(chan bool)

		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-voteTimeout:
			rf.mu.Lock()
			if rf.state == Candidate {
				//DPrintf("6. Node %d: Election timed out in term %d\n", rf.me, rf.currentTerm)
				rf.becomeFollower(rf.currentTerm, -1)
				//rf.persist()
			}
			rf.mu.Unlock()
		case <-done:
			// 正常完成所有投票请求
			rf.mu.Lock()
			if rf.state == Candidate && atomic.LoadInt32(&votes) <= int32(voteThreshold) {
				//DPrintf("7. Node %d: Failed to become leader for term %d\n", rf.me, rf.currentTerm)
				rf.becomeFollower(rf.currentTerm, -1)
				rf.resetElectionTimer()

				//rf.persist()
			}
			rf.mu.Unlock()
		}
	}()
}
