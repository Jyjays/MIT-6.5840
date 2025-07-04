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
	"bytes"
	"log"
	"os"

	// "log"
	// "os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the passed to Make(). set
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
	CurrentTerm  int

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

const BackupQuick = true

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
	log         []LogEntry
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

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.voteFor) != nil || e.Encode(rf.log) != nil {
		return nil
	}
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		DPrintf("{Node %v} fails to decode persisted state", rf.me)
	}
	rf.currentTerm, rf.voteFor, rf.log = currentTerm, voteFor, log
	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
	DPrintf("{Node %v} Lastapplied changed to %v in readPersist\n", rf.me, rf.lastApplied)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	// remove log entries up to index
	rf.log = rf.log[index-snapshotIndex:]
	rf.log[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
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
	rf.log = append(rf.log, entry)
	rf.persist()
	//DPrintf("Node %d: Log %v Index %d Term %d\n", rf.me, rf.log, index, term)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	if rf.checkNeedCommit() {
		rf.applyCond.Signal()
	}
	// NOTE - 创建协程进行复制，不会阻塞Leader继续接收日志
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
	for !rf.killed() {
		rf.mu.Lock()
		// check the commitIndex is advanced
		for rf.commitIndex <= rf.lastApplied {
			// need to wait for the commitIndex to be advanced
			rf.applyCond.Wait()
		}

		// apply log entries to state machine
		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		DPrintf("{Node %v} applies log entries from index %v to %v in term %v", rf.me, lastApplied, commitIndex, rf.currentTerm)
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		//DPrintf("applier:Node %d: commitIndex %d lastApplied %d\n", rf.me, rf.commitIndex, rf.lastApplied)
		// send the apply message to applyCh for service/State Machine Replica
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CurrentTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		DPrintf("{Node %v} Lastapplied changed to %v in applier\n", rf.me, rf.lastApplied)
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
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicateCond[peer].Wait()
		}
		rf.SendHeartbeatOrLogs(peer)

		// rf.mu.Lock()
		// if rf.checkNeedCommit() {
		// 	rf.applyCond.Signal()
		// }
		// rf.mu.Unlock()
	}
}

// According the matchIndex, check if there is a log entry that is replicated by a majority of servers
// and has not been committed yet
// if there is such a log entry, commit it
// if there is no such a log entry, return false
// if there is such a log entry, return true
// [10, 11, 12, 12, 13]
// After sort , we can find which index was replicated by majority of servers
func (rf *Raft) checkNeedCommit() bool {
	length := len(rf.matchIndex)
	if length == 0 {
		return false
	}
	matchIndex := make([]int, length)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)

	// 确保有多数派，至少有过半的节点大于quorumIndex
	quorumIndex := matchIndex[length-length/2-1]
	firstIndex := rf.getFirstLog().Index
	//DPrintf("Node %d: quorumIndex %d, commitIndex %d, term %d\n", rf.me, quorumIndex, rf.commitIndex, rf.currentTerm)
	// NOTE - Figure 8 in the paper
	if quorumIndex > rf.commitIndex && rf.log[quorumIndex-firstIndex].Term == rf.currentTerm {
		rf.commitIndex = quorumIndex
		//DPrintf("Leader's commitIndex updated to %d\n", rf.commitIndex)
		return true
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
	if Output {
		logFile, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("failed to open log file: %v", err)
		}
		defer logFile.Close()
		log.SetOutput(logFile)
	}
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Follower {
				rf.mu.Unlock()
				rf.StartElection()
			} else {
				rf.mu.Unlock()
			}
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state != Follower {
				rf.SendHeartbeats()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
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
	rf.log = make([]LogEntry, 1)
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
	firstLogIndex := rf.getFirstLog().Index
	//DPrintf("prevLogIndex %d, firstLogIndex %d\n", prevLogIndex, firstLogIndex)
	// 如果 prevLogIndex < firstLogIndex, 则内存中的日志已经被截断了，Follower的状态过于落后，
	// 需要发送 InstallSnapshot
	if prevLogIndex < firstLogIndex {
		// send installsnapshot
		args := rf.MakeInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(peer, args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.voteFor = -1
				rf.persist()
				return
			}
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		return
	}
	args := rf.MakeAppendEntriesArgs(prevLogIndex)
	rf.mu.RUnlock()
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, args, &reply) {
		rf.mu.Lock()
		// 先检查自己还是不是Leader
		if args.Term == rf.currentTerm && rf.state == Leader {
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.voteFor = -1
					rf.persist()
				} else if reply.Term == rf.currentTerm {
					if BackupQuick {
						if reply.XTerm == -1 {
							// XTerm == -1 表示没有找到匹配的日志
							// XLen有两种情况：follower的lastindex + 1，表明follower的日志太短，Leader需要更新nextIndex
							// 或者是follower的firstLog，表明follower的日志比Leader长，下次
							rf.nextIndex[peer] = Max(1, reply.XLen) // 确保 `nextIndex` 至少是 1
						} else {
							// XTerm != -1 表示找到了匹配的日志，但是term不匹配
							// 需要找到这个term的最后一个日志
							flag, last := rf.findLastLogIndexByTerm(reply.XTerm)
							if flag {
								// Leader含有XTerm的日志，从XTerm之后的日志开始覆盖
								// 在下次发送AppendLogEntiresRPC时，会接着检查prevLogIndex和prevLogTerm是否是XTerm
								// 如果是，则从XTerm之后的日志开始覆盖
								// 如果不是，则会接着回退，不用担心XTerm中的日志有冲突的问题。
								rf.nextIndex[peer] = last + 1
							} else {
								// Leader不含XTerm的日志，将整个XTerm的日志都覆盖
								rf.nextIndex[peer] = Max(1, reply.XIndex)
							}

						}
					} else {
						rf.nextIndex[peer] -= 1
					}
				}
			} else {
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
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

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	//DPrintf("Node %d: Start election for term %d\n", rf.me, rf.currentTerm+1)
	rf.becomeCandidate()
	rf.persist()
	rargs := rf.MakeRequestVoteArgs()
	rf.mu.Unlock()

	//REVIEW - wg的作用
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
					rf.becomeFollower(reply.Term)
					rf.voteFor = -1
					rf.persist()
					return
				}

				if rf.state != Candidate || rf.currentTerm != rargs.Term {
					return
				}

				// 检查投票结果
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
					if atomic.LoadInt32(&votes) > int32(voteThreshold) {
						// 再次检查状态和任期
						if rf.state == Candidate && rf.currentTerm == rargs.Term {
							rf.becomeLeader()
							rf.persist()
							rf.SendHeartbeats()
							DPrintf("Node %d: Became leader for term %d\n", rf.me, rf.currentTerm)
						}
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
				rf.becomeFollower(rf.currentTerm)
				rf.persist()
			}
			rf.mu.Unlock()
		case <-done:
			rf.mu.Lock()
			if rf.state == Candidate && atomic.LoadInt32(&votes) <= int32(voteThreshold) {
				//DPrintf("7. Node %d: Failed to become leader for term %d\n", rf.me, rf.currentTerm)
				rf.becomeFollower(rf.currentTerm)
				rf.persist()
				rf.resetElectionTimer()
			}
			rf.mu.Unlock()
		}
	}()
}
