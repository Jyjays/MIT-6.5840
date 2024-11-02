package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type State int
type Identity int

const (
	Free State = iota
	Working
	Finish
	Fail
)
const (
	MAP Identity = iota
	REDUCE
	NONE
)

var lock sync.RWMutex

type Task struct {
	TaskType Identity
	Files    []string
	Nreduce  int
}
type Coordinator struct {
	mapSize              int
	reduceSize           int
	mapAccessalbeSize    int
	reduceAccessableSize int
	state                []State
	identity             []Identity
	locations            []string
	tasktable            map[int]Task
	heartbeatTable       *WorkerHeartbeat

	//intermediate         []KeyValue
}

type WorkerHeartbeat struct {
	mu         sync.Mutex
	heartbeats map[int]time.Time // workerID -> last heartbeat timestamp
	timeout    time.Duration
}

func NewWorkerHeartbeat(timeout time.Duration) *WorkerHeartbeat {
	return &WorkerHeartbeat{
		heartbeats: make(map[int]time.Time),
		timeout:    timeout,
	}
}
func (c *Coordinator) UpdateHeartbeat(workerID int) {
	c.heartbeatTable.mu.Lock()
	defer c.heartbeatTable.mu.Unlock()
	c.heartbeatTable.heartbeats[workerID] = time.Now()
}
func (c *Coordinator) MonitorHeartbeats() {
	for {
		time.Sleep(15 * time.Second)
		c.heartbeatTable.mu.Lock()
		for workerID, lastBeat := range c.heartbeatTable.heartbeats {
			if time.Since(lastBeat) > c.heartbeatTable.timeout {
				fmt.Printf("Worker %d is considered failed (last heartbeat at %v)\n", workerID, lastBeat)
				// 这里可以添加标记失效的逻辑，或从表中移除该Worker
				delete(c.heartbeatTable.heartbeats, workerID) // 可选：移除超时的Worker
			}
		}
		c.heartbeatTable.mu.Unlock()
	}
}

func (c *Coordinator) getFreeWorker(workerType Identity) int {
	for i, state := range c.state {
		if state == Free && c.identity[i] == workerType {
			return i
		}
	}
	return -1
}

func (c *Coordinator) makeTask(workerType Identity, files []string, nreduce int) Task {
	task := Task{}
	task.TaskType = workerType
	task.Files = files
	task.Nreduce = nreduce
	return task
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	lock.RLock()
	defer lock.RUnlock()
	if c.mapAccessalbeSize > 0 {
		reply.Nreduce = c.reduceSize
		reply.Files = c.locations[c.mapSize-c.mapAccessalbeSize : c.mapSize-c.mapAccessalbeSize+1]
		reply.TaskType = MAP
		reply.TaskId = c.mapSize - c.mapAccessalbeSize

		c.state[c.mapSize-c.mapAccessalbeSize] = Working
		c.identity[c.mapSize-c.mapAccessalbeSize] = MAP

		c.mapAccessalbeSize--
		c.UpdateHeartbeat(reply.TaskId)
		c.tasktable[reply.TaskId] = c.makeTask(MAP, reply.Files, c.reduceSize)
	} else if c.reduceAccessableSize > 0 {
		reply.Nreduce = c.reduceSize
		reply.Files = []string{}
		reply.TaskType = REDUCE
		//reply.TaskSize = len(c.locations)
		reply.TaskId = c.reduceSize - c.reduceAccessableSize + c.mapSize

		//fmt.Println("machine: ", c.mapSize+c.reduceSize-c.reduceAccessableSize)
		c.state[c.mapSize+c.reduceSize-c.reduceAccessableSize] = Working
		c.identity[c.mapSize+c.reduceSize-c.reduceAccessableSize] = REDUCE

		c.reduceAccessableSize--
		c.UpdateHeartbeat(reply.TaskId)
		c.tasktable[reply.TaskId] = c.makeTask(REDUCE, reply.Files, c.reduceSize)
	} else {
		reply.TaskType = NONE
	}
	return nil
}

func (c *Coordinator) FinishTask(args *TaskArgs, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	c.state[args.TaskId] = Finish
	reply.TaskType = NONE
	return nil
}

func (c *Coordinator) HeartBeat(args *TaskArgs, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	if c.state[args.TaskId] == Fail {
		c.state[args.TaskId] = Working
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, state := range c.state {
		if state != Finish {
			return false
		}
	}
	return true
}

// func GetM_andSplits(file []string, blocksizeMB int) (int, [][]string) {
// 	blocksize := blocksizeMB * 1024 * 1024
// 	blocks := [][]string{}
// 	currentblock := []string{}
// 	totalsize := 0
// 	currentsize := 0
// 	for _, content := range file {
// 		totalsize += len(content)
// 		contentsize := len(content)
// 		if currentsize+contentsize > blocksize {
// 			blocks = append(blocks, currentblock)
// 			currentblock = []string{}
// 			currentsize = 0
// 		}
// 		currentblock = append(currentblock, content)
// 		currentsize += contentsize
// 	}
// 	if len(currentblock) > 0 {
// 		blocks = append(blocks, currentblock)
// 	}
// 	return len(blocks), blocks
// }

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// file_names, err := filepath.Glob(files[0]) // Expand the wildcard here
	// if err != nil || len(file_names) == 0 {
	// 	log.Fatal("cannot find matching files")
	// }
	file_names := []string{}
	for _, filename := range files {
		file, err := filepath.Glob(filename)
		if err != nil || len(file) == 0 {
			log.Fatal("cannot find matching files")
		}
		file_names = append(file_names, file...)
	}

	M := len(file_names)
	fmt.Println("file_names: ", file_names)
	c.mapSize, c.mapAccessalbeSize = M, M
	fmt.Println("mapsize, mapAccessableSize: ", c.mapSize, c.mapAccessalbeSize)
	c.reduceSize, c.reduceAccessableSize = nReduce, nReduce
	worker_size := M + nReduce
	fmt.Println("worker_size: ", worker_size)
	c.state = make([]State, worker_size)
	c.identity = make([]Identity, worker_size)
	c.locations = file_names

	c.heartbeatTable = NewWorkerHeartbeat(20 * time.Second)
	go c.MonitorHeartbeats()

	c.server()
	return &c
}
