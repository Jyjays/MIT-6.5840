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

type WorkerList struct {
	workerId        []int
	workerState     []State
	work_task_table map[int][]int
}

func (w *WorkerList) addTask(workerId int, taskId int) {
	w.work_task_table[workerId] = append(w.work_task_table[workerId], taskId)
	w.workerState[workerId] = Working
}

var is_done = false

type Coordinator struct {
	lock                 sync.Mutex
	mapSize              int
	reduceSize           int
	mapAccessalbeSize    int
	reduceAccessableSize int
	state_lock           sync.Mutex
	state                []State
	identity             []Identity
	locations            []string
	heartbeatTable       *WorkerHeartbeat
	workerList           WorkerList
	//intermediate         []KeyValue
}

type WorkerHeartbeat struct {
	mu         sync.Mutex
	heartbeats map[int]time.Time // TaskId -> last heartbeat timestamp
	timeout    time.Duration
}

func NewWorkerHeartbeat(timeout time.Duration) *WorkerHeartbeat {
	return &WorkerHeartbeat{
		heartbeats: make(map[int]time.Time),
		timeout:    timeout,
	}
}

func (c *Coordinator) HeartBeat(args *TaskArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.heartbeatTable.mu.Lock()
	defer c.heartbeatTable.mu.Unlock()
	for taskId := range c.workerList.work_task_table[args.WorkerId] {
		if c.state[taskId] != Finish {
			c.heartbeatTable.heartbeats[taskId] = time.Now()
		}
	}
	return nil
}

func (c *Coordinator) MonitorHeartbeats() {
	for {
		time.Sleep(15 * time.Second)
		c.heartbeatTable.mu.Lock()
		for taskId, lastHeartbeat := range c.heartbeatTable.heartbeats {
			if time.Since(lastHeartbeat) > c.heartbeatTable.timeout {
				c.state_lock.Lock()
				if c.state[taskId] != Finish {
					c.state[taskId] = Free
				}
				c.state_lock.Unlock()
			}
		}
		c.heartbeatTable.mu.Unlock()
	}
}

func (c *Coordinator) AddWorker(args *TaskArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	newid := len(c.workerList.workerId)
	c.workerList.workerId = append(c.workerList.workerId, newid)
	reply.WorkerId = newid
	c.workerList.workerState = append(c.workerList.workerState, Free)
	c.workerList.work_task_table[args.WorkerId] = []int{}
	return nil
}

// Wait all the type of task to be finished
func (c *Coordinator) WaitTask(taskType Identity) bool {
	all_finished := false
	for i, identity := range c.identity {
		if identity == taskType {
			for c.state[i] != Finish {
				time.Sleep(1 * time.Second)
			}
		}
		all_finished = true
	}
	return all_finished
}

func (c *Coordinator) getAccessiableWorker(taskType Identity) int {
	c.state_lock.Lock()
	defer c.state_lock.Unlock()
	for i, state := range c.state {
		if state == Free && c.identity[i] == taskType {
			return i
		}
	}
	return -1
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	if is_done {
		reply.TaskType = NONE
		reply.WorkerState = Finish
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.mapAccessalbeSize > 0 {
		reply.Nreduce = c.reduceSize
		reply.TaskId = c.getAccessiableWorker(MAP)
		if reply.TaskId == -1 {
			reply.TaskType = NONE
			return nil
		}
		reply.Files = c.locations[reply.TaskId : reply.TaskId+1]
		reply.TaskType = MAP
		{
			c.state_lock.Lock()
			defer c.state_lock.Unlock()
			c.state[reply.TaskId] = Working
		}
		c.identity[reply.TaskId] = MAP
		c.mapAccessalbeSize--
		//workerList.addTask(args.WorkerId, reply.TaskId)
	} else if c.reduceAccessableSize > 0 {
		ok := c.WaitTask(MAP)
		if !ok {
			reply.TaskType = NONE
			return nil
		}
		reply.TaskId = c.getAccessiableWorker(REDUCE)
		if reply.TaskId == -1 {
			reply.TaskType = NONE
			return nil
		}
		reply.Nreduce = len(c.locations)
		reply.Files = []string{}
		reply.TaskType = REDUCE
		{
			c.state_lock.Lock()
			defer c.state_lock.Unlock()
			c.state[reply.TaskId] = Working
		}
		c.identity[reply.TaskId] = REDUCE

		c.reduceAccessableSize--
	} else {
		c.WaitTask(REDUCE)
		reply.TaskType = NONE
		return nil
	}
	c.workerList.addTask(args.WorkerId, reply.TaskId)
	return nil
}

func (c *Coordinator) FinishTask(args *TaskArgs, reply *TaskReply) error {
	c.state_lock.Lock()
	defer c.state_lock.Unlock()
	c.state[args.TaskId] = Finish
	// workerList.work_task_table[args.WorkerId] = workerList.work_task_table[args.WorkerId][1:]
	//reply.TaskType = NONE
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
	c.state_lock.Lock()
	defer c.state_lock.Unlock()
	for _, state := range c.state {
		if state != Finish {
			is_done = false
			return is_done
		}
	}
	is_done = true
	return is_done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func makeWorkerList() *WorkerList {
	w := WorkerList{}
	w.workerId = []int{}
	w.work_task_table = make(map[int][]int)
	return &w
}

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
	// The fist M workers are map workers, the rest are reduce workers
	c.identity = make([]Identity, worker_size)
	for i := 0; i < M; i++ {
		c.identity[i] = MAP
	}
	for i := M; i < worker_size; i++ {
		c.identity[i] = REDUCE
	}
	c.locations = file_names
	c.heartbeatTable = NewWorkerHeartbeat(10 * time.Second)
	c.workerList = *makeWorkerList()
	go c.MonitorHeartbeats()
	c.server()
	return &c
}
