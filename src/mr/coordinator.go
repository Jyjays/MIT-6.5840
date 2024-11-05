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

// state_lock is used to protect the task state
var state_lock sync.Mutex

type Task struct {
	TaskType Identity
	Files    []string
	Nreduce  int
	IsEmpty  bool
}

type WorkerList struct {
	last_assign     int
	workerId        []int
	work_task_table map[int][]int
}

var workerList WorkerList

func (w *WorkerList) getWorker() int {
	l := len(w.workerId)
	w.last_assign = (w.last_assign + 1) % l
	return w.workerId[w.last_assign]
}

func (w *WorkerList) addTask(workerId int, taskId int) {
	w.work_task_table[workerId] = append(w.work_task_table[workerId], taskId)
}

type Coordinator struct {
	mapSize              int
	reduceSize           int
	mapAccessalbeSize    int
	reduceAccessableSize int
	state                []State
	identity             []Identity
	locations            []string
	tasktable            []Task
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

func (c *Coordinator) MonitorHeartbeats() {

}

func (c *Coordinator) getWorkableWorker() int {
	for i, state := range c.state {
		if (state == Free && c.tasktable[i].IsEmpty) || state == Finish {
			return i
		}
	}
	return -1
}
func (c *Coordinator) AddWorker(args *TaskArgs, reply *TaskReply) error {
	newid := len(workerList.workerId)
	workerList.workerId = append(workerList.workerId, newid)
	reply.WorkerId = newid
	workerList.work_task_table[args.WorkerId] = []int{}
	return nil
}

func (c *Coordinator) getFreeWorker() int {
	for i, state := range c.state {
		if state == Free {
			return i
		}
	}
	return -1
}

func (c *Coordinator) makeTask(workerType Identity, files []string, nreduce int) Task {
	task := Task{IsEmpty: true}
	task.TaskType = workerType
	task.Files = files
	task.Nreduce = nreduce
	return task
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

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	if c.mapAccessalbeSize > 0 {
		reply.Nreduce = c.reduceSize
		reply.Files = c.locations[c.mapSize-c.mapAccessalbeSize : c.mapSize-c.mapAccessalbeSize+1]
		reply.TaskType = MAP
		reply.TaskId = c.mapSize - c.mapAccessalbeSize

		c.state[c.mapSize-c.mapAccessalbeSize] = Working
		c.identity[c.mapSize-c.mapAccessalbeSize] = MAP

		c.mapAccessalbeSize--
		c.tasktable[reply.TaskId] = c.makeTask(MAP, reply.Files, c.reduceSize)
		//workerList.addTask(args.WorkerId, reply.TaskId)
	} else if c.reduceAccessableSize > 0 {
		ok := c.WaitTask(MAP)
		if !ok {
			reply.TaskType = NONE
			return nil
		}
		reply.Nreduce = len(c.locations)
		reply.Files = []string{}
		reply.TaskType = REDUCE
		//reply.TaskSize = len(c.locations)
		reply.TaskId = c.reduceSize - c.reduceAccessableSize + c.mapSize

		//fmt.Println("machine: ", c.mapSize+c.reduceSize-c.reduceAccessableSize)
		c.state[c.mapSize+c.reduceSize-c.reduceAccessableSize] = Working
		c.identity[c.mapSize+c.reduceSize-c.reduceAccessableSize] = REDUCE

		c.reduceAccessableSize--
		c.tasktable[reply.TaskId] = c.makeTask(REDUCE, reply.Files, c.reduceSize)
	} else {
		c.WaitTask(REDUCE)
		reply.TaskType = NONE
	}
	workerList.addTask(args.WorkerId, reply.TaskId)
	return nil
}
func (c *Coordinator) UpdateWorker(args *struct{}, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()

	return nil
}
func (c *Coordinator) FinishTask(args *TaskArgs, reply *TaskReply) error {
	state_lock.Lock()
	defer state_lock.Unlock()
	c.state[args.TaskId] = Finish
	// workerList.work_task_table[args.WorkerId] = workerList.work_task_table[args.WorkerId][1:]
	reply.TaskType = NONE
	return nil
}

func (c *Coordinator) HeartBeat(args *TaskArgs, reply *TaskReply) error {
	c.heartbeatTable.mu.Lock()
	defer c.heartbeatTable.mu.Unlock()
	c.heartbeatTable.heartbeats[args.WorkerId] = time.Now()
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
	c.identity = make([]Identity, worker_size)
	c.tasktable = make([]Task, worker_size)
	for i := 0; i < worker_size; i++ {
		c.state[i] = Free
		c.identity[i] = NONE
	}

	c.locations = file_names

	c.heartbeatTable = NewWorkerHeartbeat(20 * time.Second)
	//go c.MonitorHeartbeats()
	workerList = *makeWorkerList()

	c.server()
	return &c
}
