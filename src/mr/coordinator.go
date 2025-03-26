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
	workerId        []int         // workerId
	workerState     []State       // worker状态
	work_task_table map[int][]int // workerId -> taskId
}

func (w *WorkerList) addTask(workerId int, taskId int) {
	w.work_task_table[workerId] = append(w.work_task_table[workerId], taskId)
	w.workerState[workerId] = Working
}

var is_done = false

type Coordinator struct {
	lock                 sync.Mutex       // Coordinator临界区锁
	mapSize              int              // map任务个数
	reduceSize           int              // reduce任务个数
	mapAccessableSize    int              // 可分配的map任务个数
	reduceAccessableSize int              // 可分配的reduce任务个数
	state_lock           sync.Mutex       // 任务状态锁
	state                []State          // 任务状态
	identity             []Identity       // 任务类型
	locations            []string         // 文件名列表
	heartbeatTable       *WorkerHeartbeat // 心跳表
	workerList           WorkerList       // worker列表
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
	if is_done {
		reply.WorkerState = Finish
		return nil
	}
	if c.workerList.workerState[args.WorkerId] == Fail {
		return nil
	}
	//fmt.Printf("workId %d:", args.WorkerId)
	for _, taskId := range c.workerList.work_task_table[args.WorkerId] {
		if c.state[taskId] != Finish {
			//fmt.Print(taskId)
			//fmt.Print(" ")
			lastHeartbeat := c.heartbeatTable.heartbeats[taskId]
			if time.Since(lastHeartbeat) > c.heartbeatTable.timeout {
				c.workerList.workerState[args.WorkerId] = Fail
				reply.WorkerState = Fail
				break
			}
			c.heartbeatTable.heartbeats[taskId] = time.Now()
		}
	}
	//fmt.Println(" ")
	return nil
}

func (c *Coordinator) MonitorHeartbeats() {
	for {
		time.Sleep(5 * time.Second)
		c.heartbeatTable.mu.Lock()
		for taskId, lastHeartbeat := range c.heartbeatTable.heartbeats {
			if time.Since(lastHeartbeat) > c.heartbeatTable.timeout { // Heartbeat timeout
				c.state_lock.Lock()
				if c.state[taskId] != Finish && c.state[taskId] != Free {
					c.state[taskId] = Free
					switch c.identity[taskId] {
					case MAP:
						c.mapAccessableSize++
					case REDUCE:
						c.reduceAccessableSize++
					}
				}
				c.state_lock.Unlock()
			}
		}
		c.heartbeatTable.mu.Unlock()
	}
}

// 添加worker配置
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
// if the task is not finished, return false
func (c *Coordinator) WaitTask(taskType Identity) bool {

	for i, identity := range c.identity {
		// if c.state[i] == Free && identity == taskType {
		// 	return false
		// }
		if identity == taskType {
			// for c.state[i] != Finish {
			// 	time.Sleep(1 * time.Second)
			// }
			if c.state[i] != Finish {
				return false
			}
		}
	}
	return true
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

	if c.workerList.workerState[args.WorkerId] == Fail {
		reply.TaskType = NONE
		reply.WorkerState = Fail
		return nil
	}
	if c.mapAccessableSize > 0 {
		reply.Nreduce = c.reduceSize
		reply.TaskId = c.getAccessiableWorker(MAP)
		if reply.TaskId == -1 {
			reply.TaskType = NONE
			return nil
		}
		reply.Files = c.locations[reply.TaskId : reply.TaskId+1]
		reply.TaskType = MAP
		reply.WorkerId = args.WorkerId
		{
			c.state_lock.Lock()
			defer c.state_lock.Unlock()
			c.state[reply.TaskId] = Working
		}
		c.identity[reply.TaskId] = MAP
		//fmt.Printf("MapTaskId: %d added\n", reply.TaskId)
		c.heartbeatTable.heartbeats[reply.TaskId] = time.Now()
		c.mapAccessableSize--
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
		reply.WorkerId = args.WorkerId
		{
			c.state_lock.Lock()
			defer c.state_lock.Unlock()
			c.state[reply.TaskId] = Working
		}
		c.identity[reply.TaskId] = REDUCE
		c.heartbeatTable.heartbeats[reply.TaskId] = time.Now()
		//fmt.Printf("ReduceTaskId: %d added\n", reply.TaskId)
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
	if c.state[args.TaskId] != Free {
		c.state[args.TaskId] = Finish
		reply.WorkerState = Finish
	} else {
		reply.WorkerState = Fail // 如果state[args.TaskId] == Free, 说明worker已经被kill，原先的任务作废
	}
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
	fmt.Printf("All tasks are finished\n")

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
	c.mapSize, c.mapAccessableSize = M, M
	c.reduceSize, c.reduceAccessableSize = nReduce, nReduce
	worker_size := M + nReduce
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
