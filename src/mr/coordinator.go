package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

type State int
type Identity int

const (
	Free State = iota
	Working
	Finish
)
const (
	MAP Identity = iota
	REDUCE
)

var lock sync.RWMutex

type Coordinator struct {
	mapSize              int
	reduceSize           int
	mapAccessalbeSize    int
	reduceAccessableSize int
	state                []State
	identity             []Identity
	locations            []string
	//intermediate         []KeyValue
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	lock.RLock()
	defer lock.RUnlock()
	if c.mapAccessalbeSize > 0 {
		reply.Nreduce = c.reduceSize
		reply.Files = c.locations[c.mapSize-c.mapAccessalbeSize : c.mapSize-c.mapAccessalbeSize+1]
		reply.TaskType = MAP
		reply.TaskId = c.mapSize - c.mapAccessalbeSize

		c.mapAccessalbeSize--
		c.state[c.mapSize-c.mapAccessalbeSize] = Working
		c.identity[c.mapSize-c.mapAccessalbeSize] = MAP
		return nil
	}
	return nil
}

func (c *Coordinator) FinishTask(args *TaskArgs, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	if c.identity[args.TaskId] == MAP {
		c.state[args.TaskId] = Finish
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
	ret := false

	// Your code here.

	return ret
}

func GetM_andSplits(file []string, blocksizeMB int) (int, [][]string) {
	blocksize := blocksizeMB * 1024 * 1024
	blocks := [][]string{}
	currentblock := []string{}
	totalsize := 0
	currentsize := 0
	for _, content := range file {
		totalsize += len(content)
		contentsize := len(content)
		if currentsize+contentsize > blocksize {
			blocks = append(blocks, currentblock)
			currentblock = []string{}
			currentsize = 0
		}
		currentblock = append(currentblock, content)
		currentsize += contentsize
	}
	if len(currentblock) > 0 {
		blocks = append(blocks, currentblock)
	}
	return len(blocks), blocks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// TODO: Read the files and get splits

	// In this lab the file is input in the form of pg-*, which is already split
	// So we can just use the length of the files
	//file_names := []string{}
	// files : pg-*.txt, so we should get all the files

	file_names, err := filepath.Glob(files[0])
	if err != nil {
		log.Fatal("cannot find files")
	}
	M := len(file_names)
	c.mapSize, c.mapAccessalbeSize = M, M
	c.reduceSize, c.reduceAccessableSize = nReduce, nReduce
	c.state = make([]State, M+nReduce)
	c.identity = make([]Identity, M+nReduce)
	c.locations = file_names

	c.server()
	return &c
}
