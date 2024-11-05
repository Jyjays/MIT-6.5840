package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	heartbeatArgs := TaskArgs{}
	heartbeatReply := TaskReply{}
	call("Coordinator.AddWorker", &heartbeatArgs, &heartbeatReply)
	heartbeatArgs.WorkerId = heartbeatReply.WorkerId
	heartbeatReply = TaskReply{}
	// Your worker implementation here.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:

				ok := call("Coordinator.HeartBeat", &heartbeatArgs, &heartbeatReply)
				if !ok {
					return
				}
				time.Sleep(10 * time.Second)
			}
		}
	}()

	for {
		args := TaskArgs{WorkerId: heartbeatArgs.WorkerId}
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}
		switch reply.TaskType {
		case MAP:
			go single_thread_map(mapf, &reply)
		case REDUCE:
			go single_thread_reduce(reducef, &reply)
		case NONE:
			// Continue call the coordinator to get the task in case of any worker is down
			continue
		}
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func single_thread_map(mapf func(string, string) []KeyValue, reply *TaskReply) {
	intermediate := []KeyValue{}
	for _, filename := range reply.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	for _, kv := range intermediate {
		reduceTask := ihash(kv.Key) % reply.Nreduce
		filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, reduceTask)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			file, err = os.Create(filename)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
		file.Close()
	}
	args := TaskArgs{TaskId: reply.TaskId, TaskType: MAP, WorkerId: reply.WorkerId}
	re := &TaskReply{}
	for {
		fmt.Printf("Task%d finished\n", reply.TaskId)
		ok := call("Coordinator.FinishTask", &args, re)
		time.Sleep(3 * time.Second)
		if ok && re.TaskType == NONE {

			break
		}
	}

}

func single_thread_reduce(reducef func(string, []string) string, reply *TaskReply) {
	// Firstly, Sort the intermediate values by key
	// Then, call reducef
	// intermediate file name format : mr-TaskId-ReduceTaskId
	// output file name format : mr-out-ReduceTaskId
	//intermediate := []KeyValue{}
	//filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, reply.ReduceTaskId)

	inter_file_id := reply.TaskId - reply.Nreduce
	inter_files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", inter_file_id))
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskId)
	}
	intermediate := []KeyValue{}
	for _, filename := range inter_files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", inter_file_id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	args := TaskArgs{TaskId: reply.TaskId, TaskType: REDUCE, WorkerId: reply.WorkerId}
	re := &TaskReply{}
	for {
		fmt.Printf("Task %d finished\n", reply.TaskId)

		ok := call("Coordinator.FinishTask", &args, re)
		time.Sleep(2 * time.Second)
		if ok && re.TaskType == NONE {
			break
		}
	}
}
