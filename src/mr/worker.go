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

	// Your worker implementation here.
	for {
		args := struct{}{}
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}
		switch reply.TaskType {
		case MAP:
			go single_thread_map(mapf, &reply)
			//time.Sleep(1 * time.Second)
		case REDUCE:
			go single_thread_reduce(reducef, &reply)
			//time.Sleep(1 * time.Second)
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
	go func() {
		for {
			args := TaskArgs{TaskId: reply.TaskId, TaskType: MAP}

			ok := call("Coordinator.Heartbeat", &args, &struct{}{})
			if !ok {
				break
			}
			time.Sleep(10 * time.Second)
		}
	}()
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
	// write intermediate to file
	//args := TaskArgs{}
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
	call("Coordinator.FinishTask", reply, &TaskReply{})
}

func single_thread_reduce(reducef func(string, []string) string, reply *TaskReply) {
	// Firstly, Sort the intermediate values by key
	// Then, call reducef
	// intermediate file name format : mr-TaskId-ReduceTaskId
	// output file name format : mr-out-ReduceTaskId
	//intermediate := []KeyValue{}
	//filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, reply.ReduceTaskId)
	go func() {
		for {
			args := TaskArgs{TaskId: reply.TaskId, TaskType: REDUCE}
			ok := call("Coordinator.HeartBeat", &args, &struct{}{})
			if !ok {
				break
			}
			time.Sleep(10 * time.Second)
		}
	}()

	inter_files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", reply.TaskId))
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
	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
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
	call("Coordinator.FinishTask", reply, &TaskReply{})
}
