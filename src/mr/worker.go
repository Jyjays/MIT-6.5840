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
	"strings"
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

func heartbeat(heartbeatArgs *TaskArgs, heartbeatReply *TaskReply, file_name_list *[]string) {
	ok := call("Coordinator.HeartBeat", &heartbeatArgs, &heartbeatReply)
	if !ok {
		return
	}
	if heartbeatReply.WorkerState == Fail || heartbeatReply.WorkerState == Finish {
		//
		for _, filename := range *file_name_list {
			// remove the file
			os.Remove(filename)
		}
		os.Exit(1)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	heartbeatArgs := TaskArgs{}
	heartbeatReply := TaskReply{}
	call("Coordinator.AddWorker", &heartbeatArgs, &heartbeatReply)
	heartbeatArgs.WorkerId = heartbeatReply.WorkerId
	heartbeatReply = TaskReply{}
	for {
		args := TaskArgs{WorkerId: heartbeatArgs.WorkerId}
		reply := TaskReply{}
		file_name_list := []string{}
		heartbeat(&heartbeatArgs, &heartbeatReply, &file_name_list)
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}
		switch reply.TaskType {
		case MAP:
			// go single_thread_map(mapf, &reply)
			// Create a file_name_list pointer to store the file name
			single_thread_map(mapf, &reply, &file_name_list)
		case REDUCE:
			// go single_thread_reduce(reducef, &reply)
			file_name_list := []string{}
			//file_name_list := []string{}
			single_thread_reduce(reducef, &reply, &file_name_list)
		case NONE:
			// Continue call the coordinator to get the task in case of any worker is down
			heartbeat(&heartbeatArgs, &heartbeatReply, &file_name_list)
			//heartbeat(&heartbeatArgs, &heartbeatReply, &file_name_list)
			if heartbeatReply.WorkerState == Fail || heartbeatReply.WorkerState == Finish {
				break
			}
			continue
			// break
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

func single_thread_map(mapf func(string, string) []KeyValue, reply *TaskReply, file_name_list *[]string) {
	fmt.Printf("Map TaskId: %d, Worker: %d\n", reply.TaskId, reply.WorkerId)
	delete_tmp_file(reply.TaskId, MAP)
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
		// Temporarily name
		filename := fmt.Sprintf("mr-inter-%d-%d-%d", reply.TaskId, reduceTask, reply.WorkerId)
		_, err := os.Stat(filename)
		fileAlreadyExists := !os.IsNotExist(err)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open or create file %v: %v", filename, err)
		}
		if !fileAlreadyExists {
			*file_name_list = append(*file_name_list, filename)
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
		//fmt.Printf("Task%d Ok\n", reply.TaskId)
		heartbeat(&args, re, file_name_list)
		for _, filename := range *file_name_list {
			lastDashIndex := strings.LastIndex(filename, "-")
			if lastDashIndex != -1 {
				newFilename := filename[:lastDashIndex]
				os.Rename(filename, newFilename)
			}
		}

		*file_name_list = []string{}
		ok := call("Coordinator.FinishTask", &args, re)
		if re.WorkerState == Fail {
			os.Exit(1) // exit if the worker is failed
		} else if ok && re.WorkerState == Finish {
			break
		}

	}
}

func single_thread_reduce(reducef func(string, []string) string, reply *TaskReply, file_name_list *[]string) {
	fmt.Printf("Reduce TaskId: %d, Worker: %d\n", reply.TaskId, reply.WorkerId)
	delete_tmp_file(reply.TaskId, REDUCE)
	inter_file_id := reply.TaskId - reply.Nreduce
	inter_files, err := filepath.Glob(fmt.Sprintf("mr-inter-*-%d", inter_file_id))
	//fmt.Printf("inter_files: %v\n", inter_files)
	if err != nil {
		log.Fatalf("reduce :cannot read %v", reply.TaskId)
	}

	intermediate := []KeyValue{}
	for _, filename := range inter_files {
		//time.Sleep(100 * time.Millisecond)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		//REVIEW - why is this an infinite loop?
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
	oname := fmt.Sprintf("mr-out-%d-%d", inter_file_id, reply.WorkerId)
	ofile, _ := os.Create(oname)
	*file_name_list = append(*file_name_list, oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 计算一个Key的所有Value
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 这里的values是一个slice，存储了所有的Value
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 将一个Key的所有Value传入reduce函数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	args := TaskArgs{TaskId: reply.TaskId, TaskType: REDUCE, WorkerId: reply.WorkerId}
	re := &TaskReply{}
	for {
		//fmt.Printf("Task%d Ok\n", reply.TaskId)
		heartbeat(&args, re, file_name_list)
		// rename the file to mr-out-taskid
		out_name := fmt.Sprintf("mr-out-%d", inter_file_id)
		os.Rename(oname, out_name)
		*file_name_list = []string{}
		ok := call("Coordinator.FinishTask", &args, re)
		if re.WorkerState == Fail {
			os.Exit(1)
		} else if ok && re.WorkerState == Finish {
			break
		}

	}
}
func delete_tmp_file(taskId int, tasktype Identity) {
	switch tasktype {
	case MAP:
		inter_files, err := filepath.Glob(fmt.Sprintf("mr-inter-%d-*-*", taskId))
		if err != nil {
			log.Fatalf("map: cannot read %v", taskId)
		}
		for _, filename := range inter_files {
			os.Remove(filename)
		}
	case REDUCE:
		oname := fmt.Sprintf("mr-out-%d-*", taskId)
		os.Remove(oname)
	}
}
