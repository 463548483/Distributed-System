package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)

	var lastTaskType TaskType
	var lastTaskIndex int
	for {
		args := ApplyForTaskArgs {
			WorkerId: id,
			LastTaskType: lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}
		reply := ApplyForTaskReply{}
		call("Master apply for task", &args, &reply)//ToDo update call func
		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf. response)
		case ReduceTask:
			doReduceTask(reducef, response)
		case NoTask:
			time.Sleep(1 * time.Second)
		case ExitTask:
			return
		default:
			panic(fmt.Sprintf("unexpected jobtype %v", response.TaskType))
		}
		lastTaskIndex = reply.AssignTask.Index
		lastTaskType = reply.AssignTask.TaskType
		log.Printf("finish %s task %d", lastTaskType, lastTaskIndex)

	}
	log.Printf("Worker %d finish and exit", id)
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func doMapTask(id int, taskId int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Fail to open file %s", fileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Fail to read file %s", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hashed := ihash(kv.Key) % nReduce
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}

	for i := 0; i < nReduce; i++ {
		outFile, _ := os.Create(tmpMapOutFile(id, taskId, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}
}

func doReduceTask(id int, taskId int, nMap int, reducef func(string, []string) string) {
	var lines []string
	// combine the files from different map task, nMap??
	for i := 0; i < nMap; i++ {
		file, err := os.Open(finalMapOutFile(i, taskId))
		if err != nil {
			log.Fatalf("Fail to open file", finalMapOutFile(i, taskId))
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Fail to read file", finalMapOutFile(i, taskId))
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	} 
	
	//convert file output to hashmap
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key: split[0],
			Value: split[1],
		})
	}

	sort.Sort(ByKey(kva))

	outFile, _ := os.Create(tmpReduceOutFile(id, taskId))

	//combine values of same key, send to reducef
	i:= 0 
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
