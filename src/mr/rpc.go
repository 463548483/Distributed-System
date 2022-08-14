package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskStatus int
type TaskType int
type JobStage int

const(
	MapTask TaskType=iota
	ReduceTask
	NoTask
	ExitTask
)

const(
	MAP JobStage=iota
	REDUCE
)

type Task struct{
	Type TaskType
	Index int
	File string
	WorkerId int
	StartTime time.Time
}

type ApplyForTaskArgs struct {
	WorkerId int
	LastTaskId int
	LastTaskType TaskType
}

type ApplyForTaskReply struct {
	AssignTask Task 
	nMap int
	nReduce int
}

// Add your RPC definitions here.
func tmpMapOutFile(workerId int, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerId, mapId, reduceId)
}

func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprint("mr-out-%d", reduceId)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
