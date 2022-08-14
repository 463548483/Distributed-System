package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TempDir="tmp"
const TaskTimeout = 10 

type Master struct {
	// Your definitions here.
	mu sync.Mutex
	nMap int
	nReduce int
	stage JobStage
	tasks map[string]Task
	toDoTasks chan Task
}

func MakeMaster(files []string, nReduce int) *Master {
	c := Master{
		nMap: len(files),
		nReduce: nReduce,
		tasks: make(map[string]Task),
		toDoTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	for i, file := range(files) {
		task := Task{
			Index: i,
			Type: MapTask,
			WorkerId: -1,
			File: file,
		}
		c.tasks[createTaskId(task.Type, task.Index)] = task
		//todo channel
	}
	log.Printf("Master start\n")
	c.server()

	//travel all the tasks, if time out, reassign task
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.mu.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != -1 && time.Now().Sub(task.StartTime) > TaskTimeout {
					task.WorkerId = -1
					c.toDoTasks <- task
				}
			}
		}
		c.mu.Unlock()
	} ()
	return &c
}

func (c * Master) ApplyForTask(args * ApplyForTaskArgs, reply * ApplyForTaskReply) error {
	//deal with last task completion
	if args.LastTaskId != -1 {
		c.recordLastTask(args)
	}

	//assign new task
	task, ok := <-c.toDoTasks
	if !ok {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("Assign %s task %d to worker %d\n", task.Type, task.Index, args.WorkerId)
	task.WorkerId = args.WorkerId
	task.StartTime = time.Now()

	reply.AssignTask = task
	reply.nMap = c.nMap
	reply.nReduce = c.nReduce
	return nil
}

func createTaskId(taskType TaskType, id int) string {
	return fmt.Sprintf("mr-%d-%d", taskType, id)
}

func (c * Master) toNext() {
	if c.stage == MAP {
		log.Printf("Map tasks completed, start Reduce")
		//create and assign reduce task
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task{Index: i, Type: ReduceTask, WorkerId: -1}
			c.tasks[createTaskId(task.Type, i)] = task
			c.toDoTasks <- task
		}
	} else if c.stage == REDUCE {
		//inform worker to exit
		log.Printf("Reduce tasks complete, all done")
		close(c.toDoTasks)
	}


}

func (c * Master) recordLastTask(args * ApplyForTaskArgs) {
	c.mu.Lock()
	taskId := createTaskId(args.LastTaskType, args.LastTaskId)
	if task, ok := c.tasks[taskId]; ok && task.WorkerId == args.WorkerId {
		log.Printf("%d worker finished %s-%d task", args.WorkerId, args.LastTaskType, args.LastTaskId)
		if args.LastTaskType == MapTask {
			for i := 0; i < c.nReduce; i++ {
				err := os.Rename(
					tmpMapOutFile(args.WorkerId, args.LastTaskType, args.LastTaskId),
					finalMapOutFile(args.LastTaskId, i))
				if err != nil {
					log.Fatalf("Failed to mark Map output file `%s` as final: %e",
					tmpMapOutFile(args.WorkerId, args.LastTaskId), err)
				}
			}
		} else if args.LastTaskType == ReduceTask {
			err := os.Rename(
				tmpReduceOutFile(args.WorkerId, args.LastTaskType, args.LastTaskId),
				finalReduceOutFile(args.LastTaskId, i))
			if err != nil {
				log.Fatalf("Failed to mark Map output file `%s` as final: %e",
				tmpReduceOutFile(args.WorkerId, args.LastTaskId), err)
			}
		}
		delete(c.tasks, taskId)
		if len(c.tasks) == 0 {
			c.toNext()
		}
	}
	c.mu.Unlock()
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nMap==0&&m.nReduce==0
}
