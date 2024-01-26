package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	//"time"
)

type Coordinator struct {
	// Fields to keep track of the coordinator's state and tasks
	nReduce     int
	nMap        int
	mapFiles    []string
	mapTasks    []TaskStatus
	reduceTasks []TaskStatus
	mu          sync.Mutex
}

type TaskStatus struct {
	ID     int
	State  TaskState
	Worker int
	//Timeout time.Time
}

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

const (
	NoTask int = iota
	MapTask
	ReduceTask
)

var NewWorkerID = 1

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DispatchTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
dispatch:
	// Logic for assigning tasks to workers
	if c.allMapTasksCompleted() {
		if c.allReduceTasksCompleted() {
			// No more tasks to assign
			reply.TaskType = NoTask
		} else {
			// Assign a reduce task to a worker
			reduceTaskID := c.nextReduceTask()
			if reduceTaskID == -1 {
				time.Sleep(5 * time.Second)
				goto dispatch
			}
			reply.TaskType = ReduceTask
			reply.TaskID = reduceTaskID
			//reply.TaskFile = c.reduceFiles[reduceTaskID]
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			c.reduceTasks[reduceTaskID].State = InProgress
			if args.WorkerID == 0 {
				args.WorkerID = NewWorkerID
				NewWorkerID++
			}
			c.reduceTasks[reduceTaskID].Worker = args.WorkerID
			go c.CheckWorkIsCompleted(ReduceTask, reduceTaskID)
			//comment it after finishing the done function
		}
	} else {
		// Assign a map task to a worker
		mapTaskID := c.nextMapTask()
		if mapTaskID == -1 {
			time.Sleep(5 * time.Second)
			goto dispatch
		}
		reply.TaskType = MapTask
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		reply.TaskID = mapTaskID
		reply.TaskFile = c.mapFiles[mapTaskID]
		c.mapTasks[mapTaskID].State = InProgress
		c.mapTasks[mapTaskID].Worker = args.WorkerID
		//comment it after finishing the done function
		go c.CheckWorkIsCompleted(MapTask, mapTaskID)
	}

	return nil
}

func (c *Coordinator) CheckWorkIsCompleted(TaskType int, TaskID int) {
	time.Sleep(10 * time.Second)
	if TaskType == MapTask {
		if c.mapTasks[TaskID].State == InProgress {
			c.mapTasks[TaskID].State = Idle
		}
	} else {
		if c.reduceTasks[TaskID].State == InProgress {
			c.reduceTasks[TaskID].State = Idle
		}
	}
}

func (c *Coordinator) CompleteWork(currentTask *CurrentTask, reply *ExampleReply) error {
	if currentTask.CurrentType == MapTask {
		c.mapTasks[currentTask.CurrentID].State = Completed
		fmt.Printf("map task %d is completed\n", currentTask.CurrentID)
	} else {
		c.reduceTasks[currentTask.CurrentID].State = Completed
		fmt.Printf("reduce task %d is completed\n", currentTask.CurrentID)
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.mapTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) nextMapTask() int {
	for i, task := range c.mapTasks {
		if task.State == Idle {
			return i
		}
	}
	return -1 // No available map tasks
}

func (c *Coordinator) nextReduceTask() int {
	for i, task := range c.reduceTasks {
		if task.State == Idle {
			return i
		}
	}
	return -1 // No available reduce tasks
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	return c.allMapTasksCompleted() && c.allReduceTasksCompleted()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	c := Coordinator{
		nReduce:     nReduce,
		nMap:        len(files),
		mapFiles:    files,
		mapTasks:    make([]TaskStatus, len(files)),
		reduceTasks: make([]TaskStatus, nReduce),
	}

	// Initialize map task status
	for i := range c.mapTasks {
		c.mapTasks[i] = TaskStatus{ID: i, State: Idle}
	}

	// Initialize reduce task status
	for i := range c.reduceTasks {
		c.reduceTasks[i] = TaskStatus{ID: i, State: Idle}
	}
	c.server()
	return &c
}
