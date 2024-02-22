package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	filesToProcess []string // read-only
	mapTasksCompleted int
	mapTasksLock sync.Mutex
	reduceTasksCompleted int
	reduceTasksLock sync.Mutex
	totalMapTasks int
	totalReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	for c.mapTasksCompleted < c.totalMapTasks {
		c.mapTasksLock.Lock()
		if c.mapTasksCompleted < c.totalMapTasks {
			reply.FileToProcess = c.filesToProcess[c.mapTasksCompleted]
			reply.MapOrReduceTask = MapTask
			// TODO: Add a new handler to the coordinator that accepts acks of tasks. Only then increment it.
			// Also add logic for waiting 10
			c.mapTasksCompleted++
			c.mapTasksLock.Unlock()
			return nil
		}
	}

	for c.reduceTasksCompleted < c.totalReduceTasks {
		c.reduceTasksLock.Lock()
		if c.reduceTasksCompleted < c.totalReduceTasks {
			reply.FileToProcess = fmt.Sprint(c.reduceTasksCompleted)
			reply.MapOrReduceTask = ReduceTask
			c.reduceTasksCompleted++
			c.reduceTasksLock.Unlock()
			return nil
		}
	}

	reply.FileToProcess = ""
	reply.MapOrReduceTask = ExitTask
	return nil
	 
	// return fmt.Errorf("\nsome error occurred while issuing tasks")
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.

	c := Coordinator{
		filesToProcess: files,
		mapTasksCompleted: 0,
		reduceTasksCompleted: 0,
		totalMapTasks: len(files),
		totalReduceTasks: nReduce,
	}

	c.server()
	return &c
}
