package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.SetOutput(io.Discard)
}

type Task struct {
	Type      TaskType
	Id        int
	Files     []string
	StartTime time.Time
	Done      bool
}

type Coordinator struct {
	mu          sync.Mutex
	nMap        int
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Schedule(request *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updateTaskStatus(request)
	return c.checkTaskStatus(reply)
}

// If receive MAP/REDUCE request from worker, mark MAP/REDUCE task done and decrease MAP/REDUCE count.
func (c *Coordinator) updateTaskStatus(request *TaskRequest) {
	taskId := request.Id

	if request.Type == MAP && !c.mapTasks[taskId].Done {
		c.mapTasks[taskId].Done = true
		c.nMap--
		log.Printf("map task %v done", taskId)

		for i, file := range request.Files {
			if len(file) > 0 {
				c.reduceTasks[i].Files = append(c.reduceTasks[i].Files, file)
			}
		}
	}

	if request.Type == REDUCE && !c.reduceTasks[taskId].Done {
		log.Printf("reduce task %v done", taskId)
		c.reduceTasks[taskId].Done = true
		c.nReduce--
	}
}

// Check all MAP/REDUCE task status, wait if all worker has onging task less than 10 seconds, otherwise restart timeout task.
// By unsetting StartTime in Task, first time calling checkTaskStatus starts corresponding MAP/REDUCE task.
// If both MAP and REDUCE count = 0, send DONE signal to worker.
func (c *Coordinator) checkTaskStatus(reply *TaskReply) error {
	// 10 seconds timeout limit for worker
	now := time.Now()
	startTimeLimit := now.Add(-10 * time.Second)
	if c.nMap > 0 {
		for idx := range c.mapTasks {
			t := &c.mapTasks[idx]
			if !t.Done && t.StartTime.Before(startTimeLimit) {
				reply.Type = MAP
				reply.Id = t.Id
				reply.Files = t.Files
				reply.NReduce = len(c.reduceTasks)
				t.StartTime = now
				return nil
			}
		}
		reply.Type = WAIT
	} else if c.nReduce > 0 {
		for idx := range c.reduceTasks {
			t := &c.reduceTasks[idx]
			if !t.Done && t.StartTime.Before(startTimeLimit) {
				reply.Type = REDUCE
				reply.Id = t.Id
				reply.Files = t.Files
				t.StartTime = now
				return nil
			}
		}
		reply.Type = WAIT
	} else {
		reply.Type = DONE
	}
	log.Printf("response Type: %v , task ID: %v", reply.Type, reply.Id)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nReduce == 0 && c.nMap == 0 {
		ret = true
		log.Printf("All tasks are done")
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			Type:  MAP,
			Id:    i,
			Files: []string{file},
			Done:  false,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type:  REDUCE,
			Id:    i,
			Files: []string{},
			Done:  false,
		}
	}
	c.server()
	return &c
}
