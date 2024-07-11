package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	WAIT = iota
	MAP
	REDUCE
	DONE
)

type TaskRequest struct {
	// Possible value:
	// * WAIT: initial status, wait MAP/REDUCE task to be done.
	// * MAP/REDUCE: tell Coordinator to mark MAP/REDUCE task done and decrease their task count.
	Type  TaskType
	Id    int
	Files []string
}

type TaskReply struct {
	// Possible value:
	// * WAIT: wait MAP/REDUCE task to be done.
	// * MAP/REDUCE: tell Worker to do MAP/REDUCE task.
	// * DONE: tell Worker to finish job
	Type    TaskType
	Id      int
	NReduce int
	Files   []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
