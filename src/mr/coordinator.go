package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskTimeout = 10 * time.Second // 任务超时
)

type GlobalStage int

const (
	Map GlobalStage = iota
	Reduce
	Finish
)

type FileState int

const (
	Undo FileState = iota
	Doing
	Done
)

type Task struct {
	Filename   string
	FileId     int
	FileStatus FileState
	StartTime  int64
}

type Coordinator struct {
	Maptask    []Task
	Reducetask []Task

	mu sync.Mutex
}

type NoneArgs struct {
}
type StageReply struct {
	Stage GlobalStage
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) GetGlobalStage(args *NoneArgs, reply *StageReply) error {
	mapstage := true

	for _, value := range c.Maptask {

		if value.FileStatus != Done {
			mapstage = false
			break
		}
	}
	if mapstage {
		reducestage := true
		for _, value := range c.Reducetask {
			//fmt.Println(value)
			if value.FileStatus != Done {
				reducestage = false
				break
			}
		}
		if reducestage {
			reply.Stage = Finish
		} else {
			reply.Stage = Reduce
		}
	} else {
		reply.Stage = Map
	}
	return nil
}
func (c *Coordinator) GetMapTask(args *NoneArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	*reply = Task{Filename: "", FileId: -1, FileStatus: Undo, StartTime: 0}
	for key, v := range c.Maptask {
		if v.FileStatus == Undo || (v.FileStatus == Doing && time.Now().UnixNano()-v.StartTime > TaskTimeout.Nanoseconds()) {
			v.StartTime = time.Now().UnixNano()
			v.FileStatus = Doing
			c.Maptask[key] = v
			*reply = v
			break
		}
	}

	return nil
}
func (c *Coordinator) GetReduceTask(args *NoneArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	*reply = Task{Filename: "", FileId: -1, FileStatus: Undo, StartTime: 0}
	for key, v := range c.Reducetask {
		if v.FileStatus == Undo || (v.FileStatus == Doing && time.Now().UnixNano()-v.StartTime > TaskTimeout.Nanoseconds()) {
			v.StartTime = time.Now().UnixNano()
			v.FileStatus = Doing
			c.Reducetask[key] = v
			*reply = v
			break
		}
	}

	return nil
}
func (c *Coordinator) ChangeFileStatus_Map(args *ChangeArgs, reply *ChangeReply) error {

	c.Maptask[args.Index].FileStatus = Done
	reply.Index = args.Index

	return nil
}
func (c *Coordinator) ChangeFileStatus_Reduce(args *ChangeArgs, reply *ChangeReply) error {

	c.Reducetask[args.Index].FileStatus = Done
	reply.Index = args.Index
	return nil
}

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
	c := Coordinator{}

	for index, v := range files {
		c.Maptask = append(c.Maptask, Task{Filename: v, FileId: index, FileStatus: Undo, StartTime: 0})
	}

	for i := 0; i < nReduce; i++ {
		c.Reducetask = append(c.Reducetask, Task{Filename: "mr-" + "*" + strconv.Itoa(i), FileId: i, FileStatus: Undo, StartTime: 0})
	}

	c.server()
	return &c
}
