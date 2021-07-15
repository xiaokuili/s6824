package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	task []*Task
	// partitioning count
	shuffleR int
	mu       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// 实现一个map函数
func (c *Coordinator) RequestTask(args *MrArgs, reply *MrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tmp := EmptyTask()
	done := true
	mapDone := true
	for _, t := range c.task {
		if t.IsIdle() && t.Style == Map {

			t.Status = In_progress
			t.SetTrigger()
			tt := *t
			reply.T = &tt

			return nil
		}

		if t.Style == Reduce && t.IsNotResponse() {

			tmp = t
		}

		if t.Style == Reduce && t.IsIdle() {

			tmp = t
		}

		if t.Style == Map && t.IsNotResponse() {

			tmp = t
		}

		if t.Style == Map && !t.IsCompleted() {
			mapDone = false
		}

		if t.Status != Completed {
			done = false
		}
	}
	if done && len(c.task) > 0 {
		return errors.New("")
	}

	if !mapDone {
		reply.T = &*EmptyTask()
		return nil
	}

	tmp.SetTrigger()
	tmp.Status = In_progress
	tt := *tmp
	reply.T = &tt

	return nil
}

func (c *Coordinator) ReplyTask(args *MrArgs, reply *MrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	initReduce := true
	updateT := EmptyTask()
	// log.Printf("[reply]: task: %v", args.T)
	for _, t := range c.task {
		// 判断是否是完成的map任务
		if args.T.Style == Map && args.T.Status == Completed {
			// 1. 判断当前任务是否是reduce任务
			if t.Style == Reduce {
				// 2. 添加map任务生成的文件路径到相应的reduce任务中
				// 例如:reduce的id是1, 则添加mr-maptaskid-1 到argfile
				initReduce = false

				t.FileArg = "mr-" + mapOutFile(args.T.ID, t.ID) + "," + t.FileArg
			}
		}
		if t.ID == args.T.ID && t.Style == args.T.Style {
			// 如果之前接受, 就不在计算了
			if t.Status == Completed {
				return nil
			}
			updateT = t

		}

	}
	if initReduce == true {
		for i := 0; i < args.T.R; i++ {
			t := NewTask(i, Reduce)
			t.SetFileArgs("mr-" + mapOutFile(args.T.ID, i))
			c.task = append(c.task, t)
		}
	}

	updateT.Status = args.T.Status

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.task) <= 1 {
		return false
	}
	for _, t := range c.task {
		if t.Status != Completed {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.shuffleR = nReduce
	i := 0
	for _, file := range files {
		t := NewTask(i, Map)
		t.SetFileArgs(file)
		t.SetR(nReduce)

		c.task = append(c.task, t)
		i = i + 1
	}

	c.server()
	return &c
}
