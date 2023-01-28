package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	NReduce        int
	InputFileNames []string
	Workers        []*RegisteredWorker
	IdleWorkers    *BlockingQueue[*RegisteredWorker] //	数据类型*RegisterWorker
	addWorkerMutex *sync.Mutex
	done           bool
}

// Your code here -- RPC handlers for the worker to call.

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

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	idleWorkers := NewBlockingQueue[*RegisteredWorker]()
	c := Coordinator{
		NReduce:        nReduce,
		InputFileNames: files,
		Workers:        make([]*RegisteredWorker, 0),
		IdleWorkers:    &idleWorkers,
		addWorkerMutex: &sync.Mutex{},
		done:           false,
	}

	c.server()
	go c.run()
	return &c
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	if len(args.Worker.SockName) == 0 {
		return nil
	}
	c.addWorkerMutex.Lock()
	registerWorker := &RegisteredWorker{
		WorkerNum:    len(c.Workers),
		WorkerStatus: workerStatusAvailable,
		SockName:     args.Worker.SockName,
		nTasks:       args.Worker.nTasks,
	}
	c.Workers = append(c.Workers, registerWorker)
	c.addWorkerMutex.Unlock()
	c.IdleWorkers.offer(registerWorker)
	return nil
}

// 循环处理
func (c *Coordinator) run() {
	wg := sync.WaitGroup{}

	// map阶段
	wg.Add(len(c.InputFileNames))
	// 存储中间文件
	intermediateFileNames := make([]SynchronousSlice[string], c.NReduce)
	for i := 0; i < c.NReduce; i++ {
		intermediateFileNames[i] = NewSynchronousSlice[string]()
	}

	// 直接一个mapTask处理一个inputFile，先不调整文件大小了
	for mapTaskNumber := 0; mapTaskNumber < len(c.InputFileNames); mapTaskNumber++ {
		go func(mapTaskNumber int, fileName string) {
			defer wg.Done()
			c.handleMapTask(mapTaskNumber, fileName, intermediateFileNames)
		}(mapTaskNumber, c.InputFileNames[mapTaskNumber])
	}
	wg.Wait()

	// map阶段结束，进入reduce阶段
	wg.Add(c.NReduce)
	for reduceTaskNumber := 0; reduceTaskNumber < c.NReduce; reduceTaskNumber++ {
		go func(reduceTaskNum int) {
			defer wg.Done()
			c.handleReduceTask(reduceTaskNum, intermediateFileNames[reduceTaskNum].GetElementsSnap())
		}(reduceTaskNumber)
	}
	wg.Wait()
	c.done = true
}

func (c *Coordinator) handleMapTask(mapTaskNumber int, fileName string, intermediateFileNames []SynchronousSlice[string]) {
	for {
		idleWorker := c.IdleWorkers.get()
		if idleWorker == nil {
			time.Sleep(waitIdleWorkerTime)
			continue
		}
		if idleWorker.WorkerStatus == workerStatusUnavailable {
			continue
		}

		//TODO：这里有三次原子操作，需要优化
		if atomic.LoadInt32(&idleWorker.nTasks) == 0 {
			continue
		}

		reOffer := false
		atomic.AddInt32(&idleWorker.nTasks, -1)
		if atomic.LoadInt32(&idleWorker.nTasks) > 0 {
			err := c.IdleWorkers.offer(idleWorker)
			for err != nil {
				time.Sleep(waitQueueTime)
				err = c.IdleWorkers.offer(idleWorker)
			}
			reOffer = true
		}

		args := &RunMapTaskArgs{
			FileName:      fileName,
			MapTaskNumber: mapTaskNumber,
			NReduce:       c.NReduce,
		}
		reply := &RunMapTaskReply{}

		res := callWorker(idleWorker.SockName, RunMapTaskRpcName, args, reply)
		//TODO
		if !res {
			log.Fatalf("call Worker fail")
		}

		atomic.AddInt32(&idleWorker.nTasks, 1)
		if !reOffer && atomic.LoadInt32(&idleWorker.nTasks) == 1 {
			c.IdleWorkers.offer(idleWorker)
			reOffer = true
		}

		// shuffle
		for _, intermediaFileName := range reply.IntermediateFileNames {
			reduceNum := getReduceTaskNumber(intermediaFileName)
			if reduceNum == -1 {
				continue
			}
			intermediateFileNames[reduceNum].Append(intermediaFileName)
		}
		break
	}
}

func (c *Coordinator) handleReduceTask(reduceTaskNumber int, fileNames []string) {
	for {
		idleWorker := c.IdleWorkers.get()
		if idleWorker == nil {
			time.Sleep(waitIdleWorkerTime)
			continue
		}
		if idleWorker.WorkerStatus == workerStatusUnavailable {
			continue
		}

		//TODO：这里有三次原子操作，需要优化
		if atomic.LoadInt32(&idleWorker.nTasks) == 0 {
			continue
		}

		reOffer := false
		atomic.AddInt32(&idleWorker.nTasks, -1)
		if atomic.LoadInt32(&idleWorker.nTasks) > 0 {
			err := c.IdleWorkers.offer(idleWorker)
			for err != nil {
				time.Sleep(waitQueueTime)
				err = c.IdleWorkers.offer(idleWorker)
			}
			reOffer = true
		}

		args := &RunReduceTaskArgs{
			FileNames:        fileNames,
			ReduceTaskNumber: reduceTaskNumber,
		}
		reply := &RunReduceTaskReply{}

		res := callWorker(idleWorker.SockName, RunReduceTaskRpcName, args, reply)
		//TODO
		if !res {
			log.Fatalf("call Worker fail")
		}

		atomic.AddInt32(&idleWorker.nTasks, 1)
		if !reOffer && atomic.LoadInt32(&idleWorker.nTasks) == 1 {
			c.IdleWorkers.offer(idleWorker)
			reOffer = true
		}
		break
	}
}

func getReduceTaskNumber(s string) int {
	if len(s) == 0 {
		return -1
	}
	parts := strings.Split(s, "-")
	num, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		log.Fatalf("cannot convert num: %s", parts[len(parts)-1])
		return -1
	}
	return num
}

func callWorker(sockName string, rpcName string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
