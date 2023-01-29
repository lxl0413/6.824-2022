package mr

import (
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
	Workers        *sync.Map
	IdleWorkers    *BlockingQueue[*RegisteredWorker] //	数据类型*RegisterWorker
	addWorkerMutex *sync.Mutex                       //数据类型string->*RegisterWorker，sockName->worker的映射
	maxWorkerNum   int                               //最大的workerNum值
	status         int32                             //1-运行ing/0-关闭中/-1已关闭
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

	return atomic.LoadInt32(&c.status) == -1
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
		Workers:        &sync.Map{},
		IdleWorkers:    &idleWorkers,
		addWorkerMutex: &sync.Mutex{},
		maxWorkerNum:   0,
		status:         1,
	}

	c.server()
	go c.run()
	return &c
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	if len(args.SockName) == 0 {
		return nil
	}

	if atomic.LoadInt32(&c.status) != 1 {
		reply.WorkerClosing = true
		return nil
	}

	c.addWorkerMutex.Lock()
	// 再判断一次
	if atomic.LoadInt32(&c.status) != 1 {
		reply.WorkerClosing = true
		return nil
	}
	registerWorker := &RegisteredWorker{
		WorkerNum:    c.maxWorkerNum,
		WorkerStatus: workerStatusAvailable,
		SockName:     args.SockName,
	}
	c.maxWorkerNum++
	c.addWorkerMutex.Unlock()

	c.Workers.Store(registerWorker.SockName, registerWorker)

	err := c.IdleWorkers.offer(registerWorker)
	if err != nil {
		log.Fatalf("failed to register worker")
	}
	log.Printf("Register worker(%s) success", args.SockName)
	return nil
}

// 循环处理
func (c *Coordinator) run() {
	log.Println("start coordinator.run()")
	wg := sync.WaitGroup{}

	// map阶段
	wg.Add(len(c.InputFileNames))
	// 存储中间文件
	intermediateFileNames := make([]SynchronousSlice[string], c.NReduce)
	for i := 0; i < c.NReduce; i++ {
		intermediateFileNames[i] = NewSynchronousSlice[string]()
	}

	log.Println("start map phase")
	// 直接一个mapTask处理一个inputFile，先不调整文件大小了
	for mapTaskNumber := 0; mapTaskNumber < len(c.InputFileNames); mapTaskNumber++ {
		go func(mapTaskNum int, fileName string) {
			defer wg.Done()
			//log.Printf("map-task-%d start, file:%s", mapTaskNum, fileName)

			ch := make(chan int, 1) // 一定要初始化！
			once := int32(0)        //只能有一个协程进入shuffle阶段

			go func() {
				c.handleMapTask(mapTaskNum, fileName, intermediateFileNames, &once, ch)
			}()

			timeout := time.NewTimer(waitWorkerTry)
			select {
			case <-ch:
				break
			case <-timeout.C:
				c.handleMapTask(mapTaskNum, fileName, intermediateFileNames, &once, ch)
			}
		}(mapTaskNumber, c.InputFileNames[mapTaskNumber])
	}
	wg.Wait()

	log.Println("start reduce phase")
	// map阶段结束，进入reduce阶段
	wg.Add(c.NReduce)

	// 处理有重新分配reduceTask的情况
	reassignReduceTask := NewSynchronousSlice[*ReassignTask]()
	for reduceTaskNumber := 0; reduceTaskNumber < c.NReduce; reduceTaskNumber++ {
		go func(reduceTaskNum int) {
			defer wg.Done()
			//log.Printf("reduce-task-%d start", reduceTaskNum)

			ch := make(chan int, 1) // 一定要初始化！
			once := int32(0)        //只能有协程进入shuffle阶段
			iFileNames := intermediateFileNames[reduceTaskNum].GetElementsSnap()

			go func() {
				c.handleReduceTask(reduceTaskNum, iFileNames, &once, ch)
			}()

			timeout := time.NewTimer(waitWorkerTry)
			select {
			case <-ch:
				break
			case <-timeout.C:
				// 先占位，拿到index后再赋
				reassignTask := &ReassignTask{
					OriginTaskNum: reduceTaskNum,
					IsSuccess:     false,
				}
				index := reassignReduceTask.Append(reassignTask)

				// 保证reassignTaskNum是自增的
				reassignTaskNum := index + c.NReduce
				reassignTask.ReassignTaskNum = reassignTaskNum
				go func() {
					c.handleReduceTask(reassignTaskNum, iFileNames, &once, ch)
				}()

				// 最多等20s，相当于最长运行10+20=30s。再等不到这个reduceTask就不要了
				timeout2 := time.NewTimer(waitReduceWorkerDone)
				select {
				case successTaskID := <-ch:
					reassignTask.SuccessTaskNum = successTaskID
					reassignTask.IsSuccess = true
				case <-timeout2.C:
					reassignTask.SuccessTaskNum = -1
				}
			}
		}(reduceTaskNumber)
	}
	wg.Wait()

	c.clearOrRenameReassignReduceTaskOutput(reassignReduceTask.GetElementsSnap())
	c.closeMapReduce()
}

// 协程里写入taskNum
func (c *Coordinator) handleMapTask(mapTaskNumber int, fileName string, intermediateFileNames []SynchronousSlice[string], once *int32, ch chan int) {
	for {
		if atomic.LoadInt32(once) == 1 {
			break
		}
		idleWorker := c.IdleWorkers.get()
		if idleWorker == nil {
			//log.Printf("no available idleWorker, mapTaskNum:%d", mapTaskNumber)
			time.Sleep(waitIdleWorkerTime)
			continue
		}
		if atomic.LoadInt32(&idleWorker.WorkerStatus) == workerStatusUnavailable {
			continue
		}

		args := &RunMapTaskArgs{
			FileName:      fileName,
			MapTaskNumber: mapTaskNumber,
			NReduce:       c.NReduce,
		}
		reply := &RunMapTaskReply{}

		thisWorkerSuccess := false
		for retry := 0; retry < singleWorkerRetry; retry++ {
			// 有别的worker已经完成了
			if atomic.LoadInt32(once) == 1 {
				break
			}
			thisWorkerSuccess = c.callWorker(idleWorker.SockName, RunMapTaskRpcName, args, reply)
			log.Printf("handleMapTask(%d) callWorker res: %v", mapTaskNumber, thisWorkerSuccess)
			if thisWorkerSuccess {
				// 只能有一个worker进入shuffle阶段，用cas锁保证
				// 先cas加锁再写channel，保证只有一个协程能写入
				if atomic.CompareAndSwapInt32(once, 0, 1) {
					log.Printf("mapTaskNumber(%d) success", mapTaskNumber)
					ch <- mapTaskNumber
				} else {
					thisWorkerSuccess = false
				}
				break
			}
		}

		c.IdleWorkers.offer(idleWorker)

		if !thisWorkerSuccess {
			return
		}

		// shuffle
		for _, intermediaFileName := range reply.IntermediateFileNames {
			reduceNum := getReduceTaskNumber(intermediaFileName)
			if reduceNum == -1 {
				continue
			}
			intermediateFileNames[reduceNum].Append(intermediaFileName)
		}
	}
}

func (c *Coordinator) handleReduceTask(reduceTaskNumber int, fileNames []string, once *int32, ch chan int) {
	for {
		if atomic.LoadInt32(once) == 1 {
			break
		}
		idleWorker := c.IdleWorkers.get()
		if idleWorker == nil {
			time.Sleep(waitIdleWorkerTime)
			continue
		}
		if atomic.LoadInt32(&idleWorker.WorkerStatus) == workerStatusUnavailable {
			continue
		}

		args := &RunReduceTaskArgs{
			FileNames:        fileNames,
			ReduceTaskNumber: reduceTaskNumber,
		}
		reply := &RunReduceTaskReply{}

		for retry := 0; retry < singleWorkerRetry; retry++ {
			// 有别的worker已经完成了
			if atomic.LoadInt32(once) == 1 {
				break
			}
			thisWorkerSuccess := c.callWorker(idleWorker.SockName, RunReduceTaskRpcName, args, reply)
			log.Printf("handleReduceTask(%d) callWorker res: %v", reduceTaskNumber, thisWorkerSuccess)
			if thisWorkerSuccess {
				// 先cas加锁再写channel，保证只有一个协程能写入
				if atomic.CompareAndSwapInt32(once, 0, 1) {
					log.Printf("reduceTaskNumber(%d) success", reduceTaskNumber)
					ch <- reduceTaskNumber
				}
				break
			}
		}

		c.IdleWorkers.offer(idleWorker)
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

// 清除掉错误/多余的文件，修改正确文件的文件名
func (c *Coordinator) clearOrRenameReassignReduceTaskOutput(reassignTasks []*ReassignTask) error {
	for _, task := range reassignTasks {
		// 清除错误/多余的文件
		if !task.IsSuccess {
			err := mayClearFile(getOutputFileName(task.OriginTaskNum))
			if err != nil {
				log.Printf("clearOrRenameReassignReduceTaskOutput err: %v", err)
			}
			err = mayClearFile(getOutputFileName(task.ReassignTaskNum))
			if err != nil {
				log.Printf("clearOrRenameReassignReduceTaskOutput err: %v", err)
			}
			continue
		}

		// 原taskNum成功了，删掉可能存在的新文件就好
		if task.SuccessTaskNum == task.OriginTaskNum {
			err := mayClearFile(getOutputFileName(task.ReassignTaskNum))
			if err != nil {
				log.Printf("clearOrRenameReassignReduceTaskOutput err: %v", err)
			}
			continue
		}

		// 删除原task生产的文件，重命名reassignWorker生辰的文件
		err := mayClearFile(getOutputFileName(task.OriginTaskNum))
		if err != nil {
			log.Printf("clearOrRenameReassignReduceTaskOutput err: %v", err)
		}

		err = os.Rename(getOutputFileName(task.SuccessTaskNum), getOutputFileName(task.OriginTaskNum))
		if err != nil {
			log.Printf("clearOrRenameReassignReduceTaskOutput err: %v", err)
		}
	}
	return nil
}

// 关闭所有worker
func (c *Coordinator) closeMapReduce() {
	atomic.StoreInt32(&c.status, 0) // 进入正在关闭状态

	c.addWorkerMutex.Lock()
	defer c.addWorkerMutex.Unlock()

	closeWorkerFunc := func(k, v interface{}) bool {
		worker, ok := v.(*RegisteredWorker)
		if !ok {
			log.Println("closeMapReduce type assert error")
			return true
		}
		args := &CloseWorkerArgs{}
		reply := &CloseWorkerReply{}
		c.callWorker(worker.SockName, CloseWorkerRpcName, args, reply)
		return true
	}

	c.Workers.Range(closeWorkerFunc)

	atomic.StoreInt32(&c.status, -1) //	关闭完成
}

func (c *Coordinator) callWorker(sockName string, rpcName string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", sockName)
	// 将worker置为unavailable
	if err != nil {
		if workerData, ok := c.Workers.Load(sockName); ok {
			worker, ok := workerData.(*RegisteredWorker)
			if !ok {
				log.Printf("callWorker type assert error")
				return false
			}
			atomic.StoreInt32(&worker.WorkerStatus, workerStatusUnavailable)
		}
		return false
	}
	defer client.Close()

	err = client.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	log.Printf("callWorker err:%v, rpcName:%s", err, rpcName)
	return false
}
