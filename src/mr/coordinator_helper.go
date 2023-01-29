package mr

import (
	"time"
)

const (
	workerStatusAvailable   = iota //还能继续分配任务
	workerStatusUnavailable        //worker不可用
)

type RegisteredWorker struct {
	WorkerNum    int
	WorkerStatus int32
	SockName     string
}

// 目前只会重新分配一个task，所以结构这样就可以了。
// 如果可以重新分配多次task，reassignTask要改成数组
type ReassignTask struct {
	OriginTaskNum   int
	ReassignTaskNum int
	SuccessTaskNum  int
	IsSuccess       bool
}

const waitIdleWorkerTime = time.Second
const waitQueueTime = time.Millisecond
const waitWorkerTry = time.Second * 10
const waitReduceWorkerDone = time.Second * 20

const singleWorkerRetry = 2
