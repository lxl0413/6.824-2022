package mr

import "time"

const (
	workerStatusAvailable   = iota //还能继续分配任务
	workerStatusUnavailable        //worker不可用
)

type RegisteredWorker struct {
	WorkerNum    int
	WorkerStatus int
	SockName     string
	nTasks       int32
}

const waitIdleWorkerTime = time.Second
const waitQueueTime = time.Millisecond
