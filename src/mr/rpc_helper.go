package mr

const (
	RunMapTaskRpcName    = "Worker.RunMapTask"
	RunReduceTaskRpcName = "Worker.RunReduceTask"
	CloseWorkerRpcName   = "Worker.CloseWorker"

	RegisterWorkerRpcName = "Coordinator.RegisterWorker"
)

type IncomeWorker struct {
	SockName string
	nTasks   int32
}

type RegisterWorkerArgs struct {
	Worker IncomeWorker
}

type RegisterWorkerReply struct {
	WorkerClosing bool
}

type CloseWorkerArgs struct {
}

type CloseWorkerReply struct {
}

type RunMapTaskArgs struct {
	FileName      string
	MapTaskNumber int
	NReduce       int
}

type RunMapTaskReply struct {
	IsBusy                bool
	IntermediateFileNames []string
}

type RunReduceTaskArgs struct {
	FileNames        []string
	ReduceTaskNumber int
}

type RunReduceTaskReply struct {
	IsBusy bool
}
