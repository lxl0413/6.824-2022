package mr

const (
	RunMapTaskRpcName    = "WorkerStruct.RunMapTask"
	RunReduceTaskRpcName = "WorkerStruct.RunReduceTask"
	CloseWorkerRpcName   = "WorkerStruct.CloseWorker"

	RegisterWorkerRpcName = "Coordinator.RegisterWorker"
)

type RegisterWorkerArgs struct {
	SockName string
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
	IsBusy         bool
	OutputFileName string
}
