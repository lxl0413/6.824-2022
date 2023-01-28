package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker := WorkerStruct{
		Mapf:        mapf,
		ReduceF:     reducef,
		nTask:       10, //	先跑十个协程
		closeSignal: false,
	}
	worker.server()
	worker.registerWorker()
	for !worker.done() {
		time.Sleep(2 * time.Second)
	}
}

func (w *WorkerStruct) registerWorker() {
	args := &RegisterWorkerArgs{
		Worker: IncomeWorker{
			SockName: w.SockName,
			nTasks:   10,
		},
	}
	reply := &RegisterWorkerReply{}
	ret := callCoordinator(RegisterWorkerRpcName, args, reply)
	if !ret {
		log.Fatalf("cannot register worker")
	}
	if reply.WorkerClosing {
		w.closeSignal = true
	}
}

func (w *WorkerStruct) done() bool {
	return w.closeSignal
}

func (w *WorkerStruct) CloseWorker(args *CloseWorkerArgs, reply *CloseWorkerReply) error {
	//TODO:用协程池来管理协程，并退出它们
	w.closeSignal = true
	return nil
}

func (w *WorkerStruct) RunMapTask(args *RunMapTaskArgs, reply *RunMapTaskReply) error {
	if atomic.LoadInt32(&w.nTask) <= 0 {
		reply.IsBusy = true
		return nil
	}
	atomic.AddInt32(&w.nTask, -1)
	defer atomic.AddInt32(&w.nTask, 1)

	file, err := os.Open(args.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", args.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", args.FileName)
	}
	file.Close()
	kva := w.Mapf(args.FileName, string(content))

	intermediate := make([][]KeyValue, args.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % args.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	fileNames := make([]string, 0)
	wg := sync.WaitGroup{}
	wg.Add(args.NReduce)

	for index, kvs := range intermediate {
		if len(kvs) == 0 {
			wg.Done()
			continue
		}

		// 把中间值写入文件中
		// mr-X-Y, X is the Map task number, Y is the reduce task number.
		fileName := fmt.Sprintf("mr-%d-%d", args.MapTaskNumber, index)
		fileNames = append(fileNames, fileName)
		go func(fileName string, kvs []KeyValue) {
			defer wg.Done()
			file, err := os.Create(fileName)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot create file %s", fileName)
			}

			enc := json.NewEncoder(file)
			err = enc.Encode(&kvs)
			if err != nil {
				log.Fatalf("cannot encode intermediate keys")
			}
		}(fileName, kvs)
	}

	wg.Wait()
	reply.IntermediateFileNames = fileNames
	return nil
}

func (w *WorkerStruct) RunReduceTask(args *RunReduceTaskArgs, reply *RunReduceTaskReply) error {
	if atomic.LoadInt32(&w.nTask) <= 0 {
		reply.IsBusy = true
		return nil
	}
	atomic.AddInt32(&w.nTask, -1)
	defer atomic.AddInt32(&w.nTask, 1)

	intermediate := []KeyValue{}
	for _, fileName := range args.FileNames {
		file, err := os.Open(fileName)

		if err != nil {
			log.Fatalf("cannot open file %s", fileName)
		}
		dec := json.NewDecoder(file)
		var kvs []KeyValue
		err = dec.Decode(&kvs)
		if err != nil {
			log.Fatalf("cannot decode fileName: %s", fileName)
		}
		intermediate = append(intermediate, kvs...)
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oFileName := fmt.Sprintf("mr-out-%d", args.ReduceTaskNumber)
	oFile, err := os.Create(oFileName)
	if err != nil {
		log.Fatalf("cannot create file %s", oFileName)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.ReduceF(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	oFile.Close()
	return nil
}

func (w *WorkerStruct) checkWorkerInit() bool {
	if len(w.SockName) == 0 {
		return false
	}
	return true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := callCoordinator("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func callCoordinator(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
