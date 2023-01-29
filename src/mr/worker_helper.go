package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type IMapF func(string, string) []KeyValue
type IReduceF func(string, []string) string

type WorkerStruct struct {
	Mapf       IMapF
	ReduceF    IReduceF
	SockName   string
	uid        int
	tmpFileDir int
	status     int32 //2-已经有任务在运行,1-空闲,0-准备关闭
}

func (w *WorkerStruct) server() {
	rpc.Register(w)
	rpc.HandleHTTP()

	sockName := w.genWorkerSock()
	//err := os.Remove(sockName)
	//if err != nil {
	//	log.Fatalf("worker remove err: %v", err)
	//}
	//_, err := os.Stat(sockName)
	//for !os.IsNotExist(err) {
	//	sockName = w.genWorkerSock()
	//	_, err = os.Stat(sockName)
	//}

	l, err := net.Listen("unix", sockName)

	if err != nil {
		log.Fatalf("worker listen error: %v", err)
	}

	w.SockName = sockName
	go http.Serve(l, nil)
}

func (w *WorkerStruct) genWorkerSock() string {
	s := "/var/tmp/824-mr-worker-"
	s += strconv.Itoa(os.Getpid())
	w.tmpFileDir = os.Getpid()
	return s
}

func (w *WorkerStruct) getWorkerTmpFileDirPath() string {
	return fmt.Sprintf("%s-%d", tmpFileDir, w.tmpFileDir)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const tmpFileDir = "mr-tmp-files"
