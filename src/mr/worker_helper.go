package mr

import (
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
	Mapf        IMapF
	ReduceF     IReduceF
	SockName    string
	nTask       int32
	closeSignal bool
}

func (w *WorkerStruct) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	sockName := genWorkerSock()
	os.Remove(sockName)
	l, err := net.Listen("unix", sockName)
	if err != nil {
		log.Fatal("worker listen error:", err)
	}
	w.SockName = sockName
	go http.Serve(l, nil)
}

func genWorkerSock() string {
	s := "/var/tmp/824-mr-worker-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
