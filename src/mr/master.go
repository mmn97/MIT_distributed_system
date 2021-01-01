package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	nReduce      int
	nMap         int
	files        []string
	mapStates    []mapState
	reduceStates []reduceState
	mu           sync.Mutex
}

type mapState struct {
	state int
	t     time.Time
}

type reduceState struct {
	state int
	t     time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// // Define an error to represent tasks are finished
// type ErrorMsg string

// //
// func (err ErrorMsg) Error() string {
// 	return fmt.Sprintf("%v are finished", err)
// }

func (m *Master) MapStart(args int, reply *MapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mapFinished := true
	reply.Filename = ""
	for i, v := range m.mapStates {
		if v.state == 0 {
			m.mapStates[i].state = 1
			m.mapStates[i].t = time.Now()
			reply.NReduce = m.nReduce
			reply.MapID = i
			reply.Filename = m.files[i]
			reply.MapFinished = false
			return nil
		} else if v.state == 1 {
			mapFinished = false
		}
	}
	reply.MapFinished = mapFinished
	return nil
}

func (m *Master) MapFinish(mapID int, reply *int) error {
	m.mu.Lock()
	m.mapStates[mapID].state = 2
	m.mu.Unlock()
	return nil
}

func (m *Master) ReduceStart(args int, reply *ReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reduceFinished := true
	reply.ReduceID = -1
	for i, v := range m.reduceStates {
		if v.state == 0 {
			m.reduceStates[i].state = 1
			m.reduceStates[i].t = time.Now()
			reply.NMap = m.nMap
			reply.ReduceID = i
			reply.ReduceFinished = false
			return nil
		} else if v.state == 1 {
			reduceFinished = false
		}
	}
	reply.ReduceFinished = reduceFinished
	return nil
}

func (m *Master) ReduceFinish(reduceID int, reply *int) error {
	m.mu.Lock()
	m.reduceStates[reduceID].state = 2
	m.mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Re-issue map tasks if the master waits for more than ten seconds.
	for i, v := range m.mapStates {
		if v.state == 0 {
			return false
		}
		if v.state == 1 {
			elapsed := time.Since(v.t)
			if elapsed > time.Second*10 {
				m.mapStates[i].state = 0
			}
			return false
		}
	}
	// Re-issue reduce tasks if the master waits for more than ten seconds.
	for i, v := range m.reduceStates {
		if v.state == 0 {
			return false
		}
		if v.state == 1 {
			elapsed := time.Since(v.t)
			if elapsed > time.Second*10 {
				m.reduceStates[i].state = 0
			}
			return false
		}
	}
	// All map and reduce tasks are finished.
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.files = files
	m.nMap = len(files)
	m.mapStates = make([]mapState, m.nMap)
	m.reduceStates = make([]reduceState, m.nReduce)
	m.server()
	return &m
}
