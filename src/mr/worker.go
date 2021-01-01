package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// Ask the master for map tasks first
		callMapSuccess, filename, nReduce, mapID, mapFinished := callMapStart()
		if callMapSuccess {
			if !mapFinished {
				if filename != "" {
					if !mapTask(filename, nReduce, mapID, mapf) {
						return
					}
				} else {
					time.Sleep(time.Second)
				}

			} else {
				for {
					// Ask the master for reduce tasks after map tasks finished.
					callReduceSuccess, nMap, reduceID, reduceFinished := callReduceStart()
					if callReduceSuccess && !reduceFinished {
						if reduceID != -1 {
							if !reduceTask(reduceID, nMap, reducef) {
								fmt.Printf("Reduce task fails. \n")
								return
							}
						} else {
							time.Sleep(time.Second)
						}
					} else {
						//fmt.Printf("Call reduce fails or reduce tasks are finished. \n")
						return
					}
				}

			}

		} else {
			fmt.Printf("Call map fails. \n")
			return
		}
		time.Sleep(time.Second)
	}
}

func mapTask(filename string, nReduce int, mapID int, mapf func(string, string) []KeyValue) bool {
	intermediateVals := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediateVals = append(intermediateVals, kva...)

	// Store intermediate values into local files
	oMap := make(map[int][]KeyValue)
	for i := 0; i < nReduce; i++ {
		oMap[i] = []KeyValue{}
	}
	for _, kv := range intermediateVals {
		id := ihash(kv.Key) % nReduce
		oMap[id] = append(oMap[id], kv)
	}
	for i := 0; i < nReduce; i++ {
		curdir, curerr := os.Getwd()
		if curerr != nil {
			log.Fatalf("cannot get current directory. ")
		}
		tmpfile, _ := ioutil.TempFile(curdir, "tmp-*")
		defer os.Remove(tmpfile.Name())
		enc := json.NewEncoder(tmpfile)
		for _, kv := range oMap[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		tmpfile.Close()
		oname := fmt.Sprintf("mr-%v-%v", mapID, i)
		os.Rename(tmpfile.Name(), oname)
	}
	return callMapFinish(mapID)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func callMapStart() (bool, string, int, int, bool) {
	//workerID := os.Getpid()
	reply := MapReply{}
	if call("Master.MapStart", 0, &reply) {
		return true, reply.Filename, reply.NReduce, reply.MapID, reply.MapFinished
	}
	return false, "", 0, 0, false
}

func callMapFinish(mapID int) bool {
	var i int = 0
	return call("Master.MapFinish", mapID, &i)
}

func reduceTask(reduceID int, nMap int, reducef func(string, []string) string) bool {
	//fmt.Printf("This is reduce task number %v. \n", reduceID)
	kva := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file %v. ", file)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	// if reduceID == 0 {
	// 	file, _ := os.Create("debug")
	// 	for i := 0; i < len(kva); i++ {
	// 		fmt.Fprintf(file, "%v %v\n", kva[i].Key, kva[i].Value)
	// 	}
	// 	file.Close()
	// }
	curdir, _ := os.Getwd()
	tmpfile, _ := ioutil.TempFile(curdir, "tmp-*")
	defer os.Remove(tmpfile.Name())
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		//fmt.Printf("j - i is %v.  \n", j-i)
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		//fmt.Printf("length of values is %v.  \n", len(values))
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tmpfile.Close()
	oname := fmt.Sprintf("mr-out-%v", reduceID)
	os.Rename(tmpfile.Name(), oname)
	return callReduceFinish(reduceID)
}

func callReduceStart() (bool, int, int, bool) {
	reply := ReduceReply{}
	if call("Master.ReduceStart", 0, &reply) {
		return true, reply.NMap, reply.ReduceID, reply.ReduceFinished
	}
	return false, 0, 0, false
}

func callReduceFinish(reduceID int) bool {
	var i int = 0
	return call("Master.ReduceFinish", reduceID, &i)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	//fmt.Printf("Error is %v. \n", err)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
