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
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	args := TaskArgs{
		WorkerID: 0,
	}
	for {
		reply := TaskReply{}
		ok := call("Coordinator.DispatchTask", &args, &reply)
		if ok {

			if reply.TaskType == MapTask {
				file, err := os.Open(reply.TaskFile)
				if err != nil {
					log.Fatalf("cannot open %v", reply.TaskFile)
				}
				content, err := ioutil.ReadAll(file)
				file.Close()
				if err != nil {
					log.Fatalf("cannot read %v", reply.TaskFile)
				}
				kvs := mapf(reply.TaskFile, string(content))
				intermediate := make([][]KeyValue, reply.NReduce)
				for _, kv := range kvs {
					reduceTask := ihash(kv.Key) % reply.NReduce
					intermediate[reduceTask] = append(intermediate[reduceTask], kv)
				}
				for i := 0; i < reply.NReduce; i++ {
					intermediateFile := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
					file, err := os.Create(intermediateFile)
					if err != nil {
						log.Fatalf("Cannot create %v: %v", intermediateFile, err)
						continue
					}
					enc := json.NewEncoder(file)
					for _, kv := range intermediate[i] {
						if err := enc.Encode(&kv); err != nil {
							log.Fatalf("Encode error: %v", err)
						}
					}
					file.Close()

				}
				currentTask := CurrentTask{
					CurrentType: MapTask,
					CurrentID:   reply.TaskID,
				}
				ok := call("Coordinator.CompleteWork", &currentTask, nil)
				if !ok {
					fmt.Printf("call CompleteWork failed\n")
				}
			} else if reply.TaskType == ReduceTask {
				intermediate := []KeyValue{}
				for i := 0; i < reply.NMap; i++ {
					intermediateFile := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
					file, err := os.Open(intermediateFile)
					if err != nil {
						log.Fatalf("Cannot open %v: %v", intermediateFile, err)
						continue
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					file.Close()
				}

				// Sort intermediate key-value pairs
				// (You may need to implement a sorting function)
				sort.Sort(ByKey(intermediate))
				// Call reduce function
				outputFile := fmt.Sprintf("mr-out-%d", reply.TaskID)
				out, err := os.Create(outputFile)
				if err != nil {
					log.Fatalf("Cannot create %v: %v", outputFile, err)
					return
				}
				for i := 0; i < len(intermediate); {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(out, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				out.Close()
				currentTask := CurrentTask{
					CurrentType: ReduceTask,
					CurrentID:   reply.TaskID,
				}
				ok := call("Coordinator.CompleteWork", &currentTask, nil)
				if !ok {
					fmt.Printf("call CompleteWork failed\n")
				}
			} else {
				break
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
