package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	// CallExample()

	request := TaskRequest{Type: WAIT}
	reply := TaskReply{}

	for {
		reply = SendToCoordinator(&request)
		switch reply.Type {
		case MAP:
			mappedFiles, err := doMap(mapf, &reply)
			if err != nil {
				break
			}
			// send request to coordinator to mark map task done.
			request.Id = reply.Id
			request.Type = MAP
			request.Files = mappedFiles
		case REDUCE:
			reducedFiles := doReduce(reducef, &reply)
			// send request to coordinator to mark reduce task done.
			request.Id = reply.Id
			request.Type = REDUCE
			request.Files = reducedFiles
		case WAIT:
			time.Sleep(500 * time.Millisecond)
		case DONE:
			return
		default:
			panic(fmt.Sprintf("unknown task type: %v", reply.Type))
		}
	}
}

func SendToCoordinator(request *TaskRequest) TaskReply {
	reply := TaskReply{}
	ok := call("Coordinator.Schedule", request, &reply)
	if !ok {
		log.Fatal("ask for task failed")
	}
	return reply
}

func doMap(mapf func(string, string) []KeyValue, reply *TaskReply) ([]string, error) {
	log.Printf("do map task: %v", reply.Id)
	filename := reply.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: %v, error: %v", filename, err)
		return []string{}, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file: %v, error: %v", filename, err)
		return []string{}, err
	}

	// get a set of KV pairs with k is the word and v is the 1.
	intermediate := mapf(filename, string(content))

	reduceFileList := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % reply.NReduce
		reduceFileList[reduceId] = append(reduceFileList[reduceId], kv)
	}

	encodedFiles := make([]string, reply.NReduce)
	for reduceId, kvList := range reduceFileList {
		oname := fmt.Sprintf("mr-%v-%v", reply.Id, reduceId)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		for _, kv := range kvList {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		encodedFiles[reduceId] = oname
	}

	return encodedFiles, nil
}

func doReduce(reducef func(string, []string) string, reply *TaskReply) []string {
	log.Printf("do reduce task: %v", reply.Id)
	intermediate := make([]KeyValue, 0)
	for _, filename := range reply.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v, error: %v", filename, err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	oname := fmt.Sprintf("mr-out-%v", reply.Id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	sort.Sort(ByKey(intermediate))

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
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return []string{oname}
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
