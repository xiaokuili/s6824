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
	"strconv"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

func mapOutFile(m, r int) string {
	return strconv.Itoa(m) + "-" + strconv.Itoa(r)
}

type mrWorker struct {
}

func (mr *mrWorker) callReq(t *Task) bool {
	args := &MrArgs{}
	apply := &MrReply{T: t}
	return call("Coordinator.RequestTask", args, apply)

}

func (mr *mrWorker) run(t *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	if t.Style == Map {
		return mr.runMap(t, mapf)
	}
	if t.Style == Reduce {
		return mr.runReduce(t, reducef)
	}
	return false
}

func (mr *mrWorker) runMap(t *Task, mapf func(string, string) []KeyValue) bool {
	// 读取文件
	// 执行函数
	// 写入文件: 每一行都是(key, value), 基于key写入到不同的文件
	intermediate := make([][]KeyValue, t.R)
	filename := t.FileArg
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

	for _, kv := range kva {
		intermediate[ihash(kv.Key)%t.R] = append(intermediate[ihash(kv.Key)%t.R], kv)
	}

	for i, v := range intermediate {
		oname := "mr-" + strconv.Itoa(t.ID) + "-" + strconv.Itoa(i)
		os.Remove(oname)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalln(err)
			}
		}
		ofile.Close()

	}
	return true

}

func (mr *mrWorker) runReduce(t *Task, reducef func(string, []string) string) bool {
	// 读取文件
	// 执行函数
	// 写入文件

	oname := "mr-out-" + strconv.Itoa(t.ID)
	os.Remove(oname)
	ofile, _ := os.Create(oname)
	filenames := strings.Split(t.FileArg, ",")

	kva := make([]KeyValue, 0)
	for _, filename := range filenames {
		if filename == "" {
			break
		}

		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
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
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	return true
}

func (mr *mrWorker) callReply(t *Task) bool {

	args := &MrArgs{T: t}
	apply := &MrReply{}
	return call("Coordinator.ReplyTask", args, apply)
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := EmptyTask()
		w := &mrWorker{}
		if ok := w.callReq(task); !ok {
			os.Exit(0)
			log.Println("stop worker")
		}
		if w.run(task, mapf, reducef) {
			task.Status = Completed
		}
		w.callReply(task)

	}

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	// fmt.Println(err)
	return false
}
