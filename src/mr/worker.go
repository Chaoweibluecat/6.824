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
	"syscall"
	"time"
)

const MAP_TASK int = 1
const REDUCE_TASK int = 2
const PENDING int = 3
const EXIT int = 4

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MyWorker struct {
	files []*os.File
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getReduceNum() (int, int) {
	reply := ReduceTaskNumReply{}
	request := EmptyStruct{}
	call("Master.GetMRCount", &request, &reply)
	return reply.MNum, reply.RNum
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reduceNum, mapNum := getReduceNum()
	for {
		//output files for
		task := fetchTask()
		// map
		if task.TaskType == MAP_TASK {
			doMap(mapf, task, reduceNum)
		} else if task.TaskType == REDUCE_TASK {
			doReduce(task.Id, mapNum, reducef)
		} else if task.TaskType == PENDING {
			time.Sleep(1 * time.Second)
		} else {
			syscall.Shutdown(1, 2)
		}
	}
}

// reduce
func doReduce(reduceId int, mapNum int, reducef func(string, []string) string) {
	keyToPacketValues := make(map[string][]string)
	ks := []string{}
	//  轮询每一个worker的输出文件
	for i := 0; i < mapNum; i++ {
		path := "mr-" + fmt.Sprint(i) + "-" + fmt.Sprint(reduceId)
		file, err := os.Open(path)
		// no file found
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			keyToPacketValues[kv.Key] = append(keyToPacketValues[kv.Key], kv.Value)
		}
	}
	// 排序并写结果
	outputFile, _ := os.Create("mr-out-" + fmt.Sprint(reduceId))
	for k := range keyToPacketValues {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		values := keyToPacketValues[k]
		if len(values) == 0 {
			continue
		}
		keyOutPutReduced := reducef(k, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", k, keyOutPutReduced)
	}
	reportTaskDone(reduceId, REDUCE_TASK)
}

func reportTaskDone(id int, tp int) {
	args := ReportTaskDoneRequest{tp, id}
	res := EmptyStruct{}
	call("Master.TaskDone", &args, &res)
}

func fetchTask() *FetchTaskReply {
	// declare a reply structure.
	reply := FetchTaskReply{}
	request := EmptyStruct{}
	// send the RPC request, wait for the reply.
	call("Master.FetchTask", &request, &reply)
	fmt.Printf("客户端收到的 reply: %+v\n", reply) // 打印 reply

	return &reply
}

func doMap(mapf func(string, string) []KeyValue, task *FetchTaskReply, reduceNum int) {
	name := task.FileName
	file, _ := os.Open(name)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	output := mapf(name, string(content))
	//mapResultInit
	partition := [][]KeyValue{}
	for i := 0; i < reduceNum; i++ {
		temp := []KeyValue{}
		partition = append(partition, temp)
	}
	// mapResult partition
	for _, kv := range output {
		idx := ihash(kv.Key) % reduceNum
		partition[idx] = append(partition[idx], kv)
	}
	// write mapresult
	for idx, outputPacket := range partition {
		packetFileName := "mr-" + fmt.Sprint(task.Id) + "-" + fmt.Sprint(idx)
		file, _ := os.Create(packetFileName)
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range outputPacket {
			err := enc.Encode(&kv)
			if err != nil {
				panic("we fucked up")
			}
		}
	}
	reportTaskDone(task.Id, MAP_TASK)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
