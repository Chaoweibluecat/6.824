package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// 超时未完成的任意,由超时检查的go rountine放回, fetch_task时消费
	delayedIds    []int
	stage         int
	reduceNum     int
	mapNum        int
	mapCounter    int
	reduceCounter int
	mapTasks      []bool
	reduceTasks   []bool
	mutex         sync.Mutex
	files         []string
}

const MAPPING_AVAILABLE int = 0
const WAITING_MAP_FINISHING int = 1
const REDUCING_AVAILABLE int = 2
const WAITING_REDUCE_FINISHING int = 3
const DONE int = 4

func (m *Master) FetchTask(args EmptyStruct, reply *FetchTaskReply) error {
	print("client fetching task")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.stage == MAPPING_AVAILABLE {
		reply.TaskType = MAP_TASK
		if m.mapCounter < m.mapNum {
			reply.Id = m.mapCounter
			reply.FileName = m.files[m.mapCounter]
			m.mapCounter += 1
			fmt.Printf("id: %d, file :%s", reply.Id, reply.FileName)
		} else if len(m.delayedIds) != 0 {
			reply.Id = m.delayedIds[0]
			reply.FileName = m.files[reply.Id]
			m.delayedIds = m.delayedIds[1:]
		} else {
			println("we fucked up")
		}
		if len(m.delayedIds) == 0 && m.mapCounter == m.mapNum {
			m.stage = WAITING_MAP_FINISHING
		}
	} else if m.stage == WAITING_MAP_FINISHING || m.stage == WAITING_REDUCE_FINISHING {
		reply.TaskType = PENDING
	} else if m.stage == REDUCING_AVAILABLE {
		reply.TaskType = REDUCE_TASK
		if m.reduceCounter < m.reduceNum {
			reply.Id = m.reduceCounter
			m.reduceCounter += 1
		} else if len(m.delayedIds) > 0 {
			reply.Id = m.delayedIds[0]
			m.delayedIds = m.delayedIds[1:]
		} else {
			println("we fucked up")
		}
		if m.reduceCounter == m.reduceNum && len(m.delayedIds) == 0 {
			m.stage = WAITING_REDUCE_FINISHING
		}
	} else {
		reply.TaskType = EXIT
	}
	if reply.TaskType == REDUCE_TASK || reply.TaskType == MAP_TASK {
		go func() {
			time.Sleep(10 * time.Second)
			m.mutex.Lock()
			defer m.mutex.Unlock()
			if reply.TaskType == MAP_TASK && !m.mapTasks[reply.Id] {
				m.delayedIds = append(m.delayedIds, reply.Id)
				if m.stage == WAITING_MAP_FINISHING {
					m.stage = MAPPING_AVAILABLE
				}
			} else if reply.TaskType == REDUCE_TASK && !m.reduceTasks[reply.Id] {
				m.delayedIds = append(m.delayedIds, reply.Id)
				if m.stage == WAITING_REDUCE_FINISHING {
					m.stage = REDUCING_AVAILABLE
				}
			}
		}()
	}
	return nil
}

func (m *Master) TaskDone(args *ReportTaskDoneRequest, reply *EmptyStruct) error {

	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args.Tp == MAP_TASK {
		log.Printf("current maps %v", m.mapTasks)
		m.mapTasks[args.TaskId] = true
		allDone := true
		for _, d := range m.mapTasks {
			allDone = allDone && d
		}
		if allDone {
			m.stage = REDUCING_AVAILABLE
		}
	} else {
		m.reduceTasks[args.TaskId] = true
		allDone := true
		for _, d := range m.reduceTasks {
			allDone = allDone && d
		}
		if allDone {
			m.stage = DONE
		}
	}
	log.Printf("TaskDone %v, Current Stage %d\n", args, m.stage)
	return nil
}

func (m *Master) GetMRCount(args *ReportTaskDoneRequest, reply *ReduceTaskNumReply) error {
	reply.RNum = m.reduceNum
	reply.MNum = m.mapNum
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.stage == DONE
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.reduceNum = nReduce
	m.mapNum = len(files)
	m.files = files
	m.stage = MAPPING_AVAILABLE
	m.reduceCounter = 0
	m.mapCounter = 0
	m.mapTasks = make([]bool, m.mapNum)
	m.reduceTasks = make([]bool, m.reduceNum)
	log.Printf("MasterInfo %v\n", m)

	m.server()
	// f := FetchTaskReply{}
	// m.FetchTask(nil, &f)
	// println(1)
	return &m
}
