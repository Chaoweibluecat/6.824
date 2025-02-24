package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"../mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	// files := []string{"pg-being_ernest.txt", "pg-dorian_gray.txt"}
	// m := mr.MakeMaster(m, 1)
	// m := mr.MakeMaster(files, 1)

	// t1 := mr.FetchTaskReply{}
	// e := mr.EmptyStruct{}
	// m.FetchTask(e, &t1)
	// r := mr.ReportTaskDoneRequest{t1.TaskType, t1.Id}s
	// m.TaskDone(&r, nil)

	// m.FetchTask(e, &t1)
	// r = mr.ReportTaskDoneRequest{t1.TaskType, t1.Id}
	// m.TaskDone(&r, nil)

	// m.FetchTask(e, &t1)

	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
