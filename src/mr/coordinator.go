package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Coordinator struct {
	mu             sync.Mutex
	Status         string
	Filenames      chan string
	ReduceValues   chan []string
	ReduceName     chan string
	FinalAnswer    []KeyValue
	tmpReducePairs []KeyValue
	AssignedTask   map[string]time.Time
	NameToKeys     map[string][]string
	ReduceNum      int
	MapNum         int
	ReceiveMapNum  int
	FinalNum       int
	TaskDone       bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForTask(x int, t *Task) error {
	c.mu.Lock()
	t.TaskType = ""
	if c.Status == "MAP" {

		if c.MapNum == 0 {
			t.TaskType = "WAIT"

		} else {
			t.TaskType = "MAP"
			t.MapFilename = <-c.Filenames
			c.AssignedTask[t.MapFilename] = time.Now()
			c.MapNum--
		}
	}
	if c.Status == "REDUCE" {
		if c.ReduceNum == 0 {

			t.TaskType = ""
			if c.FinalNum != 0 {
				t.TaskType = "WAIT"
			}
			c.TaskDone = true
			c.mu.Unlock()
			return nil
		}
		t.TaskType = "REDUCE"
		t.MapFilename = <-c.ReduceName
		t.ReducePairs = <-c.ReduceValues
		c.NameToKeys[t.MapFilename] = t.ReducePairs
		c.AssignedTask[t.MapFilename] = time.Now()
		if c.ReduceNum == 1 {
			c.TaskDone = true
		}
		c.ReduceNum--
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveMap(pp *PairWithArray, x *int) error {
	c.mu.Lock()
	p := (*pp).Second
	filename := (*pp).First
	delete(c.AssignedTask, filename)
	c.tmpReducePairs = append(c.tmpReducePairs, p...)
	c.ReceiveMapNum--
	if c.ReceiveMapNum == 0 {
		sort.Sort(ByKey(c.tmpReducePairs))
		i := 0
		for i < len(c.tmpReducePairs) {
			j := i + 1
			for j < len(c.tmpReducePairs) && c.tmpReducePairs[j].Key == c.tmpReducePairs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, c.tmpReducePairs[k].Value)
			}
			c.ReduceValues <- values
			c.ReduceName <- c.tmpReducePairs[i].Key
			c.AssignedTask[c.tmpReducePairs[i].Key] = time.Now()
			c.ReduceNum++
			c.FinalNum++
			i = j
		}
		c.Status = "REDUCE"
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduce(pp *PairWithString, x *int) error {
	c.mu.Lock()
	p := (*pp).Second
	name := (*pp).First
	delete(c.AssignedTask, name)
	c.FinalAnswer = append(c.FinalAnswer, p)
	if c.FinalNum == 1 {
		oname := "mr-out-1"
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(c.FinalAnswer) {
			fmt.Fprintf(ofile, "%v %v\n", c.FinalAnswer[i].Key, c.FinalAnswer[i].Value)
			i++
		}
	}
	c.FinalNum--
	c.mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	//fmt.Printf("P: %v\n", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	if c.TaskDone && c.FinalNum == 0 {
		c.mu.Unlock()
		return true
	}
	nowt := time.Now()
	for name, t := range c.AssignedTask {
		if nowt.Sub(t).Seconds() > 15 {
			//fmt.Println("Here")
			if c.Status == "MAP" {
				c.MapNum++
				c.Filenames <- name
			} else {
				c.ReduceNum++
				c.ReduceName <- name
				c.ReduceValues <- c.NameToKeys[name]
			}
		}
	}
	c.mu.Unlock()
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Filenames = make(chan string, 1000000)
	c.ReduceValues = make(chan []string, 1000000)
	c.ReduceName = make(chan string, 1000000)
	c.Status = "MAP"
	c.TaskDone = false
	c.AssignedTask = make(map[string]time.Time)
	c.NameToKeys = make(map[string][]string)
	for _, s := range files {
		c.Filenames <- s
		c.MapNum++
		c.ReceiveMapNum++
	}
	if c.MapNum == 0 {
		c.TaskDone = true
	}

	c.server()
	return &c
}
