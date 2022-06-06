package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	TaskId            int
	CurrentPhase      Phase
	ReduceTaskChannel chan *Task
	MapTaskChannel    chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state     State
	TaskPtr   *Task
	StartTime time.Time
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
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		CurrentPhase:      MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.makeMapTasks(files)

	c.server()

	go c.CrashDetector()

	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, fn := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{fn},
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskPtr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		// fmt.Println("make a map task: ", &task)
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskPtr.TaskId
	meta := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskPtr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		// fmt.Println("make a reduce task: ", &task)
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") &&
			strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, file.Name())
		}
	}
	return s
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.CurrentPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("map task[%d] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("reduce task[%d] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		panic("An undefined phase has occurred!")
	}

	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.CurrentPhase == MapPhase {
		c.makeReduceTasks()
		c.CurrentPhase = ReducePhase
	} else if c.CurrentPhase == ReducePhase {
		c.CurrentPhase = AllDone
	}
}

func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskPtr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskPtr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	// Because MetaMap contains both MapTask and ReduceTask, so we must check both.
	if mapDoneNum > 0 && mapUnDoneNum == 0 && reduceDoneNum == 0 && reduceUnDoneNum == 0 {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}
	return false
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}

	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			// fmt.Printf("Map task[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("[WARNING]Map task[%d] is finished, already!\n", args.TaskId)
		}
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			// fmt.Printf("Reduce task[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("[WARNING]Reduce task[%d] is finished, already!\n", args.TaskId)
		}
	default:
		panic("The task type undefined!")
	}
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	mu.Lock()
	defer mu.Unlock()

	if c.CurrentPhase == AllDone {
		fmt.Println("All tasks are finished, the coordinator is going to exit.")
		return true
	}
	return false
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CurrentPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.StartTime) > 10*time.Second {
				fmt.Printf("The task[%d] may has crashed, and has taken %d seconds.\n",
					v.TaskPtr.TaskId, time.Since(v.StartTime)/1e9)
				switch v.TaskPtr.TaskType {
				case MapTask:
					{
						c.MapTaskChannel <- v.TaskPtr
						v.state = Waiting
					}
				case ReduceTask:
					{
						c.ReduceTaskChannel <- v.TaskPtr
						v.state = Waiting
					}
				}
			}
		}
		mu.Unlock()
	}
}
