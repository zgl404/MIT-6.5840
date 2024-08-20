package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	stage := GetGlobalStage()
	for {
		switch stage {
		case Map:
			ExecMap(mapf)
		case Reduce:
			ExecReduce(reducef)
		case Finish:
			return
		}

		time.Sleep(time.Millisecond * 500)
		stage = GetGlobalStage()
	}
}
func GetGlobalStage() GlobalStage {
	args := NoneArgs{}
	reply := StageReply{}

	ok := call("Coordinator.GetGlobalStage", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Now the Global stage is %v\n", reply.Stage)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Stage
}
func GetMapTask() Task {
	args := NoneArgs{}
	reply := Task{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Now the Task is %v %v\n", reply.Filename, reply.FileStatus)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
func GetReduceTask() Task {
	args := NoneArgs{}
	reply := Task{}

	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Now the Task is %v %v\n", reply.Filename, reply.FileStatus)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
func ExecMap(mapf func(string, string) []KeyValue) {
	task := GetMapTask()

	if task.Filename != "" && task.FileId != -1 {
		file, err := os.Open(task.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", task.Filename)
		}
		defer file.Close()

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.Filename)
		}

		kva := mapf(task.Filename, string(content))

		buckets := make([][]KeyValue, 10)
		for _, kv := range kva {
			bucketIndex := ihash(kv.Key) % 10
			buckets[bucketIndex] = append(buckets[bucketIndex], kv)
		}

		for i, bucket := range buckets {
			if bucket != nil {
				writeToFile(bucket, i, task.FileId)
			}
		}
		ChangeFileStatusMap(ChangeArgs{Index: task.FileId})
	}

}

func ExecReduce(reducef func(string, []string) string) {
	task := GetReduceTask()
	intermediate := []KeyValue{}
	if task.Filename != "" && task.FileId != -1 {
		matches, err := filepath.Glob(task.Filename)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		// 遍历匹配的文件并读取内容
		for _, file := range matches {
			temp := readFromFile(file)
			intermediate = append(intermediate, temp...)
		}
		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(task.FileId)
		ofile, _ := os.Create(oname)
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

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		ChangeFileStatusReduce(ChangeArgs{Index: task.FileId})
	}

}

func ChangeFileStatusMap(args ChangeArgs) {

	reply := ChangeReply{}

	ok := call("Coordinator.ChangeFileStatus_Map", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Success receive the index %v\n", reply.Index)
	} else {
		fmt.Printf("call failed!\n")
	}

}
func ChangeFileStatusReduce(args ChangeArgs) {

	reply := ChangeReply{}

	ok := call("Coordinator.ChangeFileStatus_Reduce", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Success receive the index %v\n", reply.Index)
	} else {
		fmt.Printf("call failed!\n")
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

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

	fmt.Println(err)
	return false
}

func writeToFile(bucket []KeyValue, index int, taskid int) {
	// 在函数退出时解锁
	fileName := fmt.Sprintf("mr-%d-%d", taskid, index)

	file, err := os.Create(fileName)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	for _, value := range bucket {
		_, _ = fmt.Fprintf(file, "%s %s\n", value.Key, value.Value)
	}
}
func readFromFile(filename string) []KeyValue {
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v", filename, err)
	}
	defer file.Close()

	// 创建 Scanner 来读取文件
	scanner := bufio.NewScanner(file)

	// 创建一个映射来存储键值对
	mapping := []KeyValue{}
	kva := KeyValue{}
	// 逐行读取文件
	for scanner.Scan() {
		line := scanner.Text() // 获取当前行的内容

		// 去除每行字符串两端的空格
		parts := strings.SplitN(line, " ", 2)

		if len(parts) == 2 {
			kva.Key = parts[0]
			kva.Value = parts[1]
			mapping = append(mapping, kva)
		}
	}

	// 检查是否有可能的错误发生在扫描过程中
	if err := scanner.Err(); err != nil {
		log.Fatal("error reading file:", err)
	}
	return mapping
}
