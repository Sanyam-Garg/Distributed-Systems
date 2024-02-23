package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
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

func writeOrAppendToFile(key *string, value *string, filenameForKey *string) error {
	dataToWrite := fmt.Sprintf("%v %v\n", *key, *value)
	_, err := os.Stat(*filenameForKey)
	// fmt.Print(filenameForKey, dataToWrite, err)
	if err != nil {
		// File doesn't exist, create it
		if os.IsNotExist(err) {
			err = createFile(*filenameForKey, dataToWrite)
			if err != nil {
				return fmt.Errorf("error creating file: %v", err)
			}
		} else {
			return fmt.Errorf("error checking file: %v", err)
		}
	} else {
		// File exists, append to it
		err = appendToFile(*filenameForKey, dataToWrite)
		if err != nil {
			return fmt.Errorf("error appending to file: %v", err)
		}
	}

	return nil
}

func createFile(filename, data string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(data)
	return err
}

func appendToFile(filename, data string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(data)
	return err
}

func runMapTask(reply *Reply, mapf func(string, string) []KeyValue) error {
	contents, err := ioutil.ReadFile(reply.FileToProcess)
	if err != nil {
		return fmt.Errorf("\nerror while reading contents of file %v", reply.FileToProcess)
	}
	keyValuePairs := mapf(reply.FileToProcess, string(contents))

	for _, keyValuePair := range keyValuePairs {
		key := keyValuePair.Key
		value := keyValuePair.Value
		reduceTaskNumForKey := ihash(key) % reply.NReduceTasks
		filenameForKey := fmt.Sprintf("mr-%d-%d", reply.TaskNum, reduceTaskNumForKey)
		writeOrAppendToFile(&key, &value, &filenameForKey)
	}
	return nil
}

func runReduceTask(reply Reply, reducef func(string, []string) string) error {
	fileRegex, err := regexp.Compile(fmt.Sprintf("mr-[0-9]-%d", reply.TaskNum))
	if err != nil {
		return fmt.Errorf("\nerror while compiling regex: %v", err)
	}

	var reduceOutput []KeyValue
	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if fileRegex.MatchString(info.Name()) {
			// Read file and append to reduceOutput array
			file, ferr := os.Open(path)

			if ferr != nil {
				file.Close()
				return fmt.Errorf("error while opening file %v", path)
			}

			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				key, value := strings.Split(line, " ")[0], strings.Split(line, " ")[1]
				// fmt.Printf("[TaskNum: %v] key: %v, value: %v", reply.TaskNum, key, value)
				keyValuePair := KeyValue{
					Key:   key,
					Value: value,
				}
				reduceOutput = append(reduceOutput, keyValuePair)
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("\nerror while collecting files for reduce task %v", reply.TaskNum)
	}

	
	sort.Sort(ByKey(reduceOutput))

	file, err := os.Create(fmt.Sprintf("mr-out-%d", reply.TaskNum))
	if err != nil {
		return fmt.Errorf("error while writing reduce output to file %v", file.Name())
	}

	defer file.Close()

	i := 0
	for i < len(reduceOutput) {
		j := i + 1
		for j < len(reduceOutput) && reduceOutput[j].Key == reduceOutput[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, reduceOutput[k].Value)
		}
		output := reducef(reduceOutput[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", reduceOutput[i].Key, output)

		i = j
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := Args{}

		// declare a reply structure.
		reply := Reply{}

		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			// reply.Y should be 100.
			if int(reply.MapOrReduceTask) == int(ExitTask) {
				break
			} else if reply.MapOrReduceTask == MapTask {
				fmt.Print(reply)
				err := runMapTask(&reply, mapf)
				if err != nil {
					// TODO: Somehow the coordinator should be able to detect this
					fmt.Println(err.Error())
					continue
				}
			} else {
				err := runReduceTask(reply, reducef)
				if err != nil {
					// TODO: Somehow the coordinator should be able to detect this
					fmt.Println(err.Error())
					continue
				}
			}
			// fmt.Printf("reply.FileToProcess %v, reply.MapOrReduceTask %v, reply.TaskNum %v\n", reply.FileToProcess, reply.MapOrReduceTask, reply.TaskNum)

		} else {
			fmt.Printf("call failed!\n")
		}
		time.Sleep(time.Second * 5)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	fmt.Printf("connecting to socket unix://%v\n", sockname)
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
