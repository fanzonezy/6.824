package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup;
	wg.Add(ntasks);
	go func () {
		tasks := make(chan int, ntasks);
		defer close(tasks);
		initTasks(tasks, ntasks)
		for currTaskNumber := range tasks { // once there is a unfinished task.
			addr := <- registerChan
			var args DoTaskArgs;
			switch phase {
			case mapPhase:
				args = DoTaskArgs{
					JobName:       jobName,
					File:          mapFiles[currTaskNumber],
					Phase:         mapPhase,
					TaskNumber:    currTaskNumber,
					NumOtherPhase: n_other,
				}
			case reducePhase:
				args = DoTaskArgs{
					JobName:       jobName,
					Phase:         reducePhase,
					TaskNumber:    currTaskNumber,
					NumOtherPhase: n_other,
				}
			}
			go executeOne(addr, args, &wg, registerChan, tasks);
		}
	} ();
	wg.Wait();

	fmt.Printf("Schedule: %v done\n", phase)
}

func executeOne(addr string,
				args DoTaskArgs,
				wg *sync.WaitGroup,
				registerChan chan string,
				tasks chan int) bool {
	defer func () {
		fmt.Printf("Schedule(%s): adding %s back to available workers\n", args.Phase, addr);
		registerChan <- addr;
	} (); // recording one task is done.
	success := call(addr, "Worker.DoTask", args, nil);
	if !success {
		// reschedule
		fmt.Printf("Schedule(%s): task %d failed. Add it back.\n", args.Phase, args.TaskNumber);
		tasks <- args.TaskNumber;  // add failed task back
	} else {
		wg.Done();
	}
	return success;
}

func initTasks(tasks chan int, ntasks int) {
	for i := 0; i < ntasks; i++ {
		tasks <- i;
	}
}