package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//my code

	/* Sequential
	switch phase {
	case mapPhase:
		for i, f := range mr.files {
			doMap(mr.jobName, i, f, mr.nReduce, mapF)
		}
	case reducePhase:
		for i := 0; i < mr.nReduce; i++ {
			doReduce(mr.jobName, i, len(mr.files), reduceF)
		}
	}
	*/

	task_ch := make(chan int)

	for i := 0; i < ntasks; i++ {
		//get worker
		<- mr.registerChannel
		mr.Lock()
		worker := mr.workers[0]
		mr.workers = mr.workers[1:]
		mr.Unlock()

		//make args
		args := new(DoTaskArgs)
		args.JobName = mr.jobName
		args.Phase = phase
		args.File = mr.files[i]
		args.TaskNumber = i
		args.NumOtherPhase = nios

		//go rpc call worker's DoTask
		go func() {
			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if ok == false {
				fmt.Printf("DoTask: RPC %s register error\n", worker)
			}
			//register worker again
			rg_args := new(RegisterArgs)
			rg_args.Worker = worker
			mr.Register(rg_args, new(struct{}))

			task_ch <- i
		}()
	}

	//use channel wait for cur phase done
	for i := 0; i < ntasks; i++ {
		<-task_ch
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
