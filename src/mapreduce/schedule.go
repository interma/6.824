package mapreduce

import "fmt"

//
func (mr *Master) fire_task_i(phase jobPhase, i int, task_ch chan int) {
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		nios = mr.nReduce
	case reducePhase:
		nios = len(mr.files)
	}

	//get worker
	//worker := <- mr.registerChannel
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
		//register worker again
		rg_args := new(RegisterArgs)
		rg_args.Worker = worker
		mr.Register(rg_args, new(struct{}))


		if ok == false {
			fmt.Printf("DoTask: RPC %s error\n", worker)
			task_ch <- i
		}else {
			task_ch <- -1
		}
	}()
}

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
		mr.fire_task_i(phase, i, task_ch)
	}

	//use channel wait for cur phase done
	for i := 0; i < ntasks; i++ {
		idx := <-task_ch
		if idx >= 0 {
			i-- // retry fail task
			mr.fire_task_i(phase, idx, task_ch)
		}
	}


	fmt.Printf("Schedule: %v phase done\n", phase)
}
