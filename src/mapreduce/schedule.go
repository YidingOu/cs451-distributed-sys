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
        for i:=0; i < ntasks; i++ {
            worker := <- mr.registerChannel
            task := new(DoTaskArgs)
            task.Phase = phase
            task.JobName = mr.jobName
            task.NumOtherPhase = nios
            task.File = mr.files[i]
            task.TaskNumber = i
            success := call(worker, "Worker.DoTask", task, new(struct{}))
            if success == true {
                go func ()  {
                    mr.registerChannel <- worker
                }()
            } else{
                i = i - 1
                fmt.Printf("doMap ERROR\n")
            }
        }
    case reducePhase:
        ntasks = mr.nReduce
        nios = len(mr.files)
        for i:=0; i < ntasks; i++ {
            worker := <- mr.registerChannel
            task := new(DoTaskArgs)
            task.Phase = phase
            task.JobName = mr.jobName
            task.TaskNumber = i
            task.NumOtherPhase = nios
            success := call(worker, "Worker.DoTask", task, new(struct{}))
            if success == true {
                go func ()  {
                    mr.registerChannel <- worker
                }()
            } else{
                i = i - 1
                fmt.Printf("doReduce ERROR!\n")
            }
        }
    }

    fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

    // All ntasks tasks have to be scheduled on workers, and only once all of
    // them have been completed successfully should the function return.
    // Remember that workers may fail, and that any given worker may finish
    // multiple tasks.
    //
    // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
    //
    fmt.Printf("Schedule: %v phase done\n", phase)
}
