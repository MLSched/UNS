package simulator

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	"UNS/utils"
	"log"
	"math"
	"sort"
	"strconv"
	"time"
)

func printAllocations(allocations []*objects.JobAllocation) {
	for _, a := range allocations {
		s, _ := utils.MarshalJsonPB(a)
		log.Println(s)
	}
}

func printPredictResults(result interfaces.PredictResult, printUnix bool) {
	jobIDs := make(map[string]bool)
	result.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		jobID := allocation.GetJobID()
		if jobIDs[jobID] {
			return
		}
		jobIDs[jobID] = true
		startNano := *result.GetStartExecutionNanoTime()
		endNano := *result.GetFinishNanoTime()
		if printUnix {
			log.Printf("predictResult job ID %s, start time %s, predict end time %s\n", jobID, time.Unix(0, startNano).String(), time.Unix(0, endNano))
		} else {
			log.Printf("predictResult job ID %s, start time %d, predict end time %d\n", jobID, *result.GetStartExecutionNanoTime(), *result.GetFinishNanoTime())
		}
	})
}

func buildJobExecutionHistory(allocation *objects.JobAllocation, finishTime int64) *objects.JobExecutionHistory {
	taskExecutionHistories := make([]*objects.TaskExecutionHistory, 0, len(allocation.GetTaskAllocations()))
	ftaskAllocation := allocation.GetTaskAllocations()[0]
	for i, taskAllocation := range allocation.GetTaskAllocations() {
		taskExecutionHistories = append(taskExecutionHistories, &objects.TaskExecutionHistory{
			ExecutionID:                  strconv.Itoa(i),
			NodeID:                       taskAllocation.GetNodeID(),
			JobID:                        taskAllocation.GetJobID(),
			TaskID:                       taskAllocation.GetTaskID(),
			StartExecutionTimeNanoSecond: taskAllocation.GetStartExecutionTimeNanoSecond().GetValue(),
			DurationNanoSecond:           finishTime - ftaskAllocation.GetStartExecutionTimeNanoSecond().GetValue(),
			HostMemoryAllocation:         taskAllocation.GetHostMemoryAllocation(),
			CPUSocketAllocations:         taskAllocation.GetCPUSocketAllocations(),
			AcceleratorAllocation:        taskAllocation.GetAcceleratorAllocation(),
		})
	}
	return &objects.JobExecutionHistory{
		JobID:                  allocation.GetJobID(),
		ResourceManagerID:      allocation.GetResourceManagerID(),
		PartitionID:            allocation.GetPartitionID(),
		TaskExecutionHistories: taskExecutionHistories,
	}
}

// iterJobsBySubmitTime 获取根据submitTime排序的下一波任务。(submitTime int64, jobs []*objects.Job, next func())
func iteratorJobsBySubmitTime(jobs []*objects.Job) func() (int64, []*objects.Job, func() bool) {
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(jobs)
		},
		LessFunc: func(i, j int) bool {
			return jobs[i].GetSubmitTimeNanoSecond() < jobs[j].GetSubmitTimeNanoSecond()
		},
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = o
		},
	}
	sort.Sort(sorter)
	currIndex := 0
	next := func(batchJobsSize int) bool {
		currIndex += batchJobsSize
		if currIndex >= len(jobs) {
			return false
		}
		return true
	}
	return func() (int64, []*objects.Job, func() bool) {
		if currIndex >= len(jobs) {
			return math.MaxInt64, nil, func() bool {
				return false
			}
		}
		submitTime := jobs[currIndex].GetSubmitTimeNanoSecond()
		nextBatchJobs := make([]*objects.Job, 0, 1)
		for i := currIndex; i < len(jobs); i++ {
			if jobs[i].GetSubmitTimeNanoSecond() == submitTime {
				nextBatchJobs = append(nextBatchJobs, jobs[i])
			}
		}
		return submitTime, nextBatchJobs, func() bool {
			return next(len(nextBatchJobs))
		}
	}
}

func fastFail(err error) {
	if err != nil {
		panic(err)
	}
}
