package base

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"fmt"
	"log"
	"sort"
)

// Base 支持
type Base struct {
	SupportJobTypes       map[objects.JobType]bool
	SupportTaskGroupTypes map[objects.TaskGroupType]bool
}

func New(SupportJobTypes []objects.JobType, SupportTaskGroupTypes []objects.TaskGroupType) *Base {
	jobTypeMap := make(map[objects.JobType]bool)
	for _, jobType := range SupportJobTypes {
		jobTypeMap[jobType] = true
	}
	taskGroupTypeMap := make(map[objects.TaskGroupType]bool)
	for _, taskGroupType := range SupportTaskGroupTypes {
		taskGroupTypeMap[taskGroupType] = true
	}

	return &Base{
		SupportJobTypes:       jobTypeMap,
		SupportTaskGroupTypes: taskGroupTypeMap,
	}
}

func (p *Base) PrerequisiteCheck(partitionContext *partition.Context, allocations []*objects.JobAllocation) error {
	for _, allocation := range allocations {
		job := partitionContext.GetUnfinishedJob(allocation.GetJobID())
		if !p.SupportJobTypes[job.GetJobType()] {
			reason := fmt.Sprintf("Base prerequisiteCheck failed, encounter unsupported job type. jobID = %s, jobType = %s", job.GetJobID(), job.GetJobType())
			log.Printf(reason)
			return fmt.Errorf(reason)
		}
		if !p.SupportTaskGroupTypes[job.GetTaskGroup().GetTaskGroupType()] {
			reason := fmt.Sprintf("Base prerequisiteCheck failed, encounter unsupported task group type. jobID = %s, jobType = %s, taskGroupType = %s",
				job.GetJobID(), job.GetJobType(), job.GetTaskGroup().GetTaskGroupType())
			log.Printf(reason)
			return fmt.Errorf(reason)
		}
		for _, taskAllocation := range allocation.GetTaskAllocations() {
			if taskAllocation.GetStartExecutionTimeNanoSecond() == nil && !taskAllocation.GetPlaceholder() {
				reason := fmt.Sprintf("a task's start execution time is unset and it is not a placeholder")
				log.Printf(reason)
				return fmt.Errorf(reason)
			}
			//if err := p.ValidateExecutionRanges(taskAllocation.GetExecutionRanges()); err != nil {
			//	reason := fmt.Sprintf("Base prerequisiteCheck failed, validate execution ranges failed. jobID = %s, jobType = %s, taskGroupType = %s, taskID = %s, err = %s",
			//		job.GetJobID(), job.GetJobType(), job.GetTaskGroup().GetTaskGroupType(), taskAllocation.GetTaskID(), err.Error())
			//	log.Printf(reason)
			//	return fmt.Errorf(reason)
			//}
		}
	}
	return nil
}

//func (p *Base) ValidateExecutionRanges(ers []*objects.ExecutionRange) error {
//	utils.SortExecutionRanges(ers)
//	// ExecutionRanges最多包含一个右开放的区间
//	containsDurationUnset := false
//	for _, er := range ers {
//		if er.GetDurationNanoSecond() == nil && containsDurationUnset {
//			return errors.New("a task execution ranges cannot have two or more duration unset")
//		}
//		if er.GetDurationNanoSecond() == nil {
//			containsDurationUnset = true
//		}
//		if er.GetStartExecutionTimeNanoSecond() == nil && !er.GetPlaceholder() {
//			reason := fmt.Sprintf("a task execution range's start execution time is unset and it is not a placeholder")
//			log.Printf(reason)
//			return fmt.Errorf(reason)
//		}
//	}
//	return nil
//}
//
//func (p *Base) GetLastExecutionRange(ers []*objects.ExecutionRange) *objects.ExecutionRange {
//	utils.SortExecutionRanges(ers)
//	m := int64(-1)
//	var last *objects.ExecutionRange = nil
//	for _, er := range ers {
//		if er.GetDurationNanoSecond() == nil {
//			return er
//		}
//		if s := er.GetStartExecutionTimeNanoSecond().GetValue(); s > m {
//			m = s
//			last = er
//		}
//	}
//	return last
//}

type EachPredictResult struct {
	StartExecutionNanoTime *int64
	FinishNanoTime         *int64
}

func (r *EachPredictResult) GetStartExecutionNanoTime() *int64 {
	if r == nil {
		return nil
	}
	return r.StartExecutionNanoTime
}

func (r *EachPredictResult) GetFinishNanoTime() *int64 {
	if r == nil {
		return nil
	}
	return r.FinishNanoTime
}

type PredictResult struct {
	Results map[*objects.TaskAllocation]*EachPredictResult
}

func NewPredictResult() *PredictResult {
	return &PredictResult{Results: make(map[*objects.TaskAllocation]*EachPredictResult)}
}

func (r *PredictResult) UpdateStartExecutionTime(allocation *objects.TaskAllocation, startExecutionNanoTime int64) {
	if _, ok := r.Results[allocation]; !ok {
		r.Results[allocation] = &EachPredictResult{}
	}
	r.Results[allocation].StartExecutionNanoTime = &startExecutionNanoTime
}

func (r *PredictResult) UpdateFinishTime(allocation *objects.TaskAllocation, finishNanoTime int64) {
	if _, ok := r.Results[allocation]; !ok {
		r.Results[allocation] = &EachPredictResult{}
	}
	r.Results[allocation].FinishNanoTime = &finishNanoTime
}

func (r *PredictResult) IsResultComplete(taskAllocation *objects.TaskAllocation) bool {
	result := r.Results[taskAllocation]
	if result == nil {
		return false
	}
	if result.StartExecutionNanoTime != nil && result.FinishNanoTime != nil {
		return true
	}
	return false
}

func (r *PredictResult) GetResult(taskAllocation *objects.TaskAllocation) interfaces.EachPredictResult {
	return r.Results[taskAllocation]
}

func (r *PredictResult) Range(f func(taskAllocation *objects.TaskAllocation, result interfaces.EachPredictResult)) {
	taskAllocations := make([]*objects.TaskAllocation, 0, len(r.Results))
	for allocation := range r.Results {
		taskAllocations = append(taskAllocations, allocation)
	}
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(taskAllocations)
		},
		LessFunc: func(i, j int) bool {
			return taskAllocations[i].GetJobID() < taskAllocations[j].GetJobID()
		},
		SwapFunc: func(i, j int) {
			t := taskAllocations[i]
			taskAllocations[i] = taskAllocations[j]
			taskAllocations[j] = t
		},
	}
	sort.Sort(sorter)
	for _, taskAllocation := range taskAllocations {
		f(taskAllocation, r.Results[taskAllocation])
	}
}

func (r *PredictResult) Combine(target interfaces.PredictResult) interfaces.PredictResult {
	if _, ok := target.(*PredictResult); !ok {
		panic("PredictResult Combine target type does not match.")
	}
	newPr := &PredictResult{Results: make(map[*objects.TaskAllocation]*EachPredictResult)}
	for ta, e := range r.Results {
		newPr.Results[ta] = e
	}
	target.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		newPr.Results[allocation] = result.(*EachPredictResult)
	})
	return newPr
}
