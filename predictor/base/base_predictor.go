package base

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"fmt"
	"log"
)

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
		if allocation.GetFinished() {
			reason := fmt.Sprintf("Base prerequisiteCheck failed, encounter finished allocation. jobID = %s", job.GetJobID())
			log.Printf(reason)
			return fmt.Errorf(reason)
		}
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
		if allocation.GetStartExecutionTimeNanoSecond() == 0 && !allocation.GetPlaceholder() {
			reason := fmt.Sprintf("Base prerequisiteCheck failed, a job allocation's start execution time is unset and it is not a placeholder. jobID = %s", job.GetJobID())
			log.Printf(reason)
			return fmt.Errorf(reason)
		}
	}
	return nil
}

type EachPredictResult struct {
	StartExecutionNanoTime *int64
	FinishNanoTime         *int64
}

func (r *EachPredictResult) GetStartExecutionNanoTime() int64 {
	if r == nil {
		return 0
	}
	if r.StartExecutionNanoTime != nil {
		return *r.StartExecutionNanoTime
	}
	return 0
}

func (r *EachPredictResult) GetFinishNanoTime() int64 {
	if r.FinishNanoTime != nil {
		return *r.FinishNanoTime
	}
	return 0
}

type PredictResult struct {
	Results map[*objects.JobAllocation]*EachPredictResult
}

func NewPredictResult() *PredictResult {
	return &PredictResult{Results: make(map[*objects.JobAllocation]*EachPredictResult)}
}

func (r *PredictResult) UpdateStartExecutionTime(allocation *objects.JobAllocation, startExecutionNanoTime int64) {
	if _, ok := r.Results[allocation]; !ok {
		r.Results[allocation] = &EachPredictResult{}
	}
	r.Results[allocation].StartExecutionNanoTime = &startExecutionNanoTime
}

func (r *PredictResult) UpdateFinishTime(allocation *objects.JobAllocation, finishNanoTime int64) {
	if _, ok := r.Results[allocation]; !ok {
		r.Results[allocation] = &EachPredictResult{}
	}
	r.Results[allocation].FinishNanoTime = &finishNanoTime
}

func (r *PredictResult) IsResultComplete(allocation *objects.JobAllocation) bool {
	result := r.Results[allocation]
	if result == nil {
		return false
	}
	if result.StartExecutionNanoTime != nil && result.FinishNanoTime != nil {
		return true
	}
	return false
}

func (r *PredictResult) GetResult(allocation *objects.JobAllocation) (interfaces.EachPredictResult, bool) {
	return r.Results[allocation], r.IsResultComplete(allocation)
}
