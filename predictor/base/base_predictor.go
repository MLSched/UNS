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
		if allocation.GetStartExecutionTimeSecond() == 0 && !allocation.GetPlaceholder() {
			reason := fmt.Sprintf("Base prerequisiteCheck failed, a job allocation's start execution time is unset and it is not a placeholder. jobID = %s", job.GetJobID())
			log.Printf(reason)
			return fmt.Errorf(reason)
		}
	}
	return nil
}

type EachPredictResult struct {
	StartExecutionTime float64
	FinishTime         float64
}

func (r *EachPredictResult) GetStartExecutionTime() float64 {
	return r.StartExecutionTime
}

func (r *EachPredictResult) GetFinishTime() float64 {
	return r.FinishTime
}

type PredictResult struct {
	Results map[*objects.JobAllocation]*EachPredictResult
}

func NewPredictResult() *PredictResult {
	return &PredictResult{Results: make(map[*objects.JobAllocation]*EachPredictResult)}
}

func (r *PredictResult) UpdateStartExecutionTime(allocation *objects.JobAllocation, startExecutionTime float64) {
	if _, ok := r.Results[allocation]; !ok {
		r.Results[allocation] = &EachPredictResult{}
	}
	r.Results[allocation].StartExecutionTime = startExecutionTime
}

func (r *PredictResult) UpdateFinishTime(allocation *objects.JobAllocation, finishTime float64) {
	if _, ok := r.Results[allocation]; !ok {
		r.Results[allocation] = &EachPredictResult{}
	}
	r.Results[allocation].FinishTime = finishTime
}

func (r *PredictResult) GetResult(allocation *objects.JobAllocation) interfaces.EachPredictResult {
	return r.Results[allocation]
}
