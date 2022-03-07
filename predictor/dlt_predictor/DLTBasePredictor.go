package dlt_predictor

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/base"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"encoding/json"
	"fmt"
	"log"
	"math"
)

type DLTBasePredictor struct {
	*base.Base
	impl DLTPredictorTemplate
}

type DLTPredictorTemplate interface {
	getDataParallelMiniBatchDurationNanoSecond(ctx *PredictSessionContext, allocation *objects.JobAllocation) int64
	getMiniBatchDurationNanoSecond(ctx *PredictSessionContext, jobID string, acceleratorType string) int64
	getSpaceSharingMiniBatchDurationNanoSecond(ctx *PredictSessionContext, accelerator *objects.Accelerator, jobIDs []string) map[string]int64
	getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) int64
}

type PredictSessionContext struct {
	partitionContext                    *partition.Context
	endNanoTime                         int64
	result                              *base.PredictResult
	acceleratorID2Allocations           map[string][]*objects.JobAllocation
	acceleratorID2SingleTaskAllocations map[string][]*objects.JobAllocation
	acceleratorID2GangTaskAllocations   map[string][]*objects.JobAllocation
}

func NewDLTBasePredictor(impl DLTPredictorTemplate) *DLTBasePredictor {
	return &DLTBasePredictor{
		Base: base.New([]objects.JobType{
			objects.JobType_jobTypeDLT,
		}, []objects.TaskGroupType{
			objects.TaskGroupType_taskGroupTypeSingle,
			objects.TaskGroupType_taskGroupTypeGang,
		}),
		impl: impl,
	}
}

func (p *DLTBasePredictor) PrerequisiteCheck(partitionContext *partition.Context, allocations []*objects.JobAllocation) error {
	if err := p.Base.PrerequisiteCheck(partitionContext, allocations); err != nil {
		return err
	}
	return nil
}

func (p *DLTBasePredictor) Predict(partitionContext *partition.Context, allocations []*objects.JobAllocation) (interfaces.PredictResult, error) {
	return p.PredictByEndTime(partitionContext, allocations, math.MaxInt64)
}

func (p *DLTBasePredictor) PredictByEndTime(partitionContext *partition.Context, allocations []*objects.JobAllocation, endNanoTime int64) (interfaces.PredictResult, error) {
	if err := p.PrerequisiteCheck(partitionContext, allocations); err != nil {
		return nil, err
	}
	ctx := p.buildPredictSessionContext(partitionContext, allocations, endNanoTime)
	// now := partitionContext.Now()
	// firstly, predict jobs with TaskGroupType of 'single'
	err := p.predictSingleJobs(ctx)
	if err != nil {
		return nil, err
	}
	// secondly, predict gang jobs with startExecutionTimeSecond already known.
	err = p.predictGangJobsWithStartExecutionTimeKnown(ctx)
	if err != nil {
		return nil, err
	}
	// thirdly, predict gang jobs with unknown startExecutionTimeSecond (it must be a placeholder gang job)
	err = p.predictGangJobsWithPlaceholder(ctx)
	if err != nil {
		return nil, err
	}

	return ctx.result, nil
}

func (p *DLTBasePredictor) buildPredictSessionContext(partitionContext *partition.Context, allocations []*objects.JobAllocation, endNanoTime int64) *PredictSessionContext {
	result := base.NewPredictResult()
	acceleratorID2Allocations := make(map[string][]*objects.JobAllocation)
	acceleratorID2SingleTaskAllocations := make(map[string][]*objects.JobAllocation)
	acceleratorID2GangTaskAllocations := make(map[string][]*objects.JobAllocation)
	for _, allocation := range allocations {
		var add = func(allocation *objects.JobAllocation, acceleratorID2Allocations map[string][]*objects.JobAllocation) {
			taskAllocations := allocation.GetTaskAllocations()
			for _, taskAllocation := range taskAllocations {
				accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
				if _, ok := acceleratorID2Allocations[accID]; !ok {
					acceleratorID2Allocations[accID] = make([]*objects.JobAllocation, 0)
				}
				acceleratorID2Allocations[accID] = append(acceleratorID2Allocations[accID], allocation)
			}
		}
		add(allocation, acceleratorID2Allocations)
		switch partitionContext.UnfinishedJobs[allocation.GetJobID()].GetTaskGroup().GetTaskGroupType() {
		case objects.TaskGroupType_taskGroupTypeGang:
			add(allocation, acceleratorID2GangTaskAllocations)
		case objects.TaskGroupType_taskGroupTypeSingle:
			add(allocation, acceleratorID2SingleTaskAllocations)
		}
	}
	return &PredictSessionContext{
		partitionContext:                    partitionContext,
		endNanoTime:                         endNanoTime,
		result:                              result,
		acceleratorID2Allocations:           acceleratorID2Allocations,
		acceleratorID2SingleTaskAllocations: acceleratorID2SingleTaskAllocations,
		acceleratorID2GangTaskAllocations:   acceleratorID2GangTaskAllocations,
	}
}

func (p *DLTBasePredictor) isAllocationPredicted(ctx *PredictSessionContext, allocation *objects.JobAllocation) bool {
	return ctx.result.IsResultComplete(allocation)
}

func (p *DLTBasePredictor) predictSingleJobs(ctx *PredictSessionContext) error {
	// firstly, predict jobs with TaskGroupType of 'single'
	for acceleratorID, singleTaskJobAllocations := range ctx.acceleratorID2SingleTaskAllocations {
		accelerator := ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorID]
		for _, allocation := range singleTaskJobAllocations {
			ctx.result.UpdateStartExecutionTime(allocation, allocation.GetStartExecutionTimeNanoSecond())
		}
		r, err := p.predictSpaceSharingSingleAllocations(ctx, accelerator, singleTaskJobAllocations)
		if err != nil {
			return err
		}
		for _, allocation := range singleTaskJobAllocations {
			ctx.result.UpdateFinishTime(allocation, r[allocation])
		}
	}
	return nil
}

func (p *DLTBasePredictor) predictGangJobsWithStartExecutionTimeKnown(ctx *PredictSessionContext) error {
	for _, gangTaskAllocations := range ctx.acceleratorID2GangTaskAllocations {
		for _, allocation := range gangTaskAllocations {
			if p.isAllocationPredicted(ctx, allocation) {
				continue
			}
			ctx.result.UpdateStartExecutionTime(allocation, allocation.GetStartExecutionTimeNanoSecond())
			if p.getStartExecutionNanoTimeInSession(ctx, allocation) != 0 {
				r, err := p.predictGangInternal(ctx, allocation)
				if err != nil {
					return err
				}
				ctx.result.UpdateFinishTime(allocation, r)
			}
		}
	}
	return nil
}

func (p *DLTBasePredictor) predictGangJobsWithPlaceholder(ctx *PredictSessionContext) error {
	for _, gangTaskAllocations := range ctx.acceleratorID2GangTaskAllocations {
	nextAlloc:
		for _, allocation := range gangTaskAllocations {
			if p.isAllocationPredicted(ctx, allocation) {
				continue
			}
			if allocation.GetStartExecutionTimeNanoSecond() == 0 && allocation.GetPlaceholder() {
				placeholderAcceleratorIDs := make([]string, 0, len(allocation.GetTaskAllocations()))
				for _, taskAllocation := range allocation.GetTaskAllocations() {
					placeholderAcceleratorIDs = append(placeholderAcceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
				}
				// check all jobs running on placeholder accelerators, find the latest finished one of them
				startExecutionTime := 0.
				for _, acceleratorID := range placeholderAcceleratorIDs {
					previousAllocations := ctx.acceleratorID2Allocations[acceleratorID]
					for _, previousAllocation := range previousAllocations {
						if previousAllocation.GetJobID() == allocation.GetJobID() {
							continue
						}
						if _, complete := ctx.result.GetResult(previousAllocation); !complete {
							ctx.result.UpdateStartExecutionTime(allocation, 0)
							ctx.result.UpdateFinishTime(allocation, 0)
							continue nextAlloc
						}
						r, _ := ctx.result.GetResult(previousAllocation)
						startExecutionTime = math.Max(float64(r.GetFinishNanoTime()), startExecutionTime)
					}
				}
				ctx.result.UpdateStartExecutionTime(allocation, int64(startExecutionTime))
				r, err := p.predictGangInternal(ctx, allocation)
				if err != nil {
					return err
				}
				ctx.result.UpdateFinishTime(allocation, r)
			}
		}
	}
	return nil
}

func (p *DLTBasePredictor) predictGangInternal(ctx *PredictSessionContext, allocation *objects.JobAllocation) (int64, error) {
	bs := ctx.partitionContext.GetUnfinishedJob(allocation.GetJobID()).GetTaskGroup().GetTaskGroupInfoBytes()
	info := &objects.GangTaskGroup{}
	err := json.Unmarshal(bs, info)
	if err != nil {
		return 0, err
	}
	extraBs := info.GetExtra()
	extra := &objects.GangTaskGroupDLTExtra{}
	err = json.Unmarshal(extraBs, extra)
	if err != nil {
		return 0, err
	}
	gangType := extra.GetDLTGangType()
	if gangType != objects.DLTGangType_DLTGangTypeDataParallel {
		reason := fmt.Sprintf("Currently, DLT Predictor only supports DataParallel Gang Job")
		log.Printf(reason)
		return 0, fmt.Errorf(reason)
	}
	return p.predictDataParallel(ctx, allocation)
}

func (p *DLTBasePredictor) predictDataParallel(ctx *PredictSessionContext, allocation *objects.JobAllocation) (int64, error) {
	totalMiniBatches := p.impl.getJobTotalMiniBatches(ctx, allocation.GetJobID())
	// gang task group shares the same start time stamp
	startNanoTime := p.getStartExecutionNanoTimeInSession(ctx, allocation)
	duration := totalMiniBatches * p.impl.getDataParallelMiniBatchDurationNanoSecond(ctx, allocation)
	if startNanoTime+duration > ctx.endNanoTime {
		return 0, nil
	}
	return startNanoTime + duration, nil
}

func (p *DLTBasePredictor) getStartExecutionNanoTimeInSession(ctx *PredictSessionContext, allocation *objects.JobAllocation) int64 {
	r, _ := ctx.result.GetResult(allocation)
	if r != nil {
		return r.GetStartExecutionNanoTime()
	}
	return r.GetStartExecutionNanoTime()
}

func (p *DLTBasePredictor) predictSpaceSharingSingleAllocations(ctx *PredictSessionContext, accelerator *objects.Accelerator, allocations []*objects.JobAllocation) (map[*objects.JobAllocation]int64, error) {
	startNanoTime2Allocations := map[int64][]*objects.JobAllocation{}
	sortedStartTime := make([]int64, 0, len(allocations))
	jobID2RemainingMiniBatches := make(map[string]float64)
	for _, allocation := range allocations {
		startNanoTime := p.getStartExecutionNanoTimeInSession(ctx, allocation)
		sortedStartTime = append(sortedStartTime, startNanoTime)
		if _, ok := startNanoTime2Allocations[startNanoTime]; !ok {
			startNanoTime2Allocations[startNanoTime] = make([]*objects.JobAllocation, 0, 1)
		}
		startNanoTime2Allocations[startNanoTime] = append(startNanoTime2Allocations[startNanoTime], allocation)
		jobID2RemainingMiniBatches[allocation.GetJobID()] = float64(p.impl.getJobTotalMiniBatches(ctx, allocation.GetJobID()))
	}
	utils.SortInt64(sortedStartTime)
	runningAllocations := make([]*objects.JobAllocation, 0, 2)
	runningAllocations = append(runningAllocations, startNanoTime2Allocations[sortedStartTime[0]]...)
	currTime := sortedStartTime[0]
	if currTime > ctx.endNanoTime {
		return nil, nil
	}
	nextStartTimeIdx := 1

	makeError := func(runningAllocations ...*objects.JobAllocation) error {
		jobIDs := make([]string, 0, len(runningAllocations))
		for _, allocation := range runningAllocations {
			jobIDs = append(jobIDs, allocation.GetJobID())
		}
		reason := fmt.Sprintf("predictSpaceSharedSingleAllocations: only two jobs space-sharing is acceptable, but encounter %d space sharing jobs, jobIDs = %+v", len(runningAllocations), jobIDs)
		log.Printf(reason)
		return fmt.Errorf(reason)
	}
	if len(runningAllocations) > 2 {
		return nil, makeError(runningAllocations...)
	}
	result := make(map[*objects.JobAllocation]int64)
	for len(result) != len(allocations) {
		runningJobIDs := make([]string, 0, len(runningAllocations))
		for _, runningAllocation := range runningAllocations {
			runningJobIDs = append(runningJobIDs, runningAllocation.GetJobID())
		}
		miniBatchDurations := p.impl.getSpaceSharingMiniBatchDurationNanoSecond(ctx, accelerator, runningJobIDs)
		// calculate the closest event: any of jobs finished, or a new job comes in.
		runningJobFinishTimes := make(map[string]int64)
		closestFinishedJobTime := int64(math.MaxInt64)
		for _, runningJobID := range runningJobIDs {
			miniBatches := jobID2RemainingMiniBatches[runningJobID]
			miniBatchDuration := miniBatchDurations[runningJobID]
			finishTime := currTime + int64(math.Ceil(miniBatches*float64(miniBatchDuration)))
			runningJobFinishTimes[runningJobID] = finishTime
			if finishTime < closestFinishedJobTime {
				closestFinishedJobTime = finishTime
			}
		}
		newJobStartTime := int64(math.MaxInt64)
		if nextStartTimeIdx < len(sortedStartTime) {
			newJobStartTime = sortedStartTime[nextStartTimeIdx]
		}
		if ctx.endNanoTime < newJobStartTime && ctx.endNanoTime < closestFinishedJobTime {
			// predict endTime is earlier than newJobStartTime and closestFinishedJobTime
			// which means both events cannot happen earlier then the prediction ends.
			// This indicates that the prediction is over.
			return result, nil
		}
		passDurationForRunningAllocations := func(currTime int64, duration int64) {
			resultRunningAllocations := make([]*objects.JobAllocation, 0, len(runningAllocations))
			for _, allocation := range runningAllocations {
				duration2Finish := int64(math.Ceil(jobID2RemainingMiniBatches[allocation.GetJobID()] * float64(miniBatchDurations[allocation.GetJobID()])))
				if duration >= duration2Finish {
					// finished job.
					jobID2RemainingMiniBatches[allocation.GetJobID()] = 0
					result[allocation] = currTime + duration2Finish
					continue
				}
				passedMiniBatches := float64(duration) / float64(miniBatchDurations[allocation.GetJobID()])
				remainingMiniBatches := jobID2RemainingMiniBatches[allocation.GetJobID()] - passedMiniBatches
				jobID2RemainingMiniBatches[allocation.GetJobID()] = remainingMiniBatches
				resultRunningAllocations = append(resultRunningAllocations, allocation)
			}
			runningAllocations = resultRunningAllocations
		}
		if newJobStartTime < closestFinishedJobTime {
			// new job comes in is earlier than other job finishes.
			nextStartTimeIdx++
			passedDuration := newJobStartTime - currTime
			passDurationForRunningAllocations(currTime, passedDuration)
			currTime = newJobStartTime
			if len(runningAllocations)+len(startNanoTime2Allocations[newJobStartTime]) > 2 {
				runningAllocations = append(runningAllocations, startNanoTime2Allocations[newJobStartTime]...)
				return nil, makeError(runningAllocations...)
			}
			runningAllocations = append(runningAllocations, startNanoTime2Allocations[newJobStartTime]...)
		} else {
			// new job comes in is later than other job finishes.
			passedDuration := closestFinishedJobTime - currTime
			passDurationForRunningAllocations(currTime, passedDuration)
			currTime = closestFinishedJobTime
		}
	}
	return result, nil
}

func (p *DLTBasePredictor) getDataParallelMiniBatchDurationNanoSecond(ctx *PredictSessionContext, allocation *objects.JobAllocation) int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getMiniBatchDurationNanoSecond(ctx *PredictSessionContext, jobID string, acceleratorType string) int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getSpaceSharingMiniBatchDurationNanoSecond(ctx *PredictSessionContext, accelerator *objects.Accelerator, jobIDs []string) map[string]int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) int64 {
	panic("template method.")
}
