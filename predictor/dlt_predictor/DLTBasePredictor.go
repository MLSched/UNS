package dlt_predictor

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/base"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

type DLTBasePredictor struct {
	*base.Base
	impl DLTPredictorTemplate
}

type DLTPredictorTemplate interface {
	getSingleTaskSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorID string, jobIDs []string) map[string]int64
	getDataParallelTasksSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64
	getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) int64
}

type PredictSessionContext struct {
	partitionContext          *partition.Context
	endNanoTime               int64
	result                    *base.PredictResult
	acceleratorID2Allocations map[string][]*objects.JobAllocation
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
	// firstly, predict allocations with start execution time already known
	err := p.predictAllocationsWithStartExecutionTimeKnown(ctx, allocations)
	if err != nil {
		return nil, err
	}
	// secondly, mark allocations which are placeholders start execution time.
	marked := p.markStartExecutionTime(ctx, allocations)
	if marked {
		// if marked, then predict allocations once again.
		err := p.predictAllocationsWithStartExecutionTimeKnown(ctx, allocations)
		if err != nil {
			return nil, err
		}
	}

	return ctx.result, nil
}

func (p *DLTBasePredictor) buildPredictSessionContext(partitionContext *partition.Context, allocations []*objects.JobAllocation, endNanoTime int64) *PredictSessionContext {
	result := base.NewPredictResult()
	acceleratorID2Allocations := make(map[string][]*objects.JobAllocation)
	for _, allocation := range allocations {
		taskAllocations := allocation.GetTaskAllocations()
		for _, taskAllocation := range taskAllocations {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
			if _, ok := acceleratorID2Allocations[accID]; !ok {
				acceleratorID2Allocations[accID] = make([]*objects.JobAllocation, 0)
			}
			acceleratorID2Allocations[accID] = append(acceleratorID2Allocations[accID], allocation)
		}
	}
	return &PredictSessionContext{
		partitionContext:          partitionContext,
		endNanoTime:               endNanoTime,
		result:                    result,
		acceleratorID2Allocations: acceleratorID2Allocations,
	}
}

func (p *DLTBasePredictor) isAllocationPredicted(ctx *PredictSessionContext, allocation *objects.JobAllocation) bool {
	return ctx.result.IsResultComplete(allocation)
}

func (p *DLTBasePredictor) predictAllocationsWithStartExecutionTimeKnown(ctx *PredictSessionContext, allocations []*objects.JobAllocation) error {
	// firstly, predict jobs with TaskGroupType of 'single'
	startExecutionTimeKnownAllocation := make([]*objects.JobAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		if r, _ := ctx.result.GetResult(allocation); r.GetStartExecutionNanoTime() == 0 {
			ctx.result.UpdateStartExecutionTime(allocation, allocation.GetStartExecutionTimeNanoSecond())
		}
		if p.getStartExecutionNanoTimeInSession(ctx, allocation) != 0 {
			startExecutionTimeKnownAllocation = append(startExecutionTimeKnownAllocation, allocation)
		}
	}
	r, err := p.predictSpaceSharingAllocations(ctx, startExecutionTimeKnownAllocation)
	if err != nil {
		return err
	}
	for allocation, finishTime := range r {
		ctx.result.UpdateFinishTime(allocation, finishTime)
	}
	return nil
}

//func (p *DLTBasePredictor) predictGangJobsWithStartExecutionTimeKnown(ctx *PredictSessionContext) error {
//	for _, gangTaskAllocations := range ctx.acceleratorID2GangTaskAllocations {
//		for _, allocation := range gangTaskAllocations {
//			if p.isAllocationPredicted(ctx, allocation) {
//				continue
//			}
//			ctx.result.UpdateStartExecutionTime(allocation, allocation.GetStartExecutionTimeNanoSecond())
//			if p.getStartExecutionNanoTimeInSession(ctx, allocation) != 0 {
//				r, err := p.predictGangInternal(ctx, allocation)
//				if err != nil {
//					return err
//				}
//				ctx.result.UpdateFinishTime(allocation, r)
//			}
//		}
//	}
//	return nil
//}

func (p *DLTBasePredictor) markStartExecutionTime(ctx *PredictSessionContext, allocations []*objects.JobAllocation) bool {
	marked := false
nextAlloc:
	for _, allocation := range allocations {
		if p.isAllocationPredicted(ctx, allocation) {
			continue
		}
		if p.getStartExecutionNanoTimeInSession(ctx, allocation) == 0 && allocation.GetPlaceholder() {
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
			marked = true
			ctx.result.UpdateStartExecutionTime(allocation, int64(startExecutionTime))
		}
	}
	return marked
}

//func (p *DLTBasePredictor) predictGangInternal(ctx *PredictSessionContext, allocation *objects.JobAllocation) (int64, error) {
//	extra, err := p.getGangTaskGroupDLTExtra(ctx, allocation.GetJobID())
//	if err != nil {
//		return 0, err
//	}
//	gangType := extra.GetDLTGangType()
//	if gangType != objects.DLTGangType_DLTGangTypeDataParallel {
//		reason := fmt.Sprintf("Currently, DLT Predictor only supports DataParallel Gang Job")
//		log.Printf(reason)
//		return 0, fmt.Errorf(reason)
//	}
//	return p.predictDataParallel(ctx, allocation)
//}

func (p *DLTBasePredictor) getGangTaskGroupDLTExtra(ctx *PredictSessionContext, jobID string) (*objects.GangTaskGroupDLTExtra, error) {
	bs := ctx.partitionContext.GetUnfinishedJob(jobID).GetTaskGroup().GetTaskGroupInfoBytes()
	info := &objects.GangTaskGroup{}
	err := json.Unmarshal(bs, info)
	if err != nil {
		return nil, err
	}
	extraBs := info.GetExtra()
	extra := &objects.GangTaskGroupDLTExtra{}
	err = json.Unmarshal(extraBs, extra)
	if err != nil {
		return nil, err
	}
	return extra, nil
}

//func (p *DLTBasePredictor) predictDataParallel(ctx *PredictSessionContext, allocation *objects.JobAllocation) (int64, error) {
//	totalMiniBatches := p.impl.getJobTotalMiniBatches(ctx, allocation.GetJobID())
//	// gang task group shares the same start time stamp
//	startNanoTime := p.getStartExecutionNanoTimeInSession(ctx, allocation)
//	duration := totalMiniBatches * p.impl.get(ctx, allocation)
//	if startNanoTime+duration > ctx.endNanoTime {
//		return 0, nil
//	}
//	return startNanoTime + duration, nil
//}

// getStartExecutionNanoTimeInSession 获取到一次会话中，临时存储的allocation的开始执行时间。
// 为什么不能直接用allocation.GetStartExecutionNanoTime？
// 因为placeholder任务的存在，它们不知道自己什么时候开始，我们需要将其他全部任务的结束时间算出后，才能计算他们的开始执行时间。
// 而allocation在Predict中是只读的，我们需要另外的数据结构存储它们临时的开始时间。
func (p *DLTBasePredictor) getStartExecutionNanoTimeInSession(ctx *PredictSessionContext, allocation *objects.JobAllocation) int64 {
	r, _ := ctx.result.GetResult(allocation)
	if r != nil {
		return r.GetStartExecutionNanoTime()
	}
	return r.GetStartExecutionNanoTime()
}

//func (p *DLTBasePredictor) predictSpaceSharingSingleAllocations(ctx *PredictSessionContext, accelerator *objects.Accelerator, allocations []*objects.JobAllocation) (map[*objects.JobAllocation]int64, error) {
//	startNanoTime2Allocations := map[int64][]*objects.JobAllocation{}
//	sortedStartTime := make([]int64, 0, len(allocations))
//	jobID2RemainingMiniBatches := make(map[string]float64)
//	for _, allocation := range allocations {
//		startNanoTime := p.getStartExecutionNanoTimeInSession(ctx, allocation)
//		sortedStartTime = append(sortedStartTime, startNanoTime)
//		if _, ok := startNanoTime2Allocations[startNanoTime]; !ok {
//			startNanoTime2Allocations[startNanoTime] = make([]*objects.JobAllocation, 0, 1)
//		}
//		startNanoTime2Allocations[startNanoTime] = append(startNanoTime2Allocations[startNanoTime], allocation)
//		jobID2RemainingMiniBatches[allocation.GetJobID()] = float64(p.impl.getJobTotalMiniBatches(ctx, allocation.GetJobID()))
//	}
//	utils.SortInt64(sortedStartTime)
//	runningAllocations := make([]*objects.JobAllocation, 0, 2)
//	runningAllocations = append(runningAllocations, startNanoTime2Allocations[sortedStartTime[0]]...)
//	currTime := sortedStartTime[0]
//	if currTime > ctx.endNanoTime {
//		return nil, nil
//	}
//	nextStartTimeIdx := 1
//
//	makeError := func(runningAllocations ...*objects.JobAllocation) error {
//		jobIDs := make([]string, 0, len(runningAllocations))
//		for _, allocation := range runningAllocations {
//			jobIDs = append(jobIDs, allocation.GetJobID())
//		}
//		reason := fmt.Sprintf("predictSpaceSharedSingleAllocations: only two jobs space-sharing is acceptable, but encounter %d space sharing jobs, jobIDs = %+v", len(runningAllocations), jobIDs)
//		log.Printf(reason)
//		return fmt.Errorf(reason)
//	}
//	if len(runningAllocations) > 2 {
//		return nil, makeError(runningAllocations...)
//	}
//	result := make(map[*objects.JobAllocation]int64)
//	for len(result) != len(allocations) {
//		runningJobIDs := make([]string, 0, len(runningAllocations))
//		for _, runningAllocation := range runningAllocations {
//			runningJobIDs = append(runningJobIDs, runningAllocation.GetJobID())
//		}
//		miniBatchDurations, err := p.impl.getSpaceSharingMiniBatchDurationNanoSecond(ctx, accelerator, runningJobIDs)
//		// calculate the closest event: any of jobs finished, or a new job comes in.
//		runningJobFinishTimes := make(map[string]int64)
//		closestFinishedJobTime := int64(math.MaxInt64)
//		for _, runningJobID := range runningJobIDs {
//			miniBatches := jobID2RemainingMiniBatches[runningJobID]
//			miniBatchDuration := miniBatchDurations[runningJobID]
//			finishTime := currTime + int64(math.Ceil(miniBatches*float64(miniBatchDuration)))
//			runningJobFinishTimes[runningJobID] = finishTime
//			if finishTime < closestFinishedJobTime {
//				closestFinishedJobTime = finishTime
//			}
//		}
//		newJobStartTime := int64(math.MaxInt64)
//		if nextStartTimeIdx < len(sortedStartTime) {
//			newJobStartTime = sortedStartTime[nextStartTimeIdx]
//		}
//		if ctx.endNanoTime < newJobStartTime && ctx.endNanoTime < closestFinishedJobTime {
//			// predict endTime is earlier than newJobStartTime and closestFinishedJobTime
//			// which means both events cannot happen earlier then the prediction ends.
//			// This indicates that the prediction is over.
//			return result, nil
//		}
//		passDurationForRunningAllocations := func(currTime int64, duration int64) {
//			resultRunningAllocations := make([]*objects.JobAllocation, 0, len(runningAllocations))
//			for _, allocation := range runningAllocations {
//				duration2Finish := int64(math.Ceil(jobID2RemainingMiniBatches[allocation.GetJobID()] * float64(miniBatchDurations[allocation.GetJobID()])))
//				if duration >= duration2Finish {
//					// finished job.
//					jobID2RemainingMiniBatches[allocation.GetJobID()] = 0
//					result[allocation] = currTime + duration2Finish
//					continue
//				}
//				passedMiniBatches := float64(duration) / float64(miniBatchDurations[allocation.GetJobID()])
//				remainingMiniBatches := jobID2RemainingMiniBatches[allocation.GetJobID()] - passedMiniBatches
//				jobID2RemainingMiniBatches[allocation.GetJobID()] = remainingMiniBatches
//				resultRunningAllocations = append(resultRunningAllocations, allocation)
//			}
//			runningAllocations = resultRunningAllocations
//		}
//		if newJobStartTime < closestFinishedJobTime {
//			// new job comes in is earlier than other job finishes.
//			nextStartTimeIdx++
//			passedDuration := newJobStartTime - currTime
//			passDurationForRunningAllocations(currTime, passedDuration)
//			currTime = newJobStartTime
//			if len(runningAllocations)+len(startNanoTime2Allocations[newJobStartTime]) > 2 {
//				runningAllocations = append(runningAllocations, startNanoTime2Allocations[newJobStartTime]...)
//				return nil, makeError(runningAllocations...)
//			}
//			runningAllocations = append(runningAllocations, startNanoTime2Allocations[newJobStartTime]...)
//		} else {
//			// new job comes in is later than other job finishes.
//			passedDuration := closestFinishedJobTime - currTime
//			passDurationForRunningAllocations(currTime, passedDuration)
//			currTime = closestFinishedJobTime
//		}
//	}
//	return result, nil
//}

func (p *DLTBasePredictor) predictSpaceSharingAllocations(ctx *PredictSessionContext, allocations []*objects.JobAllocation) (map[*objects.JobAllocation]int64, error) {
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
	runningAllocations := make(map[string]*objects.JobAllocation)
	for _, allocation := range startNanoTime2Allocations[sortedStartTime[0]] {
		runningAllocations[allocation.GetJobID()] = allocation
	}
	currTime := sortedStartTime[0]
	if currTime > ctx.endNanoTime {
		return nil, nil
	}
	nextStartTimeIdx := 1

	result := make(map[*objects.JobAllocation]int64)
	getSpaceSharedAllocations := func(runningAllocation *objects.JobAllocation) []*objects.JobAllocation {
		spaceSharedRunningAllocations := make([]*objects.JobAllocation, 0)
		acceleratorIDs := p.getAllocatedAcceleratorIDs(ctx, runningAllocation)
		for _, acceleratorID := range acceleratorIDs {
			acs := ctx.acceleratorID2Allocations[acceleratorID]
			for _, c := range acs {
				if a, ok := runningAllocations[c.GetJobID()]; ok {
					spaceSharedRunningAllocations = append(spaceSharedRunningAllocations, a)
				}
			}
		}
		return spaceSharedRunningAllocations
	}
	for len(result) != len(allocations) {
		jobID2MiniBatchDuration := make(map[string]int64)
		for _, runningAllocation := range runningAllocations {
			if _, ok := jobID2MiniBatchDuration[runningAllocation.GetJobID()]; ok {
				continue
			}
			spaceSharedRunningAllocations := getSpaceSharedAllocations(runningAllocation)
			r, err := p.getSpaceSharingMiniBatchDurationNanoSecond(ctx, spaceSharedRunningAllocations)
			if err != nil {
				return nil, err
			}
			for jobID, miniBatchDuration := range r {
				jobID2MiniBatchDuration[jobID] = miniBatchDuration
			}
		}
		// calculate the closest event: any of jobs finished, or a new job comes in.
		runningJobFinishTimes := make(map[string]int64)
		closestFinishedJobTime := int64(math.MaxInt64)
		for runningJobID := range runningAllocations {
			miniBatches := jobID2RemainingMiniBatches[runningJobID]
			miniBatchDuration := jobID2MiniBatchDuration[runningJobID]
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
			resultRunningAllocations := make(map[string]*objects.JobAllocation)
			for _, allocation := range runningAllocations {
				duration2Finish := int64(math.Ceil(jobID2RemainingMiniBatches[allocation.GetJobID()] * float64(jobID2MiniBatchDuration[allocation.GetJobID()])))
				if duration >= duration2Finish {
					// finished job.
					jobID2RemainingMiniBatches[allocation.GetJobID()] = 0
					result[allocation] = currTime + duration2Finish
					continue
				}
				passedMiniBatches := float64(duration) / float64(jobID2MiniBatchDuration[allocation.GetJobID()])
				remainingMiniBatches := jobID2RemainingMiniBatches[allocation.GetJobID()] - passedMiniBatches
				jobID2RemainingMiniBatches[allocation.GetJobID()] = remainingMiniBatches
				resultRunningAllocations[allocation.GetJobID()] = allocation
			}
			runningAllocations = resultRunningAllocations
		}
		if newJobStartTime < closestFinishedJobTime {
			// new job comes in is earlier than other job finishes.
			nextStartTimeIdx++
			passedDuration := newJobStartTime - currTime
			passDurationForRunningAllocations(currTime, passedDuration)
			currTime = newJobStartTime
			for _, allocation := range startNanoTime2Allocations[newJobStartTime] {
				runningAllocations[allocation.GetJobID()] = allocation
			}
		} else {
			// new job comes in is later than other job finishes.
			passedDuration := closestFinishedJobTime - currTime
			passDurationForRunningAllocations(currTime, passedDuration)
			currTime = closestFinishedJobTime
		}
	}
	return result, nil
}

func (p *DLTBasePredictor) getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getSpaceSharingMiniBatchDurationNanoSecond(ctx *PredictSessionContext, allocations []*objects.JobAllocation) (map[string]int64, error) {
	if len(allocations) > 2 {
		return nil, fmt.Errorf("space sharing only supports two jobs, but received allocations of %d", len(allocations))
	}
	getTaskGroupType := func(allocation *objects.JobAllocation) objects.TaskGroupType {
		return p.getTaskGroup(ctx, allocation.GetJobID()).GetTaskGroupType()
	}
	getJobIDs := func(allocations []*objects.JobAllocation) []string {
		jobIDs := make([]string, 0, len(allocations))
		for _, allocation := range allocations {
			jobIDs = append(jobIDs, allocation.GetJobID())
		}
		return jobIDs
	}
	taskGroupType := getTaskGroupType(allocations[0])
	for _, allocation := range allocations {
		if taskGroupType != getTaskGroupType(allocation) {
			return nil, fmt.Errorf("only supports same type of task group type space sharing")
		}
	}
	acceleratorIDs := p.getAllocatedAcceleratorIDs(ctx, allocations[0])
	for _, allocation := range allocations {
		if utils.StringSliceEquals(acceleratorIDs, p.getAllocatedAcceleratorIDs(ctx, allocation)) {
			return nil, fmt.Errorf("only supports same acceleratorIDs space sharing")
		}
	}
	if taskGroupType == objects.TaskGroupType_taskGroupTypeSingle {
		return p.getSingleTaskSpaceSharingMiniBatchDuration(ctx, acceleratorIDs[0], getJobIDs(allocations)), nil
	} else if taskGroupType == objects.TaskGroupType_taskGroupTypeGang {
		getGangType := func(allocation *objects.JobAllocation) objects.DLTGangType {
			gangTaskGroupExtra, err := p.getGangTaskGroupDLTExtra(ctx, allocations[0].GetJobID())
			if err != nil {
				return objects.DLTGangType_DLTGangTypeUnknown
			}
			return gangTaskGroupExtra.GetDLTGangType()
		}
		gangType := getGangType(allocations[0])
		for _, allocation := range allocations {
			t := getGangType(allocation)
			if t == objects.DLTGangType_DLTGangTypeUnknown || t != gangType {
				return nil, fmt.Errorf("only supports same DLT gang type")
			}
		}
		if gangType == objects.DLTGangType_DLTGangTypeDataParallel {
			return p.getDataParallelTasksSpaceSharingMiniBatchDuration(ctx, acceleratorIDs, getJobIDs(allocations)), nil
		} else {
			return nil, fmt.Errorf("unsupported DLT gang type %s", gangType)
		}
	} else {
		return nil, fmt.Errorf("unsupported task group type %s", taskGroupType)
	}
}

func (p *DLTBasePredictor) getSingleTaskSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorID string, jobIDs []string) map[string]int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getDataParallelTasksSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getTaskGroup(ctx *PredictSessionContext, jobID string) *objects.TaskGroup {
	return ctx.partitionContext.UnfinishedJobs[jobID].GetTaskGroup()
}

func (p *DLTBasePredictor) getAllocatedAcceleratorIDs(ctx *PredictSessionContext, allocation *objects.JobAllocation) []string {
	acceleratorIDs := make([]string, 0)
	for _, taskAllocation := range allocation.GetTaskAllocations() {
		acceleratorIDs = append(acceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
	}
	sort.Strings(acceleratorIDs)
	return acceleratorIDs
}
