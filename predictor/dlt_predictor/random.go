package dlt_predictor

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/base"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"math"
)

type RandomPredictor struct {
	*DLTBasePredictor
}

func NewRandomPredictor() *RandomPredictor {
	return &RandomPredictor{
		DLTBasePredictor: NewDLTBasePredictor(),
	}
}

type PredictSessionContext struct {
	partitionContext                    *partition.Context
	result                              *base.PredictResult
	acceleratorID2Allocations           map[string][]*objects.JobAllocation
	acceleratorID2SingleTaskAllocations map[string][]*objects.JobAllocation
	acceleratorID2GangTaskAllocations   map[string][]*objects.JobAllocation
}

func (p *RandomPredictor) buildPredictSessionContext(partitionContext *partition.Context, allocations []*objects.JobAllocation) *PredictSessionContext {
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
		result:                              result,
		acceleratorID2Allocations:           acceleratorID2Allocations,
		acceleratorID2SingleTaskAllocations: acceleratorID2SingleTaskAllocations,
		acceleratorID2GangTaskAllocations:   acceleratorID2GangTaskAllocations,
	}
}

func (p *RandomPredictor) Predict(partitionContext *partition.Context, allocations []*objects.JobAllocation) (interfaces.PredictResult, error) {
	if err := p.PrerequisiteCheck(partitionContext, allocations); err != nil {
		return nil, err
	}
	ctx := p.buildPredictSessionContext(partitionContext, allocations)
	//now := partitionContext.Now()
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

func (p *RandomPredictor) isAllocationPredicted(ctx *PredictSessionContext, allocation *objects.JobAllocation) bool {
	if r := ctx.result.GetResult(allocation); r != nil && r.GetStartExecutionTime() != 0 && r.GetFinishTime() != 0 {
		return true
	}
	return false
}

func (p *RandomPredictor) predictSingleJobs(ctx *PredictSessionContext) error {
	// firstly, predict jobs with TaskGroupType of 'single'
	for acceleratorID, singleTaskJobAllocations := range ctx.acceleratorID2SingleTaskAllocations {
		accelerator := ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorID]
		for _, allocation := range singleTaskJobAllocations {
			ctx.result.UpdateStartExecutionTime(allocation, float64(allocation.GetStartExecutionTimeSecond()))
		}
		r, err := p.predictSpaceSharingSingleAllocations(ctx, accelerator, singleTaskJobAllocations)
		if err != nil {
			return err
		}
		for allocation, endTime := range r {
			ctx.result.UpdateFinishTime(allocation, endTime)
		}
	}
	return nil
}

func (p *RandomPredictor) predictGangJobsWithStartExecutionTimeKnown(ctx *PredictSessionContext) error {
	for _, gangTaskAllocations := range ctx.acceleratorID2GangTaskAllocations {
		for _, allocation := range gangTaskAllocations {
			if p.isAllocationPredicted(ctx, allocation) {
				continue
			}
			ctx.result.UpdateStartExecutionTime(allocation, float64(allocation.GetStartExecutionTimeSecond()))
			if p.getStartExecutionTimeInSession(ctx, allocation) != 0 {
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

func (p *RandomPredictor) predictGangJobsWithPlaceholder(ctx *PredictSessionContext) error {
	for _, gangTaskAllocations := range ctx.acceleratorID2GangTaskAllocations {
		for _, allocation := range gangTaskAllocations {
			if p.isAllocationPredicted(ctx, allocation) {
				continue
			}
			if allocation.GetStartExecutionTimeSecond() == 0 && allocation.GetPlaceholder() {
				placeholderAcceleratorIDs := make([]string, 0, len(allocation.GetTaskAllocations()))
				for _, taskAllocation := range allocation.GetTaskAllocations() {
					placeholderAcceleratorIDs = append(placeholderAcceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
				}
				// check all jobs running on placeholder accelerators, find the latest finished one of them
				startExecutionTime := 0.
				for _, acceleratorID := range placeholderAcceleratorIDs {
					previousAllocations := ctx.acceleratorID2Allocations[acceleratorID]
					for _, previousAllocation := range previousAllocations {
						if !p.isAllocationPredicted(ctx, previousAllocation) {
							reason := fmt.Sprintf("predict placeholder gang jobs failed, find unpredicted previous jobs, cannot make sure the startExecutionTime of the gang job. the unfinished job allocation jobID = %s, placeholder gang jobID = %s", previousAllocation.GetJobID(), allocation.GetJobID())
							log.Printf(reason)
							return fmt.Errorf(reason)
						}
						startExecutionTime = math.Max(ctx.result.GetResult(previousAllocation).GetFinishTime(), startExecutionTime)
					}
				}
				ctx.result.UpdateStartExecutionTime(allocation, startExecutionTime)
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

func (p *RandomPredictor) predictGangInternal(ctx *PredictSessionContext, allocation *objects.JobAllocation) (float64, error) {
	bs := allocation.GetExtra()
	extra := &objects.GangTaskGroupDLTExtra{}
	err := json.Unmarshal(bs, extra)
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

func (p *RandomPredictor) predictDataParallel(ctx *PredictSessionContext, allocation *objects.JobAllocation) (float64, error) {
	extra, err := ctx.partitionContext.ExtractDLTJobExtra(ctx.partitionContext.GetUnfinishedJob(allocation.GetJobID()))
	if err != nil {
		return 0, err
	}
	totalMiniBatches := extra.GetTotalMiniBatches()
	// gang task group shares the same start time stamp
	startTime := p.getStartExecutionTimeInSession(ctx, allocation)
	duration := float64(totalMiniBatches) / p.getDataParallelMiniBatchDurationSecond(ctx, allocation)
	return startTime + duration, nil
}

func (p *RandomPredictor) getDataParallelMiniBatchDurationSecond(ctx *PredictSessionContext, allocation *objects.JobAllocation) float64 {
	acceleratorType := func() string {
		acceleratorID := allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
		return ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorID].GetAcceleratorMetaInfo().GetBriefType()
	}()
	duration := p.getMiniBatchDurationSecond(ctx, allocation.GetJobID(), acceleratorType)
	consolidationPenalty := p.getDataParallelConsolidationPenalty(ctx, allocation)
	return duration * consolidationPenalty
}

func (p *RandomPredictor) getDataParallelConsolidationPenalty(ctx *PredictSessionContext, allocation *objects.JobAllocation) float64 {
	nodeIDs := make(map[string]bool)
	CPUSocketIDs := make(map[string]bool)

nextAlloc:
	for _, taskAllocation := range allocation.GetTaskAllocations() {
		nodeID := taskAllocation.GetNodeID()
		nodeIDs[nodeID] = true
		acceleratorAllocation := taskAllocation.GetAcceleratorAllocation()
		accID := acceleratorAllocation.GetAcceleratorID()
		node := ctx.partitionContext.View.NodeID2Node[nodeID]
		for _, CPUSocket := range node.GetCPUSockets() {
			for _, nodeAccelerator := range CPUSocket.GetAccelerators() {
				if nodeAccelerator.GetAcceleratorID() == accID {
					CPUSocketIDs[CPUSocket.GetCPUSocketID()] = true
					continue nextAlloc
				}
			}
		}
	}
	if len(nodeIDs) > 1 {
		return 1.1
	}
	if len(CPUSocketIDs) > 1 {
		return 1.05
	}
	return 1
}

func (p *RandomPredictor) getStartExecutionTimeInSession(ctx *PredictSessionContext, allocation *objects.JobAllocation) float64 {
	return ctx.result.GetResult(allocation).GetStartExecutionTime()
}

func (p *RandomPredictor) getMiniBatchDurationSecond(ctx *PredictSessionContext, jobID string, acceleratorType string) float64 {
	acceleratorPenalty := float64(crc32.ChecksumIEEE([]byte(acceleratorType)) % 400 / 100.)
	baseDuration := float64(int(crc32.ChecksumIEEE([]byte(jobID)))%1000+100) / 10000.
	return acceleratorPenalty * baseDuration
}

func (p *RandomPredictor) predictSpaceSharingSingleAllocations(ctx *PredictSessionContext, accelerator *objects.Accelerator, allocations []*objects.JobAllocation) (map[*objects.JobAllocation]float64, error) {
	startTime2Allocations := map[float64][]*objects.JobAllocation{}
	sortedStartTime := make([]float64, 0, len(allocations))
	jobID2RemainingMiniBatches := make(map[string]float64)
	for _, allocation := range allocations {
		startTime := p.getStartExecutionTimeInSession(ctx, allocation)
		if _, ok := startTime2Allocations[startTime]; !ok {
			startTime2Allocations[startTime] = make([]*objects.JobAllocation, 0, 1)
		}
		jobID2RemainingMiniBatches[allocation.GetJobID()] = p.getJobTotalMiniBatches(ctx, allocation.GetJobID())
	}
	utils.SortFloat64(sortedStartTime)
	runningAllocations := make([]*objects.JobAllocation, 0, 2)
	runningAllocations = append(runningAllocations, startTime2Allocations[sortedStartTime[0]]...)
	currTime := sortedStartTime[0]
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
	result := make(map[*objects.JobAllocation]float64)
	for len(result) != len(allocations) {
		runningJobIDs := make([]string, 0, len(runningAllocations))
		for _, runningAllocation := range runningAllocations {
			runningJobIDs = append(runningJobIDs, runningAllocation.GetJobID())
		}
		miniBatchDurations := p.getSpaceSharingMiniBatchDurationSecond(ctx, accelerator, runningJobIDs)
		// calculate the closest event: any of jobs finished, or a new job comes in.
		runningJobFinishTimes := make(map[string]float64)
		closestFinishedJobTime := math.MaxFloat64
		for _, runningJobID := range runningJobIDs {
			miniBatches := jobID2RemainingMiniBatches[runningJobID]
			miniBatchDuration := miniBatchDurations[runningJobID]
			finishTime := currTime + miniBatches*miniBatchDuration
			runningJobFinishTimes[runningJobID] = finishTime
			if finishTime < closestFinishedJobTime {
				closestFinishedJobTime = finishTime
			}
		}
		newJobStartTime := math.MaxFloat64
		if nextStartTimeIdx < len(sortedStartTime) {
			newJobStartTime = sortedStartTime[nextStartTimeIdx]
		}
		passDurationForRunningAllocations := func(currTime float64, duration float64) {
			resultRunningAllocations := make([]*objects.JobAllocation, 0, len(runningAllocations))
			for _, allocation := range runningAllocations {
				duration2Finish := jobID2RemainingMiniBatches[allocation.GetJobID()] * miniBatchDurations[allocation.GetJobID()]
				if duration >= duration2Finish {
					// finished job.
					jobID2RemainingMiniBatches[allocation.GetJobID()] = 0
					result[allocation] = currTime + duration2Finish
					continue
				}
				passedMiniBatches := duration / miniBatchDurations[allocation.GetJobID()]
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
			if len(runningAllocations)+len(startTime2Allocations[newJobStartTime]) > 2 {
				runningAllocations = append(runningAllocations, startTime2Allocations[newJobStartTime]...)
				return nil, makeError(runningAllocations...)
			}
			runningAllocations = append(runningAllocations, startTime2Allocations[newJobStartTime]...)
		} else {
			// new job comes in is later than other job finishes.
			passedDuration := closestFinishedJobTime - currTime
			passDurationForRunningAllocations(currTime, passedDuration)
			currTime = closestFinishedJobTime
		}
	}
	return result, nil
}

func (p *RandomPredictor) getSpaceSharingMiniBatchDurationSecond(ctx *PredictSessionContext, accelerator *objects.Accelerator, jobIDs []string) map[string]float64 {
	if len(jobIDs) == 1 {
		return map[string]float64{jobIDs[0]: p.getMiniBatchDurationSecond(ctx, jobIDs[0], accelerator.GetAcceleratorMetaInfo().GetBriefType())}
	}
	if len(jobIDs) != 2 {
		panic("getSpaceSharingMiniBatchDurationSecond jobIDs len must be 1 or 2.")
	}
	nonSpaceSharingDurationSecond := make(map[string]float64)
	for _, jobID := range jobIDs {
		nonSpaceSharingDurationSecond[jobID] = p.getMiniBatchDurationSecond(ctx, jobID, accelerator.GetAcceleratorMetaInfo().GetBriefType())
	}
	penaltyFactor0 := float64(int(crc32.ChecksumIEEE([]byte(jobIDs[0]+jobIDs[1])))%400+100) / 100.
	penaltyFactor1 := float64(int(crc32.ChecksumIEEE([]byte(jobIDs[1]+jobIDs[2])))%400+100) / 100.
	penaltyFactors := []float64{penaltyFactor0, penaltyFactor1}
	result := make(map[string]float64)
	for idx, jobID := range jobIDs {
		result[jobID] = penaltyFactors[idx] * nonSpaceSharingDurationSecond[jobID]
	}
	return result
}

func (p *RandomPredictor) getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) float64 {
	return float64(int(crc32.ChecksumIEEE([]byte(jobID)))%10000 + 1000)
}
