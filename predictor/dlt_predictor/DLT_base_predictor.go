package dlt_predictor

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
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

// DLTBasePredictor
// 支持DLT任务，包含Single和Gang类型的TaskGroup的执行时间预测。
// 仅支持非抢占的task
type DLTBasePredictor struct {
	*base.Base
	impl DLTPredictorTemplate
}

type DLTPredictorTemplate interface {
	getSingleTaskSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorID string, jobIDs []string) map[string]int64
	getDataParallelTasksSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64
	getMaximumAcceleratorMemoryCostBytes(ctx *PredictSessionContext, jobID string) int64
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

// Predict 预测全部allocations的开始时间和结束时间。由于只支持Single和Gang类型的时间预测，所以预测结果仅包含单一数字。
func (p *DLTBasePredictor) Predict(partitionContext *partition.Context, allocations []*objects.JobAllocation) (interfaces.PredictResult, error) {
	r, err := p.PredictByEndTime(partitionContext, allocations, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	r.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		if result.GetFinishNanoTime() == nil {
			panic("incomplete predict result")
		}
	})
	return r, nil
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
	return ctx.result.IsResultComplete(allocation.GetTaskAllocations()[0])
}

func (p *DLTBasePredictor) extractStartTime(allocation *objects.JobAllocation) *int64 {
	t := p.extractRepresentTaskAllocation(allocation).GetStartExecutionTimeNanoSecond()
	if t != nil {
		v := t.GetValue()
		return &v
	}
	return nil
}

func (p *DLTBasePredictor) extractRepresentTaskAllocation(allocation *objects.JobAllocation) *objects.TaskAllocation {
	return allocation.GetTaskAllocations()[0]
}

func (p *DLTBasePredictor) predictAllocationsWithStartExecutionTimeKnown(ctx *PredictSessionContext, allocations []*objects.JobAllocation) error {
	// firstly, predict jobs with TaskGroupType of 'single'
	startExecutionTimeKnownAllocation := make([]*objects.JobAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		taskAllocation := p.extractRepresentTaskAllocation(allocation)
		if taskAllocation.GetStartExecutionTimeNanoSecond() == nil && p.getStartExecutionNanoTimeInSession(ctx, allocation) == nil {
			// skip allocations with unknown start execution time.
			continue
		}
		if p.getStartExecutionNanoTimeInSession(ctx, allocation) == nil {
			p.updateJobStartExecutionTime(ctx, allocation, taskAllocation.GetStartExecutionTimeNanoSecond().GetValue())
		}
		startExecutionTimeKnownAllocation = append(startExecutionTimeKnownAllocation, allocation)
	}
	r, err := p.predictSpaceSharingAllocations(ctx, startExecutionTimeKnownAllocation)
	if err != nil {
		return err
	}
	for allocation, finishTime := range r {
		p.updateJobFinishTime(ctx, allocation, finishTime)
	}
	return nil
}

func (p *DLTBasePredictor) markStartExecutionTime(ctx *PredictSessionContext, allocations []*objects.JobAllocation) bool {
	marked := false
nextAlloc:
	for _, allocation := range allocations {
		if p.isAllocationPredicted(ctx, allocation) {
			continue
		}
		if p.getStartExecutionNanoTimeInSession(ctx, allocation) == nil && p.extractRepresentTaskAllocation(allocation).GetPlaceholder() {
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
					taskAllocation := p.extractRepresentTaskAllocation(previousAllocation)
					if taskAllocation.GetPlaceholder() && taskAllocation.GetStartExecutionTimeNanoSecond() == nil {
						continue
					}
					if !ctx.result.IsResultComplete(taskAllocation) {
						p.updateJobStartExecutionTime(ctx, previousAllocation, 0)
						p.updateJobFinishTime(ctx, previousAllocation, 0)
						//ctx.result.UpdateStartExecutionTime(taskAllocation, 0)
						//ctx.result.UpdateFinishTime(taskAllocation, 0)
						continue nextAlloc
					}
					r := ctx.result.GetResult(taskAllocation)
					startExecutionTime = math.Max(float64(*r.GetFinishNanoTime()), startExecutionTime)
				}
			}
			if startExecutionTime != 0. {
				marked = true
			}
			p.updateJobStartExecutionTime(ctx, allocation, int64(startExecutionTime))
			//ctx.result.UpdateStartExecutionTime(p.extractRepresentTaskAllocation(allocation), int64(startExecutionTime))
		}
	}
	return marked
}

func (p *DLTBasePredictor) getGangTaskGroupDLTExtra(ctx *PredictSessionContext, jobID string) (*objects.GangTaskGroupDLTExtra, error) {
	info := ctx.partitionContext.GetUnfinishedJob(jobID).GetTaskGroup().GetGangTaskGroupInfo()
	extraBs := info.GetExtra()
	extra := &objects.GangTaskGroupDLTExtra{}
	err := json.Unmarshal(extraBs, extra)
	if err != nil {
		return nil, err
	}
	return extra, nil
}

// getStartExecutionNanoTimeInSession 获取到一次会话中，临时存储的allocation的开始执行时间。
// 为什么不能直接用allocation.GetStartExecutionNanoTime？
// 因为placeholder任务的存在，它们不知道自己什么时候开始，我们需要将其他全部任务的结束时间算出后，才能计算他们的开始执行时间。
// 而allocation在Predict中是只读的，我们需要另外的数据结构存储它们临时的开始时间。
func (p *DLTBasePredictor) getStartExecutionNanoTimeInSession(ctx *PredictSessionContext, allocation *objects.JobAllocation) *int64 {
	r := ctx.result.GetResult(p.extractRepresentTaskAllocation(allocation))
	return r.GetStartExecutionNanoTime()
}

func (p *DLTBasePredictor) predictSpaceSharingAllocations(ctx *PredictSessionContext, allocations []*objects.JobAllocation) (map[*objects.JobAllocation]int64, error) {
	if len(allocations) == 0 {
		return map[*objects.JobAllocation]int64{}, nil
	}
	startNanoTime2Allocations := map[int64][]*objects.JobAllocation{}
	sortedStartTime := make([]int64, 0, len(allocations))
	jobID2RemainingMiniBatches := make(map[string]float64)
	for _, allocation := range allocations {
		startNanoTimePtr := p.getStartExecutionNanoTimeInSession(ctx, allocation)
		startNanoTime := *startNanoTimePtr
		sortedStartTime = append(sortedStartTime, startNanoTime)
		if _, ok := startNanoTime2Allocations[startNanoTime]; !ok {
			startNanoTime2Allocations[startNanoTime] = make([]*objects.JobAllocation, 0, 1)
		}
		startNanoTime2Allocations[startNanoTime] = append(startNanoTime2Allocations[startNanoTime], allocation)
		remainingMiniBatches := float64(p.impl.getJobTotalMiniBatches(ctx, allocation.GetJobID()))
		//log.Printf("allocation job ID = %s, remaining mini batches = %f\n", allocation.GetJobID(), remainingMiniBatches)
		jobID2RemainingMiniBatches[allocation.GetJobID()] = remainingMiniBatches
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
		spaceSharedRunningAllocationsMap := make(map[string]*objects.JobAllocation)
		acceleratorIDs := p.getAllocatedAcceleratorIDs(ctx, runningAllocation)
		for _, acceleratorID := range acceleratorIDs {
			acs := ctx.acceleratorID2Allocations[acceleratorID]
			for _, c := range acs {
				if a, ok := runningAllocations[c.GetJobID()]; ok {
					spaceSharedRunningAllocationsMap[a.GetJobID()] = a
				}
			}
		}
		spaceSharedRunningAllocations := make([]*objects.JobAllocation, 0, len(spaceSharedRunningAllocationsMap))
		for _, a := range spaceSharedRunningAllocationsMap {
			spaceSharedRunningAllocations = append(spaceSharedRunningAllocations, a)
		}
		return spaceSharedRunningAllocations
	}
	for len(result) != len(allocations) {
		jobID2MiniBatchDuration := make(map[string]int64)
		if err := p.checkRunningAllocations(ctx, runningAllocations); err != nil {
			return nil, err
		}
		for _, runningAllocation := range runningAllocations {
			if _, ok := jobID2MiniBatchDuration[runningAllocation.GetJobID()]; ok {
				continue
			}
			spaceSharedRunningAllocations := getSpaceSharedAllocations(runningAllocation)
			r, err := p.getSpaceSharingMiniBatchDuration(ctx, spaceSharedRunningAllocations)
			if err != nil {
				return nil, err
			}
			for jobID, miniBatchDuration := range r {
				jobID2MiniBatchDuration[jobID] = miniBatchDuration
			}
		}
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
		if newJobStartTime+1e9 < closestFinishedJobTime {
			// 新任务到来要比任务结束的早
			// 当任务结束与任务开始离得很近时，容易出现舍入误差。
			// 所以，给快要结束的任务提前结束的机会，提前1秒
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

func (p *DLTBasePredictor) getSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, allocations []*objects.JobAllocation) (map[string]int64, error) {
	if len(allocations) > 2 {
		log.Printf("space sharing failed.")
		for _, a := range allocations {
			s, _ := utils.MarshalJsonPB(a)
			log.Printf("%v", s)
		}
		reason := fmt.Sprintf("space sharing only supports two jobs, but received allocations of %d", len(allocations))
		//return interfaces.NonPlaceholderUnsetStartTimeError.Set(reason)
		return nil, interfaces.SpaceSharingMoreThanTwoError.Set(reason)
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
			reason := fmt.Sprintf("only supports same type of task group type space sharing")
			return nil, interfaces.SpaceSharingDiffTaskTypeError.Set(reason)
			//return nil, fmt.Errorf("only supports same type of task group type space sharing")
		}
	}
	acceleratorIDs := p.getAllocatedAcceleratorIDs(ctx, allocations[0])
	for _, allocation := range allocations {
		if !utils.StringSliceEquals(acceleratorIDs, p.getAllocatedAcceleratorIDs(ctx, allocation)) {
			reason := fmt.Sprintf("only supports same acceleratorIDs space sharing")
			return nil, interfaces.SpaceSharingDiffAccIDError.Set(reason)
		}
	}
	if err := p.checkAcceleratorMemoryCapacity(ctx, acceleratorIDs, getJobIDs(allocations)); err != nil {
		return nil, err
	}
	if taskGroupType == objects.TaskGroupType_taskGroupTypeSingle {
		return p.impl.getSingleTaskSpaceSharingMiniBatchDuration(ctx, acceleratorIDs[0], getJobIDs(allocations)), nil
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
			return p.impl.getDataParallelTasksSpaceSharingMiniBatchDuration(ctx, acceleratorIDs, getJobIDs(allocations)), nil
		} else {
			reason := fmt.Sprintf("unsupported DLT gang type %s", gangType)
			return nil, interfaces.UnsupportedDLTGangTypeError.Set(reason)
		}
	} else {
		reason := fmt.Sprintf("unsupported task group type %s", taskGroupType)
		return nil, interfaces.UnsupportedTaskGroupTypeError.Set(reason)
	}
}

func (p *DLTBasePredictor) getSingleTaskSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorID string, jobIDs []string) map[string]int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getDataParallelTasksSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) getTaskGroup(ctx *PredictSessionContext, jobID string) *objects.TaskGroup {
	return ctx.partitionContext.GetUnfinishedJob(jobID).GetTaskGroup()
}

func (p *DLTBasePredictor) getAllocatedAcceleratorIDs(ctx *PredictSessionContext, allocation *objects.JobAllocation) []string {
	return pb_gen.GetAllocatedAcceleratorIDs(allocation)
}

func (p *DLTBasePredictor) getJob(ctx *PredictSessionContext, jobID string) *objects.Job {
	return ctx.partitionContext.GetUnfinishedJob(jobID)
}

func (p *DLTBasePredictor) getJobs(ctx *PredictSessionContext, jobIDs []string) []*objects.Job {
	jobs := make([]*objects.Job, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		jobs = append(jobs, p.getJob(ctx, jobID))
	}
	return jobs
}

func (p *DLTBasePredictor) getAccelerator(ctx *PredictSessionContext, acceleratorID string) *objects.Accelerator {
	return ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorID]
}

func (p *DLTBasePredictor) getAccelerators(ctx *PredictSessionContext, acceleratorIDs []string) []*objects.Accelerator {
	accelerators := make([]*objects.Accelerator, 0, len(acceleratorIDs))
	for _, acceleratorID := range acceleratorIDs {
		accelerators = append(accelerators, p.getAccelerator(ctx, acceleratorID))
	}
	return accelerators
}

func (p *DLTBasePredictor) getDataParallelConsolidationLevel(ctx *PredictSessionContext, allocation *objects.JobAllocation) configs.ConsolidationLevel {
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
		return configs.ConsolidationLevel_DiffNode
	}
	if len(CPUSocketIDs) > 1 {
		return configs.ConsolidationLevel_DiffCPUSocket
	}
	return configs.ConsolidationLevel_SameCPUSocket
}

func (p *DLTBasePredictor) getMaximumAcceleratorMemoryCostBytes(ctx *PredictSessionContext, jobID string) int64 {
	panic("template method.")
}

func (p *DLTBasePredictor) checkAcceleratorMemoryCapacity(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) error {
	acceleratorTypes := make(map[string]bool)
	for _, acceleratorID := range acceleratorIDs {
		acc := p.getAccelerator(ctx, acceleratorID)
		t := acc.GetAcceleratorMetaInfo().GetBriefType()
		if acceleratorTypes[t] {
			continue
		}
		remainingBytes := acc.GetAcceleratorMetaInfo().GetAcceleratorMemory().GetBytesCapacity()
		for _, jobID := range jobIDs {
			bytes := p.impl.getMaximumAcceleratorMemoryCostBytes(ctx, jobID)
			if bytes > remainingBytes {
				reason := fmt.Sprintf("DLTBasePredictor checkAcceleratorMemoryCapacity accelerator memory not enough, accelerator type is %s", t)
				log.Printf(reason)
				return interfaces.SpaceSharingOutOfMemoryError.Set(reason)
			}
			remainingBytes -= bytes
		}
	}
	return nil
}

func (p *DLTBasePredictor) updateJobStartExecutionTime(ctx *PredictSessionContext, jobAllocation *objects.JobAllocation, value int64) {
	p.rangeAllTaskAllocations(jobAllocation, func(taskAllocation *objects.TaskAllocation) {
		ctx.result.UpdateStartExecutionTime(taskAllocation, value)
	})
}

func (p *DLTBasePredictor) updateJobFinishTime(ctx *PredictSessionContext, jobAllocation *objects.JobAllocation, value int64) {
	p.rangeAllTaskAllocations(jobAllocation, func(taskAllocation *objects.TaskAllocation) {
		ctx.result.UpdateFinishTime(taskAllocation, value)
	})
}

func (p *DLTBasePredictor) rangeAllTaskAllocations(jobAllocation *objects.JobAllocation, do func(taskAllocation *objects.TaskAllocation)) {
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		do(taskAllocation)
	}
}

// checkRunningAllocations 检查同时运行的allocation是否合法。
// 目前主要检查的是：若当有分布式的allocation，且它们占用了不止一个节点时，
// 则不允许它们共享同一个节点：理由是，当多个分布式任务占用同一个节点时，它们的网络带宽会造成arbitrary的性能下降
func (p *DLTBasePredictor) checkRunningAllocations(ctx *PredictSessionContext, runningAllocations map[string]*objects.JobAllocation) error {
	spanNodesAllocationsNodeIDs := make(map[string]bool, 0)
	for _, allocation := range runningAllocations {
		if len(allocation.GetTaskAllocations()) <= 1 {
			continue
		}
		nodeIDs := make(map[string]bool)
		for _, taskAllocation := range allocation.GetTaskAllocations() {
			nodeIDs[taskAllocation.GetNodeID()] = true
		}
		if len(nodeIDs) <= 1 {
			// 忽略仅在一个节点内的分布式任务
			continue
		}
		for nodeID := range nodeIDs {
			if spanNodesAllocationsNodeIDs[nodeID] {
				// 当该节点已经被spanNodesAllocationsNodeIDs记录过时，则表明当前的任务与之前的任务共享了节点，不允许这样的分配发生.
				reason := fmt.Sprintf("DLTBasePredictor finds multiple jobs which span multiple nodes sharing the same node.")
				return interfaces.MultiSpanNodesGangTasksError.Set(reason)
			}
			spanNodesAllocationsNodeIDs[nodeID] = true
		}
	}
	return nil
}
