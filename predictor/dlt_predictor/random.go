package dlt_predictor

import (
	"UNS/pb_gen/objects"
	"hash/crc32"
)

type RandomPredictor struct {
	*DLTBasePredictor
}

func NewRandomPredictor() *RandomPredictor {
	p := &RandomPredictor{}
	DLTBase := NewDLTBasePredictor(p)
	p.DLTBasePredictor = DLTBase
	return p
}

func (p *RandomPredictor) getDataParallelMiniBatchDurationNanoSecond(ctx *PredictSessionContext, allocation *objects.JobAllocation) int64 {
	acceleratorType := func() string {
		acceleratorID := allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
		return ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorID].GetAcceleratorMetaInfo().GetBriefType()
	}()
	duration := p.getMiniBatchDurationNanoSecond(ctx, allocation.GetJobID(), acceleratorType)
	consolidationPenalty := p.getDataParallelConsolidationPenalty(ctx, allocation)
	return int64(float64(duration) * consolidationPenalty)
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

func (p *RandomPredictor) getMiniBatchDurationNanoSecond(ctx *PredictSessionContext, jobID string, acceleratorType string) int64 {
	parallelCount := int64(len(ctx.partitionContext.GetUnfinishedJob(jobID).GetTaskGroup().GetTasks()))
	acceleratorPenalty := int64(crc32.ChecksumIEEE([]byte(acceleratorType)) % 400 / 100)
	baseDuration := int64(((crc32.ChecksumIEEE([]byte(jobID)))%1000 + 100) * 10e6)
	return acceleratorPenalty * baseDuration / parallelCount
}

func (p *RandomPredictor) getSpaceSharingMiniBatchDurationNanoSecond(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64 {
	if len(jobIDs) == 1 {
		return map[string]int64{jobIDs[0]: p.getMiniBatchDurationNanoSecond(ctx, jobIDs[0], ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorIDs[0]].GetAcceleratorMetaInfo().GetBriefType())}
	}
	if len(jobIDs) != 2 {
		panic("getSpaceSharingMiniBatchDurationNanoSecond jobIDs len must be 1 or 2.")
	}
	nonSpaceSharingDurationSecond := make(map[string]int64)
	for _, jobID := range jobIDs {
		nonSpaceSharingDurationSecond[jobID] = p.getMiniBatchDurationNanoSecond(ctx, jobID, ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorIDs[0]].GetAcceleratorMetaInfo().GetBriefType())
	}
	penaltyFactor0 := float64(int(crc32.ChecksumIEEE([]byte(jobIDs[0]+jobIDs[1])))%400+100) / 100.
	penaltyFactor1 := float64(int(crc32.ChecksumIEEE([]byte(jobIDs[1]+jobIDs[0])))%400+100) / 100.
	penaltyFactors := []float64{penaltyFactor0, penaltyFactor1}
	result := make(map[string]int64)
	for idx, jobID := range jobIDs {
		result[jobID] = int64(penaltyFactors[idx] * float64(nonSpaceSharingDurationSecond[jobID]))
	}
	return result
}

func (p *RandomPredictor) getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) int64 {
	return int64(int(crc32.ChecksumIEEE([]byte(jobID)))%10000 + 1000)
}

func (p *RandomPredictor) getSingleTaskSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorID string, jobIDs []string) map[string]int64 {
	return p.getSpaceSharingMiniBatchDurationNanoSecond(ctx, []string{acceleratorID}, jobIDs)
}

func (p *RandomPredictor) getDataParallelTasksSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64 {
	return p.getSpaceSharingMiniBatchDurationNanoSecond(ctx, acceleratorIDs, jobIDs)
}
