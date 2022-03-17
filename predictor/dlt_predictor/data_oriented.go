package dlt_predictor

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"UNS/utils"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"sort"
	"strings"
)

type DataOrientedPredictor struct {
	*DLTBasePredictor
	configuration *configs.DLTPredictorDataOrientedConfiguration
	data          *configs.DLTPredictorDataOrientedDataFormat
}

func NewDataOrientedPredictor(configuration *configs.DLTPredictorDataOrientedConfiguration) *DataOrientedPredictor {
	p := &DataOrientedPredictor{
		configuration: configuration,
	}
	p.loadData()
	DLTBase := NewDLTBasePredictor(p)
	p.DLTBasePredictor = DLTBase
	return p
}

func (p *DataOrientedPredictor) loadData() {
	bytes, err := ioutil.ReadFile(p.configuration.GetDataSourcePath())
	if err != nil {
		panic(err)
	}
	data := &configs.DLTPredictorDataOrientedDataFormat{}
	err = utils.Unmarshal(string(bytes), data)
	if err != nil {
		panic(err)
	}
	p.data = data
}

func (p *DataOrientedPredictor) getDataParallelMiniBatchDurationNanoSecond(ctx *PredictSessionContext, allocation *objects.JobAllocation) int64 {
	acceleratorType := func() string {
		acceleratorID := allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
		return ctx.partitionContext.View.AcceleratorID2Accelerator[acceleratorID].GetAcceleratorMetaInfo().GetBriefType()
	}()
	duration := p.getMiniBatchDurationNanoSecond(ctx, ctx.partitionContext.GetUnfinishedJob(allocation.GetJobID()), acceleratorType)
	consolidationPenalty := p.getDataParallelConsolidationPenalty(ctx, allocation)
	return int64(float64(duration) * consolidationPenalty)
}

func (p *DataOrientedPredictor) getDataParallelConsolidationPenalty(ctx *PredictSessionContext, allocation *objects.JobAllocation) float64 {
	consolidationLevel := p.getDataParallelConsolidationLevel(ctx, allocation)
	d := p.getDLTJobData(allocation.GetJobID())
	return float64(d.GetConsolidationLevel2Penalties()[int64(consolidationLevel)])
}

func (p *DataOrientedPredictor) getMiniBatchDurationNanoSecond(ctx *PredictSessionContext, job *objects.Job, acceleratorType string) int64 {
	return p.getDLTJobData(job.GetJobID()).GetAcceleratorType2MiniBatchDurationNanoSecond()[acceleratorType]
}

func (p *DataOrientedPredictor) getSpaceSharingMiniBatchDurationNanoSecond(ctx *PredictSessionContext, accelerators []*objects.Accelerator, jobs []*objects.Job) map[string]int64 {
	jobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		jobIDs = append(jobIDs, job.GetJobID())
	}
	if len(jobIDs) == 1 {
		//if jobIDs[0] == "c0cb989683049a1f41b8d6d2" {
		//	fmt.Println("jobID c0cb989683049a1f41b8d6d2")
		//}
		return map[string]int64{jobIDs[0]: p.getMiniBatchDurationNanoSecond(ctx, jobs[0], accelerators[0].GetAcceleratorMetaInfo().GetBriefType())}
	}
	if len(jobIDs) != 2 {
		panic("getSpaceSharingMiniBatchDurationNanoSecond jobIDs len must be 1 or 2.")
	}
	joined := strings.Join(jobIDs, "")
	hashes := crc32.ChecksumIEEE([]byte(joined))
	calculateSharingDurationNanoSecond := func(job *objects.Job) int64 {
		d := p.getDLTJobData(job.GetJobID())
		penaltyRange := float64(d.GetMaxSpaceSharingPenalty() - d.GetMinSpaceSharingPenalty())
		solely := p.getMiniBatchDurationNanoSecond(ctx, job, accelerators[0].GetAcceleratorMetaInfo().GetBriefType())
		ratio := float64(hashes%100) / 100
		calculated := int64(float64(solely) * (float64(d.GetMinSpaceSharingPenalty()) + penaltyRange*ratio))
		return calculated
	}
	result := make(map[string]int64)
	for _, job := range jobs {
		result[job.GetJobID()] = calculateSharingDurationNanoSecond(job)
	}
	return result
}

func (p *DataOrientedPredictor) getJobTotalMiniBatches(ctx *PredictSessionContext, jobID string) int64 {
	return p.getDLTJobData(jobID).GetTotalMiniBatches()
}

func (p *DataOrientedPredictor) getSingleTaskSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorID string, jobIDs []string) map[string]int64 {
	sort.Strings(jobIDs)
	return p.getSpaceSharingMiniBatchDurationNanoSecond(ctx, p.getAccelerators(ctx, []string{acceleratorID}), p.getJobs(ctx, jobIDs))
}

func (p *DataOrientedPredictor) getDataParallelTasksSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorIDs []string, jobIDs []string) map[string]int64 {
	sort.Strings(jobIDs)
	sort.Strings(acceleratorIDs)
	return p.getSpaceSharingMiniBatchDurationNanoSecond(ctx, p.getAccelerators(ctx, acceleratorIDs), p.getJobs(ctx, jobIDs))
}

func (p *DataOrientedPredictor) getDLTJobData(jobID string) *configs.DLTJobData {
	d, ok := p.data.GetJobID2DLTJobData()[jobID]
	if !ok {
		panic(fmt.Sprintf("DataOrientedPredictor getDLTJobData find not-exists jobID %s", jobID))
	}
	return d
}

func (p *DataOrientedPredictor) getMaximumAcceleratorMemoryCostBytes(ctx *PredictSessionContext, jobID string) int64 {
	d := p.getDLTJobData(jobID)
	return d.GetMaximumAcceleratorMemoryCostBytes()
}
