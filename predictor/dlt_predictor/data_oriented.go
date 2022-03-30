package dlt_predictor

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"UNS/utils"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
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

func (p *DataOrientedPredictor) getDataParallelMiniBatchDurationNanoSecond(ctx *PredictSessionContext, allocation *pb_gen.JobAllocation) int64 {
	acceleratorType := func() string {
		acceleratorID := allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
		return ctx.partitionContext.MetalViews.AcceleratorID2Accelerator[acceleratorID].GetAcceleratorMetaInfo().GetBriefType()
	}()
	d := p.getDLTJobData(allocation.GetJobID())
	duration := p.getMiniBatchDurationNanoSecond(ctx, allocation.GetJobID(), acceleratorType)
	if d.GetConsolidationLevel2MiniBatchDurations() != nil {
		durations := d.GetConsolidationLevel2MiniBatchDurations()[int64(p.getDataParallelConsolidationLevel(ctx, allocation))]
		return durations.GetAccType2Duration()[acceleratorType]
	}
	consolidationPenalty := p.getDataParallelConsolidationPenalty(ctx, allocation)
	return int64(float64(duration) * consolidationPenalty)
}

func (p *DataOrientedPredictor) getDataParallelConsolidationPenalty(ctx *PredictSessionContext, allocation *pb_gen.JobAllocation) float64 {
	consolidationLevel := p.getDataParallelConsolidationLevel(ctx, allocation)
	d := p.getDLTJobData(allocation.GetJobID())
	return float64(d.GetConsolidationLevel2Penalties()[int64(consolidationLevel)])
}

func (p *DataOrientedPredictor) getMiniBatchDurationNanoSecond(ctx *PredictSessionContext, jobID string, acceleratorType string) int64 {
	return p.getDLTJobData(jobID).GetAcceleratorType2MiniBatchDuration().GetAccType2Duration()[acceleratorType]
}

func (p *DataOrientedPredictor) getSpaceSharingMiniBatchDurationNanoSecond(ctx *PredictSessionContext, accelerators []*objects.Accelerator, jobs []*objects.Job) map[string]int64 {
	jobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		jobIDs = append(jobIDs, job.GetJobID())
	}
	if len(jobIDs) == 1 {
		return map[string]int64{jobIDs[0]: p.getMiniBatchDurationNanoSecond(ctx, jobs[0].GetJobID(), accelerators[0].GetAcceleratorMetaInfo().GetBriefType())}
	}
	if len(jobIDs) != 2 {
		panic("getSpaceSharingMiniBatchDuration jobIDs len must be 1 or 2.")
	}
	acceleratorType := accelerators[0].GetAcceleratorMetaInfo().GetBriefType()
	if result := p.getSpecifiedSpaceSharingMiniBatchDuration(ctx, acceleratorType, jobIDs); len(result) == len(jobs) {
		return result
	} else {
		return p.getSampledSpaceSharingMiniBatchDuration(ctx, acceleratorType, jobIDs)
	}

}

func (p *DataOrientedPredictor) getSampledSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorType string, jobIDs []string) map[string]int64 {
	joined := strings.Join(jobIDs, "")
	hashes := crc32.ChecksumIEEE([]byte(joined))
	calculateSharingDurationNanoSecond := func(jobID string) int64 {
		d := p.getDLTJobData(jobID)
		penaltyRange := float64(d.GetMaxSpaceSharingPenalty() - d.GetMinSpaceSharingPenalty())
		solely := p.getMiniBatchDurationNanoSecond(ctx, jobID, acceleratorType)
		ratio := float64(hashes%100) / 100
		calculated := int64(float64(solely) * (float64(d.GetMinSpaceSharingPenalty()) + penaltyRange*ratio))
		return calculated
	}
	result := make(map[string]int64)
	for _, jobID := range jobIDs {
		result[jobID] = calculateSharingDurationNanoSecond(jobID)
	}
	return result
}

func (p *DataOrientedPredictor) getSpecifiedSpaceSharingMiniBatchDuration(ctx *PredictSessionContext, acceleratorType string, jobIDs []string) map[string]int64 {
	result := make(map[string]int64)
	for i := 0; i < 2; i++ {
		selfID := jobIDs[i]
		peerID := jobIDs[1-i]
		d := p.getDLTJobData(selfID)
		durations := d.GetSpaceSharingMiniBatchDurations()[peerID]
		if durations == nil {
			continue
		}
		result[selfID] = durations.GetAccType2Duration()[acceleratorType]
	}
	return result
}

func (p *DataOrientedPredictor) getJobTotalMiniBatches(jobID string) int64 {
	return p.getDLTJobData(jobID).GetTotalMiniBatches()
}

func (p *DataOrientedPredictor) getSolelyFastestMiniBatchDuration(jobID string) int64 {
	accType2MiniBatchDuration := p.getDLTJobData(jobID).GetAcceleratorType2MiniBatchDuration().GetAccType2Duration()
	min := int64(math.MaxInt64)
	for _, duration := range accType2MiniBatchDuration {
		if duration < min {
			min = duration
		}
	}
	return min
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
