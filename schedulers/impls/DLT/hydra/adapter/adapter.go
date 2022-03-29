package adapter

import (
	"UNS/pb_gen"
	"UNS/pb_gen/objects"
	predictorinterfaces "UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/hydra/types"
	"UNS/schedulers/partition"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"strconv"
)

var getCachedJobExecutionTime, addJobExecutionTimeCache = func() (func(jobID string) map[types.GPUType]int64, func(jobID string, type2ExecutionTime map[types.GPUType]int64)) {
	cache := make(map[string]map[types.GPUType]int64)
	return func(jobID string) map[types.GPUType]int64 {
			return cache[jobID]
		}, func(jobID string, type2ExecutionTime map[types.GPUType]int64) {
			cache[jobID] = type2ExecutionTime
		}
}()

var GPUTypes = func() func(ctx *partition.Context) []types.GPUType {
	GPUTypes := make([]types.GPUType, 0)
	return func(ctx *partition.Context) []types.GPUType {
		if len(GPUTypes) == 0 {
			accTypes := make(map[string]bool)
			for _, acc := range ctx.MetalViews.AcceleratorID2Accelerator {
				accTypes[acc.GetAcceleratorMetaInfo().GetBriefType()] = true
			}
			for t := range accTypes {
				GPUTypes = append(GPUTypes, types.GPUType(t))
			}
		}
		return GPUTypes
	}
}()

var GPUs = func() func(ctx *partition.Context) map[types.GPUID]types.GPU {
	GPUs := make(map[types.GPUID]types.GPU)
	return func(ctx *partition.Context) map[types.GPUID]types.GPU {
		if len(GPUs) == 0 {
			for accID, acc := range ctx.MetalViews.AcceleratorID2Accelerator {
				GPUs[GPUID(accID)] = &GPU{acc: acc}
			}
		}
		return GPUs
	}
}()

var GPUID, AccID = func() (func(AccID string) types.GPUID, func(GPUID types.GPUID) string) {
	accID2GPUID := make(map[string]types.GPUID)
	GPUID2AccID := make(map[types.GPUID]string)
	i := 0
	return func(AccID string) types.GPUID {
			if v, ok := accID2GPUID[AccID]; ok {
				return v
			}
			i++
			gpuID := types.GPUID(i)
			accID2GPUID[AccID] = gpuID
			GPUID2AccID[gpuID] = AccID
			return accID2GPUID[AccID]
		}, func(gpuID types.GPUID) string {
			return GPUID2AccID[gpuID]
		}
}()

var GPUType2ProfileAccID = func() func(ctx *partition.Context) map[types.GPUType]string {
	GPUType2ProfileAccID := make(map[types.GPUType]string)
	return func(ctx *partition.Context) map[types.GPUType]string {
		if len(GPUType2ProfileAccID) == 0 {
			for accID, acc := range ctx.MetalViews.AcceleratorID2Accelerator {
				t := acc.GetAcceleratorMetaInfo().GetBriefType()
				if _, ok := GPUType2ProfileAccID[types.GPUType(t)]; ok {
					continue
				}
				GPUType2ProfileAccID[types.GPUType(t)] = accID
			}
		}
		return GPUType2ProfileAccID
	}
}()

type ScheduleContext struct {
	PC                   *partition.Context
	Cluster              types.Cluster
	UnallocatedJobMetas  []types.JobMeta
	JobMetas             []types.JobMeta
	JobMetasMap          map[types.JobName]types.JobMeta
	JobID2RemainingTime  map[string]map[types.GPUType]int64
	JobID2RemainingRatio map[string]float64
}

func BuildScheduleContext(pc *partition.Context, predictor predictorinterfaces.Predictor) *ScheduleContext {
	ctx := &ScheduleContext{
		PC: pc,
	}

	unProfiled := make([]*objects.Job, 0)
	for _, job := range pc.UnfinishedJobs {
		if getCachedJobExecutionTime(job.GetJobID()) == nil {
			unProfiled = append(unProfiled, job)
		}
	}
	profileJobs(pc, predictor, unProfiled)
	pr, err := predictor.Predict(pc, pc.GetAllocationsSlice())
	if err != nil {
		panic(err)
	}
	jobID2RemainingTime := make(map[string]map[types.GPUType]int64)
	jobID2RemainingRatio := make(map[string]float64)
	now := pc.FixedNow()
	pr.Range(func(allocation *objects.TaskAllocation, result predictorinterfaces.EachPredictResult) {
		jobID := allocation.GetJobID()
		if _, ok := jobID2RemainingTime[jobID]; ok {
			return
		}
		finishTime := *result.GetFinishNanoTime()
		startTime := *result.GetStartExecutionNanoTime()
		remainingTime := finishTime - now
		remainingRatio := float64(remainingTime) / float64(finishTime-startTime)
		jobID2RemainingTime[jobID] = map[types.GPUType]int64{getGPUType(pc, allocation): remainingTime}
		jobID2RemainingRatio[jobID] = remainingRatio
	})
	for _, job := range pc.AllocationViews.UnallocatedJobs {
		jobID := job.GetJobID()
		if _, ok := jobID2RemainingTime[jobID]; ok {
			continue
		}
		jobID2RemainingTime[jobID] = getCachedJobExecutionTime(jobID)
		jobID2RemainingRatio[jobID] = 1.
	}
	ctx.JobID2RemainingRatio = jobID2RemainingRatio
	ctx.JobID2RemainingTime = jobID2RemainingTime
	unallocatedJobs := ctx.PC.AllocationViews.UnallocatedJobs
	jobMetas, unallocatedJobMetas := func() ([]types.JobMeta, []types.JobMeta) {
		metas := make([]types.JobMeta, 0)
		unallocatedJobMetas := make([]types.JobMeta, 0)
		for _, job := range ctx.PC.UnfinishedJobs {
			meta := &JobMeta{job}
			metas = append(metas, meta)
			if _, ok := unallocatedJobs[job.GetJobID()]; ok {
				unallocatedJobMetas = append(unallocatedJobMetas, meta)
			}
		}
		return metas, unallocatedJobMetas
	}()
	jobMetasMap := func() map[types.JobName]types.JobMeta {
		r := make(map[types.JobName]types.JobMeta)
		for _, meta := range jobMetas {
			r[meta.JobName()] = meta
		}
		return r
	}()
	ctx.JobMetas = jobMetas
	ctx.UnallocatedJobMetas = unallocatedJobMetas
	ctx.JobMetasMap = jobMetasMap
	gpus := GPUs(pc)
	jobQueues := make(map[types.GPUID]types.GPUJobQueue)
	for gpuID, g := range gpus {
		jobQueues[gpuID] = &GPUJobQueue{
			g:    g,
			jobs: make([]types.Job, 0),
		}
	}
	for _, jobAllocation := range ctx.PC.Allocations {
		g := getGPU(pc, jobAllocation.GetTaskAllocations()[0])
		jobQueues[g.ID()].SetJobs(&Job{
			ctx: ctx,
			Job: pc.GetJob(jobAllocation.GetJobID()),
		})
	}
	c := &Cluster{
		ctx:       ctx,
		jobQueues: jobQueues,
		gpus:      gpus,
	}
	ctx.Cluster = c
	return ctx
}

func getGPUType(pc *partition.Context, allocation *objects.TaskAllocation) types.GPUType {
	return types.GPUType(pc.MetalViews.AcceleratorID2Accelerator[allocation.GetAcceleratorAllocation().GetAcceleratorID()].GetAcceleratorMetaInfo().GetBriefType())
}

func getGPUID(allocation *objects.TaskAllocation) types.GPUID {
	return GPUID(allocation.GetAcceleratorAllocation().GetAcceleratorID())
}

func getGPU(pc *partition.Context, allocation *objects.TaskAllocation) types.GPU {
	return GPUs(pc)[getGPUID(allocation)]
}

func profileJobs(pc *partition.Context, predictor predictorinterfaces.Predictor, jobs []*objects.Job) {
	buildAllocation := func(job *objects.Job, gpuType types.GPUType) *pb_gen.JobAllocation {
		task := job.GetTaskGroup().GetTasks()[0]
		taskAllocation := &objects.TaskAllocation{
			NodeID:                       pc.MetalViews.AcceleratorID2NodeID[GPUType2ProfileAccID(pc)[gpuType]],
			JobID:                        job.GetJobID(),
			TaskID:                       task.GetTaskID(),
			StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 0},
			AcceleratorAllocation: &objects.AcceleratorAllocation{
				AcceleratorID: GPUType2ProfileAccID(pc)[gpuType],
			},
		}
		return &pb_gen.JobAllocation{
			JobAllocation: &objects.JobAllocation{
				JobID:             job.GetJobID(),
				ResourceManagerID: pc.Meta.GetResourceManagerID(),
				PartitionID:       pc.Meta.GetPartitionID(),
				TaskAllocations:   []*objects.TaskAllocation{taskAllocation}},
		}
	}
	jobAllocations := make([]*pb_gen.JobAllocation, 0, len(jobs))
	for _, gpuType := range GPUTypes(pc) {
		for _, job := range jobs {
			jobAllocations = append(jobAllocations, buildAllocation(job, gpuType))
		}
	}
	pr, err := predictor.PredictSolely(pc, jobAllocations)
	if err != nil {
		panic(fmt.Sprintf("hydra profileJobs failed, err = %v", err))
	}
	jobID2ExecutionTime := make(map[string]map[types.GPUType]int64)
	pr.Range(func(allocation *objects.TaskAllocation, result predictorinterfaces.EachPredictResult) {
		jobID := allocation.GetJobID()
		if jobID2ExecutionTime[jobID] == nil {
			jobID2ExecutionTime[jobID] = make(map[types.GPUType]int64)
		}
		gpuType := pc.MetalViews.AcceleratorID2Accelerator[allocation.GetAcceleratorAllocation().GetAcceleratorID()].GetAcceleratorMetaInfo().GetBriefType()
		jobID2ExecutionTime[jobID][types.GPUType(gpuType)] = *result.GetFinishNanoTime() - *result.GetStartExecutionNanoTime()
	})
	for jobID, executionTimes := range jobID2ExecutionTime {
		addJobExecutionTimeCache(jobID, executionTimes)
	}
}

type Cluster struct {
	ctx       *ScheduleContext
	jobQueues map[types.GPUID]types.GPUJobQueue
	gpus      map[types.GPUID]types.GPU
}

func (c *Cluster) GPUJobQueues() map[types.GPUID]types.GPUJobQueue {
	return c.jobQueues
}

func (c *Cluster) EmptyGPUJobQueues() []types.GPUJobQueue {
	r := make([]types.GPUJobQueue, 0)
	for _, q := range c.jobQueues {
		if len(q.Jobs()) == 0 {
			r = append(r, q)
		}
	}
	return r
}

func (c *Cluster) GPU(gpuID types.GPUID) types.GPU {
	return c.gpus[gpuID]
}

func (c *Cluster) GPUs() map[types.GPUType][]types.GPU {
	panic("implement me")
}

func (c *Cluster) GPUTypes() []types.GPUType {
	return GPUTypes(c.ctx.PC)
}

func (c *Cluster) Now() types.Time {
	return types.Time(c.ctx.PC.FixedNow())
}

func (c *Cluster) CurrRunningJob(gpuID types.GPUID) types.Job {
	jobs := c.jobQueues[gpuID].Jobs()
	if len(jobs) > 0 {
		return jobs[0]
	}
	return nil
}

func (c *Cluster) ClosestTimeToFinishAnyJob() types.Time {
	panic("implement me")
}

func (c *Cluster) InitJob(jobMeta types.JobMeta) types.Job {
	meta := jobMeta.(*JobMeta)
	return &Job{
		ctx: c.ctx,
		Job: meta.job,
	}
}

type Job struct {
	ctx  *ScheduleContext
	Job  *objects.Job
	Meta *JobMeta
}

func (j *Job) JobName() types.JobName {
	return types.JobName(j.Job.GetJobID())
}

func (j *Job) ExecutionDetail() types.JobExecutionDetail {
	panic("implement me")
}

func (j *Job) FirstExecutionTime() types.Time {
	panic("implement me")
}

func (j *Job) FinishExecutionTime() types.Time {
	panic("implement me")
}

func (j *Job) RemainingRatio() float64 {
	return j.ctx.JobID2RemainingRatio[j.Job.GetJobID()]
}

func (j *Job) RemainingDuration(gpuType types.GPUType) types.Duration {
	return types.Duration(j.ctx.JobID2RemainingTime[j.Job.GetJobID()][gpuType])
}

func (j *Job) IsRunning() bool {
	panic("implement me")
}

func (j *Job) IsFinished() bool {
	panic("implement me")
}

func (j *Job) QueueDelay() types.Duration {
	panic("implement me")
}

func (j *Job) JobMeta() types.JobMeta {
	return j.ctx.JobMetasMap[j.JobName()]
}

func (j *Job) Violation() (bool, types.Duration) {
	panic("implement me")
}

func (j *Job) JCT() types.Time {
	panic("implement me")
}

func (j *Job) HasDDL() bool {
	panic("implement me")
}

func (j *Job) ActualRuntimeOnGPUs() types.Duration {
	panic("implement me")
}

type JobMeta struct {
	job *objects.Job
}

func (j *JobMeta) JobName() types.JobName {
	return types.JobName(j.job.GetJobID())
}

func (j *JobMeta) DDL() types.Time {
	return types.Time(j.job.GetDeadline())
}

func (j *JobMeta) Durations() map[types.GPUType]types.Duration {
	panic("implement me")
}

func (j *JobMeta) Duration(gpu types.GPU) types.Duration {
	return types.Duration(getCachedJobExecutionTime(j.job.GetJobID())[gpu.Type()])
}

func (j *JobMeta) SubmitTime() types.Time {
	return types.Time(j.job.GetSubmitTimeNanoSecond())
}

type JobExecutionRange struct {
}

func (j *JobExecutionRange) TimeRange() types.TimeRange {
	panic("implement me")
}

type JobExecutionDetail struct {
}

func (j *JobExecutionDetail) SumRuntimeOnGPUs() types.Duration {
	panic("implement me")
}

func (j *JobExecutionDetail) ExecutionRanges() map[types.GPU][]types.JobExecutionRange {
	panic("implement me")
}

type GPU struct {
	acc *objects.Accelerator
}

func (G *GPU) ID() types.GPUID {
	return GPUID(G.acc.GetAcceleratorID())
}

func (G *GPU) Type() types.GPUType {
	return types.GPUType(G.acc.GetAcceleratorMetaInfo().GetBriefType())
}

func (G *GPU) String() string {
	return strconv.Itoa(int(G.ID()))
}

type GPUJobQueue struct {
	g    types.GPU
	jobs []types.Job
}

func (G *GPUJobQueue) GPU() types.GPU {
	return G.g
}

func (G *GPUJobQueue) Jobs() []types.Job {
	return G.jobs
}

func (G *GPUJobQueue) SetJobs(jobs ...types.Job) {
	G.jobs = jobs
}

func (G *GPUJobQueue) ClearQueue() []types.Job {
	jobs := G.jobs
	G.jobs = make([]types.Job, 0)
	return jobs
}

func (G *GPUJobQueue) FirstJobRemainingDuration() types.Duration {
	panic("implement me")
}
