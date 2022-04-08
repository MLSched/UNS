package queue_based

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	base2 "UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"sort"
)

type SJFScheduler struct {
	*QueueBasedSchedulerTemplate

	Config *configs.SJFSchedulerConfiguration
}

func (s *SJFScheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func BuildSJF(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.SJFSchedulerConfiguration)
	sche := &SJFScheduler{
		Config: c,
	}
	var err error
	provideMode := base2.ProvideTypeDefault | base2.ProvideTypeOnlyNonSpaceSharing
	sche.QueueBasedSchedulerTemplate, err = BuildTemplate(&QueueBasedSchedulerParam{
		Impl:                         sche,
		PredictorConfiguration:       c.PredictorConfiguration,
		Pusher:                       pusher,
		PartitionContextAware:        partitionContextAware,
		IntervalNano:                 c.GetIntervalNano(),
		SyncMode:                     c.GetSyncMode(),
		AllocationProvideMode:        provideMode,
		ReturnAllSchedulingDecisions: c.ReturnAllScheduleDecisions,
	})
	if err != nil {
		return nil, err
	}
	return sche, nil
}

func (s *SJFScheduler) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	type jobWithExecutionTime struct {
		Job           *objects.Job
		ExecutionTime int64
	}
	jobWithETs := make([]*jobWithExecutionTime, 0, len(jobs))
	for _, job := range jobs {
		jobWithETs = append(jobWithETs, &jobWithExecutionTime{
			Job:           job,
			ExecutionTime: s.Predictor.PredictSolelyFastestExecutionTime(job),
		})
	}
	sort.Slice(jobWithETs, func(i, j int) bool {
		return jobWithETs[i].ExecutionTime < jobWithETs[j].ExecutionTime
	})
	result := make([]*objects.Job, 0, len(jobWithETs))
	for _, jt := range jobWithETs {
		result = append(result, jt.Job)
	}
	return result
}

func (s *SJFScheduler) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	possibleAllocation := param.JobAllocation
	pr := param.PredictResult
	pc := param.PC
	r := pr.GetResult(possibleAllocation.GetTaskAllocations()[0])
	//if possibleAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() != pc.FixedNow() {
	//	return JobAllocationScore(math.Inf(-1))
	//}
	job := pc.GetJob(possibleAllocation.GetJobID())
	JCT := *r.GetFinishNanoTime() - job.GetSubmitTimeNanoSecond()
	return JobAllocationScore(-JCT)
}
