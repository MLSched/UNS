package queue_based

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	base2 "UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"log"
	"math"
	"sort"
	"sync"
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
	sche.QueueBasedSchedulerTemplate, err = BuildTemplate(sche, c.PredictorConfiguration, pusher, partitionContextAware, c.GetIntervalNano(), c.GetSyncMode())
	if err != nil {
		return nil, err
	}
	return sche, nil
}

func (s *SJFScheduler) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	pc = pc.Clone(false)
	basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
	if err != nil {
		log.Printf("[SJF Scheduler] predict failed, err=%v", err)
		return nil
	}
	type jobAndShortestJCT struct {
		Job *objects.Job
		JCT int64
	}
	jobAndShortestJCTs := make([]*jobAndShortestJCT, 0, len(jobs))
	var mu = &sync.Mutex{}
	var wg = &sync.WaitGroup{}
	for _, job := range jobs {
		job := job
		wg.Add(1)
		go func() {
			cloned := pc.Clone(false)
			accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(cloned, basePredictResult)
			possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job, base2.ProvideTypeOnlyUnoccupied)
			shortestJCT := int64(math.MaxInt64)
			for _, possibleAllocation := range possibleAllocations {
				cancel := s.TempAllocJob(cloned, possibleAllocation)
				pr, err := s.Predictor.Predict(pc, s.RelatedJobAllocations(cloned, accID2SortedTaskAllocations, possibleAllocation))
				if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
					s.MarkGangJobStartTime(possibleAllocation, *pr.GetResult(possibleAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
				}
				if err != nil {
					log.Printf("[SJF Scheduler] predict failed inside, err=%v", err)
					continue
				}
				r := pr.GetResult(possibleAllocation.GetTaskAllocations()[0])
				job := pc.GetUnfinishedJob(possibleAllocation.GetJobID())
				JCT := *r.GetFinishNanoTime() - job.GetSubmitTimeNanoSecond()
				if JCT < shortestJCT {
					shortestJCT = JCT
				}
				cancel()
			}
			mu.Lock()
			defer mu.Unlock()
			jobAndShortestJCTs = append(jobAndShortestJCTs, &jobAndShortestJCT{
				Job: job,
				JCT: shortestJCT,
			})
			wg.Done()
		}()
	}
	wg.Wait()
	sort.Slice(jobAndShortestJCTs, func(i, j int) bool {
		return jobAndShortestJCTs[i].JCT < jobAndShortestJCTs[j].JCT
	})
	result := make([]*objects.Job, 0, len(jobAndShortestJCTs))
	for _, j := range jobAndShortestJCTs {
		result = append(result, j.Job)
	}
	return result
}

func (s *SJFScheduler) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	possibleAllocation := param.JobAllocation
	pr := param.PredictResult
	pc := param.PC
	r := pr.GetResult(possibleAllocation.GetTaskAllocations()[0])
	//start := possibleAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond()
	//if start != pc.FixedNow() {
	//	return JobAllocationScore(math.Inf(-1))
	//}
	job := pc.GetUnfinishedJob(possibleAllocation.GetJobID())
	JCT := *r.GetFinishNanoTime() - job.GetSubmitTimeNanoSecond()
	return JobAllocationScore(-JCT)
}