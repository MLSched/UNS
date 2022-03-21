package SJF

import (
	"UNS/pb_gen/configs"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	interfaces2 "UNS/predictor/interfaces"
	base2 "UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"log"
	"math"
	"sort"
	"sync"
)

type Scheduler struct {
	*base2.DLTSchedulerTemplate

	Predictor           interfaces2.Predictor
	AllocationsProvider base2.AllocationsProvider
	Config              *configs.SJFSchedulerConfiguration
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.SJFSchedulerConfiguration)
	sche := &Scheduler{
		Config:    c,
		Predictor: predictor.BuildPredictor(c.PredictorConfiguration),
		AllocationsProvider: &base2.AllocationsProviderImpl{
			MaxGangAllocations: math.MaxInt64,
		},
	}
	sche.DLTSchedulerTemplate = base2.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventsobjs.SSUpdateAllocationsEvent {
	originalPC := s.GetPartitionContext().Clone(false)
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	unallocatedJobs := pc.GetUnallocatedJobs()
	if len(unallocatedJobs) == 0 {
		return nil
	}
	if !s.IfHasUnallocated(pc) {
		return nil
	}
	sorted := s.PrioritySort(pc, unallocatedJobs)
	for _, job := range sorted {
		basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
		if err != nil {
			log.Printf("[SJF Scheduler] predict failed, err=%v", err)
			return nil
		}
		accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
		shortestJCT := int64(math.MaxInt64)
		var shortestJCTJobAllocation *objects.JobAllocation = nil
		for _, possibleAllocation := range possibleAllocations {
			cancel := s.TempAllocJob(pc, possibleAllocation)
			pr, err := s.Predictor.Predict(pc, s.RelatedJobAllocations(pc, accID2SortedTaskAllocations, possibleAllocation))
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
				shortestJCTJobAllocation = possibleAllocation
			}
			cancel()
		}
		s.TempAllocJob(pc, shortestJCTJobAllocation)
		unallocatedJobs = pc.GetUnallocatedJobs()
	}

	newJobAllocations := s.FilterScheduleAbleJobAllocations(pc, originalPC)
	return &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: newJobAllocations}
}

func (s *Scheduler) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
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
			possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
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
