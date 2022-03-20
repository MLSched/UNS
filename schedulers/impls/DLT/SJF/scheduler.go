package SJF

import (
	"UNS/pb_gen/configs"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	interfaces2 "UNS/predictor/interfaces"
	base2 "UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"log"
	"math"
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
	for s.IfHasUnallocated(pc) {
		basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
		if err != nil {
			log.Printf("[SJF Scheduler] predict failed, err=%v", err)
			continue
		}
		var globalShortestJCTJobAllocation *objects.JobAllocation
		var globalShortestJCT int64 = math.MaxInt64
		var mu = &sync.Mutex{}
		var wg = &sync.WaitGroup{}
		for _, job := range unallocatedJobs {
			job := job
			wg.Add(1)
			go func() {
				cloned := pc.Clone(false)
				accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(cloned, basePredictResult)
				possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
				shortestJCT := int64(math.MaxInt64)
				var shortestJCTJobAllocation *objects.JobAllocation = nil
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
						shortestJCTJobAllocation = possibleAllocation
					}
					cancel()
				}
				mu.Lock()
				defer mu.Unlock()
				if shortestJCT < globalShortestJCT {
					globalShortestJCT = shortestJCT
					globalShortestJCTJobAllocation = shortestJCTJobAllocation
				}
				wg.Done()
			}()
		}
		wg.Wait()
		s.TempAllocJob(pc, globalShortestJCTJobAllocation)
		unallocatedJobs = pc.GetUnallocatedJobs()
	}
	newJobAllocation := s.FilterScheduleAbleJobAllocations(pc, originalPC)
	return &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: newJobAllocation}
}
