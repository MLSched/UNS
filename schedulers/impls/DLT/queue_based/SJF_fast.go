package queue_based

import (
	"fmt"
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	eventobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	interfaces2 "github.com/MLSched/UNS/predictor/interfaces"
	base2 "github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	"log"
	"math/rand"
	"sort"
)

type SJFFastScheduler struct {
	*QueueBasedSchedulerTemplate

	Config *configs.SJFFastSchedulerConfiguration
}

func (s *SJFFastScheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func BuildSJFFast(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.SJFFastSchedulerConfiguration)
	sche := &SJFFastScheduler{
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

func (s *SJFFastScheduler) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
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
	rand.Seed(1)
	result := make([]*objects.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetJobID() < result[j].GetJobID()
	})
	rand.Seed(1)
	sort.SliceStable(result, func(i, j int) bool {
		rate := rand.Float64()
		ratio := 0.03
		if rate < ratio {
			fmt.Println("hit random order")
			return rate < ratio/2
		}
		rate = rand.Float64()
		ratio = 0.99
		if rate < ratio {
			return float64(result[i].GetDeadline()) < float64(result[j].GetDeadline())
		}
		//	return jobWithETs[i].ExecutionTime < jobWithETs[j].ExecutionTime
		//if float64(result[i].GetDeadline()) < float64(result[j].GetDeadline()) {
		//	return true
		//}
		//ratio = 0.1
		//if rate < ratio {
		//	fmt.Println("hit fake sjf.")
		//	return rate < ratio/2
		//}
		return jobWithETs[i].ExecutionTime < jobWithETs[j].ExecutionTime
	})
	return result
}

func (s *SJFFastScheduler) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	possibleAllocation := param.JobAllocation
	pr := param.PredictResult
	pc := param.PC
	r := pr.GetResult(possibleAllocation.GetTaskAllocations()[0])
	//if possibleAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() != pc.FixedNow() {
	//	return JobAllocationScore(math.Inf(-1))
	//}
	job := pc.GetJob(possibleAllocation.GetJobID())
	JCT := *r.GetFinishNanoTime() - job.GetSubmitTimeNanoSecond()
	rate := rand.Float64()
	if rate < 0. {
		return -JobAllocationScore(rand.Int())
	}
	//return -JobAllocationScore(float64(start)*1e9 - float64(finish))
	return JobAllocationScore(-JCT)
}

func (s *SJFFastScheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	originalPC := s.GetPartitionContext().Clone(false)
	log.Printf("unallocated accIDs = %v", originalPC.AllocationViews.UnallocatedAcceleratorIDs)
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	if !s.IfHasUnallocated(pc) {
		return nil
	}
	unallocatedJobs := pc.AllocationViews.UnallocatedJobs
	sorted := s.impl.PrioritySort(pc, unallocatedJobs)
	var basePredictResult interfaces2.PredictResult
	basePredictResult, _ = s.Predictor.Predict(pc, pc.AllocationViews.AllocationsSlice)
	for _, job := range sorted {
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(&base2.GetPossibleAllocationsParams{
			PC:            pc,
			PredictResult: basePredictResult,
			Job:           job,
			ProvideType:   s.AllocationProvideMode,
			MaxCount:      s.MaxPossibleAllocationsCount,
		})

		var bestScore *JobAllocationScore = nil
		var bestJobAllocation *pb_gen.JobAllocation = nil
		for _, possibleAllocation := range possibleAllocations {
			tryAlloc := func() {
				cancel := s.TempAllocJob(pc, possibleAllocation)
				defer cancel()
				pr, err := s.Predictor.PredictSolely(pc, []*pb_gen.JobAllocation{possibleAllocation})
				if err != nil {
					if interfaces2.IsMultiSpanNodesGangTasksError(err) || interfaces2.IsSpaceSharingOutOfMemoryError(err) {
						return
					}
					log.Printf("[Queue Based Scheduler] predict failed inside, err=%v", err)
					return
				}
				score := s.impl.GetJobAllocationScore(&JobAllocationScorerParam{
					PC:            pc,
					PredictResult: pr,
					Job:           job,
					JobAllocation: possibleAllocation,
				})
				if bestScore == nil {
					bestScore = &score
					bestJobAllocation = possibleAllocation
				} else if score > *bestScore {
					bestScore = &score
					bestJobAllocation = possibleAllocation
				}
			}
			tryAlloc()
		}
		s.TempAllocJob(pc, bestJobAllocation)
		solelyPR, _ := s.Predictor.PredictSolely(pc, []*pb_gen.JobAllocation{bestJobAllocation})
		basePredictResult = basePredictResult.Merge(solelyPR)
		unallocatedJobs = pc.AllocationViews.UnallocatedJobs
	}

	newJobAllocations := s.GetNewJobAllocations(pc, originalPC)
	if !s.ReturnAllSchedulingDecisions {
		newJobAllocations = s.FilterScheduleAbleJobAllocations(s.GetNewJobAllocations(pc, originalPC), originalPC)
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(newJobAllocations)}
}
