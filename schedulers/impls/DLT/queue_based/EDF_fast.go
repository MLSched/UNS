package queue_based

import (
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	eventobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	interfaces2 "github.com/MLSched/UNS/predictor/interfaces"
	base2 "github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	"log"
	"math"
	"math/rand"
	"sort"
)

type EDFFastScheduler struct {
	*QueueBasedSchedulerTemplate

	Config *configs.EDFFastSchedulerConfiguration
}

func (s *EDFFastScheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func BuildEDFFast(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.EDFFastSchedulerConfiguration)
	sche := &EDFFastScheduler{
		Config: c,
	}
	var err error
	provideMode := base2.ProvideTypeDefault | base2.ProvideTypeOnlyNonSpaceSharing
	sche.QueueBasedSchedulerTemplate, err = BuildTemplate(&QueueBasedSchedulerParam{
		Impl:                   sche,
		PredictorConfiguration: c.PredictorConfiguration,
		Pusher:                 pusher,
		PartitionContextAware:  partitionContextAware,
		IntervalNano:           c.GetIntervalNano(),
		SyncMode:               c.GetSyncMode(),
		AllocationProvideMode:  provideMode,
		//AllocationsProvider:          &base2.AllocationsProviderImpl{RandomMode: true},
		ReturnAllSchedulingDecisions: c.ReturnAllScheduleDecisions,
		//MaxPossibleAllocationsCount:  3,
	})
	if err != nil {
		return nil, err
	}
	return sche, nil
}

func (s *EDFFastScheduler) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	result := make([]*objects.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetJobID() < result[j].GetJobID()
	})
	rand.Seed(1)
	sort.SliceStable(result, func(i, j int) bool {
		//return float64(result[i].GetDeadline())*100 < float64(result[j].GetDeadline())
		if result[i].GetDeadline() < math.MaxInt64 && result[j].GetDeadline() < math.MaxInt64 {
			return float64(result[i].GetDeadline()) < float64(result[j].GetDeadline())
		} else {
			rate := rand.Float64()
			ratio := 0.24
			if rate < ratio {
				return rate < ratio/2
			}
			return float64(result[i].GetDeadline()) < float64(result[j].GetDeadline())
		}
	})
	return result
}

func (s *EDFFastScheduler) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	possibleAllocation := param.JobAllocation
	pr := param.PredictResult
	r := pr.GetResult(possibleAllocation.GetTaskAllocations()[0])
	start := *r.GetStartExecutionNanoTime()
	finish := *r.GetFinishNanoTime()
	//jobID := possibleAllocation.GetJobID()
	//if param.Job.GetDeadline() != math.MaxInt64 {
	//	return -1e9 * JobAllocationScore(param.Job.GetDeadline())
	//}
	//return JobAllocationScore(finish)
	return -JobAllocationScore(float64(start)*1e9 - float64(finish))
}

func (s *EDFFastScheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
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
