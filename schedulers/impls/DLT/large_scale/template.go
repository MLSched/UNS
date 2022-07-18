package large_scale

import (
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	eventobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/predictor"
	interfaces2 "github.com/MLSched/UNS/predictor/interfaces"
	base2 "github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	"log"
	"math"
)

type QueueBasedSchedulerTemplate struct {
	*base2.DLTSchedulerTemplate

	Predictor                    interfaces2.Predictor
	AllocationsProvider          base2.AllocationsProvider
	AllocationProvideMode        base2.ProvideType
	ReturnAllSchedulingDecisions bool

	MaxPossibleAllocationsCount int

	impl QueueBasedSchedulerInterface
}

type QueueBasedSchedulerInterface interface {
	interfaces.Scheduler
	DoSchedule() *eventobjs.SSUpdateAllocationsEvent
	PrioritySort(jobs map[string]*objects.Job) []*objects.Job
	GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore
}

type QueueBasedSchedulerParam struct {
	Impl                         QueueBasedSchedulerInterface
	PredictorConfiguration       *configs.PredictorConfiguration
	Pusher                       base2.EventPusher
	PartitionContextAware        base2.PartitionContextAware
	IntervalNano                 int64
	SyncMode                     bool
	AllocationsProvider          base2.AllocationsProvider
	AllocationProvideMode        base2.ProvideType
	ReturnAllSchedulingDecisions bool
	MaxPossibleAllocationsCount  int
}

func BuildTemplate(param *QueueBasedSchedulerParam) (*QueueBasedSchedulerTemplate, error) {
	sche := &QueueBasedSchedulerTemplate{
		Predictor: predictor.BuildPredictor(param.PredictorConfiguration),
		AllocationsProvider: func() base2.AllocationsProvider {
			if param.AllocationsProvider == nil {
				return &base2.AllocationsProviderImpl{}
			}
			return param.AllocationsProvider
		}(),
		impl:                         param.Impl,
		AllocationProvideMode:        param.AllocationProvideMode,
		ReturnAllSchedulingDecisions: param.ReturnAllSchedulingDecisions,
		MaxPossibleAllocationsCount: func() int {
			if param.MaxPossibleAllocationsCount == 0 {
				return math.MaxInt64
			}
			return param.MaxPossibleAllocationsCount
		}(),
	}
	sche.DLTSchedulerTemplate = base2.NewIntervalSchedulerTemplate(sche, param.IntervalNano, param.PartitionContextAware, param.SyncMode, param.Pusher)
	return sche, nil
}

func (s *QueueBasedSchedulerTemplate) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	panic("template method.")
}

func (s *QueueBasedSchedulerTemplate) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	return s.impl.DoSchedule()
}

func (s *QueueBasedSchedulerTemplate) DoScheduleTemplate() *eventobjs.SSUpdateAllocationsEvent {
	if !s.ReturnAllSchedulingDecisions {
		panic("must return all scheduling decisions")
	}
	originalPC := s.GetPartitionContext().Clone(false)
	log.Printf("unallocated accIDs = %v", originalPC.AllocationViews.UnallocatedAcceleratorIDs)
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	if !s.IfHasUnallocated(pc) {
		return nil
	}
	unallocatedJobs := originalPC.AllocationViews.UnallocatedJobs
	unallocatedJobsSize := len(unallocatedJobs)
	sorted := s.impl.PrioritySort(unallocatedJobs)
	splitN := 10
	log.Printf("ls scheduler, unallocatedJobsSize = %d, splitN = %d\n", unallocatedJobsSize, splitN)
	pcs := originalPC.Split(splitN)
	basePRs := make([]interfaces2.PredictResult, 0, len(pcs))
	for _, pc := range pcs {
		basePredictResult, err := s.Predictor.Predict(pc, pc.AllocationViews.AllocationsSlice)
		if err != nil {
			log.Printf("[Queue Based Scheduler] predict failed, err=%v", err)
			return nil
		}
		basePRs = append(basePRs, basePredictResult)
	}
	for idx, job := range sorted {
		pcIdx := idx / (unallocatedJobsSize / splitN)
		if idx%1000 == 0 {
			log.Printf("jobs scheduled for %d, pcIdx = %d\n", idx, pcIdx)
		}
		pc := pcs[pcIdx]
		pc.UnfinishedJobs[job.JobID] = job
		basePredictResult := basePRs[pcIdx]
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
		basePRs[pcIdx] = basePredictResult.Merge(solelyPR)
		unallocatedJobs = pc.AllocationViews.UnallocatedJobs
	}

	allocations := make([]*pb_gen.JobAllocation, 0)
	for _, pc := range pcs {
		allocations = append(allocations, pc.AllocationViews.AllocationsSlice...)
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(allocations)}
}

func (s *QueueBasedSchedulerTemplate) GetSchedulerID() string {
	return s.impl.GetSchedulerID()
}

func (s *QueueBasedSchedulerTemplate) GetPredictor() interfaces2.Predictor {
	return s.Predictor
}

type JobAllocationScorerParam struct {
	PC            *partition.Context
	PredictResult interfaces2.PredictResult
	Job           *objects.Job
	JobAllocation *pb_gen.JobAllocation
}

type JobAllocationScore float64

func (s *QueueBasedSchedulerTemplate) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	panic("template method.")
}
