package queue_based

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	interfaces2 "UNS/predictor/interfaces"
	base2 "UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"log"
	"math"
)

type QueueBasedSchedulerTemplate struct {
	*base2.DLTSchedulerTemplate

	Predictor                    interfaces2.Predictor
	AllocationsProvider          base2.AllocationsProvider
	AllocationProvideMode        base2.ProvideType
	ReturnAllSchedulingDecisions bool

	impl QueueBasedSchedulerInterface
}

type QueueBasedSchedulerInterface interface {
	interfaces.Scheduler
	PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job
	GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore
}

type QueueBasedSchedulerParam struct {
	Impl                         QueueBasedSchedulerInterface
	PredictorConfiguration       *configs.PredictorConfiguration
	Pusher                       base2.EventPusher
	PartitionContextAware        base2.PartitionContextAware
	IntervalNano                 int64
	SyncMode                     bool
	AllocationProvideMode        base2.ProvideType
	ReturnAllSchedulingDecisions bool
}

func BuildTemplate(param *QueueBasedSchedulerParam) (*QueueBasedSchedulerTemplate, error) {
	sche := &QueueBasedSchedulerTemplate{
		Predictor:                    predictor.BuildPredictor(param.PredictorConfiguration),
		AllocationsProvider:          &base2.AllocationsProviderImpl{},
		impl:                         param.Impl,
		AllocationProvideMode:        param.AllocationProvideMode,
		ReturnAllSchedulingDecisions: param.ReturnAllSchedulingDecisions,
	}
	sche.DLTSchedulerTemplate = base2.NewIntervalSchedulerTemplate(sche, param.IntervalNano, param.PartitionContextAware, param.SyncMode, param.Pusher)
	return sche, nil
}

func (s *QueueBasedSchedulerTemplate) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	panic("template method.")
}

func (s *QueueBasedSchedulerTemplate) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
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
	for _, job := range sorted {
		basePredictResult, err := s.Predictor.Predict(pc, pc.AllocationViews.AllocationsSlice)
		if err != nil {
			log.Printf("[SJF Scheduler] predict failed, err=%v", err)
			return nil
		}
		nodeID2TaskAllocations := pc.AllocationViews.NodeID2TaskAllocations
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(&base2.GetPossibleAllocationsParams{
			PC:            pc,
			PredictResult: basePredictResult,
			Job:           job,
			ProvideType:   s.AllocationProvideMode,
			MaxCount:      math.MaxInt64,
		})
		var bestScore *JobAllocationScore = nil
		var bestJobAllocation *pb_gen.JobAllocation = nil
		for _, possibleAllocation := range possibleAllocations {
			cancel := s.TempAllocJob(pc, possibleAllocation)
			pr, err := s.Predictor.Predict(pc, s.RelatedJobAllocationsByNodes(pc, nodeID2TaskAllocations, possibleAllocation))
			if err != nil {
				if interfaces2.IsMultiSpanNodesGangTasksError(err) || interfaces2.IsSpaceSharingOutOfMemoryError(err) {
					continue
				}
				log.Printf("[Queue Based Scheduler] predict failed inside, err=%v", err)
				continue
			}
			if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
				s.MarkGangJobStartTime(possibleAllocation, *pr.GetResult(possibleAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
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
			cancel()
		}
		s.TempAllocJob(pc, bestJobAllocation)
		unallocatedJobs = pc.AllocationViews.UnallocatedJobs
	}

	newJobAllocations := s.GetNewJobAllocations(pc, originalPC)
	if !s.ReturnAllSchedulingDecisions {
		newJobAllocations = s.FilterScheduleAbleJobAllocations(s.GetNewJobAllocations(pc, originalPC), originalPC)
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(newJobAllocations)}
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
