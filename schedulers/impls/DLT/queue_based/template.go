package queue_based

import (
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

	Predictor           interfaces2.Predictor
	AllocationsProvider base2.AllocationsProvider
	Config              *configs.SJFSchedulerConfiguration

	impl QueueBasedSchedulerInterface
}

type QueueBasedSchedulerInterface interface {
	interfaces.Scheduler
	PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job
	GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore
}

func BuildTemplate(Impl QueueBasedSchedulerInterface,
	predictorConfiguration *configs.PredictorConfiguration,
	pusher base2.EventPusher,
	partitionContextAware base2.PartitionContextAware,
	intervalNano int64,
	syncMode bool) (*QueueBasedSchedulerTemplate, error) {
	sche := &QueueBasedSchedulerTemplate{
		Predictor: predictor.BuildPredictor(predictorConfiguration),
		AllocationsProvider: &base2.AllocationsProviderImpl{
			MaxGangAllocations: math.MaxInt64,
		},
		impl: Impl,
	}
	sche.DLTSchedulerTemplate = base2.NewIntervalSchedulerTemplate(sche, intervalNano, partitionContextAware, syncMode, pusher)
	return sche, nil
}

func (s *QueueBasedSchedulerTemplate) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	panic("template method.")
}

func (s *QueueBasedSchedulerTemplate) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	originalPC := s.GetPartitionContext().Clone(false)
	log.Printf("unallocated accIDs = %v", originalPC.GetUnallocatedAcceleratorIDs())
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	if !s.IfHasUnallocated(pc) {
		return nil
	}
	unallocatedJobs := pc.GetUnallocatedJobs()
	sorted := s.impl.PrioritySort(pc, unallocatedJobs)
	for _, job := range sorted {
		basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
		if err != nil {
			log.Printf("[SJF Scheduler] predict failed, err=%v", err)
			return nil
		}
		accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job, base2.ProvideTypeTotal)
		var bestScore *JobAllocationScore = nil
		var bestJobAllocation *objects.JobAllocation = nil
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
		unallocatedJobs = pc.GetUnallocatedJobs()
	}

	newJobAllocations := s.FilterScheduleAbleJobAllocations(pc, originalPC)
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: newJobAllocations}
}

func (s *QueueBasedSchedulerTemplate) GetSchedulerID() string {
	return s.impl.GetSchedulerID()
}

type JobAllocationScorerParam struct {
	PC            *partition.Context
	PredictResult interfaces2.PredictResult
	Job           *objects.Job
	JobAllocation *objects.JobAllocation
}

type JobAllocationScore float64

func (s *QueueBasedSchedulerTemplate) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	panic("template method.")
}
