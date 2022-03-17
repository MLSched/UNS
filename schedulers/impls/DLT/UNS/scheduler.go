package UNS

import (
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	predictorinterfaces "UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"log"
	"math"
	"time"
)

type Scheduler struct {
	*base.IntervalSchedulerTemplate
	Config              *configs.UNSSchedulerConfiguration
	Predictor           predictorinterfaces.Predictor
	AllocationsProvider base.AllocationsProvider

	MaxLatency time.Duration
	MaxRound   int
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.UNSSchedulerConfiguration)
	sche := &Scheduler{
		Config:    c,
		Predictor: predictor.BuildPredictor(c.PredictorConfiguration),
		AllocationsProvider: &base.AllocationsProviderImpl{
			//MaxGangAllocations: len(partitionContextAware().View.AcceleratorID2Accelerator) * 2,
			MaxGangAllocations: math.MaxInt64,
		},
	}
	sche.IntervalSchedulerTemplate = base.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	return s.testSchedule()
}

func (s *Scheduler) testSchedule() *eventobjs.SSUpdateAllocationsEvent {
	pc := s.GetPartitionContext().Clone()
	// Clone后将时间固定住
	t := pc.Now()
	pc.Time = &t
	//accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
	newAllocations := make([]*objects.JobAllocation, 0)
nextJob:
	for _, job := range pc.GetUnallocatedJobs() {
		basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
		if err != nil {
			panic(err)
		}
		accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
		if len(possibleAllocations) == 0 {
			continue
		}
		for _, possibleAllocation := range possibleAllocations {
			targetAllocation := possibleAllocation
			pc.UnfinishedJobs[job.GetJobID()] = job
			pc.Allocations[job.GetJobID()] = targetAllocation
			pr, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
			if err != nil {
				panic(err)
			}
			if *pr.GetResult(targetAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime() == pc.Now() {
				newAllocations = append(newAllocations, targetAllocation)
				continue nextJob
			}
			delete(pc.UnfinishedJobs, job.GetJobID())
			delete(pc.Allocations, job.GetJobID())
		}
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: newAllocations}
}

func (s *Scheduler) serialSchedule() {
	pc := s.GetPartitionContext().Clone()
	// Clone后将时间固定住
	t := pc.Now()
	pc.Time = &t
	pcs := make([]*partition.Context, 0)
	pcs = append(pcs, pc)
	round := 0
	for round < s.MaxRound {
		round++
		for _, pc := range pcs {
			pc := pc
			basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
			if err != nil {
				reason := "UNS Scheduler Predict basePredictResult failed, which should not happened since this prediction is guaranteed to be success"
				log.Println(reason)
				panic(reason)
			}
			accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
			jobs := pc.GetUnallocatedJobs()
			for _, job := range jobs {
				possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
				for _, allocation := range possibleAllocations {
					pc.Allocations[allocation.GetJobID()] = allocation
					// TODO
				}
			}
		}
	}
}
