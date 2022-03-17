package UNS

import (
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/predictor"
	predictorinterfaces "UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"context"
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

func (s *Scheduler) StartService() {

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
	//ctx, cancel := context.WithTimeout(context.Background(), s.MaxLatency)
	//defer cancel()
	return nil
}

func (s *Scheduler) serialSchedule(ctx context.Context) {
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
