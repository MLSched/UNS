package UNS

import (
	"UNS/events"
	"UNS/pb_gen/configs"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
)

type Scheduler struct {
	Config *configs.UNSSchedulerConfiguration
}

func (s *Scheduler) DoSchedule() int64 {
	panic("implement me")
}

func (s *Scheduler) HandleEvent(event *events.Event) {
	panic("implement me")
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func (s *Scheduler) StartService() {

}

func Build(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.UNSSchedulerConfiguration)
	return &Scheduler{
		Config: c,
	}, nil
}
