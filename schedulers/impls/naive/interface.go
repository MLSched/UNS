package naive

import (
	"UNS/events"
	"UNS/pb_gen/configs"
	"UNS/schedulers"
)

type Scheduler struct {

}

func (s *Scheduler) GetSchedulerID() string {
	panic("implement me")
}

func (s *Scheduler) HandleEvent(event *events.Event) {
	panic("implement me")
}

func New(configuration *configs.NaiveSchedulerConfiguration, pusher schedulers.EventPusher) *Scheduler {
	return &Scheduler{}
}