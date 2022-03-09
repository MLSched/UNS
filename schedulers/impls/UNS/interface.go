package UNS

import (
	"UNS/events"
	"UNS/pb_gen/configs"
	"UNS/schedulers/interfaces"
	"encoding/json"
)

type Scheduler struct {
	Config *configs.UNSSchedulerConfiguration
}

func (s *Scheduler) HandleEvent(event *events.Event) {
	panic("implement me")
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func (s *Scheduler) StartService() {

}

func Build(configurationBytes []byte, pusher interfaces.EventPusher) (interfaces.Scheduler, error) {
	c := &configs.UNSSchedulerConfiguration{}
	err := json.Unmarshal(configurationBytes, c)
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		Config: c,
	}, nil
}
