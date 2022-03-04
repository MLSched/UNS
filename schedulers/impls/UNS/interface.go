package UNS

import (
	"UNS/pb_gen/configs"
	"UNS/schedulers"
	"encoding/json"
)

type Scheduler struct {
	Config *configs.UNSSchedulerConfiguration
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func (s *Scheduler) StartService() {

}

func Build(configurationBytes []byte, pusher schedulers.EventPusher) (schedulers.Scheduler, error) {
	c := &configs.UNSSchedulerConfiguration{}
	err := json.Unmarshal(configurationBytes, c)
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		Config: c,
	}, nil
}