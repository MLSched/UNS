package impls

import (
	"UNS/pb_gen/configs"
	"UNS/schedulers"
	"UNS/schedulers/impls/UNS"
	"UNS/schedulers/impls/naive"
	"encoding/json"
	"fmt"
)

func Build(configuration *configs.SchedulerConfiguration, pusher schedulers.EventPusher) (schedulers.Scheduler, error) {
	switch configuration.GetSchedulerType() {
	case configs.SchedulerType_naive:
		c := &configs.NaiveSchedulerConfiguration{}
		err := json.Unmarshal(configuration.ConfigurationBytes, c)
		if err != nil {
			return nil, err
		}
		return naive.New(c, pusher), nil
	case configs.SchedulerType_UNS:
		c := &configs.UNSSchedulerConfiguration{}
		err := json.Unmarshal(configuration.ConfigurationBytes, c)
		if err != nil {
			return nil, err
		}
		return UNS.New(c, pusher), nil
	default:
		return nil, fmt.Errorf("unsupported scheduler type, type is [%s]", configuration.GetSchedulerType())
	}
}