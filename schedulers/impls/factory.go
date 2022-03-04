package impls

import (
	"UNS/pb_gen/configs"
	"UNS/schedulers"
	"UNS/schedulers/impls/UNS"
	"UNS/schedulers/impls/naive"
	"fmt"
)

type ABFactory func(configurationBytes []byte, pusher schedulers.EventPusher) (schedulers.Scheduler, error)

var factories = map[configs.SchedulerType]ABFactory{
	configs.SchedulerType_naive: naive.Build,
	configs.SchedulerType_UNS: UNS.Build,
}

func Build(configuration *configs.SchedulerConfiguration, pusher schedulers.EventPusher) (schedulers.Scheduler, error) {
	if factory, ok := factories[configuration.GetSchedulerType()]; ok {
		return factory(configuration.GetConfigurationBytes(), pusher)
	}
	return nil, fmt.Errorf("unsupported scheduler type, type is [%s]", configuration.GetSchedulerType())
}