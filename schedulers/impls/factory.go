package impls

import (
	"UNS/pb_gen/configs"
	"UNS/schedulers/impls/UNS"
	"UNS/schedulers/impls/naive"
	"UNS/schedulers/interfaces"
	"fmt"
)

type ABFactory func(configurationBytes []byte, pusher interfaces.EventPusher) (interfaces.Scheduler, error)

var factories = map[configs.SchedulerType]ABFactory{
	configs.SchedulerType_schedulerTypeNaive: naive.Build,
	configs.SchedulerType_schedulerTypeUNS:   UNS.Build,
}

func Build(configuration *configs.SchedulerConfiguration, pusher interfaces.EventPusher) (interfaces.Scheduler, error) {
	if factory, ok := factories[configuration.GetSchedulerType()]; ok {
		return factory(configuration.GetConfigurationBytes(), pusher)
	}
	return nil, fmt.Errorf("unsupported scheduler type, type is [%s]", configuration.GetSchedulerType())
}
