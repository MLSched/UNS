package impls

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	"UNS/schedulers/impls/UNS"
	"UNS/schedulers/impls/base"
	"UNS/schedulers/impls/naive"
	"UNS/schedulers/interfaces"
	"fmt"
)

type Factory func(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error)

var factories = map[configs.SchedulerType]Factory{
	configs.SchedulerType_schedulerTypeNaive: naive.Build,
	configs.SchedulerType_schedulerTypeUNS:   UNS.Build,
}

func Build(schedulerBuildParams *base.SchedulerBuildParams) (interfaces.Scheduler, error) {
	if factory, ok := factories[schedulerBuildParams.SchedulerConfiguration.GetSchedulerType()]; ok {
		return factory(pb_gen.ExtractSchedulerConfiguration(schedulerBuildParams.SchedulerConfiguration), schedulerBuildParams.EventPusher, schedulerBuildParams.PartitionContextAware)
	}
	return nil, fmt.Errorf("unsupported scheduler type, type is [%s]", schedulerBuildParams.SchedulerConfiguration.GetSchedulerType())
}
