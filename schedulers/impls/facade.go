package impls

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	UNSMethods "UNS/schedulers/impls/DLT/UNS/methods"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/impls/DLT/hydra"
	"UNS/schedulers/impls/DLT/naive"
	"UNS/schedulers/impls/DLT/queue_based"
	"UNS/schedulers/interfaces"
	"fmt"
)

type Factory func(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error)

var factories = map[configs.SchedulerType]Factory{
	configs.SchedulerType_schedulerTypeNaive:   naive.Build,
	configs.SchedulerType_schedulerTypeUNS:     UNSMethods.Build,
	configs.SchedulerType_schedulerTypeSJF:     queue_based.BuildSJF,
	configs.SchedulerType_schedulerTypeEDF:     queue_based.BuildEDF,
	configs.SchedulerType_schedulerTypeHydra:   hydra.Build,
	configs.SchedulerType_schedulerTypeEDFFast: queue_based.BuildEDFFast,
}

func Build(schedulerBuildParams *base.SchedulerBuildParams) (interfaces.Scheduler, error) {
	if factory, ok := factories[schedulerBuildParams.SchedulerConfiguration.GetSchedulerType()]; ok {
		return factory(pb_gen.ExtractSchedulerConfiguration(schedulerBuildParams.SchedulerConfiguration), schedulerBuildParams.EventPusher, schedulerBuildParams.PartitionContextAware)
	}
	return nil, fmt.Errorf("unsupported scheduler type, type is [%s]", schedulerBuildParams.SchedulerConfiguration.GetSchedulerType())
}
