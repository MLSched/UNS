package impls

import (
	"fmt"
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	UNSMethods "github.com/MLSched/UNS/schedulers/impls/DLT/UNS/methods"
	"github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/impls/DLT/hydra"
	"github.com/MLSched/UNS/schedulers/impls/DLT/large_scale"
	"github.com/MLSched/UNS/schedulers/impls/DLT/naive"
	"github.com/MLSched/UNS/schedulers/impls/DLT/queue_based"
	"github.com/MLSched/UNS/schedulers/interfaces"
)

type Factory func(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error)

var factories = map[configs.SchedulerType]Factory{
	configs.SchedulerType_schedulerTypeNaive:     naive.Build,
	configs.SchedulerType_schedulerTypeUNS:       UNSMethods.Build,
	configs.SchedulerType_schedulerTypeSJF:       queue_based.BuildSJF,
	configs.SchedulerType_schedulerTypeEDF:       queue_based.BuildEDF,
	configs.SchedulerType_schedulerTypeHydra:     hydra.Build,
	configs.SchedulerType_schedulerTypeEDFFast:   queue_based.BuildEDFFast,
	configs.SchedulerType_schedulerTypeSJFFast:   queue_based.BuildSJFFast,
	configs.SchedulerType_schedulerTypeLSSearch:  large_scale.BuildLSSearch,
	configs.SchedulerType_schedulerTypeLSCompare: large_scale.BuildLSCompare,
}

func Build(schedulerBuildParams *base.SchedulerBuildParams) (interfaces.Scheduler, error) {
	if factory, ok := factories[schedulerBuildParams.SchedulerConfiguration.GetSchedulerType()]; ok {
		return factory(pb_gen.ExtractSchedulerConfiguration(schedulerBuildParams.SchedulerConfiguration), schedulerBuildParams.EventPusher, schedulerBuildParams.PartitionContextAware)
	}
	return nil, fmt.Errorf("unsupported scheduler type, type is [%s]", schedulerBuildParams.SchedulerConfiguration.GetSchedulerType())
}
