package methods

import (
	"UNS/pb_gen/configs"
	"UNS/schedulers/impls/DLT/UNS/methods/MCTS"
	base2 "UNS/schedulers/impls/DLT/UNS/methods/base"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
)

func Build(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.UNSSchedulerConfiguration)
	sche := &base2.Scheduler{
		Config: c,
	}
	sche.DLTSchedulerTemplate = base.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	// 根据配置生成具体的调度方法
	//sche.ScheduleMethod = narrow_tree.BuildNarrowTreeMethod(sche, c)
	sche.ScheduleMethod = MCTS.BuildMCTSMethod(sche, c)
	return sche, nil
}
