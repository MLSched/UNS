package base

import (
	"github.com/MLSched/UNS/events"
	"github.com/MLSched/UNS/pb_gen/configs"
	"github.com/MLSched/UNS/schedulers/partition"
)

type EventPusher func(SchedulerID string, event *events.Event)
type PartitionContextAware func() *partition.Context
type SchedulerBuildParams struct {
	SchedulerConfiguration *configs.SchedulerConfiguration
	EventPusher            EventPusher
	PartitionContextAware  PartitionContextAware
}
