package base

import (
	"UNS/events"
	"UNS/pb_gen/configs"
	"UNS/schedulers/partition"
)

type EventPusher func(SchedulerID string, event *events.Event)
type PartitionContextAware func() *partition.Context
type SchedulerBuildParams struct {
	SchedulerConfiguration *configs.SchedulerConfiguration
	EventPusher            EventPusher
	PartitionContextAware  PartitionContextAware
}
