package interfaces

import (
	"UNS/events"
	eventsobjs "UNS/pb_gen/events"
	"UNS/resourcemgr"
)

type Scheduler interface {
	GetSchedulerID() string
	StartService()
	events.EventHandler
}

type EventPusher func(SchedulerID string, event *events.Event)

type Service interface {
	StartService()
	RegisterRM(event *eventsobjs.RMRegisterResourceManagerEvent, resourceMgr resourcemgr.Interface) *events.Result
	Push(rmID string, partitionID string, event *events.Event)
}