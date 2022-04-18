package interfaces

import (
	"github.com/MLSched/UNS/events"
	eventsobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/resourcemgr"
)

type Scheduler interface {
	GetSchedulerID() string
	StartService()
	events.EventHandler
}

type Service interface {
	StartService()
	RegisterRM(event *eventsobjs.RMRegisterResourceManagerEvent, resourceMgr resourcemgr.Interface) *events.Result
	Push(rmID string, partitionID string, event *events.Event)
}
