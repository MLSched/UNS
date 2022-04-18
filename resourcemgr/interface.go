package resourcemgr

import "github.com/MLSched/UNS/events"

type Interface interface {
	GetResourceManagerID() string
	events.EventHandler
}
