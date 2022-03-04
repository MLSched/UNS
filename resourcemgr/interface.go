package resourcemgr

import "UNS/events"

type Interface interface {
	GetResourceManagerID() string
	events.EventHandler
}
