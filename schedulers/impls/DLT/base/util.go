package base

import (
	"UNS/events"
	eventobjs "UNS/pb_gen/events"
	"UNS/schedulers/partition"
	"fmt"
	"log"
)

func UpdatePartitionContext(event *events.Event, c *partition.Context) error {
	err := func() error {
		switch eo := event.Data.(type) {
		case *eventobjs.RMUpdateAllocationsEvent:
			return c.UpdateAllocations(eo)
		case *eventobjs.RMUpdateJobsEvent:
			return c.UpdateJobs(eo)
		case *eventobjs.RMUpdateTimeEvent:
			return c.UpdateTime(eo)
		default:
			reason := fmt.Sprintf("MockPartition Context ID = [%s] received unknown event = [%v]", c.Meta.GetPartitionID(), event.Data)
			log.Println(reason)
			panic(reason)
		}
	}()
	return err
}
