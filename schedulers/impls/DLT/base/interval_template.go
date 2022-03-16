package base

import (
	"UNS/events"
	eventsobjs "UNS/pb_gen/events"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"log"
	"math"
	"time"
)

type IntervalSchedulerTemplate struct {
	// syncMode 表示是否使用同步模式。同步模式下所有的消息收发都是同步的。仅在模拟时使用。
	syncMode     bool
	intervalNano int64

	// 触发调度的channel，向里面放入一个
	scheduleAble          chan chan interface{}
	lastScheduledNanoTime int64

	impl                  IntervalSchedulerInterface
	partitionContextAware PartitionContextAware
	eventPusher           EventPusher
}

type IntervalSchedulerInterface interface {
	interfaces.Scheduler
	DoSchedule() *eventsobjs.SSUpdateAllocationsEvent
}

func (i *IntervalSchedulerTemplate) HandleEvent(event *events.Event) {
	if ok := i.handleUpdatePartitionContext(event); !ok {
		return
	}
	switch event.Data.(type) {
	case *eventsobjs.RMUpdateJobsEvent, *eventsobjs.RMUpdateAllocationsEvent:
		if i.syncMode {
			i.SyncSchedule()
		} else {
			i.AsyncSchedule()
		}
	}
	events.ReplySucceeded(event)
}

func (i *IntervalSchedulerTemplate) handleUpdatePartitionContext(event *events.Event) (ok bool) {
	err := UpdatePartitionContext(event, i.GetPartitionContext())
	if err != nil {
		events.Reply(event, &events.Result{
			Succeeded: false,
			Reason:    err.Error(),
		})
		return false
	}
	return true
}

func (i *IntervalSchedulerTemplate) SyncSchedule() {
	scheduleFinishChan := make(chan interface{})
	i.scheduleAble <- scheduleFinishChan
	<-scheduleFinishChan
}

func (i *IntervalSchedulerTemplate) AsyncSchedule() {
	i.scheduleAble <- nil
}

func NewIntervalSchedulerTemplate(intervalScheduler IntervalSchedulerInterface, intervalNano int64, aware PartitionContextAware, syncMode bool, pusher EventPusher) *IntervalSchedulerTemplate {
	return &IntervalSchedulerTemplate{
		syncMode:              syncMode,
		intervalNano:          intervalNano,
		scheduleAble:          make(chan chan interface{}, 1024),
		impl:                  intervalScheduler,
		partitionContextAware: aware,
		eventPusher:           pusher,
	}
}

func (i *IntervalSchedulerTemplate) DoSchedule() {
	if i.impl == nil {
		panic("IntervalSchedulerTemplate DoSchedule called, but impl is not set.")
	}
	i.lastScheduledNanoTime = i.GetPartitionContext().Now()
	updateAllocationsEvent := i.impl.DoSchedule()
	if updateAllocationsEvent == nil {
		return
	}
	if i.syncMode {
		resultChan := make(chan *events.Result)
		i.eventPusher(i.impl.GetSchedulerID(), &events.Event{
			Data:       updateAllocationsEvent,
			ResultChan: resultChan,
		})
		<-resultChan
	} else {
		i.eventPusher(i.impl.GetSchedulerID(), &events.Event{
			Data: updateAllocationsEvent,
		})
	}
}

func (i *IntervalSchedulerTemplate) StartService() {
	go func() {
		dur := time.Duration(i.intervalNano) * time.Nanosecond
		if i.syncMode {
			dur = math.MaxInt64 // sync mode doesn't provide interval scheduling automatically
		}
		timer := time.NewTimer(dur)
		scheduleCount := 0
		for {
			select {
			case <-timer.C:
				i.DoSchedule()
				scheduleCount++
				log.Printf("Interval Scheduler, DoSchedule by interval triggered, count = %d\n", scheduleCount)
				timer.Reset(dur)
			case c := <-i.scheduleAble:
				i.DoSchedule()
				scheduleCount++
				log.Printf("Interval Scheduler, DoSchedule by scheduleAble channel triggered, count = %d\n", scheduleCount)
				timer.Reset(dur)
				if c != nil {
					c <- true
				}
			}
		}
	}()
}

func (i *IntervalSchedulerTemplate) GetPartitionContext() *partition.Context {
	return i.partitionContextAware()
}
