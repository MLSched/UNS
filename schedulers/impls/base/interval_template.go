package base

import (
	"UNS/events"
	"UNS/schedulers/partition"
	"log"
	"time"
)

type IntervalSchedulerTemplate struct {
	IntervalNano int64

	scheduleAble          chan chan interface{}
	LastScheduledNanoTime int64

	impl                  IntervalSchedulerInterface
	PartitionContextAware PartitionContextAware
}

type IntervalSchedulerInterface interface {
	Schedule() int64
}

func (i *IntervalSchedulerTemplate) HandleEvent(event *events.Event) {
	type currentNanoTimeGetter interface {
		GetCurrentNanoTime() int64
	}
	switch eo := event.Data.(type) {
	case currentNanoTimeGetter:
		if eo.GetCurrentNanoTime()-i.LastScheduledNanoTime > i.IntervalNano {
			i.WaitSchedule()
		}
	}
}

func (i *IntervalSchedulerTemplate) WaitSchedule() {
	scheduleFinishChan := make(chan interface{})
	i.scheduleAble <- scheduleFinishChan
	<-scheduleFinishChan
}

func NewIntervalSchedulerTemplate(intervalScheduler IntervalSchedulerInterface, intervalNano int64, aware PartitionContextAware) *IntervalSchedulerTemplate {
	return &IntervalSchedulerTemplate{
		IntervalNano:          intervalNano,
		scheduleAble:          make(chan chan interface{}),
		impl:                  intervalScheduler,
		PartitionContextAware: aware,
	}
}

func (i *IntervalSchedulerTemplate) Schedule() {
	panic("template method called.")
}

func (i *IntervalSchedulerTemplate) StartService() {
	go func() {
		dur := time.Duration(i.IntervalNano) * time.Nanosecond
		timer := time.NewTimer(dur)
		scheduleCount := 0
		for {
			select {
			case <-timer.C:
				i.LastScheduledNanoTime = i.impl.Schedule()
				scheduleCount++
				log.Printf("Interval Scheduler, Schedule by interval triggered, count = %d\n", scheduleCount)
				timer.Reset(dur)
			case c := <-i.scheduleAble:
				i.LastScheduledNanoTime = i.impl.Schedule()
				scheduleCount++
				log.Printf("Interval Scheduler, Schedule by scheduleAble channel triggered, count = %d\n", scheduleCount)
				timer.Reset(dur)
				c <- true
			}
		}
	}()
}

func (s *IntervalSchedulerTemplate) GetPartitionContext() *partition.Context {
	return s.PartitionContextAware()
}
