package base

import (
	"UNS/events"
	"time"
)

type IntervalSchedulerTemplate struct {
	IntervalNano int64

	scheduleAble          chan chan interface{}
	LastScheduledNanoTime int64

	impl IntervalSchedulerInterface
}

type IntervalSchedulerInterface interface {
	Schedule()
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

func NewIntervalSchedulerTemplate(intervalNano int64) *IntervalSchedulerTemplate {
	return &IntervalSchedulerTemplate{
		IntervalNano: intervalNano,
		scheduleAble: make(chan chan interface{}),
	}
}

func (i *IntervalSchedulerTemplate) Schedule() {
	panic("template method called.")
}

func (i *IntervalSchedulerTemplate) StartService() {
	go func() {
		dur := time.Duration(i.IntervalNano) * time.Nanosecond
		timer := time.NewTimer(dur)
		for {
			select {
			case <-timer.C:
				i.impl.Schedule()
				timer.Reset(dur)
			case c := <-i.scheduleAble:
				i.impl.Schedule()
				timer.Reset(dur)
				c <- true
			}
		}
	}()
}
