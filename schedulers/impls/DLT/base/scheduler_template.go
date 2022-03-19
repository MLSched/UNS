package base

import (
	"UNS/events"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"
)

// DLTSchedulerTemplate 支持DLT任务的调度，包含Single和Gang两种类型的job。
type DLTSchedulerTemplate struct {
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

func (i *DLTSchedulerTemplate) HandleEvent(event *events.Event) {
	if ok := i.handleUpdatePartitionContext(event); !ok {
		return
	}
	if eo, ok := event.Data.(*eventsobjs.RMUpdateJobsEvent); ok {
		for _, job := range eo.GetNewJobs() {
			if err := i.checkSupportJob(job); err != nil {
				events.Reply(event, &events.Result{
					Succeeded: false,
					Reason:    err.Error(),
				})
				return
			}
		}
	}
	switch event.Data.(type) {
	case *eventsobjs.RMUpdateJobsEvent, *eventsobjs.RMUpdateAllocationsEvent, *eventsobjs.RMUpdateTimeEvent:
		if i.syncMode {
			i.SyncSchedule()
		} else {
			i.AsyncSchedule()
		}
	}
	events.ReplySucceeded(event)
}

func (i *DLTSchedulerTemplate) checkSupportJob(job *objects.Job) error {
	if job.GetJobType() != objects.JobType_jobTypeDLT {
		return fmt.Errorf("[DLTSchedulerTemplate] checkSupportJobTypes, only support DLT job, but received %s", job.GetJobType().String())
	}
	supportedTaskGroupTypes := map[objects.TaskGroupType]bool{
		objects.TaskGroupType_taskGroupTypeSingle: true,
		objects.TaskGroupType_taskGroupTypeGang:   true,
	}
	if _, ok := supportedTaskGroupTypes[job.GetTaskGroup().GetTaskGroupType()]; !ok {
		return fmt.Errorf("[DLTSchedulerTemplate] checkSupportJobTypes, only support single and gang task group types, but received %s", job.GetTaskGroup().GetTaskGroupType().String())
	}
	if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
		e := job.GetTaskGroup().GetGangTaskGroupInfo().GetExtra()
		extra := &objects.GangTaskGroupDLTExtra{}
		err := json.Unmarshal(e, extra)
		if err != nil {
			return fmt.Errorf("[DLTSchedulerTemplate] checkSupportJobTypes, unmarshal gang task group DLT extra failed, err=[%v]", err)
		}
		if extra.GetDLTGangType() != objects.DLTGangType_DLTGangTypeDataParallel {
			return fmt.Errorf("[DLTSchedulerTemplate] checkSupportJobTypes, only support data parallel gang job, but received %s", extra.GetDLTGangType().String())
		}
	}
	return nil
}

func (i *DLTSchedulerTemplate) handleUpdatePartitionContext(event *events.Event) (ok bool) {
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

func (i *DLTSchedulerTemplate) SyncSchedule() {
	scheduleFinishChan := make(chan interface{})
	i.scheduleAble <- scheduleFinishChan
	<-scheduleFinishChan
}

func (i *DLTSchedulerTemplate) AsyncSchedule() {
	i.scheduleAble <- nil
}

func NewIntervalSchedulerTemplate(intervalScheduler IntervalSchedulerInterface, intervalNano int64, aware PartitionContextAware, syncMode bool, pusher EventPusher) *DLTSchedulerTemplate {
	return &DLTSchedulerTemplate{
		syncMode:              syncMode,
		intervalNano:          intervalNano,
		scheduleAble:          make(chan chan interface{}, 1024),
		impl:                  intervalScheduler,
		partitionContextAware: aware,
		eventPusher:           pusher,
	}
}

func (i *DLTSchedulerTemplate) DoSchedule() {
	if i.impl == nil {
		panic("DLTSchedulerTemplate DoSchedule called, but impl is not set.")
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

func (i *DLTSchedulerTemplate) StartService() {
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

func (i *DLTSchedulerTemplate) GetPartitionContext() *partition.Context {
	return i.partitionContextAware()
}
