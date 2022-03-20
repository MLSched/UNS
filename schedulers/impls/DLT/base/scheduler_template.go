package base

import (
	"UNS/events"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
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
				log.Printf("Interval Scheduler, DoSchedule by scheduleAble channel finished, count = %d\n", scheduleCount)
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

// RelatedJobAllocations 获取一个jobAllocation所占用的acc的其他jobAllocation，若其他jobAllocation占用了更多的acc，则迭代以上过程
// 举例：job1占用了acc1，acc2，job2占用了acc2，acc3，job3占用了acc4，acc5：则最终，获得job1的relatedJobAllocations会返回job1, job2。
func (i *DLTSchedulerTemplate) RelatedJobAllocations(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, jobAllocation *objects.JobAllocation) []*objects.JobAllocation {
	visitedAccIDs := make(map[string]bool)
	isVisitedAccID := func(accID string) bool {
		_, ok := visitedAccIDs[accID]
		return ok
	}
	visitedJobAllocations := make(map[string]*objects.JobAllocation)
	isVisitedJobAllocation := func(jobAllocation *objects.JobAllocation) bool {
		_, ok := visitedJobAllocations[jobAllocation.GetJobID()]
		return ok
	}
	jobAllocationsQueue := list.New()
	jobAllocationsQueue.PushBack(jobAllocation)
	for jobAllocationsQueue.Len() > 0 {
		f := jobAllocationsQueue.Remove(jobAllocationsQueue.Front()).(*objects.JobAllocation)
		if isVisitedJobAllocation(f) {
			continue
		}
		visitedJobAllocations[f.GetJobID()] = f
		unseenAccIDs := make([]string, 0)
		for _, taskAllocation := range f.GetTaskAllocations() {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
			if isVisitedAccID(accID) {
				continue
			}
			unseenAccIDs = append(unseenAccIDs, accID)
		}
		for _, unseenAccID := range unseenAccIDs {
			for _, taskAllocation := range accID2SortedTaskAllocations[unseenAccID] {
				jobAllocation := pc.Allocations[taskAllocation.GetJobID()]
				if isVisitedJobAllocation(jobAllocation) {
					continue
				}
				jobAllocationsQueue.PushBack(jobAllocation)
			}
		}
		for _, accID := range unseenAccIDs {
			visitedAccIDs[accID] = true
		}
	}
	result := make([]*objects.JobAllocation, 0, len(visitedJobAllocations))
	for _, a := range visitedJobAllocations {
		result = append(result, a)
	}
	return result
}

func (i *DLTSchedulerTemplate) TempAllocJob(pc *partition.Context, jobAllocation *objects.JobAllocation) (cancel func()) {
	pc.Allocations[jobAllocation.GetJobID()] = jobAllocation
	return func() {
		delete(pc.Allocations, jobAllocation.GetJobID())
	}
}

func (i *DLTSchedulerTemplate) IfHasUnallocated(pc *partition.Context) bool {
	unallocatedJobs := pc.GetUnallocatedJobs()
	if len(unallocatedJobs) == 0 {
		return false
	}
	unallocatedAcceleratorIDs := pc.GetUnallocatedAcceleratorIDs()
	if len(unallocatedAcceleratorIDs) == 0 {
		return false
	}
	return true
}

func (i *DLTSchedulerTemplate) AllocateAbleJob(pc *partition.Context, jobAllocation *objects.JobAllocation) bool {
	if i.AllocationTime(jobAllocation) == pc.FixedNow() {
		return true
	}
	return false
}

func (i *DLTSchedulerTemplate) AllocationTime(jobAllocation *objects.JobAllocation) int64 {
	return jobAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond()
}

func (i *DLTSchedulerTemplate) FilterScheduleAbleJobAllocations(predictPC *partition.Context, currPC *partition.Context) []*objects.JobAllocation {
	newJobAllocations := make(map[string]*objects.JobAllocation)
	for jobID, jobAllocation := range predictPC.Allocations {
		newJobAllocations[jobID] = jobAllocation
	}
	for jobID := range currPC.Allocations {
		delete(newJobAllocations, jobID)
	}
	result := make([]*objects.JobAllocation, 0, len(newJobAllocations))
	var earliestUnableToAllocateTime int64 = math.MaxInt64
	for _, jobAllocation := range newJobAllocations {
		if i.AllocateAbleJob(currPC, jobAllocation) {
			result = append(result, jobAllocation)
		} else if t := i.AllocationTime(jobAllocation); t < earliestUnableToAllocateTime {
			earliestUnableToAllocateTime = t
		}
	}
	return result
}

func (i *DLTSchedulerTemplate) MarkGangJobStartTime(jobAllocation *objects.JobAllocation, startTime int64) {
	// 为gang类型的job修正开始时间
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: startTime}
	}
}
