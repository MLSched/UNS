package base

import (
	"UNS/events"
	"UNS/pb_gen"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	interfaces2 "UNS/predictor/interfaces"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"container/list"
	"encoding/json"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/golang/protobuf/ptypes/wrappers"
	"log"
	"math"
	"sort"
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

	supportTaskGroupTypes []objects.TaskGroupType

	// 虽然已经结束，但是和某些任务有SpaceSharing关系，导致暂时还没有从partitionContext删除的任务ID。
	spaceSharingRelatedFinishedJobIDs mapset.Set
}

type IntervalSchedulerInterface interface {
	interfaces.Scheduler
	DoSchedule() *eventsobjs.SSUpdateAllocationsEvent
	GetPredictor() interfaces2.Predictor
}

func (i *DLTSchedulerTemplate) HandleEvent(event *events.Event) {
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
	if ok := i.handleUpdatePartitionContext(event); !ok {
		return
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

func (i *DLTSchedulerTemplate) SetSupportTaskGroupTypes(taskGroupTypes []objects.TaskGroupType) {
	i.supportTaskGroupTypes = taskGroupTypes
}

func (i *DLTSchedulerTemplate) checkSupportJob(job *objects.Job) error {
	if job.GetJobType() != objects.JobType_jobTypeDLT {
		return fmt.Errorf("[DLTSchedulerTemplate] checkSupportJobTypes, only support DLT job, but received %s", job.GetJobType().String())
	}
	supportedTaskGroupTypes := map[objects.TaskGroupType]bool{}
	for _, t := range i.supportTaskGroupTypes {
		supportedTaskGroupTypes[t] = true
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
	pc := i.GetPartitionContext()
	i.updatePartitionContextTime(event, pc)
	if e, ok := event.Data.(*eventsobjs.RMUpdateAllocationsEvent); ok {
		i.extractSpaceSharingRelatedJobIDs(e, pc)
	}
	err := func() error {
		switch eo := event.Data.(type) {
		case *eventsobjs.RMUpdateAllocationsEvent:
			return pc.UpdateAllocations(eo)
		case *eventsobjs.RMUpdateJobsEvent:
			pc.UpdateCurrentTime(eo.GetCurrentNanoTime())
			return pc.UpdateJobs(eo)
		case *eventsobjs.RMUpdateTimeEvent:
			return pc.UpdateTime(eo)
		default:
			reason := fmt.Sprintf("Partition Context ID = [%s] received unknown event = [%v]", pc.Meta.GetPartitionID(), event.Data)
			log.Println(reason)
			panic(reason)
		}
	}()
	if err != nil {
		events.Reply(event, &events.Result{
			Succeeded: false,
			Reason:    err.Error(),
		})
		return false
	}
	return true
}

func (i *DLTSchedulerTemplate) updatePartitionContextTime(event *events.Event, pc *partition.Context) {
	switch eo := event.Data.(type) {
	case *eventsobjs.RMUpdateAllocationsEvent:
		pc.UpdateCurrentTime(eo.GetCurrentNanoTime())
	case *eventsobjs.RMUpdateJobsEvent:
		pc.UpdateCurrentTime(eo.GetCurrentNanoTime())
	case *eventsobjs.RMUpdateTimeEvent:
		_ = pc.UpdateTime(eo)
	}
}

func (i *DLTSchedulerTemplate) extractSpaceSharingRelatedJobIDs(e *eventsobjs.RMUpdateAllocationsEvent, pc *partition.Context) {
	pc = pc.Clone(false)
	now := pc.FixedNow()
	pc.Time = &now
	finishedJobIDs := mapset.NewThreadUnsafeSet()
	for _, newFinishedJobID := range e.FinishedJobIDs {
		finishedJobIDs.Add(newFinishedJobID)
	}
	for i := range i.spaceSharingRelatedFinishedJobIDs.Iterator().C {
		jobID := i.(string)
		finishedJobIDs.Add(jobID)
		e.FinishedJobIDs = append(e.FinishedJobIDs, jobID)
	}
	unfinishedJobIDs := mapset.NewThreadUnsafeSet()
	for jobID := range pc.Allocations {
		if !finishedJobIDs.Contains(jobID) {
			unfinishedJobIDs.Add(jobID)
		}
	}
	predictor := i.impl.GetPredictor()
	spaceSharingSets, err := predictor.GetSpaceSharingSets(pc, pc.AllocationViews.AllocationsSlice)
	if err != nil {
		panic(err)
	}
	spaceSharingRelatedFinishedJobIDs := mapset.NewThreadUnsafeSet()
	addSetToSpaceSharingRelated := func() func(idx int, set mapset.Set) {
		addedSet := make(map[int]bool)
		return func(idx int, set mapset.Set) {
			if addedSet[idx] {
				return
			}
			set.Each(func(i interface{}) bool {
				jobID := i.(string)
				if finishedJobIDs.Contains(jobID) {
					spaceSharingRelatedFinishedJobIDs.Add(jobID)
				}
				return false
			})
		}
	}()
	for idx, set := range spaceSharingSets {
		unfinishedJobIDs.Each(func(i interface{}) bool {
			unfinishedJobID := i.(string)
			if set.Contains(unfinishedJobID) {
				addSetToSpaceSharingRelated(idx, set)
			}
			return false
		})
	}
	finishedJobIDs = finishedJobIDs.Difference(spaceSharingRelatedFinishedJobIDs)
	finishedJobIDsSlice := utils.MapSet2StringSlice(finishedJobIDs)
	e.FinishedJobIDs = finishedJobIDsSlice
	i.spaceSharingRelatedFinishedJobIDs = spaceSharingRelatedFinishedJobIDs
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
		syncMode:                          syncMode,
		intervalNano:                      intervalNano,
		scheduleAble:                      make(chan chan interface{}, 1024),
		impl:                              intervalScheduler,
		partitionContextAware:             aware,
		eventPusher:                       pusher,
		supportTaskGroupTypes:             []objects.TaskGroupType{objects.TaskGroupType_taskGroupTypeSingle, objects.TaskGroupType_taskGroupTypeGang},
		spaceSharingRelatedFinishedJobIDs: mapset.NewThreadUnsafeSet(),
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

//// RelatedJobAllocations 获取一个jobAllocation所占用的acc的其他jobAllocation，若其他jobAllocation占用了更多的acc，则迭代以上过程
//// 举例：job1占用了acc1，acc2，job2占用了acc2，acc3，job3占用了acc4，acc5：则最终，获得job1的relatedJobAllocations会返回job1, job2。
//func (i *DLTSchedulerTemplate) RelatedJobAllocations(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, jobAllocation *pb_gen.JobAllocation) []*pb_gen.JobAllocation {
//	visitedAccIDs := make(map[string]bool)
//	isVisitedAccID := func(accID string) bool {
//		_, ok := visitedAccIDs[accID]
//		return ok
//	}
//	visitedJobAllocations := make(map[string]*pb_gen.JobAllocation)
//	isVisitedJobAllocation := func(jobAllocation *pb_gen.JobAllocation) bool {
//		_, ok := visitedJobAllocations[jobAllocation.GetJobID()]
//		return ok
//	}
//	jobAllocationsQueue := list.New()
//	jobAllocationsQueue.PushBack(jobAllocation)
//	for jobAllocationsQueue.Len() > 0 {
//		f := jobAllocationsQueue.Remove(jobAllocationsQueue.Front()).(*pb_gen.JobAllocation)
//		if isVisitedJobAllocation(f) {
//			continue
//		}
//		visitedJobAllocations[f.GetJobID()] = f
//		unseenAccIDs := make([]string, 0)
//		for _, taskAllocation := range f.GetTaskAllocations() {
//			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
//			if isVisitedAccID(accID) {
//				continue
//			}
//			unseenAccIDs = append(unseenAccIDs, accID)
//		}
//		for _, unseenAccID := range unseenAccIDs {
//			for _, taskAllocation := range accID2SortedTaskAllocations[unseenAccID] {
//				jobAllocation := pc.Allocations[taskAllocation.GetJobID()]
//				if isVisitedJobAllocation(jobAllocation) {
//					continue
//				}
//				jobAllocationsQueue.PushBack(jobAllocation)
//			}
//		}
//		for _, accID := range unseenAccIDs {
//			visitedAccIDs[accID] = true
//		}
//	}
//	result := make([]*pb_gen.JobAllocation, 0, len(visitedJobAllocations))
//	for _, a := range visitedJobAllocations {
//		result = append(result, a)
//	}
//	return result
//}

// RelatedJobAllocationsByNodes 获取一个jobAllocation所在节点的其他jobAllocation，若其他jobAllocation占用了更多的节点，则迭代以上过程
// 举例：job1占用了node1，node2，job2占用了node2，node3，job3占用了node4，node5：则最终，获得job1的relatedJobAllocations会返回job1, job2。
func (i *DLTSchedulerTemplate) RelatedJobAllocationsByNodes(pc *partition.Context, nodeID2TaskAllocations map[string][]*objects.TaskAllocation, jobAllocation *pb_gen.JobAllocation) []*pb_gen.JobAllocation {
	visitedNodeIDs := make(map[string]bool)
	isVisitedNodeID := func(nodeID string) bool {
		_, ok := visitedNodeIDs[nodeID]
		return ok
	}
	visitedJobAllocations := make(map[string]*pb_gen.JobAllocation)
	isVisitedJobAllocation := func(jobAllocation *pb_gen.JobAllocation) bool {
		_, ok := visitedJobAllocations[jobAllocation.GetJobID()]
		return ok
	}
	jobAllocationsQueue := list.New()
	jobAllocationsQueue.PushBack(jobAllocation)
	for jobAllocationsQueue.Len() > 0 {
		f := jobAllocationsQueue.Remove(jobAllocationsQueue.Front()).(*pb_gen.JobAllocation)
		if isVisitedJobAllocation(f) {
			continue
		}
		visitedJobAllocations[f.GetJobID()] = f
		unseenNodeIDs := make([]string, 0)
		for _, taskAllocation := range f.GetTaskAllocations() {
			nodeID := taskAllocation.GetNodeID()
			if isVisitedNodeID(nodeID) {
				continue
			}
			unseenNodeIDs = append(unseenNodeIDs, nodeID)
		}
		for _, unseenNodeID := range unseenNodeIDs {
			for _, taskAllocation := range nodeID2TaskAllocations[unseenNodeID] {
				jobAllocation := pc.Allocations[taskAllocation.GetJobID()]
				if isVisitedJobAllocation(jobAllocation) {
					continue
				}
				jobAllocationsQueue.PushBack(jobAllocation)
			}
		}
		for _, accID := range unseenNodeIDs {
			visitedNodeIDs[accID] = true
		}
	}
	result := make([]*pb_gen.JobAllocation, 0, len(visitedJobAllocations))
	for _, a := range visitedJobAllocations {
		result = append(result, a)
	}
	return result
}

func (i *DLTSchedulerTemplate) TempAllocJob(pc *partition.Context, jobAllocation *pb_gen.JobAllocation) (cancel func()) {
	return pc.TempAllocJob(jobAllocation)
}

func (i *DLTSchedulerTemplate) IfHasUnallocated(pc *partition.Context) bool {
	unallocatedJobs := pc.AllocationViews.UnallocatedJobs
	if len(unallocatedJobs) == 0 {
		return false
	}
	unallocatedAcceleratorIDs := pc.AllocationViews.UnallocatedAcceleratorIDs
	if len(unallocatedAcceleratorIDs) == 0 {
		return false
	}
	return true
}

func (i *DLTSchedulerTemplate) AllocateAbleJob(pc *partition.Context, jobAllocation *pb_gen.JobAllocation) bool {
	if i.AllocationTime(jobAllocation) == pc.FixedNow() {
		return true
	}
	return false
}

func (i *DLTSchedulerTemplate) AllocationTime(jobAllocation *pb_gen.JobAllocation) int64 {
	return jobAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond()
}

func (i *DLTSchedulerTemplate) FilterScheduleAbleJobAllocations(newJobAllocations []*pb_gen.JobAllocation, currPC *partition.Context) []*pb_gen.JobAllocation {
	accID2SortedNewJobAllocations := make(map[string][]*pb_gen.JobAllocation)
	for _, jobAllocation := range newJobAllocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
			if _, ok := accID2SortedNewJobAllocations[accID]; !ok {
				accID2SortedNewJobAllocations[accID] = make([]*pb_gen.JobAllocation, 0)
			}
			accID2SortedNewJobAllocations[accID] = append(accID2SortedNewJobAllocations[accID], jobAllocation)
		}
	}
	for _, allocations := range accID2SortedNewJobAllocations {
		sort.SliceStable(allocations, func(i, j int) bool {
			return allocations[i].GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue() < allocations[j].GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue()
		})
	}
	result := make([]*pb_gen.JobAllocation, 0, len(newJobAllocations))
nextJobAllocation:
	for _, jobAllocation := range newJobAllocations {
		if i.AllocationTime(jobAllocation) == currPC.FixedNow() {
			if jobAllocation.GetTaskAllocations()[0].GetPlaceholder() {
				// 如果是placeholder类型的，那么在它前面不能也有新的任务分配是placeholder的，否则placeholder产生嵌套，即是无法当前立刻分配的。
				for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
					accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
					previousContainsPlaceholder := false
					for _, allocation := range accID2SortedNewJobAllocations[accID] {
						if allocation == jobAllocation {
							break
						}
						if allocation.GetTaskAllocations()[0].GetPlaceholder() {
							previousContainsPlaceholder = true
						}
					}
					if previousContainsPlaceholder {
						// 一个placeholder类型的任务开始前，已经有一个placeholder任务，则代表这两个任务都是当前的partitionContext下无法立即开始的任务，
						// 而在算法中已经标注了startTime，则根据此startTime排序后，前面的placeholder任务具有更高的优先级，当前的任务在本次调度中直接忽略
						continue nextJobAllocation
					}
				}
			}
			result = append(result, jobAllocation)
		}
	}
	return result
}

func (i *DLTSchedulerTemplate) MarkGangJobStartTime(jobAllocation *pb_gen.JobAllocation, startTime int64) {
	// 为gang类型的job修正开始时间
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: startTime}
	}
}

func (i *DLTSchedulerTemplate) GetNewJobAllocations(newPC *partition.Context, currPC *partition.Context) []*pb_gen.JobAllocation {
	newJobAllocations := make(map[string]*pb_gen.JobAllocation)
	for jobID, jobAllocation := range newPC.Allocations {
		newJobAllocations[jobID] = jobAllocation
	}
	for jobID := range currPC.Allocations {
		delete(newJobAllocations, jobID)
	}
	result := make([]*pb_gen.JobAllocation, 0, len(newJobAllocations))
	for _, jobAllocation := range newJobAllocations {
		result = append(result, jobAllocation)
	}
	return result
}

// GetJobAllocationStartTime 获取jobAllocation的任务开始时间。
// 仅针对DLT任务，因为只有gang和single类型的任务，所以job开始时间就是task的开始时间。
func (i *DLTSchedulerTemplate) GetJobAllocationStartTime(allocation *pb_gen.JobAllocation) *wrappers.Int64Value {
	return allocation.GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond()
}
