package base

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	predictorinterfaces "UNS/predictor/interfaces"
	benefitsinterfaces "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/impls/DLT/UNS/score"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/partition"
	"sort"
	"strings"
)

type Scheduler struct {
	*base.DLTSchedulerTemplate
	Config         *configs.UNSSchedulerConfiguration
	ScheduleMethod ScheduleMethod
}

type ScheduleMethod interface {
	DoSchedule() *eventobjs.SSUpdateAllocationsEvent
}

type CommonMethodParams struct {
	Predictor                  predictorinterfaces.Predictor
	AllocationsProvider        base.AllocationsProvider
	BenefitsCalculator2Weights map[benefitsinterfaces.Calculator]float64
	ScoreCalculator            score.Calculator
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func (s *Scheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	return s.ScheduleMethod.DoSchedule()
}

func (s *Scheduler) FilterAllocationsForResourceEfficiency(initialPC *partition.Context, possibleAllocations []*pb_gen.JobAllocation) []*pb_gen.JobAllocation {
	// 如果启用了资源高效选项，则优先考虑能够让任务立刻运行的allocations。
	// 如果不存在这样的allocations，则返回原来的possibleAllocations
	now := initialPC.FixedNow()
	filtered := make([]*pb_gen.JobAllocation, 0)
	for _, jobAllocation := range possibleAllocations {
		if jobAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() != now {
			continue
		}
		job := initialPC.GetJob(jobAllocation.GetJobID())
		// 如果是当前分配的，首先，若是一个single类型的任务，且占用的资源是一个未分配的资源，则可以直接运行。
		// 然后，若是gang类型的任务，则要看该任务等待的任务是否是一个未分配的任务。
		if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeSingle {
			accID := jobAllocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
			if _, ok := initialPC.AllocationViews.UnallocatedAcceleratorIDs[accID]; ok {
				filtered = append(filtered, jobAllocation)
			}
		} else {
			for _, waitingJobID := range jobAllocation.PlaceholderWaitingJobIDs {
				if _, ok := initialPC.AllocationViews.UnallocatedJobs[waitingJobID]; ok {
					// 等待的任务中存在着起初未被分配的任务，则意味着这个任务不能立刻占据它想要的Accelerator
					// 所以该任务分配不属于能够立刻分配的类型，跳过。
					continue
				}
			}
			filtered = append(filtered, jobAllocation)
		}
	}
	return filtered
}

func (s *Scheduler) GenJobAllocationFingerPrint(jobAllocation *pb_gen.JobAllocation) string {
	b := &strings.Builder{}
	b.WriteString(jobAllocation.GetJobID())
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		b.WriteByte('|')
		b.WriteString(taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
		b.WriteString(taskAllocation.GetStartExecutionTimeNanoSecond().String())
		//b.WriteString(taskAllocation.GetPlaceholder())
	}
	return b.String()
}

func (s *Scheduler) GenJobAllocationsFingerPrint(jobAllocations []*pb_gen.JobAllocation) string {
	fingerPrints := make([]string, 0, len(jobAllocations))
	for _, jobAllocation := range jobAllocations {
		fingerPrints = append(fingerPrints, s.GenJobAllocationFingerPrint(jobAllocation))
	}
	sort.Strings(fingerPrints)
	return strings.Join(fingerPrints, "\n")
}
