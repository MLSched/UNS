package impls

import (
	"github.com/MLSched/UNS/events"
	"github.com/MLSched/UNS/mock"
	eventobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/predictor"
	predictorinterfaces "github.com/MLSched/UNS/predictor/interfaces"
	UNSMethods "github.com/MLSched/UNS/schedulers/impls/DLT/UNS/methods"
	"github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/impls/DLT/hydra"
	"github.com/MLSched/UNS/schedulers/impls/DLT/queue_based"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	mapset "github.com/deckarep/golang-set"
	"github.com/golang/protobuf/ptypes/wrappers"
	"log"
	"math"
	"testing"
	"time"
)

func MockUNS(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfiguration()
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetUnsSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := UNSMethods.Build(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func MockSJF(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfiguration()
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetSjfSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := queue_based.BuildSJF(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func MockEDF(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfiguration()
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetEdfSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := queue_based.BuildEDF(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func MockEDFFast(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfiguration()
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetEdfFastSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := queue_based.BuildEDFFast(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func MockHydra(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfiguration()
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetHydraSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := hydra.Build(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func TestOneShotSchedule(t *testing.T) {
	pc := partition.MockPartition()
	localPC := partition.MockPartition()
	config := mock.DLTSimulatorConfiguration()
	var pusher = func(SchedulerID string, event *events.Event) {
		e := event.Data.(*eventobjs.SSUpdateAllocationsEvent)
		jobAllocations := e.GetNewJobAllocations()
		err := localPC.UpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
			UpdatedJobAllocations: jobAllocations,
		})
		if err != nil {
			panic(err)
		}
		go func() {
			events.ReplySucceeded(event)
		}()
	}
	now := time.Now().UnixNano()
	pc.Time = &now
	//scheduler := MockUNS(pusher, pc)
	//scheduler := MockSJF(pusher, pc)
	//scheduler := MockEDF(pusher, pc)
	scheduler := MockEDFFast(pusher, pc)
	//scheduler := MockHydra(pusher, pc)
	scheduler.StartService()
	//for _, job := range config.GetJobs() {
	//	job.SubmitTimeNanoSecond = 0
	//}
	for _, job := range config.Jobs {
		job.SubmitTimeNanoSecond = now
	}
	err := localPC.UpdateJobs(&eventobjs.RMUpdateJobsEvent{NewJobs: config.GetJobs()})
	if err != nil {
		panic(err)
	}
	resultChan := make(chan *events.Result)
	go func() {
		scheduler.HandleEvent(&events.Event{
			Data: &eventobjs.RMUpdateJobsEvent{
				NewJobs: config.GetJobs(),
				CurrentNanoTime: &wrappers.Int64Value{
					Value: 0,
				},
			},
			ResultChan: resultChan,
		})
	}()
	<-resultChan
	pred := predictor.BuildPredictor(config.GetPredictorConfiguration())
	result, err := pred.Predict(localPC, localPC.AllocationViews.AllocationsSlice)
	if err != nil {
		panic(err)
	}
	totalJCT := int64(0)
	maximumJCT := int64(0)
	withDeadlineCount := int64(0)
	totalDeadlineViolation := int64(0)
	totalDeadlineViolatedCount := int64(0)
	totalJobsCount := int64(0)
	jobIDsSet := mapset.NewThreadUnsafeSet()
	result.Range(func(allocation *objects.TaskAllocation, result predictorinterfaces.EachPredictResult) {
		jobID := allocation.GetJobID()
		if jobIDsSet.Contains(jobID) {
			return
		}
		jobIDsSet.Add(jobID)
		start := *result.GetStartExecutionNanoTime()
		finish := *result.GetFinishNanoTime()
		job := localPC.GetJob(allocation.GetJobID())
		JCT := finish - job.GetSubmitTimeNanoSecond()
		log.Printf("allocation %v, start %v, finish %v, duration %v, JCT = %v", allocation, start, finish, finish-start, JCT)
		totalJCT += JCT
		totalJobsCount++
		if JCT > maximumJCT {
			maximumJCT = JCT
		}
		if job.GetDeadline() != math.MaxInt64 {
			withDeadlineCount++
			deadlineViolationDuration := JCT - job.GetDeadline()
			if deadlineViolationDuration > 0 {
				totalDeadlineViolation += deadlineViolationDuration
				totalDeadlineViolatedCount++
			}
		}
	})
	avgJCT := float64(totalJCT) / float64(totalJobsCount)
	log.Printf("totalJobsCount %d, avg JCT = %f, makespan = %d, withDeadlines = %d, violatedJobsCount = %d, totalDeadlineViolation = %d", totalJobsCount, avgJCT, maximumJCT, withDeadlineCount, totalDeadlineViolatedCount, totalDeadlineViolation)
}
