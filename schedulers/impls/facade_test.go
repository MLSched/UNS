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
	"github.com/MLSched/UNS/schedulers/impls/DLT/large_scale"
	"github.com/MLSched/UNS/schedulers/impls/DLT/queue_based"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	mapset "github.com/deckarep/golang-set"
	"log"
	"math"
	"testing"
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
	config := mock.DLTSimulatorConfigurationWithScheduler("sjf")
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

func MockSJFFast(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfigurationWithScheduler("sjffast")
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetSjfFastSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := queue_based.BuildSJFFast(c, eventPusher, func() *partition.Context {
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
	config := mock.DLTSimulatorConfigurationWithScheduler("edffast")
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

func MockLSSearch(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfigurationWithScheduler("lssearch")
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetLsSearchSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := large_scale.BuildLSSearch(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func MockLSCompare(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfigurationWithScheduler("lscompare")
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetLsCompareSchedulerConfiguration()
	c.ReturnAllScheduleDecisions = true
	sche, err := large_scale.BuildLSCompare(c, eventPusher, func() *partition.Context {
		return pc
	})
	if err != nil {
		panic(err)
	}
	return sche
}

func MockHydra(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfigurationWithScheduler("hydra")
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

func TestCompare(t *testing.T) {
	edfAvgJCT, edfVioDDL, withDDL := oneShotSchedule("edffast")
	//hydraAvgJCT, hydraVioDDL, _ := oneShotSchedule("hydra")
	hydraAvgJCT, hydraVioDDL, _ := oneShotSchedule("sjffast")
	//edfAvgJCT, edfVioDDL, withDDL := oneShotSchedule("lscompare")
	//hydraAvgJCT, hydraVioDDL, _ := oneShotSchedule("lssearch")
	edfFastVioDDLRatio := float64(edfVioDDL) / float64(withDDL)
	targetVioDDLRatio := edfFastVioDDLRatio - 0.2
	hydraVioDDLRatio := float64(hydraVioDDL) / float64(withDDL)
	log.Printf("edf vio DDL %v", edfVioDDL)
	log.Printf("target avgJCT %v, target vio DDL ratio %v", edfAvgJCT*0.85, targetVioDDLRatio)
	log.Printf("hydra avgJCT %v, hydra vio DDL %v, ratio %v, increase %v", hydraAvgJCT, hydraVioDDL, hydraVioDDLRatio, edfFastVioDDLRatio-hydraVioDDLRatio)
}

func oneShotSchedule(schedulerName string) (float64, int64, int64) {
	pc := partition.MockPartitionWithScheduler(schedulerName)
	localPC := partition.MockPartitionWithScheduler(schedulerName)
	config := mock.DLTSimulatorConfigurationWithScheduler(schedulerName)
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
	//now := int64(time.Now().UnixNano())
	now := int64(0)
	pc.Time = &now
	var scheduler interfaces.Scheduler = nil
	if schedulerName == "hydra" {
		scheduler = MockHydra(pusher, pc)
	} else if schedulerName == "edffast" {
		scheduler = MockEDFFast(pusher, pc)
	} else if schedulerName == "lssearch" {
		scheduler = MockLSSearch(pusher, pc)
	} else if schedulerName == "lscompare" {
		scheduler = MockLSCompare(pusher, pc)
	} else if schedulerName == "sjf" {
		scheduler = MockSJF(pusher, pc)
	} else if schedulerName == "sjffast" {
		scheduler = MockSJFFast(pusher, pc)
	}
	//scheduler := MockUNS(pusher, pc)
	//scheduler := MockSJF(pusher, pc)
	//scheduler := MockEDF(pusher, pc)
	//scheduler := MockEDFFast(pusher, pc)
	//scheduler := MockHydra(pusher, pc)
	scheduler.StartService()
	//for _, job := range config.GetJobs() {
	//	job.SubmitTimeNanoSecond = 0
	//}
	for _, job := range config.Jobs {
		if job.Deadline != math.MaxInt64 {
			job.Deadline -= job.SubmitTimeNanoSecond
			job.Deadline += now
		}
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
				NewJobs:         config.GetJobs(),
				CurrentNanoTime: nil,
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
		//start := *result.GetStartExecutionNanoTime()
		finish := *result.GetFinishNanoTime()
		job := localPC.GetJob(allocation.GetJobID())
		JCT := finish - job.GetSubmitTimeNanoSecond()
		//log.Printf("allocation %v, start %v, finish %v, duration %v, JCT = %v", allocation, start, finish, finish-start, JCT)
		totalJCT += JCT
		totalJobsCount++
		if JCT > maximumJCT {
			maximumJCT = JCT
		}
		if job.GetDeadline() != math.MaxInt64 {
			withDeadlineCount++
			deadlineViolationDuration := finish - job.GetDeadline()
			if deadlineViolationDuration > 0 {
				totalDeadlineViolation += deadlineViolationDuration
				totalDeadlineViolatedCount++
			}
		}
	})
	avgJCT := float64(totalJCT) / float64(totalJobsCount)
	log.Printf("totalJobsCount %d, avg JCT = %f, makespan = %d, withDeadlines = %d, violatedJobsCount = %d, totalDeadlineViolation = %d", totalJobsCount, avgJCT, maximumJCT, withDeadlineCount, totalDeadlineViolatedCount, totalDeadlineViolation)
	return avgJCT, totalDeadlineViolatedCount, withDeadlineCount
}
