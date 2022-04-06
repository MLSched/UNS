package methods

import (
	"UNS/events"
	"UNS/mock"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	predictorinterfaces "UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"github.com/golang/protobuf/ptypes/wrappers"
	"log"
	"testing"
)

func MockUNS(eventPusher base.EventPusher, pc *partition.Context) interfaces.Scheduler {
	config := mock.DLTSimulatorConfiguration()
	c := config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["PARTITION_ID"].GetUnsSchedulerConfiguration()
	sche, err := Build(c, eventPusher, func() *partition.Context {
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
	scheduler := MockUNS(pusher, pc)
	scheduler.StartService()
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
	totalJobsCount := int64(0)
	result.Range(func(allocation *objects.TaskAllocation, result predictorinterfaces.EachPredictResult) {
		start := *result.GetStartExecutionNanoTime()
		finish := *result.GetFinishNanoTime()
		JCT := finish - localPC.GetJob(allocation.GetJobID()).GetSubmitTimeNanoSecond()
		log.Printf("allocation %v, start %v, finish %v, duration %v, JCT = %v", allocation, start, finish, finish-start, JCT)
		totalJCT += JCT
		totalJobsCount++
	})
	avgJCT := float64(totalJCT) / float64(totalJobsCount)
	log.Printf("avg JCT = %f", avgJCT)
}
