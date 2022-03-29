package simulator

import (
	"UNS/events"
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	"UNS/predictor/interfaces"
	"UNS/schedulers"
	"UNS/schedulers/partition"
	"UNS/utils"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"io/ioutil"
	"log"
	"math"
)

type DiscreteSyncDLTSimulator struct {
	config               *configs.DLTSimulatorConfiguration
	partitionContext     *partition.Context
	predictor            interfaces.Predictor
	scheduleIntervalNano int64
	updateJobAllocations []*pb_gen.JobAllocation
	nextScheduleTime     int64
}

func NewDiscreteSyncDLTSimulator(configurationPath string) *DiscreteSyncDLTSimulator {
	config := &configs.DLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(configurationPath)
	if err != nil {
		panic(err)
	}
	err = utils.Unmarshal(string(bytes), config)
	if err != nil {
		panic(err)
	}
	return &DiscreteSyncDLTSimulator{
		config: config,
	}
}

func (s *DiscreteSyncDLTSimulator) StartSimulation() {
	s.simulatePrerequisite()
	s.simulateInternal()
}

func (s *DiscreteSyncDLTSimulator) simulatePrerequisite() {
	// 1. init service
	schedulers.InitLocalSchedulersService()
	serviceInst := schedulers.GetServiceInstance()
	serviceInst.StartService()
	rmConfiguration := s.config.GetRmConfiguration()
	// 2. register resource manager
	result := serviceInst.RegisterRM(&eventobjs.RMRegisterResourceManagerEvent{
		Configuration: rmConfiguration,
	}, s)
	if !result.Succeeded {
		panic(result.Reason)
	}
	if size := len(rmConfiguration.GetCluster().GetPartitions()); size == 0 || size > 1 {
		panic("DiscreteSyncDLTSimulator partition count is not 1.")
	}
	var err error
	// 3. build partition
	s.partitionContext, err = partition.Build(rmConfiguration.GetCluster().GetPartitions()[0])
	if err != nil {
		panic(err)
	}
	time := int64(0)
	s.partitionContext.Time = &time
	// 4. build predictor
	s.predictor = predictor.BuildPredictor(s.config.PredictorConfiguration)
	// 5. extract interval nano
	s.scheduleIntervalNano = func() int64 {
		schedulerConfig := rmConfiguration.SchedulersConfiguration.GetPartitionID2SchedulerConfiguration()[s.GetPartitionID()]
		configInterface := pb_gen.ExtractSchedulerConfiguration(schedulerConfig)
		type intervalNanoGetter interface {
			GetIntervalNano() int64
		}
		switch g := configInterface.(type) {
		case intervalNanoGetter:
			return g.GetIntervalNano()
		}
		return math.MaxInt64
	}()
	// 6. update time to 0
	s.pushUpdateTime(0)
}

func (s *DiscreteSyncDLTSimulator) simulateInternal() {
	simulations := []func() *timeAndCallback{
		s.simulateClosestFinishAllocation,
		s.simulateClosestSubmitJobs(),
		s.simulateNextIntervalScheduleTime,
		s.simulateNextActiveScheduleTime,
	}
	for {
		closestTime := int64(math.MaxInt64)
		var callbacks []func()
		tacs := make([]*timeAndCallback, 0, len(simulations))
		for _, simulate := range simulations {
			tac := simulate()
			tacs = append(tacs, tac)
			if tac.nanoTime == math.MaxInt64 {
				continue
			}
			if tac.nanoTime < closestTime {
				closestTime = tac.nanoTime
				callbacks = make([]func(), 0, 1)
			}
			if tac.nanoTime == closestTime {
				callbacks = append(callbacks, tac.callback)
			}
		}
		if len(s.updateJobAllocations) != 0 {
			// updateAllocationsEvents具有最高优先级，一定要保证两个partitionContext的一致性。
			tac := s.simulateUpdateAllocations()
			callbacks = []func(){tac.callback}
			closestTime = tac.nanoTime
		}
		log.Printf("simulator time passed to %f seconds.", float64(closestTime)/1e9)
		err := s.partitionContext.UpdateTime(&eventobjs.RMUpdateTimeEvent{CurrentNanoTime: closestTime})
		if err != nil {
			panic(err)
		}
		for _, callback := range callbacks {
			callback()
		}
		if len(s.partitionContext.FinishedJobs) == len(s.getJobs()) {
			log.Printf("Simulation Finished.")
			s.partitionContext.ExecutionHistoryManager.Range(func(history *objects.JobExecutionHistory) {
				s, _ := utils.MarshalJsonPB(history)
				log.Println(s)
			})
			metrics := &Metrics{}
			metrics.Analyse(s.partitionContext)
			return
		}
		log.Printf("simulation submitted unfinished jobs %d, unallocated jobs %d, unallocated accs %d, finished jobs %d, total jobs %d.",
			len(s.partitionContext.UnfinishedJobs),
			len(s.partitionContext.AllocationViews.UnallocatedJobs),
			len(s.partitionContext.AllocationViews.UnallocatedAcceleratorIDs),
			len(s.partitionContext.FinishedJobs), len(s.getJobs()))
		//time.Sleep(10 * time.Millisecond)
	}
}

type timeAndCallback struct {
	nanoTime  int64
	callback  func()
	necessity bool
}

func (s *DiscreteSyncDLTSimulator) simulateClosestFinishAllocation() *timeAndCallback {
	allocations := s.partitionContext.GetAllocationsSlice()
	//s.printAllocations(allocations)
	predictResult, err := s.predictor.Predict(s.partitionContext, allocations)
	if err != nil {
		panic(fmt.Sprintf("DiscreteSyncDLTSimulator Predict err = %s", err))
	}
	for _, allocation := range allocations {
		r := predictResult.GetResult(allocation.GetTaskAllocations()[0])
		if *r.GetFinishNanoTime() == *r.GetStartExecutionNanoTime() {
			panic(fmt.Sprintf("predictResult Finish = %d, Start = %d， allocation jobID = %s", *r.GetFinishNanoTime(), *r.GetStartExecutionNanoTime(), allocation.GetJobID()))
		}
	}
	finishTime := int64(math.MaxInt64)
	closest2FinishAllocations := make([]*pb_gen.JobAllocation, 0)
	for _, allocation := range allocations {
		r := predictResult.GetResult(allocation.GetTaskAllocations()[0])
		jobFinishNanoTime := *r.GetFinishNanoTime()
		if jobFinishNanoTime < finishTime {
			finishTime = jobFinishNanoTime
			closest2FinishAllocations = make([]*pb_gen.JobAllocation, 0)
		}
		if jobFinishNanoTime == finishTime {
			closest2FinishAllocations = append(closest2FinishAllocations, allocation)
		}
	}
	newStartedPlaceholderAllocations := make([]*pb_gen.JobAllocation, 0)
	for _, allocation := range allocations {
		r := predictResult.GetResult(allocation.GetTaskAllocations()[0])
		if allocation.GetTaskAllocations()[0].GetPlaceholder() && allocation.GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond() == nil && r.GetStartExecutionNanoTime() != nil {
			newStartedPlaceholderAllocations = append(newStartedPlaceholderAllocations, allocation)
		}
	}
	return &timeAndCallback{
		nanoTime:  finishTime,
		necessity: true,
		callback: func() {
			jobExecutionHistories := make([]*objects.JobExecutionHistory, 0, len(closest2FinishAllocations))
			finishedJobIDs := make([]string, 0, len(closest2FinishAllocations))
			for _, allocation := range closest2FinishAllocations {
				finishedJobIDs = append(finishedJobIDs, allocation.GetJobID())
				jobExecutionHistories = append(jobExecutionHistories, buildJobExecutionHistory(allocation, finishTime))
			}
			for _, allocation := range newStartedPlaceholderAllocations {
				for _, taskAllocation := range allocation.GetTaskAllocations() {
					taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: finishTime}
				}
			}
			err := s.partitionContext.UpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
				UpdatedJobAllocations: pb_gen.UnwrapJobAllocations(newStartedPlaceholderAllocations),
				FinishedJobIDs:        finishedJobIDs,
				JobExecutionHistories: jobExecutionHistories,
				CurrentNanoTime:       &wrappers.Int64Value{Value: finishTime},
			})
			if err != nil {
				panic(err)
			}
			log.Printf("simulateClosestFinishAllocation callback called, closest to finish allocations = %+v, current nano time = %d", closest2FinishAllocations, finishTime)
			s.pushUpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
				UpdatedJobAllocations: pb_gen.UnwrapJobAllocations(newStartedPlaceholderAllocations),
				FinishedJobIDs:        finishedJobIDs,
				JobExecutionHistories: jobExecutionHistories,
				CurrentNanoTime:       &wrappers.Int64Value{Value: finishTime},
			})
		},
	}
}

func (s *DiscreteSyncDLTSimulator) simulateClosestSubmitJobs() func() *timeAndCallback {
	iter := iteratorJobsBySubmitTime(s.getJobs())
	return func() *timeAndCallback {
		submitTime, jobs, next := iter()
		if submitTime == math.MaxInt64 {
			return &timeAndCallback{
				nanoTime: math.MaxInt64,
				callback: func() {},
			}
		}
		return &timeAndCallback{
			nanoTime:  submitTime,
			necessity: true,
			callback: func() {
				err := s.partitionContext.UpdateJobs(&eventobjs.RMUpdateJobsEvent{
					NewJobs:         jobs,
					CurrentNanoTime: &wrappers.Int64Value{Value: submitTime},
				})
				if err != nil {
					panic(err)
				}
				log.Printf("simulateClosestSubmitJobs callback called, closest to submit jobs = %+v, current nano time = %d", jobs, submitTime)
				s.pushNewJobs(submitTime, jobs)
				next()
			},
		}
	}
}

func (s *DiscreteSyncDLTSimulator) simulateNextIntervalScheduleTime() *timeAndCallback {
	t := s.partitionContext.Now() + s.scheduleIntervalNano
	return &timeAndCallback{
		nanoTime:  t,
		necessity: false,
		callback: func() {
			log.Printf("simulateNextIntervalScheduleTime callback called, current nano time = %d", t)
			s.pushUpdateTime(t)
		},
	}
}

func (s *DiscreteSyncDLTSimulator) simulateNextActiveScheduleTime() *timeAndCallback {
	now := s.partitionContext.Now()
	t := int64(math.MaxInt64)
	if s.nextScheduleTime > now {
		t = s.nextScheduleTime
	}
	return &timeAndCallback{
		nanoTime:  t,
		necessity: false,
		callback: func() {
			//log.Printf("simulateNextActiveScheduleTime callback called, current nano time = %d", t)
			//s.pushUpdateTime(t)
		},
	}
}

func (s *DiscreteSyncDLTSimulator) simulateUpdateAllocations() *timeAndCallback {
	if len(s.updateJobAllocations) > 0 {
		jobAllocations := s.updateJobAllocations
		s.updateJobAllocations = s.updateJobAllocations[:0]
		t := s.partitionContext.Now()
		return &timeAndCallback{nanoTime: t, necessity: true, callback: func() {
			log.Printf("simulateUpdateAllocations callback called, current nano time = %d", t)
			s.pushUpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
				UpdatedJobAllocations: pb_gen.UnwrapJobAllocations(jobAllocations),
				CurrentNanoTime:       &wrappers.Int64Value{Value: t},
			})
		}}
	}
	return &timeAndCallback{nanoTime: math.MaxInt64, necessity: true, callback: nil}
}

func (s *DiscreteSyncDLTSimulator) pushUpdateAllocations(event *eventobjs.RMUpdateAllocationsEvent) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data:       event,
		ResultChan: resultChan,
	})
	allocations := event.GetUpdatedJobAllocations()
	jobIDs := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		jobIDs = append(jobIDs, allocation.GetJobID())
	}
	// wait sync
	r := <-resultChan
	log.Printf("simulator pushUpdateAllocations jobIDs = %+v, result %+v\n", jobIDs, r)
}

func (s *DiscreteSyncDLTSimulator) pushNewJobs(submitTime int64, newJobs []*objects.Job) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: &eventobjs.RMUpdateJobsEvent{
			NewJobs:         newJobs,
			CurrentNanoTime: &wrappers.Int64Value{Value: submitTime},
		},
		ResultChan: resultChan,
	})
	jobIDs := make([]string, 0, len(newJobs))
	for _, newJob := range newJobs {
		jobIDs = append(jobIDs, newJob.GetJobID())
	}
	// wait sync
	r := <-resultChan
	log.Printf("simulator pushNewJobs finished, newJobs = %+v, result %+v\n", jobIDs, r)
}

func (s *DiscreteSyncDLTSimulator) pushUpdateTime(currentNanoTime int64) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: &eventobjs.RMUpdateTimeEvent{
			CurrentNanoTime: currentNanoTime,
		},
		ResultChan: resultChan,
	})
	// wait sync
	r := <-resultChan
	log.Printf("simulator pushUpdateTime %d, result %+v\n", currentNanoTime, r)
}

func (s *DiscreteSyncDLTSimulator) push(event *events.Event) {
	inst := schedulers.GetServiceInstance()
	inst.Push(s.GetResourceManagerID(), s.GetPartitionID(), event)
}

func (s *DiscreteSyncDLTSimulator) getRegisterConfiguration() *configs.RMConfiguration {
	return s.config.GetRmConfiguration()
}

func (s *DiscreteSyncDLTSimulator) GetResourceManagerID() string {
	return s.config.GetResourceManagerID()
}

func (s *DiscreteSyncDLTSimulator) GetPartitionID() string {
	return s.config.GetPartitionID()
}

func (s *DiscreteSyncDLTSimulator) HandleEvent(event *events.Event) {
	err := func() error {
		switch eo := event.Data.(type) {
		case *eventobjs.SSUpdateAllocationsEvent:
			return s.handleSSUpdateAllocation(eo)
		default:
			panic(fmt.Sprintf("DiscreteSyncDLTSimulator handle unknown event %+v", event))
		}
	}()
	if err != nil {
		events.Reply(event, &events.Result{
			Succeeded: false,
			Reason:    err.Error(),
		})
	} else {
		events.ReplySucceeded(event)
	}
}

func (s *DiscreteSyncDLTSimulator) handleSSUpdateAllocation(eo *eventobjs.SSUpdateAllocationsEvent) error {
	if len(eo.GetNewJobAllocations()) == 0 {
		log.Printf("simulator handleSSUpdateAllocation received zero new allocations.")
		return nil
	}
	now := s.partitionContext.Now()
	occupiedAcceleratorIDs := make(map[string]bool)
	for _, pendingAllocation := range s.partitionContext.Allocations {
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(pendingAllocation) {
			occupiedAcceleratorIDs[acceleratorID] = true
		}
	}

	// 对调度器给予的分配，进行过滤
	allocations := pb_gen.WrapJobAllocations(eo.NewJobAllocations)
	nonPlaceholders := make([]*pb_gen.JobAllocation, 0, len(allocations))
	placeholders := make([]*pb_gen.JobAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		if s.partitionContext.Allocations[allocation.GetJobID()] != nil {
			reason := fmt.Sprintf("simulator ignores allocation of jobID = %s since it is already allocated", allocation.GetJobID())
			log.Println(reason)
			panic(reason)
		}
		if allocation.GetTaskAllocations()[0].GetPlaceholder() {
			placeholders = append(placeholders, allocation)
		} else {
			nonPlaceholders = append(nonPlaceholders, allocation)
		}
	}
	filteredAllocations := make([]*pb_gen.JobAllocation, 0, len(allocations))

	for _, nonPlaceholderAllocation := range nonPlaceholders {
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(nonPlaceholderAllocation) {
			occupiedAcceleratorIDs[acceleratorID] = true
		}
		for _, taskAllocation := range nonPlaceholderAllocation.GetTaskAllocations() {
			taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: now}
		}
		filteredAllocations = append(filteredAllocations, nonPlaceholderAllocation)
	}

nextPlaceholderAlloc:
	for _, placeholderAllocation := range placeholders {
		// 检查placeholder的allocation，它需要的资源是否已经空闲了
		if !placeholderAllocation.GetTaskAllocations()[0].GetPlaceholder() {
			continue
		}
		acceleratorIDs := pb_gen.GetAllocatedAcceleratorIDs(placeholderAllocation)
		for _, acceleratorID := range acceleratorIDs {
			if occupiedAcceleratorIDs[acceleratorID] {
				// 资源已被占用，则不赋予开始时间为now
				filteredAllocations = append(filteredAllocations, placeholderAllocation)
				continue nextPlaceholderAlloc
			}
		}
		for _, taskAllocation := range placeholderAllocation.GetTaskAllocations() {
			taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: now}
		}
		filteredAllocations = append(filteredAllocations, placeholderAllocation)
	}

	filteredJobIDs := make([]string, 0, len(filteredAllocations))
	for _, a := range filteredAllocations {
		filteredJobIDs = append(filteredJobIDs, a.GetJobID())
	}
	log.Printf("simulator update SS allocations, job IDs = %+v\n", filteredJobIDs)
	if len(filteredAllocations) > 0 {
		err := s.partitionContext.UpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
			UpdatedJobAllocations: pb_gen.UnwrapJobAllocations(filteredAllocations),
			CurrentNanoTime:       &wrappers.Int64Value{Value: s.partitionContext.Now()},
		})
		fastFail(err)
		s.updateJobAllocations = append(s.updateJobAllocations, filteredAllocations...)
	}
	return nil
}

func (s *DiscreteSyncDLTSimulator) getJobs() []*objects.Job {
	return s.config.GetJobs()
}
