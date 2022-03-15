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
	"sort"
	"strconv"
)

type MonoPartitionAsyncDLTSimulator struct {
	config                  *configs.MonoPartitionAsyncDLTSimulatorConfiguration
	partitionContext        *partition.Context
	predictor               interfaces.Predictor
	scheduleIntervalNano    int64
	updateAllocationsEvents []*eventobjs.RMUpdateAllocationsEvent
}

func NewMonoPartitionAsyncDLTSimulator(configurationPath string) *MonoPartitionAsyncDLTSimulator {
	config := &configs.MonoPartitionAsyncDLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(configurationPath)
	if err != nil {
		panic(err)
	}
	err = utils.Unmarshal(string(bytes), config)
	if err != nil {
		panic(err)
	}
	return &MonoPartitionAsyncDLTSimulator{
		config: config,
	}
}

func (s *MonoPartitionAsyncDLTSimulator) StartSimulation() {
	s.simulatePrerequisite()
	s.simulateInternal()
}

func (s *MonoPartitionAsyncDLTSimulator) simulatePrerequisite() {
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
		panic("MonoPartitionAsyncDLTSimulator partition count is not 1.")
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
}

func (s *MonoPartitionAsyncDLTSimulator) simulateInternal() {
	type timeAndCallback struct {
		nanoTime  int64
		callback  func()
		necessity bool
	}
	simulateClosestFinishAllocation := func() *timeAndCallback {
		allocations := s.partitionContext.GetPendingAllocationsSlice()
		//s.printAllocations(allocations)
		predictResult, err := s.predictor.Predict(s.partitionContext, allocations)
		if err != nil {
			panic(fmt.Sprintf("MonoPartitionAsyncDLTSimulator Predict err = %s", err))
		}
		for _, allocation := range allocations {
			r, _ := predictResult.GetResult(allocation.GetTaskAllocations()[0])
			if *r.GetFinishNanoTime() == *r.GetStartExecutionNanoTime() {
				panic(fmt.Sprintf("predictResult Finish = %d, Start = %d， allocation jobID = %s", *r.GetFinishNanoTime(), *r.GetStartExecutionNanoTime(), allocation.GetJobID()))
			}
		}
		finishTime := int64(math.MaxInt64)
		closest2FinishAllocations := make([]*objects.JobAllocation, 0)
		for _, allocation := range allocations {
			r, _ := predictResult.GetResult(allocation.GetTaskAllocations()[0])
			jobFinishNanoTime := *r.GetFinishNanoTime()
			if jobFinishNanoTime < finishTime {
				finishTime = jobFinishNanoTime
				closest2FinishAllocations = make([]*objects.JobAllocation, 0)
			}
			if jobFinishNanoTime == finishTime {
				closest2FinishAllocations = append(closest2FinishAllocations, allocation)
			}
		}
		newStartedPlaceholderAllocations := make([]*objects.JobAllocation, 0)
		for _, allocation := range allocations {
			r, _ := predictResult.GetResult(allocation.GetTaskAllocations()[0])
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
					taskExecutionHistories := make([]*objects.TaskExecutionHistory, 0, len(allocation.GetTaskAllocations()))
					finishedJobIDs = append(finishedJobIDs, allocation.GetJobID())
					ftaskAllocation := allocation.GetTaskAllocations()[0]
					for i, taskAllocation := range allocation.GetTaskAllocations() {
						taskExecutionHistories = append(taskExecutionHistories, &objects.TaskExecutionHistory{
							ExecutionID:                  strconv.Itoa(i),
							NodeID:                       taskAllocation.GetNodeID(),
							JobID:                        taskAllocation.GetJobID(),
							TaskID:                       taskAllocation.GetTaskID(),
							StartExecutionTimeNanoSecond: taskAllocation.GetStartExecutionTimeNanoSecond().GetValue(),
							DurationNanoSecond:           finishTime - ftaskAllocation.GetStartExecutionTimeNanoSecond().GetValue(),
							HostMemoryAllocation:         taskAllocation.GetHostMemoryAllocation(),
							CPUSocketAllocations:         taskAllocation.GetCPUSocketAllocations(),
							AcceleratorAllocation:        taskAllocation.GetAcceleratorAllocation(),
						})
					}
					jobExecutionHistories = append(jobExecutionHistories, &objects.JobExecutionHistory{
						JobID:                  allocation.GetJobID(),
						ResourceManagerID:      allocation.GetResourceManagerID(),
						PartitionID:            allocation.GetPartitionID(),
						TaskExecutionHistories: taskExecutionHistories,
					})
				}
				for _, allocation := range newStartedPlaceholderAllocations {
					for _, taskAllocation := range allocation.GetTaskAllocations() {
						taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: finishTime}
					}
				}
				err := s.partitionContext.UpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
					UpdatedJobAllocations: newStartedPlaceholderAllocations,
					FinishedJobIDs:        finishedJobIDs,
					JobExecutionHistories: jobExecutionHistories,
					CurrentNanoTime:       finishTime,
				})
				if err != nil {
					panic(err)
				}
				log.Printf("simulateClosestFinishAllocation callback called, closest to finish allocations = %+v, current nano time = %d", closest2FinishAllocations, finishTime)
				s.pushAllocations(finishTime, closest2FinishAllocations)
			},
		}
	}
	simulateClosestSubmitJobs := func() func() *timeAndCallback {
		iter := s.iteratorJobsBySubmitTime()
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
						CurrentNanoTime: submitTime,
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
	}()
	simulateNextIntervalScheduleTime := func() *timeAndCallback {
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
	simulateUpdateAllocations := func() *timeAndCallback {
		if len(s.updateAllocationsEvents) > 0 {
			e := s.updateAllocationsEvents[0]
			s.updateAllocationsEvents = s.updateAllocationsEvents[1:]
			t := e.GetCurrentNanoTime()
			return &timeAndCallback{nanoTime: t, necessity: true, callback: func() {
				log.Printf("simulateUpdateAllocations callback called, current nano time = %d", t)
				s.pushAllocations(t, e.GetUpdatedJobAllocations())
			}}
		}
		return &timeAndCallback{nanoTime: math.MaxInt64, necessity: true, callback: nil}
	}
	simulations := []func() *timeAndCallback{
		simulateUpdateAllocations,
		simulateClosestFinishAllocation,
		simulateClosestSubmitJobs,
		simulateNextIntervalScheduleTime,
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
		log.Printf("simulator time passed to %d", closestTime)
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
			return
		}
	}
}

func (s *MonoPartitionAsyncDLTSimulator) pushAllocations(currentNanoTime int64, allocations []*objects.JobAllocation) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: &eventobjs.RMUpdateAllocationsEvent{
			UpdatedJobAllocations: allocations,
			CurrentNanoTime:       currentNanoTime,
		},
		ResultChan: resultChan,
	})
	jobIDs := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		jobIDs = append(jobIDs, allocation.GetJobID())
	}
	// wait sync
	r := <-resultChan
	log.Printf("simulator pushUpdateAllocations jobIDs = %+v, result %+v\n", jobIDs, r)
}

func (s *MonoPartitionAsyncDLTSimulator) pushNewJobs(submitTime int64, newJobs []*objects.Job) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: &eventobjs.RMUpdateJobsEvent{
			NewJobs:         newJobs,
			CurrentNanoTime: submitTime,
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

func (s *MonoPartitionAsyncDLTSimulator) pushUpdateTime(currentNanoTime int64) {
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

func (s *MonoPartitionAsyncDLTSimulator) push(event *events.Event) {
	inst := schedulers.GetServiceInstance()
	inst.Push(s.GetResourceManagerID(), s.GetPartitionID(), event)
}

func (s *MonoPartitionAsyncDLTSimulator) getRegisterConfiguration() *configs.RMConfiguration {
	return s.config.GetRmConfiguration()
}

func (s *MonoPartitionAsyncDLTSimulator) GetResourceManagerID() string {
	return s.config.GetResourceManagerID()
}

func (s *MonoPartitionAsyncDLTSimulator) GetPartitionID() string {
	return s.config.GetPartitionID()
}

func (s *MonoPartitionAsyncDLTSimulator) HandleEvent(event *events.Event) {
	err := func() error {
		switch eo := event.Data.(type) {
		case *eventobjs.SSUpdateAllocationsEvent:
			return s.handleSSUpdateAllocation(eo)
		default:
			panic(fmt.Sprintf("MonoPartitionAsyncDLTSimulator handle unknown event %+v", event))
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

func (s *MonoPartitionAsyncDLTSimulator) handleSSUpdateAllocation(eo *eventobjs.SSUpdateAllocationsEvent) error {
	occupiedAcceleratorIDs := make(map[string]bool)
	for _, pendingAllocation := range s.partitionContext.Allocations {
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(pendingAllocation) {
			occupiedAcceleratorIDs[acceleratorID] = true
		}
	}
	allocations := eo.NewJobAllocations
	filteredAllocations := make([]*objects.JobAllocation, 0, len(allocations))
nextAlloc:
	for _, allocation := range allocations {
		if allocation.GetTaskAllocations()[0].GetPlaceholder() {
			filteredAllocations = append(filteredAllocations, allocation)
			continue
		}
		if s.partitionContext.Allocations[allocation.GetJobID()] != nil {
			reason := fmt.Sprintf("simulator ignores allocation of jobID = %s since it is already allocated", allocation.GetJobID())
			log.Println(reason)
			//panic(fmt.Sprintf("simulator ignores allocation of jobID = %s since it is already allocated", allocation.GetJobID()))
			continue nextAlloc
		}
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(allocation) {
			if occupiedAcceleratorIDs[acceleratorID] == true {
				//panic(fmt.Sprintf("simulator ignores allocation of jobID = %s, acceleratorID = %s is already occupied", allocation.GetJobID(), acceleratorID))
				log.Printf("simulator ignores allocation of jobID = %s, acceleratorID = %s is already occupied", allocation.GetJobID(), acceleratorID)
				continue nextAlloc
			}
		}
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(allocation) {
			occupiedAcceleratorIDs[acceleratorID] = true
		}
		for _, taskAllocation := range allocation.GetTaskAllocations() {
			taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: s.partitionContext.Now()}
		}
		//allocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: s.partitionContext.Now()}
		filteredAllocations = append(filteredAllocations, allocation)
	}
	filteredJobIDs := make([]string, 0, len(filteredAllocations))
	for _, a := range filteredAllocations {
		filteredJobIDs = append(filteredJobIDs, a.GetJobID())
	}
	log.Printf("simulator update SS allocations, job IDs = %+v\n", filteredJobIDs)
	err := s.partitionContext.UpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
		UpdatedJobAllocations: filteredAllocations,
		CurrentNanoTime:       s.partitionContext.Now(),
	})
	if err != nil {
		panic(err)
	}
	s.updateAllocationsEvents = append(s.updateAllocationsEvents, &eventobjs.RMUpdateAllocationsEvent{
		UpdatedJobAllocations: filteredAllocations,
		CurrentNanoTime:       s.partitionContext.Now(),
	})
	return nil
}

// iterJobsBySubmitTime 获取根据submitTime排序的下一波任务。(submitTime int64, jobs []*objects.Job, next func())
func (s *MonoPartitionAsyncDLTSimulator) iteratorJobsBySubmitTime() func() (int64, []*objects.Job, func() bool) {
	jobs := s.getJobs()
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(jobs)
		},
		LessFunc: func(i, j int) bool {
			return jobs[i].GetSubmitTimeNanoSecond() < jobs[j].GetSubmitTimeNanoSecond()
		},
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = o
		},
	}
	sort.Sort(sorter)
	currIndex := 0
	next := func(batchJobsSize int) bool {
		currIndex += batchJobsSize
		if currIndex >= len(jobs) {
			return false
		}
		return true
	}
	return func() (int64, []*objects.Job, func() bool) {
		if currIndex >= len(jobs) {
			return math.MaxInt64, nil, func() bool {
				return false
			}
		}
		submitTime := jobs[currIndex].GetSubmitTimeNanoSecond()
		nextBatchJobs := make([]*objects.Job, 0, 1)
		for i := currIndex; i < len(jobs); i++ {
			if jobs[i].GetSubmitTimeNanoSecond() == submitTime {
				nextBatchJobs = append(nextBatchJobs, jobs[i])
			}
		}
		return submitTime, nextBatchJobs, func() bool {
			return next(len(nextBatchJobs))
		}
	}
}

func (s *MonoPartitionAsyncDLTSimulator) getJobs() []*objects.Job {
	return s.config.GetJobs()
}

func (s *MonoPartitionAsyncDLTSimulator) printAllocations(allocations []*objects.JobAllocation) {
	for _, a := range allocations {
		s, _ := utils.MarshalJsonPB(a)
		log.Println(s)
	}
}
