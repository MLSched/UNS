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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"sort"
)

type SinglePartitionDLTSimulator struct {
	config               *configs.SinglePartitionDLTSimulatorConfiguration
	partitionContext     *partition.Context
	predictor            interfaces.Predictor
	scheduleIntervalNano int64
}

func NewSinglePartitionDLTSimulator(configurationPath string) *SinglePartitionDLTSimulator {
	config := &configs.SinglePartitionDLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(configurationPath)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(bytes, config)
	if err != nil {
		panic(err)
	}
	return &SinglePartitionDLTSimulator{
		config: config,
	}
}

func (s *SinglePartitionDLTSimulator) StartSimulation() {
	s.simulatePrerequisite()
	s.simulateInternal()
}

func (s *SinglePartitionDLTSimulator) simulatePrerequisite() {
	// 1. init service
	schedulers.InitLocalSchedulersService()
	serviceInst := schedulers.GetServiceInstance()
	serviceInst.StartService()
	rmConfiguration := s.config.GetRmConfiguration()
	// 2. register resource manager
	serviceInst.RegisterRM(&eventobjs.RMRegisterResourceManagerEvent{
		ResourceManagerID: s.GetResourceManagerID(),
		Configuration:     rmConfiguration,
	}, s)
	if size := len(rmConfiguration.GetCluster().GetPartitions()); size == 0 || size > 1 {
		panic("SinglePartitionDLTSimulator partition count is not 1.")
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

func (s *SinglePartitionDLTSimulator) simulateInternal() {
	type timeAndCallback struct {
		nanoTime int64
		callback func()
	}
	simulateClosest2FinishAllocation := func() *timeAndCallback {
		allocations := s.partitionContext.GetPendingAllocationsSlice()
		predictResult, err := s.predictor.Predict(s.partitionContext, allocations)
		if err != nil {
			panic(fmt.Sprintf("SinglePartitionDLTSimulator Predict err = %s", err))
		}
		finishTime := int64(math.MaxInt64)
		closest2FinishAllocations := make([]*objects.JobAllocation, 0)
		for _, allocation := range allocations {
			r, _ := predictResult.GetResult(allocation)
			if r.GetFinishNanoTime() < finishTime {
				finishTime = r.GetFinishNanoTime()
				closest2FinishAllocations = make([]*objects.JobAllocation, 0)
			}
			if r.GetFinishNanoTime() == finishTime {
				closest2FinishAllocations = append(closest2FinishAllocations, allocation)
			}
		}
		newStartedPlaceholderAllocations := make([]*objects.JobAllocation, 0)
		for _, allocation := range allocations {
			r, _ := predictResult.GetResult(allocation)
			if allocation.GetPlaceholder() && allocation.GetStartExecutionTimeNanoSecond() == 0 && r.GetStartExecutionNanoTime() != 0 {
				newStartedPlaceholderAllocations = append(newStartedPlaceholderAllocations, allocation)
			}
		}
		return &timeAndCallback{
			nanoTime: finishTime,
			callback: func() {
				for _, allocation := range closest2FinishAllocations {
					allocation.Finished = true
					allocation.DurationNanoSecond = finishTime - allocation.GetStartExecutionTimeNanoSecond()
				}
				for _, allocation := range newStartedPlaceholderAllocations {
					allocation.StartExecutionTimeNanoSecond = finishTime
				}
				s.partitionContext.HandleUpdateAllocationsEvent(&eventobjs.RMUpdateAllocationsEvent{
					JobAllocations:  append(closest2FinishAllocations, newStartedPlaceholderAllocations...),
					CurrentNanoTime: finishTime,
				}, nil)
				s.pushAllocations(finishTime, closest2FinishAllocations)
			},
		}
	}
	simulateClosestSubmitJobs := func() func() *timeAndCallback {
		iter := s.iteratorJobsBySubmitTime()
		hasNext := true
		return func() *timeAndCallback {
			submitTime, jobs, next := iter()
			if submitTime == math.MaxInt64 {
				return &timeAndCallback{
					nanoTime: math.MaxInt64,
					callback: func() {},
				}
			}
			return &timeAndCallback{
				nanoTime: submitTime,
				callback: func() {
					s.pushNewJobs(submitTime, jobs)
					hasNext = next()
				},
			}
		}
	}()
	simulateNextIntervalScheduleTime := func() *timeAndCallback {
		t := s.partitionContext.Now() + s.scheduleIntervalNano
		return &timeAndCallback{
			nanoTime: t,
			callback: func() {
				s.pushUpdateTime(t)
			},
		}
	}
	simulations := []func() *timeAndCallback{
		simulateClosest2FinishAllocation,
		simulateClosestSubmitJobs,
		simulateNextIntervalScheduleTime,
	}
	for {
		closestTime := int64(math.MaxInt64)
		var callbacks []func()
		for _, simulate := range simulations {
			tac := simulate()
			if tac.nanoTime == math.MaxInt64 {
				continue
			}
			if tac.nanoTime < closestTime {
				closestTime = tac.nanoTime
				callbacks = []func(){tac.callback}
			}
			if tac.nanoTime == closestTime {
				callbacks = append(callbacks, tac.callback)
			}
		}
		for _, callback := range callbacks {
			callback()
		}
		if closestTime == math.MaxInt64 {
			log.Printf("Simulation Finished.")
			return
		}
	}
}

func (s *SinglePartitionDLTSimulator) pushAllocations(currentNanoTime int64, allocations []*objects.JobAllocation) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: eventobjs.RMUpdateAllocationsEvent{
			JobAllocations:  allocations,
			CurrentNanoTime: currentNanoTime,
		},
		ResultChan: resultChan,
	})
	// wait sync
	<-resultChan
}

func (s *SinglePartitionDLTSimulator) pushNewJobs(submitTime int64, newJobs []*objects.Job) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: &eventobjs.RMUpdateJobsEvent{
			NewJobs:         newJobs,
			CurrentNanoTime: submitTime,
		},
		ResultChan: resultChan,
	})
	// wait sync
	<-resultChan
}

func (s *SinglePartitionDLTSimulator) pushUpdateTime(currentNanoTime int64) {
	resultChan := make(chan *events.Result)
	s.push(&events.Event{
		Data: eventobjs.RMUpdateTimeEvent{
			CurrentNanoTime: currentNanoTime,
		},
		ResultChan: resultChan,
	})
	// wait sync
	<-resultChan
}

func (s *SinglePartitionDLTSimulator) push(event *events.Event) {
	inst := schedulers.GetServiceInstance()
	inst.Push(s.GetResourceManagerID(), s.GetPartitionID(), event)
}

func (s *SinglePartitionDLTSimulator) getRegisterConfiguration() *configs.RMConfiguration {
	return s.config.GetRmConfiguration()
}

func (s *SinglePartitionDLTSimulator) GetResourceManagerID() string {
	return s.config.GetResourceManagerID()
}

func (s *SinglePartitionDLTSimulator) GetPartitionID() string {
	return s.config.GetPartitionID()
}

func (s *SinglePartitionDLTSimulator) HandleEvent(event *events.Event) {
	switch eo := event.Data.(type) {
	case *eventobjs.SSUpdateAllocationsEvent:
		s.handleUpdateAllocation(eo)
	}
}

func (s *SinglePartitionDLTSimulator) handleUpdateAllocation(eo *eventobjs.SSUpdateAllocationsEvent) {
	occupiedAcceleratorIDs := make(map[string]bool)
	for _, pendingAllocation := range s.partitionContext.PendingAllocations {
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(pendingAllocation) {
			occupiedAcceleratorIDs[acceleratorID] = true
		}
	}
	allocations := eo.NewJobAllocations
	filteredAllocations := make([]*objects.JobAllocation, 0, len(allocations))
nextAlloc:
	for _, allocation := range allocations {
		if allocation.GetPlaceholder() {
			filteredAllocations = append(filteredAllocations, allocation)
			continue
		}
		for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(allocation) {
			if occupiedAcceleratorIDs[acceleratorID] == true {
				log.Printf("simulator ignores allocation of jobID = %s, acceleratorID = %s is already occupied", allocation.GetJobID(), acceleratorID)
				continue nextAlloc
			}
		}
		filteredAllocations = append(filteredAllocations, allocation)
	}
	s.partitionContext.HandleUpdateAllocationsEvent(&eventobjs.RMUpdateAllocationsEvent{
		JobAllocations:  filteredAllocations,
		CurrentNanoTime: s.partitionContext.Now(),
	}, nil)
}

// iterJobsBySubmitTime 获取根据submitTime排序的下一波任务。(submitTime int64, jobs []*objects.Job, next func())
func (s *SinglePartitionDLTSimulator) iteratorJobsBySubmitTime() func() (int64, []*objects.Job, func() bool) {
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

func (s *SinglePartitionDLTSimulator) getJobs() []*objects.Job {
	jobs := make([]*objects.Job, 0)
	return jobs
}
