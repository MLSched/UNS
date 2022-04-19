package simulator

import (
	"fmt"
	"github.com/MLSched/UNS/events"
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	eventobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/predictor"
	"github.com/MLSched/UNS/predictor/interfaces"
	"github.com/MLSched/UNS/schedulers"
	"github.com/MLSched/UNS/schedulers/partition"
	"github.com/MLSched/UNS/utils"
	"github.com/golang/protobuf/ptypes/wrappers"
	"io/ioutil"
	"log"
	"sort"
	"sync"
	"time"
)

var asyncCheckFinishedInterval = 1000 * time.Millisecond
var asyncCheckSubmitInterval = 1000 * time.Millisecond

type ContinuousAsyncDLTSimulator struct {
	wg                      *sync.WaitGroup
	partitionEditMu         *sync.Mutex
	startSimulationTime     time.Time
	config                  *configs.DLTSimulatorConfiguration
	partitionContext        *partition.Context
	predictor               interfaces.Predictor
	scheduleIntervalNano    int64
	updateAllocationsEvents []*eventobjs.RMUpdateAllocationsEvent
}

func NewContinuousAsyncDLTSimulator(configurationPath string) *ContinuousAsyncDLTSimulator {
	config := &configs.DLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(configurationPath)
	fastFail(err)
	err = utils.Unmarshal(string(bytes), config)
	fastFail(err)
	return &ContinuousAsyncDLTSimulator{
		wg:              &sync.WaitGroup{},
		config:          config,
		partitionEditMu: &sync.Mutex{},
	}
}

func (s *ContinuousAsyncDLTSimulator) StartSimulation() {
	s.startSimulationTime = time.Now()

	s.initEnvironment()
	s.doSimulation()
}

func (s *ContinuousAsyncDLTSimulator) initEnvironment() {
	// 1. build partition
	var err error
	rmConfiguration := s.config.GetRmConfiguration()
	s.partitionContext, err = partition.Build(rmConfiguration.GetCluster().GetPartitions()[0])
	fastFail(err)
	// 2. init service
	schedulers.InitLocalSchedulersService()
	serviceInst := schedulers.GetServiceInstance()
	serviceInst.StartService()
	// 3. register resource manager
	result := serviceInst.RegisterRM(&eventobjs.RMRegisterResourceManagerEvent{
		Configuration: rmConfiguration,
	}, s)
	if !result.Succeeded {
		panic(result.Reason)
	}
	if size := len(rmConfiguration.GetCluster().GetPartitions()); size == 0 || size > 1 {
		panic("ContinuousAsyncDLTSimulator partition count is not 1.")
	}
	// 4. build predictor
	s.predictor = predictor.BuildPredictor(s.config.PredictorConfiguration)
}

func (s *ContinuousAsyncDLTSimulator) doSimulation() {
	s.wg.Add(2)
	go s.checkSubmitJobs()
	go s.checkFinishedJobs()
	s.wg.Wait()
}

func (s *ContinuousAsyncDLTSimulator) checkSubmitJobs() {
	iter := iteratorJobsBySubmitTime(s.getJobs())
	hasNext := true
	for hasNext {
		normSubmitTime, batchJobs, hasNextFunc := iter()
		hasNext = hasNextFunc()
		submitTime := s.startSimulationTime.Add(time.Duration(normSubmitTime) * time.Nanosecond)
		for now := s.partitionContext.Now(); submitTime.After(time.Unix(0, now)); now = s.partitionContext.Now() {
			log.Printf("Check submit jobs, now %s, next submit time %s\n", time.Unix(0, now), submitTime.String())
			time.Sleep(asyncCheckSubmitInterval)
		}
		log.Printf("submit jobs, now %s, submit time %s\n", time.Unix(0, s.partitionContext.Now()), submitTime.String())
		for _, job := range batchJobs {
			job.SubmitTimeNanoSecond = submitTime.UnixNano()
		}
		s.partitionEditLocked(func() {
			err := s.partitionContext.UpdateJobs(&eventobjs.RMUpdateJobsEvent{
				NewJobs: batchJobs,
			})
			fastFail(err)
		})
		s.pushNewJobs(batchJobs...)
	}
	log.Printf("Check submit jobs, no more new jobs.")
	s.wg.Done()
}

func (s *ContinuousAsyncDLTSimulator) partitionEditLocked(f func()) {
	s.partitionEditMu.Lock()
	defer s.partitionEditMu.Unlock()
	f()
}

func (s *ContinuousAsyncDLTSimulator) checkFinishedJobs() {
	var notFinished = true
	for notFinished {
		time.Sleep(asyncCheckFinishedInterval)
		s.partitionEditLocked(func() {
			now := s.partitionContext.Now()
			predictResults, err := s.predictor.Predict(s.partitionContext, s.partitionContext.AllocationViews.AllocationsSlice)
			log.Printf("Check finished jobs, now %s", time.Unix(0, now).String())
			printPredictResults(predictResults, true)
			if err != nil {
				log.Println(err)
			}
			fastFail(err)
			jobExecutionHistories := make([]*objects.JobExecutionHistory, 0)
			newlyStartedAllocations := make([]*pb_gen.JobAllocation, 0)
			isNewlyStarted := func(results interfaces.PredictResult, allocation *pb_gen.JobAllocation) bool {
				ftaskAllocation := allocation.GetTaskAllocations()[0]
				r := results.GetResult(ftaskAllocation)
				if *r.GetStartExecutionNanoTime() <= now && ftaskAllocation.GetStartExecutionTimeNanoSecond() == nil {
					return true
				}
				return false
			}
			finishedJobIDs := make([]string, 0)
			for _, allocation := range s.partitionContext.AllocationViews.AllocationsSlice {
				ftaskAllocation := allocation.GetTaskAllocations()[0]
				r := predictResults.GetResult(ftaskAllocation)
				if isNewlyStarted(predictResults, allocation) {
					newlyStartedAllocations = append(newlyStartedAllocations, allocation)
					continue
				}
				if *r.GetFinishNanoTime() > now {
					continue
				}
				finishedJobIDs = append(finishedJobIDs, allocation.GetJobID())
				jobExecutionHistory := buildJobExecutionHistory(allocation, *r.GetFinishNanoTime())
				jobExecutionHistories = append(jobExecutionHistories, jobExecutionHistory)
			}
			if len(newlyStartedAllocations) == 0 && len(finishedJobIDs) == 0 && len(jobExecutionHistories) == 0 {
				return
			}
			log.Printf("simulator newlyStartedAllocations = %v, finishedJobIDs = %v", newlyStartedAllocations, finishedJobIDs)
			ev := &eventobjs.RMUpdateAllocationsEvent{
				UpdatedJobAllocations: pb_gen.UnwrapJobAllocations(newlyStartedAllocations),
				FinishedJobIDs:        finishedJobIDs,
				JobExecutionHistories: jobExecutionHistories,
			}
			err = s.partitionContext.UpdateAllocations(ev)
			fastFail(err)
			s.pushUpdateAllocations(ev)
			if len(s.partitionContext.FinishedJobs) == len(s.getJobs()) {
				log.Printf("Simulation Finished.")
				s.partitionContext.ExecutionHistoryManager.Range(func(history *objects.JobExecutionHistory) {
					s, _ := utils.MarshalJsonPB(history)
					log.Println(s)
				})
				notFinished = false
				return
			}
		})
	}
	s.wg.Done()
}

func (s *ContinuousAsyncDLTSimulator) pushUpdateAllocations(event *eventobjs.RMUpdateAllocationsEvent) {
	s.push(&events.Event{
		Data: event,
	})
	allocations := event.GetUpdatedJobAllocations()
	jobIDs := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		jobIDs = append(jobIDs, allocation.GetJobID())
	}
	log.Printf("simulator pushUpdateAllocations jobIDs = %+v\n", jobIDs)
}

func (s *ContinuousAsyncDLTSimulator) pushNewJobs(newJobs ...*objects.Job) {
	s.push(&events.Event{
		Data: &eventobjs.RMUpdateJobsEvent{
			NewJobs: newJobs,
		},
	})
	jobIDs := make([]string, 0, len(newJobs))
	for _, newJob := range newJobs {
		jobIDs = append(jobIDs, newJob.GetJobID())
	}
	log.Printf("simulator pushNewJobs newJobs = %+v", jobIDs)
}

func (s *ContinuousAsyncDLTSimulator) push(event *events.Event) {
	inst := schedulers.GetServiceInstance()
	inst.Push(s.GetResourceManagerID(), s.GetPartitionID(), event)
}

func (s *ContinuousAsyncDLTSimulator) getRegisterConfiguration() *configs.RMConfiguration {
	return s.config.GetRmConfiguration()
}

func (s *ContinuousAsyncDLTSimulator) GetResourceManagerID() string {
	return s.config.GetResourceManagerID()
}

func (s *ContinuousAsyncDLTSimulator) GetPartitionID() string {
	return s.config.GetPartitionID()
}

func (s *ContinuousAsyncDLTSimulator) HandleEvent(event *events.Event) {
	err := func() error {
		switch eo := event.Data.(type) {
		case *eventobjs.SSUpdateAllocationsEvent:
			return s.handleSSUpdateAllocation(eo)
		default:
			panic(fmt.Sprintf("ContinuousAsyncDLTSimulator handle unknown event %+v", event))
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

func (s *ContinuousAsyncDLTSimulator) handleSSUpdateAllocation(eo *eventobjs.SSUpdateAllocationsEvent) error {
	s.partitionEditLocked(func() {
		// 获取当前集群中已经被占用的加速器
		occupiedAcceleratorIDs := make(map[string]bool)
		for _, allocation := range s.partitionContext.Allocations {
			for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(allocation) {
				occupiedAcceleratorIDs[acceleratorID] = true
			}
		}
		// 对调度器给予的分配，进行过滤
		allocations := pb_gen.WrapJobAllocations(eo.NewJobAllocations)
		nonPlaceholders := make([]*pb_gen.JobAllocation, 0, len(allocations))
		placeholders := make([]*pb_gen.JobAllocation, 0, len(allocations))
		for _, allocation := range allocations {
			if allocation.GetTaskAllocations()[0].GetPlaceholder() {
				placeholders = append(placeholders, allocation)
			} else {
				nonPlaceholders = append(nonPlaceholders, allocation)
			}
		}
		filteredAllocations := make([]*pb_gen.JobAllocation, 0, len(allocations))
		now := s.partitionContext.Now()
	nextNonPlaceholderAlloc:
		for _, nonPlaceholderAllocation := range nonPlaceholders {
			if s.partitionContext.Allocations[nonPlaceholderAllocation.GetJobID()] != nil {
				reason := fmt.Sprintf("simulator ignores allocation of jobID = %s since it is already allocated", nonPlaceholderAllocation.GetJobID())
				log.Println(reason)
				continue nextNonPlaceholderAlloc
			}
			// 这里的条件判断应该会更复杂，我这里写的比较简单，暂时没有考虑SpaceSharing。
			// 实际实现应该考虑占用同一Accelerator的任务是否可以同时运行，再拒绝掉不可以运行的jobAllocation
			for _, acceleratorID := range pb_gen.GetAllocatedAcceleratorIDs(nonPlaceholderAllocation) {
				if occupiedAcceleratorIDs[acceleratorID] == true {
					log.Printf("simulator ignores allocation of jobID = %s, acceleratorID = %s is already occupied", nonPlaceholderAllocation.GetJobID(), acceleratorID)
					continue nextNonPlaceholderAlloc
				}
			}
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
			// 我这里实际上没有做资源预留，并没有起到placeholder的作用，只是简单检查了当前资源是否完全足够
			// 实际需要一些更复杂的实现
			acceleratorIDs := pb_gen.GetAllocatedAcceleratorIDs(placeholderAllocation)
			for _, acceleratorID := range acceleratorIDs {
				if occupiedAcceleratorIDs[acceleratorID] {
					log.Printf("simulator ignores placeholder allocation of jobID = %s, acceleratorID = %s is already occupied", placeholderAllocation.GetJobID(), acceleratorID)
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
			})
			fastFail(err)
			s.pushUpdateAllocations(&eventobjs.RMUpdateAllocationsEvent{
				UpdatedJobAllocations: pb_gen.UnwrapJobAllocations(filteredAllocations),
			})
		}
	})
	return nil
}

func (s *ContinuousAsyncDLTSimulator) getJobs() []*objects.Job {
	return s.config.GetJobs()
}

func (s *ContinuousAsyncDLTSimulator) sortJobsBySubmission(jobs []*objects.Job) {
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(jobs)
		},
		LessFunc: func(i, j int) bool {
			return jobs[i].GetSubmitTimeNanoSecond() < jobs[j].GetSubmitTimeNanoSecond()
		},
		SwapFunc: func(i, j int) {
			t := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = t
		},
	}
	sort.Sort(sorter)
}

func (s *ContinuousAsyncDLTSimulator) printAllocations(allocations []*pb_gen.JobAllocation) {
	for _, a := range allocations {
		s, _ := utils.MarshalJsonPB(a)
		log.Println(s)
	}
}
