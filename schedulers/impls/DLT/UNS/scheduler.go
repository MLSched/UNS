package UNS

import (
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	predictorinterfaces "UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/UNS/benefits"
	benefitsinterfaces "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/impls/DLT/UNS/sampler"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"container/list"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"log"
	"math"
	"sort"
	"time"
)

type Scheduler struct {
	*base.DLTSchedulerTemplate
	Config              *configs.UNSSchedulerConfiguration
	Predictor           predictorinterfaces.Predictor
	AllocationsProvider base.AllocationsProvider

	BenefitsCalculator benefitsinterfaces.Calculator
	BenefitsSampler    sampler.Sampler
	MaxLatency         time.Duration
	MaxRound           int
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.UNSSchedulerConfiguration)
	sche := &Scheduler{
		Config:    c,
		Predictor: predictor.BuildPredictor(c.PredictorConfiguration),
		AllocationsProvider: &base.AllocationsProviderImpl{
			//MaxGangAllocations: len(partitionContextAware().View.AcceleratorID2Accelerator) * 2,
			MaxGangAllocations: math.MaxInt64,
		},
		BenefitsCalculator: benefits.NewJCTCalculator(),
		BenefitsSampler:    sampler.NewFixExponentSampler(10),
		MaxRound:           10,
		MaxLatency:         time.Second,
	}
	sche.DLTSchedulerTemplate = base.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	//return s.testSchedule()

	return s.serialSchedule()
}

func (s *Scheduler) testSchedule() *eventobjs.SSUpdateAllocationsEvent {
	pc := s.GetPartitionContext().Clone(false)
	// Clone后将时间固定住
	t := pc.Now()
	pc.Time = &t
	//accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
	newAllocations := make([]*objects.JobAllocation, 0)
nextJob:
	for _, job := range pc.GetUnallocatedJobs() {
		basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
		if err != nil {
			panic(err)
		}
		accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
		if len(possibleAllocations) == 0 {
			continue
		}
		for _, possibleAllocation := range possibleAllocations {
			targetAllocation := possibleAllocation
			cancel := s.tempAllocJob(pc, targetAllocation)
			//pc.UnfinishedJobs[job.GetJobID()] = job
			//pc.Allocations[job.GetJobID()] = targetAllocation
			_, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
			if err != nil {
				panic(err)
			}
			if s.allocateAbleJob(pc, targetAllocation) {
				newAllocations = append(newAllocations, targetAllocation)
				continue nextJob
			}
			//if *pr.GetResult(targetAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime() == pc.FixedNow() {
			//	newAllocations = append(newAllocations, targetAllocation)
			//	continue nextJob
			//}
			cancel()
			//delete(pc.UnfinishedJobs, job.GetJobID())
			//delete(pc.Allocations, job.GetJobID())
		}
	}
	if len(newAllocations) == 0 {
		return nil
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: newAllocations}
}

type allocContextWithBenefit struct {
	pc            *partition.Context
	Job           *objects.Job
	JobAllocation *objects.JobAllocation
	Benefit       benefitsinterfaces.Benefit
	Stub          interface{}
}

func (j *allocContextWithBenefit) GetBenefit() benefitsinterfaces.Benefit {
	return j.Benefit
}

func sortBenefits(data []sampler.WithBenefit) {
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(data)
		},
		LessFunc: func(i, j int) bool {
			return data[i].GetBenefit() < data[j].GetBenefit()
		},
		SwapFunc: func(i, j int) {
			t := data[i]
			data[i] = data[j]
			data[j] = t
		},
	}
	if !sort.IsSorted(sorter) {
		sort.Sort(sorter)
	}
}

func (s *Scheduler) serialSchedule() *eventobjs.SSUpdateAllocationsEvent {
	pc := s.GetPartitionContext().Clone(false)
	unallocatedJobs := pc.GetUnallocatedJobs()
	if len(unallocatedJobs) == 0 {
		return nil
	}
	// Clone后将时间固定住
	t := pc.Now()
	pc.Time = &t
	pcs := make([]*partition.Context, 0)
	pcs = append(pcs, pc)
	round := 0
	for round < s.MaxRound && round < len(unallocatedJobs) {
		round++
		// 在每一轮中，对所有基础的partitionContext，让所有未得到分配的任务，尝试它的所有放置可能，并获得一个benefit。
		// 获得了全部benefit之后，对它们进行排序，再sample
		withBenefits := make([]sampler.WithBenefit, 0, 1024*256)
		for _, pc := range pcs {
			pc := pc
			basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
			if err != nil {
				//log.Printf("%v", pc.GetAllocationsSlice())
				if err != nil {
					// 如果这里出错，则认为这个分配是不合理的，直接跳过
					log.Printf("total")
					for _, m := range pc.GetAllocationsSlice() {
						st, _ := utils.MarshalJsonPB(m)
						log.Printf("%s", st)
					}
					log.Printf("[UNS Scheduler] find unproper job allocation, err=[%v]", err)
					panic("test fast fail")
					//continue
				}
				reason := fmt.Sprintf("[UNS Scheduler] Predict basePredictResult failed, which should not happened since this prediction is guaranteed to be success, err=%v", err)
				log.Println(reason)
				panic(reason)
			}
			_, baseBenefitsStub := s.BenefitsCalculator.Cal(pc, basePredictResult)
			jobs := pc.GetUnallocatedJobs()
			jobIDs := s.getSortedJobIDs(jobs)
			for _, allocation := range pc.Allocations {
				if round == 3 && jobIDs[0] == "f6304007ef3e25fc37150d2c" {
					if allocation.GetJobID() == "c0cb989683049a1f41b8d6d2" && allocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() == 502071157466476 {
						//log.Printf("")
					}
				}
			}
			accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
			//log.Printf("%+v", accID2SortedTaskAllocations["inst-1-replica-0-cpusocket-0-acc-0"])
			for _, jobID := range jobIDs {
				job := jobs[jobID]
				possibleJobAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
				if possibleJobAllocations[0].GetTaskAllocations()[0].GetAllocationTimeNanoSecond() == 502071157466476 {
					//log.Printf("")
				}
				for _, jobAllocation := range possibleJobAllocations {
					// 对于每个可能的分配，临时得将该分配结果赋予给partitionContext。
					cancelAlloc := s.tempAllocJob(pc, jobAllocation)
					// 随后获取新的jobAllocation的所有相关的jobAllocations
					relatedJobAllocations := s.relatedJobAllocations(pc, accID2SortedTaskAllocations, jobAllocation)
					for _, jobAllocation := range relatedJobAllocations {
						if jobAllocation.GetJobID() == "3e6cdf99157dd45eacb23445" && jobAllocation.GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond() == nil {
							log.Printf("")
						}
					}
					// 使用这些相关的jobAllocations，提高predict的计算速度。
					partialPredictResult, err := s.Predictor.Predict(pc, relatedJobAllocations)
					if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
						s.markGangJobStartTime(jobAllocation, *partialPredictResult.GetResult(jobAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
					}
					//partialPredictResult.Range(func(allocation *objects.TaskAllocation, result predictorinterfaces.EachPredictResult) {
					//	if result.GetStartExecutionNanoTime() == nil || result.GetFinishNanoTime() == nil {
					//		log.Printf("")
					//	}
					//})
					//partialPredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
					if err != nil {
						log.Printf("total")
						for _, m := range pc.GetAllocationsSlice() {
							st, _ := utils.MarshalJsonPB(m)
							log.Printf("%s", st)
						}
						log.Printf("[UNS Scheduler] find unproper job allocation, err=[%v]", err)
						panic("fast fail")
						// 如果这里出错，则认为这个分配是不合理的，直接跳过
						//continue
					}
					// 合并得到完整的predictResult。
					// completePredictResult := basePredictResult.Combine(partialPredictResult)
					benefit, stub := s.BenefitsCalculator.CalIncrementally(pc, partialPredictResult, baseBenefitsStub)
					withBenefits = append(withBenefits, &allocContextWithBenefit{
						pc:            pc,
						Job:           job,
						JobAllocation: jobAllocation,
						Benefit:       benefit,
						Stub:          stub,
					})
					cancelAlloc()
				}
			}
		}
		sortBenefits(withBenefits)
		withBenefits = s.BenefitsSampler.Sample(withBenefits)
		nextRoundPcs := make([]*partition.Context, 0, len(withBenefits))
		for _, withBenefit := range withBenefits {
			ja := withBenefit.(*allocContextWithBenefit)
			cancel := s.tempAllocJob(ja.pc, ja.JobAllocation)
			nextRoundPcs = append(nextRoundPcs, ja.pc.Clone(false))
			cancel()
		}
		pcs = nextRoundPcs
	}
	bestPC := pcs[0]
	filteredJobAllocations := s.filterScheduleAbleJobAllocations(bestPC, pc)
	if len(filteredJobAllocations) == 0 {
		return nil
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: filteredJobAllocations}
}

func (s *Scheduler) tempAllocJob(pc *partition.Context, jobAllocation *objects.JobAllocation) (cancel func()) {
	pc.Allocations[jobAllocation.GetJobID()] = jobAllocation
	return func() {
		delete(pc.Allocations, jobAllocation.GetJobID())
	}
}

// 获取一个jobAllocation所占用的acc的其他jobAllocation，若其他jobAllocation占用了更多的acc，则迭代以上过程
// 举例：job1占用了acc1，acc2，job2占用了acc2，acc3，job3占用了acc4，acc5：则最终，获得job1的relatedJobAllocations会返回job1, job2。
func (s *Scheduler) relatedJobAllocations(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, jobAllocation *objects.JobAllocation) []*objects.JobAllocation {
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

// 从predictPC中，找出那些立即可以运行的jobAllocations
// 可立即运行的标准：
// 1. single的job，只需startExecutionTime是now即可。
// 2. gang的job，需要
func (s *Scheduler) filterScheduleAbleJobAllocations(predictPC *partition.Context, currPC *partition.Context) []*objects.JobAllocation {
	newJobAllocations := make(map[string]*objects.JobAllocation)
	for jobID, jobAllocation := range predictPC.Allocations {
		newJobAllocations[jobID] = jobAllocation
	}
	for jobID := range currPC.Allocations {
		delete(newJobAllocations, jobID)
	}
	result := make([]*objects.JobAllocation, 0, len(newJobAllocations))
	for _, jobAllocation := range newJobAllocations {
		if s.allocateAbleJob(currPC, jobAllocation) {
			result = append(result, jobAllocation)
		}
	}
	return result
}

func (s *Scheduler) allocateAbleJob(pc *partition.Context, jobAllocation *objects.JobAllocation) bool {
	if jobAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() == pc.FixedNow() {
		return true
	}
	return false
}

func (s *Scheduler) getSortedJobIDs(jobs map[string]*objects.Job) []string {
	jobIDs := make([]string, 0, len(jobs))
	for jobID := range jobs {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	return jobIDs
}

func (s *Scheduler) markGangJobStartTime(jobAllocation *objects.JobAllocation, startTime int64) {
	// 为gang类型的job修正开始时间
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: startTime}
	}
}
