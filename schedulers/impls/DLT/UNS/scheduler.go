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
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
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
		BenefitsSampler:    sampler.NewFixExponentSampler(1000),
		MaxRound:           50,
		MaxLatency:         time.Second,
	}
	sche.DLTSchedulerTemplate = base.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	originalPC := s.GetPartitionContext().Clone(false)
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	jobAllocations := make([]*objects.JobAllocation, 0)
	var count = 0
	for s.checkScheduleAble(pc) {
		count++
		log.Printf("[UNS Scheduler] do schedule count %d", count)
		jas := s.parallelSchedule(pc)
		jobAllocations = append(jobAllocations, jas...)
		if len(jas) == 0 {
			break
		}
		for _, ja := range jas {
			s.TempAllocJob(pc, ja)
		}
	}
	if len(jobAllocations) == 0 {
		return nil
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: jobAllocations}
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

// 将收益从大到小排序
func sortBenefits(data []sampler.WithBenefit) {
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(data)
		},
		LessFunc: func(i, j int) bool {
			if data[i].GetBenefit() == data[j].GetBenefit() {
				c1 := data[i].(*allocContextWithBenefit)
				c2 := data[j].(*allocContextWithBenefit)
				if c1.Job.GetJobID() == c2.Job.GetJobID() {
					f := c1.JobAllocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
					s := c2.JobAllocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
					if f == s {
						c1a := make([]string, 0, len(c1.JobAllocation.GetTaskAllocations()))
						c2a := make([]string, 0, len(c2.JobAllocation.GetTaskAllocations()))
						for _, ta := range c1.JobAllocation.GetTaskAllocations() {
							c1a = append(c1a, ta.GetAcceleratorAllocation().GetAcceleratorID())
						}
						for _, ta := range c2.JobAllocation.GetTaskAllocations() {
							c2a = append(c2a, ta.GetAcceleratorAllocation().GetAcceleratorID())
						}
						return strings.Join(c1a, "") < strings.Join(c2a, "")
					}
					return f < s
				}
				return c1.Job.GetJobID() < c2.Job.GetJobID()
			}
			return data[i].GetBenefit() > data[j].GetBenefit()
		},
		SwapFunc: func(i, j int) {
			t := data[i]
			data[i] = data[j]
			data[j] = t
		},
	}
	if !sort.IsSorted(sorter) {
		sort.Stable(sorter)
		//sort.Sort(sorter)
	}
}

func (s *Scheduler) parallelSchedule(pc *partition.Context) []*objects.JobAllocation {
	unallocatedJobs := pc.GetUnallocatedJobs()
	// Clone后将时间固定住
	pcs := make([]*partition.Context, 0)
	pcs = append(pcs, pc)
	round := 0
	for round < s.MaxRound && round < len(unallocatedJobs) {
		round++
		// 在每一轮中，对所有基础的partitionContext，让所有未得到分配的任务，尝试它的所有放置可能，并获得一个benefit。
		// 获得了全部benefit之后，对它们进行排序，再sample
		mu := &sync.Mutex{}
		withBenefits := make([]sampler.WithBenefit, 0, 1024*256)
		wg := &sync.WaitGroup{}
		wg.Add(len(pcs))
		for _, pc := range pcs {
			pc := pc
			go func() {
				r := s.predictPCBenefits(pc)
				mu.Lock()
				defer mu.Unlock()
				withBenefits = append(withBenefits, r...)
				wg.Done()
			}()
		}
		wg.Wait()
		sortBenefits(withBenefits)
		withBenefits = s.BenefitsSampler.Sample(withBenefits)
		nextRoundPcs := make([]*partition.Context, 0, len(withBenefits))
		for _, withBenefit := range withBenefits {
			ja := withBenefit.(*allocContextWithBenefit)
			cancel := s.TempAllocJob(ja.pc, ja.JobAllocation)
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
	return filteredJobAllocations
}

func (s *Scheduler) serialSchedule(pc *partition.Context) []*objects.JobAllocation {
	//pc := s.GetPartitionContext().Clone(false)
	unallocatedJobs := pc.GetUnallocatedJobs()
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
			withBenefits = append(withBenefits, s.predictPCBenefits(pc)...)
		}
		sortBenefits(withBenefits)
		withBenefits = s.BenefitsSampler.Sample(withBenefits)
		nextRoundPcs := make([]*partition.Context, 0, len(withBenefits))
		for _, withBenefit := range withBenefits {
			ja := withBenefit.(*allocContextWithBenefit)
			cancel := s.TempAllocJob(ja.pc, ja.JobAllocation)
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
	return filteredJobAllocations
}

func (s *Scheduler) checkScheduleAble(pc *partition.Context) bool {
	unallocatedJobs := pc.GetUnallocatedJobs()
	if len(unallocatedJobs) == 0 {
		return false
	}
	unallocatedAcceleratorIDs := pc.GetUnallocatedAcceleratorIDs()
	if len(unallocatedAcceleratorIDs) == 0 {
		return false
	}
	return true
}

func (s *Scheduler) predictPCBenefits(pc *partition.Context) []sampler.WithBenefit {
	withBenefits := make([]sampler.WithBenefit, 0)
	basePredictResult, err := s.Predictor.Predict(pc, pc.GetAllocationsSlice())
	if err != nil {
		log.Printf("total")
		for _, m := range pc.GetAllocationsSlice() {
			st, _ := utils.MarshalJsonPB(m)
			log.Printf("%s", st)
		}
		reason := fmt.Sprintf("[UNS Scheduler] Predict basePredictResult failed, which should not happened since this prediction is guaranteed to be success, err=%v", err)
		log.Println(reason)
		panic(reason)
		//continue
	}
	_, baseBenefitsStub := s.BenefitsCalculator.Cal(pc, basePredictResult)
	jobs := pc.GetUnallocatedJobs()
	jobIDs := s.getSortedJobIDs(jobs)
	accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
	for _, jobID := range jobIDs {
		job := jobs[jobID]
		possibleJobAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job)
		for _, jobAllocation := range possibleJobAllocations {
			// 对于每个可能的分配，临时得将该分配结果赋予给partitionContext。
			cancelAlloc := s.TempAllocJob(pc, jobAllocation)
			// 随后获取新的jobAllocation的所有相关的jobAllocations
			relatedJobAllocations := s.RelatedJobAllocations(pc, accID2SortedTaskAllocations, jobAllocation)
			// 使用这些相关的jobAllocations，提高predict的计算速度。
			partialPredictResult, err := s.Predictor.Predict(pc, relatedJobAllocations)
			if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
				s.MarkGangJobStartTime(jobAllocation, *partialPredictResult.GetResult(jobAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
			}
			if err != nil {
				log.Printf("[UNS Scheduler] predict failed, err=[%v]", err)
				if predictorinterfaces.IsSpaceSharingOutOfMemoryError(err) {
					// 忽略内存溢出造成的问题
					continue
				}
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
	sortBenefits(withBenefits)
	withBenefits = s.BenefitsSampler.Sample(withBenefits)
	return withBenefits
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
	var earliestUnableToAllocateTime int64 = math.MaxInt64
	for _, jobAllocation := range newJobAllocations {
		if s.AllocateAbleJob(currPC, jobAllocation) {
			result = append(result, jobAllocation)
		} else if t := s.AllocationTime(jobAllocation); t < earliestUnableToAllocateTime {
			earliestUnableToAllocateTime = t
		}
	}
	return result
}

func (s *Scheduler) getSortedJobIDs(jobs map[string]*objects.Job) []string {
	jobIDs := make([]string, 0, len(jobs))
	for jobID := range jobs {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	return jobIDs
}
