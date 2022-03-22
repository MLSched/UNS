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
	"UNS/schedulers/impls/DLT/UNS/score"
	"UNS/schedulers/impls/DLT/UNS/types"
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

	BenefitsCalculator        benefitsinterfaces.Calculator
	BenefitsSampler           sampler.Sampler
	ScoreCalculator           score.Calculator
	MaximumSameBenefit        int
	MaxLatency                time.Duration
	MaxRound                  int
	GreedyFallback            bool
	allocationProvideTypeMode base.ProvideType
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.UNSSchedulerConfiguration)
	sche := &Scheduler{
		Config:    c,
		Predictor: predictor.BuildPredictor(c.GetPredictorConfiguration()),
		AllocationsProvider: &base.AllocationsProviderImpl{
			MaxGangAllocations: math.MaxInt64,
		},
		//BenefitsCalculator: benefits.NewJCTCalculator(),
		BenefitsCalculator: benefits.NewDDLCalculator(),
		BenefitsSampler:    sampler.NewIncrementalSampler(50, 30, 5),
		ScoreCalculator:    score.NewConsolidationScoreCalculator(),
		MaxRound:           10,
		MaximumSameBenefit: 1,
		MaxLatency:         time.Second,
		GreedyFallback:     false,
	}
	sche.allocationProvideTypeMode = base.ProvideTypeDefault
	if c.GetNonSpaceSharing() {
		sche.allocationProvideTypeMode |= base.ProvideTypeOnlyNonSpaceSharing
	}
	sche.DLTSchedulerTemplate = base.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	originalPC := s.GetPartitionContext().Clone(false)
	log.Printf("unallocated accIDs = %v", originalPC.GetUnallocatedAcceleratorIDs())
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	jobAllocations := make([]*objects.JobAllocation, 0)
	if s.checkScheduleAble(pc) {
		jas := s.parallelSchedule(pc, false)
		jobAllocations = append(jobAllocations, jas...)
		for _, ja := range jas {
			s.TempAllocJob(pc, ja)
		}
	}
	if !s.checkScheduleAble(pc) {
		return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: jobAllocations}
	}
	// 当执行一次调度后，仍然存在未分配的任务和加速器时
	if s.GreedyFallback {
		log.Printf("[UNS Scheduler] enter greedy fallback schedule, use greedy occupation mode.")
		jas := s.parallelSchedule(pc, true)
		jobAllocations = append(jobAllocations, jas...)
	} else {
		count := 0
		for s.checkScheduleAble(pc) {
			count++
			log.Printf("[UNS Scheduler] enter loop schedule, count = %d", count)
			jas := s.parallelSchedule(pc, false)
			jobAllocations = append(jobAllocations, jas...)
			if len(jas) == 0 {
				break
			}
			for _, ja := range jas {
				s.TempAllocJob(pc, ja)
			}
		}
	}
	if len(jobAllocations) == 0 {
		return nil
	}
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: jobAllocations}
}

func benefitsSorter(data []*types.AllocContext) *utils.Sorter {
	return &utils.Sorter{
		LenFunc: func() int {
			return len(data)
		},
		LessFunc: func(i, j int) bool {
			if data[i].GetBenefit() == data[j].GetBenefit() {
				c1 := data[i]
				c2 := data[j]
				return c1.NewJobAllocationsFingerPrint < c2.NewJobAllocationsFingerPrint
			}
			return data[i].GetBenefit() > data[j].GetBenefit()
		},
		SwapFunc: func(i, j int) {
			t := data[i]
			data[i] = data[j]
			data[j] = t
		},
	}
}

// 将收益从大到小排序
func sortBenefits(data []*types.AllocContext) {
	sorter := benefitsSorter(data)
	if !sort.IsSorted(sorter) {
		sort.Stable(sorter)
		//sort.Sort(sorter)
	}
}

func (s *Scheduler) genJobAllocationFingerPrint(jobAllocation *objects.JobAllocation) string {
	b := &strings.Builder{}
	b.WriteString(jobAllocation.GetJobID())
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		b.WriteByte('|')
		b.WriteString(taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
	}
	return b.String()
}

func (s *Scheduler) genJobAllocationsFingerPrint(jobAllocations []*objects.JobAllocation) string {
	fingerPrints := make([]string, 0, len(jobAllocations))
	for _, jobAllocation := range jobAllocations {
		fingerPrints = append(fingerPrints, s.genJobAllocationFingerPrint(jobAllocation))
	}
	sort.Strings(fingerPrints)
	return strings.Join(fingerPrints, "\n")
}

func (s *Scheduler) parallelSchedule(pc *partition.Context, greedyOccupation bool) []*objects.JobAllocation {
	provideTypeMode := s.allocationProvideTypeMode
	if greedyOccupation {
		provideTypeMode |= base.ProvideTypeOnlyUnoccupied
	}
	unallocatedJobs := pc.GetUnallocatedJobs()
	// Clone后将时间固定住
	acs := make([]*types.AllocContext, 0)
	acs = append(acs, &types.AllocContext{
		PC:                           pc,
		Job:                          nil,
		JobAllocation:                nil,
		NewJobAllocations:            make([]*objects.JobAllocation, 0),
		NewJobAllocationsFingerPrint: "",
	})
	round := 0
	for round < s.MaxRound && round < len(unallocatedJobs) {
		round++
		// 在每一轮中，对所有基础的partitionContext，让所有未得到分配的任务，尝试它的所有放置可能，并获得一个benefit。
		// 获得了全部benefit之后，对它们进行排序，再sample
		mu := &sync.Mutex{}
		withBenefits := make([]*types.AllocContext, 0, 1024*256)
		wg := &sync.WaitGroup{}
		wg.Add(len(acs))
		for _, ac := range acs {
			ac := ac
			go func() {
				r := s.predictPCBenefits(ac, provideTypeMode)
				mu.Lock()
				defer mu.Unlock()
				withBenefits = append(withBenefits, r...)
				wg.Done()
			}()
		}
		wg.Wait()
		sortBenefits(withBenefits)
		withBenefits = s.DeDuplicate(withBenefits)
		withBenefits = s.FilterSameBenefitsByScore(withBenefits, s.MaximumSameBenefit)
		withBenefits = s.BenefitsSampler.Sample(withBenefits)
		nextRoundAcs := make([]*types.AllocContext, 0, len(withBenefits))
		for _, withBenefit := range withBenefits {
			ac := withBenefit
			cancel := s.TempAllocJob(ac.PC, ac.JobAllocation)
			ac.PC = ac.PC.Clone(false)
			nextRoundAcs = append(nextRoundAcs, ac)
			cancel()
		}
		acs = nextRoundAcs
	}
	if len(acs) == 0 {
		return nil
	}
	bestAC := acs[0]
	filteredJobAllocations := s.FilterScheduleAbleJobAllocations(bestAC.PC, pc)
	if len(filteredJobAllocations) == 0 {
		return nil
	}
	return filteredJobAllocations
}

func (s *Scheduler) DeDuplicate(sorted []*types.AllocContext) []*types.AllocContext {
	result := make([]*types.AllocContext, 0, len(sorted))
	var last *types.AllocContext = nil
	for _, withBenefit := range sorted {
		if last == nil || withBenefit.GetBenefit() != last.GetBenefit() {
			last = withBenefit
			result = append(result, withBenefit)
			continue
		}
		// benefit相同时，去掉jobAllocationsFingerPrint相同的withBenefit
		// 保留fingerPrint不同的withBenefit
		ac := withBenefit
		if ac.NewJobAllocationsFingerPrint != last.NewJobAllocationsFingerPrint {
			last = ac
			result = append(result, ac)
		}
	}
	return result
}

func (s *Scheduler) FilterSameBenefitsByScore(sorted []*types.AllocContext, maximumSameBenefit int) []*types.AllocContext {
	benefit2Items := make(map[benefitsinterfaces.Benefit][]*types.AllocContext)
	for _, withBenefit := range sorted {
		ac := withBenefit
		benefit := ac.GetBenefit()
		if _, ok := benefit2Items[benefit]; !ok {
			benefit2Items[benefit] = make([]*types.AllocContext, 0)
		}
		benefit2Items[benefit] = append(benefit2Items[benefit], ac)
	}
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	allBenefits := make([]benefitsinterfaces.Benefit, 0, len(benefit2Items))
	resultBenefit2Items := make(map[benefitsinterfaces.Benefit][]*types.AllocContext)
	for benefit, items := range benefit2Items {
		allBenefits = append(allBenefits, benefit)
		wg.Add(1)
		items := items
		benefit := benefit
		go func() {
			sort.Slice(items, func(i, j int) bool {
				return items[i].GetScore() > items[j].GetScore()
			})
			mu.Lock()
			defer mu.Unlock()
			if len(items) < maximumSameBenefit {
				resultBenefit2Items[benefit] = items
			} else {
				resultBenefit2Items[benefit] = items[:maximumSameBenefit]
			}
			wg.Done()
		}()
	}
	wg.Wait()
	sort.Slice(allBenefits, func(i, j int) bool {
		return allBenefits[i] > allBenefits[j]
	})
	result := make([]*types.AllocContext, 0, len(allBenefits)*maximumSameBenefit)
	for _, benefit := range allBenefits {
		items := resultBenefit2Items[benefit]
		result = append(result, items...)
	}
	return result
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

func (s *Scheduler) predictPCBenefits(ac *types.AllocContext, provideTypeMode base.ProvideType) []*types.AllocContext {
	pc := ac.PC
	acs := make([]*types.AllocContext, 0)
	basePredictResult := ac.PredictResult
	var err error
	if basePredictResult == nil {
		basePredictResult, err = s.Predictor.Predict(pc, pc.GetAllocationsSlice())
		if err != nil {
			for _, m := range pc.GetAllocationsSlice() {
				st, _ := utils.MarshalJsonPB(m)
				log.Printf("%s", st)
			}
			reason := fmt.Sprintf("[UNS Scheduler] Predict basePredictResult failed, which should not happened since this prediction is guaranteed to be success, err=%v", err)
			log.Println(reason)
			panic(reason)
		}
	}
	_, baseBenefitsStub := s.BenefitsCalculator.Cal(pc, basePredictResult)
	_, baseScoreStub := s.ScoreCalculator.GetScore(pc, pc.GetAllocationsSlice())
	jobs := pc.GetUnallocatedJobs()
	jobIDs := s.getSortedJobIDs(jobs)
	accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
	for _, jobID := range jobIDs {
		job := jobs[jobID]
		possibleJobAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job, provideTypeMode)
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
					// 忽略显存溢出造成的问题
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
			benefit, _ := s.BenefitsCalculator.CalIncrementally(pc, partialPredictResult, baseBenefitsStub)
			jobAllocationsScore, _ := s.ScoreCalculator.GetScoreIncrementally(pc, []*objects.JobAllocation{jobAllocation}, baseScoreStub)
			newJobAllocations := make([]*objects.JobAllocation, len(ac.NewJobAllocations), len(ac.NewJobAllocations)+1)
			copy(newJobAllocations, ac.NewJobAllocations)
			newJobAllocations = append(newJobAllocations, jobAllocation)
			acs = append(acs, &types.AllocContext{
				PC:                           pc,
				Job:                          job,
				JobAllocation:                jobAllocation,
				NewJobAllocations:            newJobAllocations,
				NewJobAllocationsFingerPrint: s.genJobAllocationsFingerPrint(newJobAllocations),
				Benefit:                      benefit,
				Score:                        jobAllocationsScore,
			})
			cancelAlloc()
		}
	}
	sortBenefits(acs)
	acs = s.FilterSameBenefitsByScore(acs, s.MaximumSameBenefit)
	acs = s.BenefitsSampler.Sample(acs)
	return acs
}

func (s *Scheduler) getSortedJobIDs(jobs map[string]*objects.Job) []string {
	jobIDs := make([]string, 0, len(jobs))
	for jobID := range jobs {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	return jobIDs
}
