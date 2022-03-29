package UNS

import (
	"UNS/pb_gen"
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
	MaxLatency                time.Duration // 一次调度算法执行的最大时延
	MaxRound                  int
	FallbackMode              FallbackMode
	allocationProvideTypeMode base.ProvideType
	ResourceEfficientMode     bool // 当启用资源高效模式时，在展开树节点时，将会优先考虑未被占用的资源
}

// FallbackMode 指定了当一次调度算法结束后，仍然存在未分配的任务和加速器时的策略
type FallbackMode int

const (
	LoopAllocation   FallbackMode = 0
	GreedyAllocation FallbackMode = 1 // 设置为GreedyAllocation后，当一次调度算法结束后，仍然存在未分配的任务和加速器时，贪婪地将它们调度上去
	LinearPrediction FallbackMode = 2 // 设置为LinearPrediction后，当一次调度算法结束后，仍然存在未分配的任务和加速器时，每次sample时，贪婪的sample第一个
)

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
		//BenefitsCalculator: benefits.NewMakeSpanCalculator(),
		//BenefitsCalculator: benefits.NewCompositeCalculator(map[benefitsinterfaces.Calculator]float64{
		//	benefits.NewJCTCalculator(): 1,
		//	benefits.NewDDLCalculator(): 1e20,
		//}),
		BenefitsCalculator: benefits.NewJCTCalculator(),
		//BenefitsCalculator: benefits.NewDDLCalculator(),
		//BenefitsSampler: sampler.NewIncrementalSampler(10, 8, 2),
		BenefitsSampler:    sampler.NewFixExponentSampler(10),
		ScoreCalculator:    score.NewConsolidationScoreCalculator(),
		MaxRound:           5,
		MaximumSameBenefit: 1,
		MaxLatency:         10 * time.Second,
		//MaxLatency:       1e9 * time.Second,
		FallbackMode: LinearPrediction,
		//FallbackMode: LoopAllocation,
		ResourceEfficientMode: true,
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
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	jobAllocations := make([]*pb_gen.JobAllocation, 0)
	if s.checkScheduleAble(pc) {
		jas := s.parallelSchedule(&scheduleContext{
			initialPC:       originalPC,
			pc:              pc,
			provideTypeMode: s.allocationProvideTypeMode,
			sampler:         s.BenefitsSampler,
			round:           s.MaxRound,
		})
		jobAllocations = append(jobAllocations, jas...)
		for _, ja := range jas {
			s.TempAllocJob(pc, ja)
		}
	}
	if s.checkScheduleAble(pc) {
		// 当执行一次调度后，仍然存在未分配的任务和加速器时，进入fallback调度模式
		jobAllocations = append(jobAllocations, s.fallbackSchedule(originalPC, pc)...)
	}
	if len(jobAllocations) == 0 {
		return nil
	}
	filteredJobAllocations := s.FilterScheduleAbleJobAllocations(jobAllocations, pc)
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(filteredJobAllocations)}
}

func (s *Scheduler) fallbackSchedule(originalPC *partition.Context, pc *partition.Context) []*pb_gen.JobAllocation {
	switch s.FallbackMode {
	case GreedyAllocation:
		log.Printf("[UNS Scheduler] enter greedy allocation fallback schedule")
		provideTypeMode := s.allocationProvideTypeMode | base.ProvideTypeOnlyUnoccupied
		jas := s.parallelSchedule(&scheduleContext{
			initialPC:       originalPC,
			pc:              pc,
			provideTypeMode: provideTypeMode,
			sampler:         s.BenefitsSampler,
			round:           1e9, // allocate all
		})
		return jas
	case LoopAllocation:
		count := 0
		jobAllocations := make([]*pb_gen.JobAllocation, 0)
		for s.checkScheduleAble(pc) {
			count++
			log.Printf("[UNS Scheduler] enter loop fallback schedule, count = %d", count)
			jas := s.parallelSchedule(&scheduleContext{
				initialPC:       originalPC,
				pc:              pc,
				provideTypeMode: s.allocationProvideTypeMode,
				sampler:         s.BenefitsSampler,
				round:           s.MaxRound,
			})
			jobAllocations = append(jobAllocations, jas...)
			if len(jas) == 0 {
				break
			}
			for _, ja := range jas {
				s.TempAllocJob(pc, ja)
			}
		}
		return jobAllocations
	case LinearPrediction:
		log.Printf("[UNS Scheduler] enter linear prediction fallback schedule")
		jas := s.parallelSchedule(&scheduleContext{
			initialPC:       originalPC,
			pc:              pc,
			provideTypeMode: s.allocationProvideTypeMode,
			sampler:         sampler.NewFixSampler(1),
			round:           1e9, // allocate all
		})
		return jas
	default:
		panic("Unsupported fallback mode.")
	}
}

// 将收益从大到小排序
func (s *Scheduler) sortBenefits(schedulerContext *scheduleContext, data []*types.AllocContext) {
	sort.SliceStable(data, func(i, j int) bool {
		if data[i].GetBenefit() == data[j].GetBenefit() {
			c1 := data[i]
			c2 := data[j]
			return c1.NewJobAllocationsFingerPrint < c2.NewJobAllocationsFingerPrint
		}
		return data[i].GetBenefit() > data[j].GetBenefit()
	})
}

func (s *Scheduler) genJobAllocationFingerPrint(jobAllocation *pb_gen.JobAllocation) string {
	b := &strings.Builder{}
	b.WriteString(jobAllocation.GetJobID())
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		b.WriteByte('|')
		b.WriteString(taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
	}
	return b.String()
}

func (s *Scheduler) genJobAllocationsFingerPrint(jobAllocations []*pb_gen.JobAllocation) string {
	fingerPrints := make([]string, 0, len(jobAllocations))
	for _, jobAllocation := range jobAllocations {
		fingerPrints = append(fingerPrints, s.genJobAllocationFingerPrint(jobAllocation))
	}
	sort.Strings(fingerPrints)
	return strings.Join(fingerPrints, "\n")
}

type scheduleContext struct {
	initialPC       *partition.Context
	pc              *partition.Context
	provideTypeMode base.ProvideType
	sampler         sampler.Sampler
	round           int
}

func (s *Scheduler) parallelSchedule(param *scheduleContext) []*pb_gen.JobAllocation {
	pc := param.pc
	unallocatedJobs := pc.AllocationViews.UnallocatedJobs
	unallocatedJobsCount := len(unallocatedJobs)
	baseBenefit, baseStub := s.BenefitsCalculator.ByHistory(pc, pc.GetJobExecutionHistories())
	acs := make([]*types.AllocContext, 0)
	acs = append(acs, &types.AllocContext{
		PC:                           pc,
		Job:                          nil,
		JobAllocation:                nil,
		Benefit:                      baseBenefit,
		BenefitStub:                  baseStub,
		NewJobAllocations:            make([]*pb_gen.JobAllocation, 0),
		NewJobAllocationsFingerPrint: "",
	})
	round := 0
	start := time.Now()
	for round < param.round && round < unallocatedJobsCount {
		round++
		if time.Now().Sub(start) > s.MaxLatency {
			log.Printf("[UNS Scheduler] exceeds MaxLatency, skip rest %d rounds.", param.round-round+1)
			break
		}
		// 在每一轮中，对所有基础的partitionContext，让所有未得到分配的任务，尝试它的所有放置可能，并获得一个benefit。
		// 获得了全部benefit之后，对它们进行排序，再sample
		mu := &sync.Mutex{}
		withBenefits := make([]*types.AllocContext, 0, 1024)
		wg := &sync.WaitGroup{}
		wg.Add(len(acs))
		for _, ac := range acs {
			ac := ac
			go func() {
				r := s.predictACBenefits(param, ac)
				mu.Lock()
				defer mu.Unlock()
				withBenefits = append(withBenefits, r...)
				wg.Done()
			}()
		}
		wg.Wait()
		s.sortBenefits(param, withBenefits)
		withBenefits = s.DeDuplicate(withBenefits)
		withBenefits = s.FilterSameBenefitsByScore(withBenefits, s.MaximumSameBenefit)
		withBenefits = param.sampler.Sample(withBenefits)
		nextRoundAcs := make([]*types.AllocContext, 0, len(withBenefits))
		for _, withBenefit := range withBenefits {
			ac := withBenefit
			cancel := s.TempAllocJob(ac.PC, ac.JobAllocation)
			ac.PC = ac.PC.Clone(false)
			nextRoundAcs = append(nextRoundAcs, ac)
			cancel()
		}
		acs = nextRoundAcs
		if len(acs) == 0 {
			break
		}
	}
	if len(acs) == 0 {
		return nil
	}
	bestAC := acs[0]
	return bestAC.NewJobAllocations
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
	return s.IfHasUnallocated(pc)
}

func (s *Scheduler) predictACBenefits(scheduleContext *scheduleContext, ac *types.AllocContext) []*types.AllocContext {
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
	_, baseBenefitsStub := s.BenefitsCalculator.ByPredictIncrementally(pc, basePredictResult, ac.BenefitStub)
	_, baseScoreStub := s.ScoreCalculator.GetScore(pc, pc.GetAllocationsSlice())
	accID2SortedTaskAllocations := s.AllocationsProvider.PrepareAccID2SortedTaskAllocations(pc, basePredictResult)
	nodeID2TaskAllocations := s.GetNodeID2TaskAllocations(pc)
	jobs := pc.AllocationViews.UnallocatedJobs
	jobIDs := s.getSortedJobIDs(jobs)
	for _, jobID := range jobIDs {
		job := jobs[jobID]
		getPossibleACs := func(possibleAllocations []*pb_gen.JobAllocation) []*types.AllocContext {
			possibleACs := make([]*types.AllocContext, 0)
			for _, jobAllocation := range possibleAllocations {
				// 对于每个可能的分配，临时得将该分配结果赋予给partitionContext。
				jobAllocation := jobAllocation
				attemptAlloc := func() {
					cancelAlloc := s.TempAllocJob(pc, jobAllocation)
					defer cancelAlloc()
					// 随后获取新的jobAllocation的所有相关的jobAllocations
					relatedJobAllocations := s.RelatedJobAllocationsByNodes(pc, nodeID2TaskAllocations, jobAllocation)
					// 使用这些相关的jobAllocations，提高predict的计算速度。
					partialPredictResult, err := s.Predictor.Predict(pc, relatedJobAllocations)
					if err != nil {
						if predictorinterfaces.IsMultiSpanNodesGangTasksError(err) || predictorinterfaces.IsSpaceSharingOutOfMemoryError(err) {
							// 忽略显存溢出造成的问题和多分布式任务跨节点运行时共享节点的问题
							return
						}
						for _, m := range pc.GetAllocationsSlice() {
							st, _ := utils.MarshalJsonPB(m)
							log.Printf("%s", st)
						}
						log.Printf("[UNS Scheduler] find unproper job allocation, err=[%v]", err)
						panic("fast fail")
					}
					if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
						s.MarkGangJobStartTime(jobAllocation, *partialPredictResult.GetResult(jobAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
					}
					benefit, stub := s.BenefitsCalculator.ByPredictIncrementally(pc, partialPredictResult, baseBenefitsStub)
					jobAllocationsScore, _ := s.ScoreCalculator.GetScoreIncrementally(pc, []*pb_gen.JobAllocation{jobAllocation}, baseScoreStub)
					newJobAllocations := make([]*pb_gen.JobAllocation, len(ac.NewJobAllocations), len(ac.NewJobAllocations)+1)
					copy(newJobAllocations, ac.NewJobAllocations)
					newJobAllocations = append(newJobAllocations, jobAllocation)
					possibleACs = append(possibleACs, &types.AllocContext{
						PC:                           pc,
						Job:                          job,
						JobAllocation:                jobAllocation,
						NewJobAllocations:            newJobAllocations,
						NewJobAllocationsFingerPrint: s.genJobAllocationsFingerPrint(newJobAllocations),
						Benefit:                      benefit,
						BenefitStub:                  stub,
						Score:                        jobAllocationsScore,
					})
				}
				attemptAlloc()
			}
			return possibleACs
		}
		possibleAllocations := s.AllocationsProvider.GetPossibleAllocations(pc, accID2SortedTaskAllocations, basePredictResult, job, scheduleContext.provideTypeMode)
		if s.ResourceEfficientMode {
			filtered := s.filterAllocationsForResourceEfficiency(scheduleContext, possibleAllocations)
			if len(filtered) != 0 {
				// 只有在过滤后不为空时，考虑采取filter的结果
				possibleAllocations = filtered
			}
		}
		possibleAcs := getPossibleACs(possibleAllocations)
		acs = append(acs, possibleAcs...)
	}
	s.sortBenefits(scheduleContext, acs)
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

func (s *Scheduler) filterAllocationsForResourceEfficiency(sc *scheduleContext, possibleAllocations []*pb_gen.JobAllocation) []*pb_gen.JobAllocation {
	// 如果启用了资源高效选项，则优先考虑能够让任务立刻运行的allocations。
	// 如果不存在这样的allocations，则返回原来的possibleAllocations
	now := sc.initialPC.FixedNow()
	filtered := make([]*pb_gen.JobAllocation, 0)
	for _, jobAllocation := range possibleAllocations {
		if jobAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() != now {
			continue
		}
		job := sc.initialPC.GetJob(jobAllocation.GetJobID())
		// 如果是当前分配的，首先，若是一个single类型的任务，且占用的资源是一个未分配的资源，则可以直接运行。
		// 然后，若是gang类型的任务，则要看该任务等待的任务是否是一个未分配的任务。
		if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeSingle {
			accID := jobAllocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()
			if _, ok := sc.initialPC.AllocationViews.UnallocatedAcceleratorIDs[accID]; ok {
				filtered = append(filtered, jobAllocation)
			}
		} else {
			for _, waitingJobID := range jobAllocation.PlaceholderWaitingJobIDs {
				if _, ok := sc.initialPC.AllocationViews.UnallocatedJobs[waitingJobID]; ok {
					// 等待的任务中存在着起初未被分配的任务，则意味着这个任务不能立刻占据它想要的Accelerator
					// 所以该任务分配不属于能够立刻分配的类型，跳过。
					continue
				}
			}
			filtered = append(filtered, jobAllocation)
		}
	}
	return filtered
}
