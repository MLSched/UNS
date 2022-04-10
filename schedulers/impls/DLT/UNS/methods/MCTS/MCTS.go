package MCTS

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	predictorfacade "UNS/predictor"
	"UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/UNS/benefits"
	"UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	base2 "UNS/schedulers/impls/DLT/UNS/methods/base"
	"UNS/schedulers/impls/DLT/UNS/sampler"
	"UNS/schedulers/impls/DLT/UNS/score"
	"UNS/schedulers/impls/DLT/UNS/types"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/partition"
	"UNS/utils"
	"context"
	"fmt"
	"log"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Method struct {
	*base2.Scheduler
	*base2.CommonMethodParams
	BenefitsSampler           sampler.Sampler
	MaxLatency                time.Duration // 一次调度算法执行的最大时延
	AllocationProvideTypeMode base.ProvideType
	ResourceEfficientMode     bool // 当启用资源高效模式时，在展开树节点时，将会优先考虑未被占用的资源
	MaxNodeChildrenCount      int  // 每个节点的孩子的最大数量。
}

// MCTS方法，将一个AllocContext作为一个节点，每个节点包含一个SimulatedBenefit作为它的价值，该SimulatedBenefit是通过快速的play-out得到的（类似围棋的快速落子，每次快速地找到一个任务进行分配即可）
// VisitedCount表示该节点被访问的次数。
// 将每层的节点按照UCB公式计算出一个值，选取该最大的值即可。

type Node struct {
	*types.AllocContext

	// Parent 当不是根节点时，表示父亲节点。当是根节点时，该值为nil
	Parent *Node
	// Children 表示Node的孩子节点。当为nil时，表示当前节点暂时未被扩展。
	Children []*Node
	// Modifying 表示当前节点正在被某个goroutine修改，不能被其他goroutine同时修改
	Modifying *atomic.Value
	// UnexpandedJobs 表示该节点尚未扩展的jobID
	// 扩展当前节点时，才能修改该数据。
	// 当该切片为空时，则说明该节点不能再进行扩展了。
	UnexpandedJobs []*objects.Job

	Level int // 节点所在的层级

	// TotalSimulatedBenefitMu, TotalSimulatedBenefit, TotalVisitedCount
	// 记录了该节点所拥有的叶子节点的总共的benefit，以及从该节点向下模拟的访问次数。
	TotalSimulatedBenefitMu *sync.RWMutex
	TotalSimulatedBenefit   map[interfaces2.Calculator]interfaces2.Benefit
	TotalVisitedCount       int

	// LeafBenefit 当该节点是叶子节点时，计算出一次benefit后，缓存在该字段。
	LeafBenefit map[interfaces2.Calculator]interfaces2.Benefit

	// 缓存
	PartialPredictResult interfaces.PredictResult

	// JCTBenefit 与 ConsolidationScore 用于在一个任务的一批allocation中，筛选最优的jobAllocation。贪婪地选取JCT最好的任务分配结果。当JCT一样时，选取Consolidation分数最高的。
	JCTBenefitStub         interface{}
	JCTBenefit             interfaces2.Benefit
	ConsolidationScoreStub interface{}
	ConsolidationScore     score.JobAllocationsScore

	NotWorthSelect bool
}

func BuildMCTSMethod(sche *base2.Scheduler, configuration *configs.UNSSchedulerConfiguration) *Method {
	method := &Method{
		Scheduler: sche,
		CommonMethodParams: &base2.CommonMethodParams{
			Predictor: predictorfacade.BuildPredictor(configuration.GetPredictorConfiguration()),
			AllocationsProvider: &base.AllocationsProviderImpl{
				RandomMode: true,
			},
			BenefitsCalculator2Weights: map[interfaces2.Calculator]float64{
				benefits.NewJCTCalculator(): 1,
				//benefits.NewDDLJCTCalculator(): 1,
				//benefits.NewDDLCalculator(false): 1,
				//benefits.NewMakeSpanCalculator(): 1,
			},
			ScoreCalculator: score.NewConsolidationScoreCalculator(),
		},
		MaxLatency: 10 * time.Second,
		//ResourceEfficientMode: true,
		ResourceEfficientMode: false,
		MaxNodeChildrenCount:  10,
	}
	method.AllocationProvideTypeMode = base.ProvideTypeDefault
	if configuration.GetNonSpaceSharing() {
		method.AllocationProvideTypeMode |= base.ProvideTypeOnlyNonSpaceSharing
	}
	return method
}

func (s *Method) GetPredictor() interfaces.Predictor {
	return s.Predictor
}

func (s *Method) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	originalPC := s.GetPartitionContext().Clone(false)
	t := originalPC.Now()
	originalPC.Time = &t
	pc := originalPC.Clone(false)
	if !s.IfHasUnallocated(pc) {
		return nil
	}
	scheduleCtx := newScheduleContext(&scheduleContextParams{
		Method:                    s,
		PC:                        pc,
		MaxJobAllocationsCount:    3,
		Predictor:                 s.Predictor,
		Provider:                  s.AllocationsProvider,
		ProvideTypeMode:           s.AllocationProvideTypeMode,
		BenefitCalculator2Weights: s.BenefitsCalculator2Weights,
		C:                         0.5,
		ExpandStep:                1,
	})
	closeFunc := scheduleCtx.StartNormalizationRoutine()
	jobAllocations := scheduleCtx.Search(s.MaxLatency, runtime.NumCPU())
	//jobAllocations := scheduleCtx.Search(s.MaxLatency, 1)
	filteredJobAllocations := jobAllocations
	if !s.Config.GetReturnAllScheduleDecisions() {
		filteredJobAllocations = s.FilterScheduleAbleJobAllocations(jobAllocations, pc)
	}
	closeFunc()
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(filteredJobAllocations)}
}

type scheduleContext struct {
	method *Method

	// PlayOutExpandMaxCount 当PlayOut时，扩展任务的下一层节点的最大宽度。
	// 当扩展宽度越大，则越能为该任务找到更快的放置方式，增大playOut的可靠性，但会增大开销。
	PlayOutExpandMaxCount int

	RootNode *Node
	MaxLevel int

	// MaxJobAllocationsCount 限制了一个任务在一个partition context下，最多可能的allocation的可能性。这个数字限制了每一层树的最大宽度。
	MaxJobAllocationsCount int

	//FingerPrints  map[string]*Node
	//FingerPrintMu *sync.Mutex
	FingerPrints *sync.Map

	InitialPC                    *partition.Context
	Predictor                    interfaces.Predictor
	JobID2Priority               map[string]int
	SortedJobIDWithPriorities    []jobIDWithPriority
	BenefitCalculator2Weights    map[interfaces2.Calculator]float64
	JCTCalculator                interfaces2.Calculator
	ConsolidationScoreCalculator score.Calculator
	Provider                     base.AllocationsProvider
	AllocationProvideTypeMode    base.ProvideType
	// C 在普通的MCTS中，C作为经验参数是不变量，但是对于我们的应用来说，不同的benefit定义会造成C的最优取值发生变化。
	// 所以我们动态地计算出该参数的大小，它定义为：每当计算出一个叶子节点的Benefit时，该参数为C乘上曾经得到过的全部的Benefit的值的平均。
	C float64
	// 在扩展节点时，一次最多扩展多少个任务的孩子节点
	ExpandStep int

	// bestBenefit, bestAllocations 当playOut遇到叶子节点时，更新最佳收益和allocations作为调度结果
	bestBenefitMu   *sync.Mutex
	bestBenefit     *atomic.Value
	bestAllocations []*pb_gen.JobAllocation

	normalizers                    map[interfaces2.Calculator]benefits.Normalizer
	normalizationRoutineCtx        context.Context
	normalizationRoutineChan       chan map[interfaces2.Calculator][]float64
	normalizationFuncReadyInformer chan interface{}
	normalizationFunc              benefits.NormalizationFunc
}

type scheduleContextParams struct {
	Method                    *Method
	PC                        *partition.Context
	MaxJobAllocationsCount    int
	Predictor                 interfaces.Predictor
	Provider                  base.AllocationsProvider
	ProvideTypeMode           base.ProvideType
	BenefitCalculator2Weights map[interfaces2.Calculator]float64
	C                         float64
	ExpandStep                int
}

func newScheduleContext(params *scheduleContextParams) *scheduleContext {
	pc := params.PC
	unallocatedJobsCount := len(pc.AllocationViews.UnallocatedJobs)
	ctx := &scheduleContext{
		method:       params.Method,
		FingerPrints: &sync.Map{},
		//FingerPrints:                 make(map[string]*Node),
		//FingerPrintMu:                &sync.Mutex{},
		InitialPC:                    pc,
		MaxLevel:                     unallocatedJobsCount,
		PlayOutExpandMaxCount:        10,
		MaxJobAllocationsCount:       params.MaxJobAllocationsCount,
		Predictor:                    params.Predictor,
		Provider:                     params.Provider,
		AllocationProvideTypeMode:    params.ProvideTypeMode,
		BenefitCalculator2Weights:    params.BenefitCalculator2Weights,
		ExpandStep:                   params.ExpandStep,
		JCTCalculator:                JCT.NewCalculator(),
		ConsolidationScoreCalculator: score.NewConsolidationScoreCalculator(),
		bestBenefit: func() *atomic.Value {
			bestBenefits := make(map[interfaces2.Calculator]interfaces2.Benefit)
			for calculator := range params.BenefitCalculator2Weights {
				bestBenefits[calculator] = interfaces2.Benefit(math.Inf(-1))
			}
			return utils.NewAtomic(bestBenefits)
		}(),
		bestBenefitMu:   &sync.Mutex{},
		bestAllocations: nil,
		C:               params.C,
		normalizers: func() map[interfaces2.Calculator]benefits.Normalizer {
			r := make(map[interfaces2.Calculator]benefits.Normalizer)
			for calculator := range params.BenefitCalculator2Weights {
				r[calculator] = benefits.NewZScoreNormalizer()
			}
			return r
		}(),
		normalizationFuncReadyInformer: make(chan interface{}),
	}
	ctx.JobID2Priority, ctx.SortedJobIDWithPriorities = ctx.PrioritySort(pc.AllocationViews.UnallocatedJobs)
	predictResult, err := params.Predictor.Predict(pc, pc.AllocationViews.AllocationsSlice)
	if err != nil {
		reason := fmt.Sprintf("[UNS Scheduler] MCTS base predict failed, err=[%v]", err)
		panic(reason)
	}
	ctx.RootNode = &Node{
		AllocContext: &types.AllocContext{},
	}
	ctx.initReadyNode(&readyNodeInitParam{
		Node:   ctx.RootNode,
		Level:  0,
		PC:     pc,
		Result: predictResult,
	})
	return ctx
}

type readyNodeInitParam struct {
	Node                         *Node
	Parent                       *Node
	Level                        int
	PC                           *partition.Context
	Result                       interfaces.PredictResult
	NewJobAllocationsFingerPrint string
	JCTBenefitStub               interface{}
	ConsolidationScoreStub       interface{}
}

// initReadyNode 初始化已经可以作为树上节点的节点信息。
func (s *scheduleContext) initReadyNode(param *readyNodeInitParam) {
	node := param.Node
	node.TotalSimulatedBenefitMu = &sync.RWMutex{}
	node.Parent = param.Parent
	node.PC = param.PC
	node.Children = make([]*Node, 0)
	node.PredictResult = param.Result
	node.Level = param.Level
	node.NewJobAllocationsFingerPrint = param.NewJobAllocationsFingerPrint
	node.Modifying = utils.NewAtomic(false)
	if param.JCTBenefitStub == nil {
		node.JCTBenefitStub = s.JCTCalculator.NewStub()
	} else {
		node.JCTBenefitStub = param.JCTBenefitStub
	}
	if param.ConsolidationScoreStub == nil {
		node.ConsolidationScoreStub = s.ConsolidationScoreCalculator.NewStub()
	} else {
		node.ConsolidationScoreStub = param.ConsolidationScoreStub
	}
	node.TotalSimulatedBenefit = make(map[interfaces2.Calculator]interfaces2.Benefit)
	unallocatedJobs := node.PC.AllocationViews.UnallocatedJobs
	node.UnexpandedJobs = make([]*objects.Job, 0, len(unallocatedJobs))
	prioritizedJobs := s.sortJobsByPriority(unallocatedJobs)
	for _, unallocatedJob := range prioritizedJobs {
		node.UnexpandedJobs = append(node.UnexpandedJobs, unallocatedJob)
	}
}

func (s *scheduleContext) Search(timeBudget time.Duration, parallelRoutines int) []*pb_gen.JobAllocation {
	// 手动将第一层扩展出来，避免第一层扩展时多goroutine的抢占开销。
	s.Expand(s.RootNode, -1)
	_, playOutSuccessCount := s.playOutAndPropagate(s.RootNode.Children)
	<-s.normalizationFuncReadyInformer
	end := time.Now().Add(timeBudget)
	totalPlayOutCount := utils.NewAtomicInt(playOutSuccessCount)
	defer func() {
		log.Printf("total PlayOutCount %d", totalPlayOutCount.Get())
	}()
	if len(s.InitialPC.AllocationViews.UnallocatedJobs) == 1 {
		return s.bestAllocations
	}
	searchRoutine := func(id int) {
		playOutCount := 0
		for time.Now().Before(end) {
			// 选择一批扩展出来的新的节点。若是叶子节点，则全部进行propagate，
			// 若不是，则选择第一个孩子进行propagate。
			children, ok := s.SelectNodes()
			if !ok {
				return
			}
			//if !s.isLeafNode(children[0]) {
			//	children = []*Node{children[0]}
			//}
			_, playOutSuccessCount := s.playOutAndPropagate(children)
			playOutCount += playOutSuccessCount
			//if leaves != nil {
			//	s.playOutAndPropagate(leaves)
			//	playOutCount += len(leaves)
			//} else if normalNode != nil {
			//	s.playOutAndPropagateForMonoNode(normalNode)
			//	playOutCount++
			//} else {
			//	panic("should not reach here.")
			//}
		}
		totalPlayOutCount.GetAndIncrement(playOutCount)
		log.Printf("search routine %d playout for %d", id, playOutCount)
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < parallelRoutines; i++ {
		i := i
		utils.GoWithWG(wg, i, func(id int) {
			searchRoutine(id)
		})
	}
	wg.Wait()
	return s.bestAllocations
}

// playOutAndPropagate 对一批孩子节点做playOut和BackPropagation。
// 保证这批孩子有公共的父亲节点
func (s *scheduleContext) playOutAndPropagate(nodes []*Node) (map[interfaces2.Calculator]interfaces2.Benefit, int) {
	if len(nodes) == 0 {
		return nil, 0
	}
	parent := nodes[0].Parent
	for _, child := range nodes {
		if child.Parent != parent {
			panic("playOutAndPropagate nodes don't share a parent.")
		}
	}
	mu := &sync.Mutex{}
	totalBenefits := make(map[interfaces2.Calculator]interfaces2.Benefit)
	playOutBenefitsResults := make(map[interfaces2.Calculator][]float64)
	for calculator := range s.BenefitCalculator2Weights {
		playOutBenefitsResults[calculator] = make([]float64, 0)
	}
	wg := &sync.WaitGroup{}
	playOutSuccessCount := utils.NewAtomicInt(0)
	for _, node := range nodes {
		node := node
		utils.GoWithWG(wg, 0, func(_ int) {
			var newBenefits map[interfaces2.Calculator]interfaces2.Benefit
			var ok bool
			newBenefits, ok = s.PlayOut(node)
			if !ok {
				return
			}
			s.addSimulatedBenefit(node, newBenefits, 1)
			mu.Lock()
			defer mu.Unlock()
			s.addUpBenefits(totalBenefits, newBenefits)
			for calculator, benefit := range newBenefits {
				playOutBenefitsResults[calculator] = append(playOutBenefitsResults[calculator], float64(benefit))
			}
			playOutSuccessCount.GetAndIncrement(1)
		})
	}
	wg.Wait()
	s.normalizationRoutineChan <- playOutBenefitsResults
	s.BackPropagation(parent, totalBenefits, len(nodes))
	return totalBenefits, playOutSuccessCount.Get()
}

func (s *scheduleContext) playOutAndPropagateForMonoNode(node *Node) (map[interfaces2.Calculator]interfaces2.Benefit, bool) {
	benefit, ok := s.PlayOut(node)
	if !ok {
		return nil, false
	}
	s.addSimulatedBenefit(node, benefit, 1)
	s.BackPropagation(node, benefit, 1)
	return benefit, true
}

func (s *scheduleContext) addUpBenefits(dst, src map[interfaces2.Calculator]interfaces2.Benefit) {
	for calculator, value := range src {
		dst[calculator] += value
	}
}

func (s *scheduleContext) Expand(node *Node, expandJobsCount int) bool {
	if s.isLeafNode(node) {
		panic("leaf node cannot be expanded.")
	}
	if len(node.UnexpandedJobs) == 0 {
		panic("node has no more jobs to be expanded.")
	}
	unexpandedJobs := node.UnexpandedJobs
	if expandJobsCount < 0 {
		expandJobsCount = len(unexpandedJobs)
	}
	defer func() {
		node.UnexpandedJobs = unexpandedJobs[expandJobsCount:]
	}()
	candidateExpandJobs := unexpandedJobs[:expandJobsCount]
	predictResult := node.PredictResult
	nodeID2TaskAllocations := node.PC.AllocationViews.NodeID2TaskAllocations
	// 使用固定的JCTCalculator作为node选择标准
	// 当JCT分数一致时，使用consolidation评分
	JCTBenefitStub := node.JCTBenefitStub
	consolidationScoreStub := node.ConsolidationScoreStub
	index := 0
	// 将扩展出的节点个数：未分配的任务数量*每个任务最多产生的分配个数。
	resultNodes := make([]*Node, len(candidateExpandJobs)*s.MaxJobAllocationsCount)
	wg := &sync.WaitGroup{}
	for _, job := range candidateExpandJobs {
		innerIndex := index
		job := job
		utils.GoWithWG(wg, innerIndex, func(idx int) {
			cloned := node.PC.Clone(false)
			expandedNodes := s.ExpandForJob(&expandContext{
				PC:                     cloned,
				Node:                   node,
				NodeID2TaskAllocations: nodeID2TaskAllocations,
				Job:                    job,
				JCTBenefitStub:         JCTBenefitStub,
				ConsolidationScoreStub: consolidationScoreStub,
				PredictResult:          predictResult,
				PlayOutMode:            false,
			})
			copy(resultNodes[innerIndex*s.MaxJobAllocationsCount:(innerIndex+1)*s.MaxJobAllocationsCount], expandedNodes)
		})
		index++
	}
	wg.Wait()
	for _, resultNode := range resultNodes {
		if resultNode != nil {
			node.Children = append(node.Children, resultNode)
		}
	}
	if len(node.UnexpandedJobs) == 0 && len(node.Children) == 0 {
		return false
	}
	return true
}

func (s *scheduleContext) ExpandForJob(ctx *expandContext) []*Node {
	node := ctx.Node
	pc := ctx.PC
	getPossibleNodes := func(possibleAllocations []*pb_gen.JobAllocation) []*Node {
		params := &getPossibleNodesParams{
			PC:                     pc,
			NodeID2TaskAllocations: ctx.NodeID2TaskAllocations,
			Job:                    ctx.Job,
			Node:                   node,
			JCTBenefitStub:         ctx.JCTBenefitStub,
			ConsolidationScoreStub: ctx.ConsolidationScoreStub,
			PossibleAllocations:    possibleAllocations,
		}
		return s.getPossibleNodes(params)
	}
	maxCount := func() int {
		if ctx.PlayOutMode {
			return s.PlayOutExpandMaxCount
		}
		return math.MaxInt64
	}()
	for _, ja := range pc.AllocationViews.AllocationsSlice {
		taskAllocation := ja.GetTaskAllocations()[0]
		r := ctx.PredictResult.GetResult(taskAllocation)
		if r.GetFinishNanoTime() == nil {
			log.Printf("before get possible allocaitons, r is nil, r = %v, taskAllocation %v, ctx.Job = %v, start = %v", r, taskAllocation, ctx.Job, r.GetStartExecutionNanoTime())
			ctx.PredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
				log.Printf("pr allocation %v, result %v", allocation, result)
			})
			for _, allocation := range pc.AllocationViews.AllocationsSlice {
				for _, taskAllocation := range allocation.GetTaskAllocations() {
					log.Printf("pc allocation %v", taskAllocation)
				}
			}
		}
	}
	possibleAllocations := s.Provider.GetPossibleAllocations(&base.GetPossibleAllocationsParams{
		PC:            pc,
		PredictResult: ctx.PredictResult,
		Job:           ctx.Job,
		ProvideType:   s.AllocationProvideTypeMode,
		MaxCount:      maxCount,
	})
	//resourceEfficientAllocations := make([]*pb_gen.JobAllocation, 0)
	if s.method.ResourceEfficientMode {
		filtered := s.method.FilterAllocationsForResourceEfficiency(s.InitialPC, possibleAllocations)
		//resourceEfficientAllocations = filtered
		if len(filtered) != 0 {
			// 只有在过滤后不为空时，考虑采取filter的结果
			possibleAllocations = filtered
		}
	}
	// TODO debug
	//for _, allocation := range possibleAllocations {
	//	cancel := pc.TempAllocJob(allocation)
	//	_, err := s.Predictor.Predict(pc, pc.AllocationViews.AllocationsSlice)
	//	if interfaces.IsSpaceSharingMoreThanTwoError(err) {
	//		panic(err)
	//	}
	//	cancel()
	//}
	//
	var nodes []*Node
	//if len(resourceEfficientAllocations) > 0 {
	// 首先尝试使用resource efficient模式的分配
	//nodes = getPossibleNodes(resourceEffiecientAllocations)
	//}
	// 若分配不到结果，再使用全部的possibleAllocations
	//if len(nodes) == 0 {
	//	nodes = getPossibleNodes(possibleAllocations)
	//}
	nodes = getPossibleNodes(possibleAllocations)
	nodes = s.sortAndFilterPossibleNodes(nodes)
	selectCount := utils.MinInt64(int64(s.MaxJobAllocationsCount), int64(len(nodes)))
	selectedNodes := nodes[:selectCount]
	// 为选中的节点赋予树上的信息，以及必要的初始化信息
	for _, selected := range selectedNodes {
		cloned := pc.Clone(false)
		cloned.TempAllocJob(selected.JobAllocation)
		merged := ctx.PredictResult.Merge(selected.PartialPredictResult)
		for _, ja := range cloned.AllocationViews.AllocationsSlice {
			taskAllocation := ja.GetTaskAllocations()[0]
			r := merged.GetResult(taskAllocation)
			if r.GetFinishNanoTime() == nil {
				log.Printf("r is nil, taskAllocation %v, start = %d", taskAllocation, r.GetStartExecutionNanoTime())
			}
		}
		s.initReadyNode(&readyNodeInitParam{
			//selected, node, node.Level+1, cloned, ctx.PredictResult.Merge(selected.PartialPredictResult), s.method.GenJobAllocationsFingerPrint(selected.NewJobAllocations
			Node:                         selected,
			Parent:                       node,
			Level:                        node.Level + 1,
			PC:                           cloned,
			Result:                       merged,
			NewJobAllocationsFingerPrint: s.method.GenJobAllocationsFingerPrint(selected.NewJobAllocations),
			JCTBenefitStub:               selected.JCTBenefitStub,
			ConsolidationScoreStub:       selected.ConsolidationScoreStub,
		})
	}
	if ctx.PlayOutMode {
		return selectedNodes
	}
	// 如果不是play-out模式，还需要过滤重复的节点。
	s.fingerPrintLocked(func() {
		//for i, node := range selectedNodes {
		//_, ok := s.FingerPrints.LoadOrStore(node.NewJobAllocationsFingerPrint, node)
		//if ok {
		//	selectedNodes[i] = nil
		//}
		//if _, ok := s.FingerPrints[node.NewJobAllocationsFingerPrint]; ok {
		//	selectedNodes[i] = nil
		//	//selectedNodes[i] = cached
		//} else {
		//	s.FingerPrints[node.NewJobAllocationsFingerPrint] = node
		//}
		//}
	})
	filteredDuplicatedSelectedNodes := func() []*Node {
		r := make([]*Node, 0, len(selectedNodes))
		for _, n := range selectedNodes {
			if n == nil {
				continue
			}
			r = append(r, n)
		}
		return r
	}()
	return filteredDuplicatedSelectedNodes
}

type expandContext struct {
	PC                     *partition.Context
	NodeID2TaskAllocations map[string][]*objects.TaskAllocation
	Job                    *objects.Job
	Node                   *Node
	JCTBenefitStub         interface{}
	ConsolidationScoreStub interface{}
	PossibleAllocations    []*pb_gen.JobAllocation
	PredictResult          interfaces.PredictResult
	PlayOutMode            bool
}

type getPossibleNodesParams struct {
	PC                     *partition.Context
	NodeID2TaskAllocations map[string][]*objects.TaskAllocation
	Job                    *objects.Job
	Node                   *Node
	JCTBenefitStub         interface{}
	ConsolidationScoreStub interface{}
	PossibleAllocations    []*pb_gen.JobAllocation
}

func (s *scheduleContext) fingerPrintLocked(f func()) {
	//s.FingerPrintMu.Lock()
	//defer s.FingerPrintMu.Unlock()
	f()
}

func (s *scheduleContext) isNodeExpandable(node *Node) bool {
	return len(node.UnexpandedJobs) > 0
}

func (s *scheduleContext) addSimulatedBenefit(node *Node, newTotalBenefit map[interfaces2.Calculator]interfaces2.Benefit, count int) {
	node.TotalSimulatedBenefitMu.Lock()
	defer node.TotalSimulatedBenefitMu.Unlock()
	s.addUpBenefits(node.TotalSimulatedBenefit, newTotalBenefit)
	node.TotalVisitedCount += count
}

func (s *scheduleContext) getNodeTotalSimulatedBenefit(node *Node) (map[interfaces2.Calculator]interfaces2.Benefit, int) {
	node.TotalSimulatedBenefitMu.RLock()
	defer node.TotalSimulatedBenefitMu.RUnlock()
	totalBenefit := make(map[interfaces2.Calculator]interfaces2.Benefit)
	for calculator, benefit := range node.TotalSimulatedBenefit {
		totalBenefit[calculator] = benefit
	}
	totalVisitedCount := node.TotalVisitedCount
	return totalBenefit, totalVisitedCount
}

func (s *scheduleContext) getPossibleNodes(params *getPossibleNodesParams) []*Node {
	pc := params.PC
	nodeID2TaskAllocations := params.NodeID2TaskAllocations
	job := params.Job
	node := params.Node
	JCTBenefitStub := params.JCTBenefitStub
	consolidationScoreStub := params.ConsolidationScoreStub
	possibleAllocations := params.PossibleAllocations
	possibleNodes := make([]*Node, 0)
	for _, jobAllocation := range possibleAllocations {
		// 对于每个可能的分配，临时得将该分配结果赋予给partitionContext。
		jobAllocation := jobAllocation
		attemptAlloc := func() {
			cancelAlloc := pc.TempAllocJob(jobAllocation)
			defer cancelAlloc()
			// 随后获取新的jobAllocation的所有相关的jobAllocations
			relatedJobAllocations := s.method.RelatedJobAllocationsByNodes(pc, nodeID2TaskAllocations, jobAllocation)
			// 使用这些相关的jobAllocations，提高predict的计算速度。
			partialPredictResult, err := s.Predictor.Predict(pc, relatedJobAllocations)
			if err != nil {
				if interfaces.IsMultiSpanNodesGangTasksError(err) || interfaces.IsSpaceSharingOutOfMemoryError(err) {
					// 忽略显存溢出造成的问题和多分布式任务跨节点运行时共享节点的问题
					return
				}
				log.Printf("[UNS Scheduler] MCTS find unproper job allocation, err=[%v]", err)
				panic("fast fail")
			}
			if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
				s.method.MarkGangJobStartTime(jobAllocation, *partialPredictResult.GetResult(jobAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
			}
			consolidationScore, consolidationScoreStub := s.ConsolidationScoreCalculator.GetScoreIncrementally(pc, []*pb_gen.JobAllocation{jobAllocation}, consolidationScoreStub)
			JCTBenefit, JCTBenefitStub := s.JCTCalculator.ByPredictIncrementally(pc, partialPredictResult, JCTBenefitStub)
			newJobAllocations := make([]*pb_gen.JobAllocation, len(node.NewJobAllocations), len(node.NewJobAllocations)+1)
			copy(newJobAllocations, node.NewJobAllocations)
			newJobAllocations = append(newJobAllocations, jobAllocation)
			possibleNodes = append(possibleNodes, s.initPossibleNode(&possibleNodeInitParam{
				PC:                     pc,
				Job:                    job,
				JobAllocation:          jobAllocation,
				NewJobAllocations:      newJobAllocations,
				ConsolidationScore:     consolidationScore,
				ConsolidationScoreStub: consolidationScoreStub,
				JCTBenefit:             JCTBenefit,
				JCTBenefitStub:         JCTBenefitStub,
				PartialPredictResult:   partialPredictResult,
			}))
		}
		attemptAlloc()
	}
	return possibleNodes
}

type possibleNodeInitParam struct {
	PC                     *partition.Context
	Job                    *objects.Job
	JobAllocation          *pb_gen.JobAllocation
	NewJobAllocations      []*pb_gen.JobAllocation
	ConsolidationScore     score.JobAllocationsScore
	ConsolidationScoreStub interface{}
	JCTBenefit             interfaces2.Benefit
	JCTBenefitStub         interface{}
	PartialPredictResult   interfaces.PredictResult
}

// 初始化可能成为树上节点的信息。每个任务会产生一系列可能的节点，在其中筛选性能最好的一些节点。
func (s *scheduleContext) initPossibleNode(param *possibleNodeInitParam) *Node {
	return &Node{
		AllocContext: &types.AllocContext{
			PC:                param.PC,
			Job:               param.Job,
			JobAllocation:     param.JobAllocation,
			NewJobAllocations: param.NewJobAllocations,
		},
		ConsolidationScore:     param.ConsolidationScore,
		ConsolidationScoreStub: param.ConsolidationScoreStub,
		JCTBenefit:             param.JCTBenefit,
		JCTBenefitStub:         param.JCTBenefitStub,
		PartialPredictResult:   param.PartialPredictResult,
	}
}

func (s *scheduleContext) isLeafNode(node *Node) bool {
	return node.Level == s.MaxLevel
}

func (s *scheduleContext) isNodeBeforeLeaf(node *Node) bool {
	return node.Level == s.MaxLevel-1
}

func (s *scheduleContext) sortAndFilterPossibleNodes(possibleNodes []*Node) []*Node {
	if len(possibleNodes) == 0 {
		return possibleNodes
	}
	sort.Slice(possibleNodes, func(i, j int) bool {
		if possibleNodes[i].JCTBenefit > possibleNodes[j].JCTBenefit {
			return true
		} else if possibleNodes[i].JCTBenefit < possibleNodes[j].JCTBenefit {
			return false
		} else {
			return possibleNodes[i].ConsolidationScore > possibleNodes[j].ConsolidationScore
		}
	})
	result := make([]*Node, 0, len(possibleNodes))
	lastNodeBenefit := possibleNodes[0].JCTBenefit
	result = append(result, possibleNodes[0])
	for i := 1; i < len(possibleNodes); i++ {
		node := possibleNodes[i]
		if node.JCTBenefit == lastNodeBenefit {
			// 过滤掉收益一样的allocation，只保留最为consolidated的放置结果
			continue
		} else {
			result = append(result, node)
			lastNodeBenefit = node.JCTBenefit
		}
	}
	return result
}

type NodeWithUCT struct {
	Node     *Node
	UCT      float64
	Priority int
}

func (s *scheduleContext) filterPrioritizedChildren(node *Node) []*NodeWithUCT {
	children := node.Children
	nodesWithUCT := make([]*NodeWithUCT, 0, len(children))
	_, parentVisitedCount := s.getNodeTotalSimulatedBenefit(node)
	// 临时存储normalizationFunc，由于normalizationFunc可能会随时变化，临时存储下来后，可以让所有的孩子节点使用相同的标准化方法
	normalizationFunc := s.normalizationFunc
	for _, childNode := range children {
		uct := s.UCT(childNode, parentVisitedCount, normalizationFunc)
		priority := s.JobID2Priority[childNode.Job.GetJobID()]
		nodesWithUCT = append(nodesWithUCT, &NodeWithUCT{
			Node:     childNode,
			UCT:      uct,
			Priority: priority,
		})
	}
	sort.Slice(nodesWithUCT, func(i, j int) bool {
		if nodesWithUCT[i].UCT == nodesWithUCT[j].UCT {
			// 当UCT相等时，使用Priority来决定node的优先级。（即使用了先验知识）
			return nodesWithUCT[i].Priority < nodesWithUCT[j].Priority
		}
		return nodesWithUCT[i].UCT > nodesWithUCT[j].UCT
	})
	return nodesWithUCT
}

// SelectNodes 选择一批叶子节点。保证他们具有共同的祖先。
func (s *scheduleContext) SelectNodes() ([]*Node, bool) {
	root := s.RootNode
	node := root
nextLevel:
	for {
		if !s.isNodeWorthSelect(root) {
			// 当根节点不值得选择时，直接返回空。这时我们认为这颗树不存在值得选择的节点。
			return nil, false
		}
		//if s.isNodeBeforeLeaf(node) && node.Children != nil {
		//	// 当一个节点是叶子节点的父亲，并且已经被扩展过了，将它全部的孩子返回。
		//	return node.Children
		//}
		nodesWithUCT := s.filterPrioritizedChildren(node)
		// 向下找一个节点进行延伸，按照UCT和优先级进行排查
		hasWorthSelectChild := false
		for _, childNodeWithUCT := range nodesWithUCT {
			childNode := childNodeWithUCT.Node
			if s.isLeafNode(childNode) {
				node = root
				continue nextLevel
			}
			if !s.isNodeWorthSelect(childNode) {
				continue
			}
			// 如果节点不可扩展时，说明它的所有孩子节点已经生成。
			if !s.isNodeExpandable(childNode) {
				// 若当前节点的孩子为叶子节点，则不向下延伸，在同级延伸。
				if s.isNodeBeforeLeaf(childNode) {
					continue
				} else {
					// 若当前节点的孩子不是叶子节点，则说明该节点的孩子还有延伸的价值，向下延伸。
					node = childNode
					continue nextLevel
				}
			}
			hasWorthSelectChild = true
			var nextChild = false
			var retryFromRoot = false
			var expandedChildren []*Node
			if failedTry := s.tryModify(childNode, func() {
				if !s.isNodeWorthSelect(childNode) {
					// 避免并发问题，获取锁后double-check
					nextChild = true
					return
				}
				if !s.isNodeExpandable(childNode) {
					// 避免并发问题，获取锁后double-check
					nextChild = true
					return
				}
				// 预定成功，选择该节点，扩展它，然后修改Expanding状态。
				ok := s.Expand(childNode, s.ExpandStep)
				if !ok {
					// 当扩展失败时，说明该节点没有孩子节点，即失去了存在的价值，直接级联删除该节点。
					s.removeNodeCascade(childNode)
					// 从root重新开始寻找
					node = s.RootNode
					retryFromRoot = true
					return
				}
				if s.isNodeBeforeLeaf(childNode) && !s.isNodeExpandable(childNode) {
					// 该节点是叶子节点前的节点并且它的所有孩子节点（叶子节点）都被扩展了，则所有的叶子节点的benefit即将获得，不需要再进行扩展。
					// 所以将标记为notWorthSelect
					for _, grandChildNode := range childNode.Children {
						s.markNodeNotWorthSelect(grandChildNode)
					}
					s.markNodeNotWorthSelect(childNode)
				}
				expandedChildren = childNode.Children
				return
			}); failedTry {
				nextChild = true
			}
			if nextChild {
				continue
			} else if retryFromRoot {
				continue nextLevel
			} else if expandedChildren != nil {
				return expandedChildren, true
			} else {
				panic("unreachable.")
			}
		}
		if !hasWorthSelectChild {
			// 当一个节点的全部孩子都不值得选择后，它自身也变为一个不值得选择的节点。
			s.modify(node, func() {
				s.markNodeNotWorthSelect(node)
			})
		}
		// 找不到可以向下延伸的节点，从root重新开始寻找
		node = root
	}
}

func (s *scheduleContext) BackPropagation(node *Node, totalBenefit map[interfaces2.Calculator]interfaces2.Benefit, count int) {
	for node != nil {
		s.addSimulatedBenefit(node, totalBenefit, count)
		node = node.Parent
	}
}

func (s *scheduleContext) PlayOut(readonly *Node) (map[interfaces2.Calculator]interfaces2.Benefit, bool) {
	// playOutNode 复制一个Node用于playOut。只需部分数据。
	playOutNode := &Node{
		AllocContext: &types.AllocContext{
			PC:                readonly.PC.Clone(false),
			PredictResult:     readonly.PredictResult,
			NewJobAllocations: readonly.NewJobAllocations,
			Job:               readonly.Job,
			JobAllocation:     readonly.JobAllocation,
		},
		JCTBenefitStub:         readonly.JCTBenefitStub,
		ConsolidationScoreStub: readonly.ConsolidationScoreStub,
		Level:                  readonly.Level,
	}
deeper:
	for !s.isLeafNode(playOutNode) {
		pc := playOutNode.PC
		unallocatedJobs := pc.AllocationViews.UnallocatedJobs
		predictResult := playOutNode.PredictResult
		nodeID2TaskAllocations := pc.AllocationViews.NodeID2TaskAllocations
		// 使用固定的JCTCalculator作为node选择标准
		// 当JCT分数一致时，使用consolidation评分
		JCTBenefitStub := playOutNode.JCTBenefitStub
		consolidationScoreStub := playOutNode.ConsolidationScoreStub
		sortedJobs := s.sortJobsByPriority(unallocatedJobs)
		for _, job := range sortedJobs {
			job := job
			expandedNodes := s.ExpandForJob(&expandContext{
				PC:                     pc,
				NodeID2TaskAllocations: nodeID2TaskAllocations,
				Job:                    job,
				Node:                   playOutNode,
				JCTBenefitStub:         JCTBenefitStub,
				ConsolidationScoreStub: consolidationScoreStub,
				PredictResult:          predictResult,
				PlayOutMode:            true,
			})
			if len(expandedNodes) > 0 {
				// 向下继续扩展
				playOutNode = expandedNodes[0]
				continue deeper
			}
		}
		break
	}
	if !s.isLeafNode(playOutNode) {
		// playout 没有找到叶子节点
		return nil, false
	}
	//if node.LeafBenefit != nil {
	//	return node.LeafBenefit
	//}
	// 找到了叶子节点，获取他的Benefit
	playOutBenefits := make(map[interfaces2.Calculator]interfaces2.Benefit)
	for calculator := range s.BenefitCalculator2Weights {
		benefit, _ := calculator.ByPredict(playOutNode.PC, playOutNode.PredictResult)
		bestBenefits := s.bestBenefit.Load().(map[interfaces2.Calculator]interfaces2.Benefit)
		if benefit > bestBenefits[calculator] {
			s.bestBenefitLocked(func() {
				bestBenefits := s.bestBenefit.Load().(map[interfaces2.Calculator]interfaces2.Benefit)
				if benefit > bestBenefits[calculator] {
					// 并发安全，需要double check.
					bestBenefits[calculator] = benefit
					s.bestAllocations = playOutNode.NewJobAllocations
				}
			})
		}
		playOutBenefits[calculator] = benefit
	}
	return playOutBenefits, true
}

type jobIDWithPriority struct {
	JobID    string
	Priority int
}

// PrioritySort 当拓展一批未visited过的节点时，根据PrioritySort的结果选择。该排序是对不同benefit定制的。
func (s *scheduleContext) PrioritySort(jobs map[string]*objects.Job) (map[string]int, []jobIDWithPriority) {
	// TODO 优先度计算需要更改为支持多种calculator的
	for calculator := range s.BenefitCalculator2Weights {
		jobID2Priority := calculator.PrioritySort(s.InitialPC, jobs, s.Predictor)
		jobIDWithPriorities := make([]jobIDWithPriority, 0, len(jobID2Priority))
		for jobID, priority := range jobID2Priority {
			jobIDWithPriorities = append(jobIDWithPriorities, jobIDWithPriority{
				JobID:    jobID,
				Priority: priority,
			})
		}
		sort.Slice(jobIDWithPriorities, func(i, j int) bool {
			return jobIDWithPriorities[i].Priority < jobIDWithPriorities[j].Priority
		})
		return jobID2Priority, jobIDWithPriorities
	}
	panic("")
}

func (s *scheduleContext) sortJobsByPriority(jobs map[string]*objects.Job) []*objects.Job {
	result := make([]*objects.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job)
	}
	sort.Slice(result, func(i, j int) bool {
		ji := result[i]
		jj := result[j]
		return s.JobID2Priority[ji.GetJobID()] < s.JobID2Priority[jj.GetJobID()]
	})
	return result
}

// UCT Upper Confidence Bounds for Trees
func (s *scheduleContext) UCT(node *Node, parentVisitedCount int, normalizationFunc benefits.NormalizationFunc) float64 {
	parentVisitedCountF := float64(parentVisitedCount)
	totalBenefits, totalVisitedCount := s.getNodeTotalSimulatedBenefit(node)
	if totalVisitedCount == 0 {
		return math.Inf(1)
	}
	// TODO 当前衡量一个节点的价值是使用该节点下面孩子节点的平均benefit。改进：可以使用max的benefit来衡量？
	calculator2AvgSimulatedBenefit := make(map[interfaces2.Calculator]float64)
	for calculator, totalBenefit := range totalBenefits {
		calculator2AvgSimulatedBenefit[calculator] = float64(totalBenefit) / float64(totalVisitedCount)
	}
	combinedNormalizedBenefit := float64(0)
	for calculator, avgSimulatedBenefit := range calculator2AvgSimulatedBenefit {
		normalized := normalizationFunc(calculator, avgSimulatedBenefit)
		combinedNormalizedBenefit += s.BenefitCalculator2Weights[calculator] * normalized
	}
	// value + \sqrt { \log{parent visited count} / current node visited count }
	//scaled := math.Ceil(float64(totalVisitedCount) / float64(len(node.Children)))
	scaled := totalVisitedCount
	v := combinedNormalizedBenefit + math.Sqrt(math.Log(parentVisitedCountF)/float64(scaled))
	return v
}

func (s *scheduleContext) reserveModifying(node *Node) bool {
	return node.Modifying.CompareAndSwap(false, true)
}

func (s *scheduleContext) tryModify(node *Node, f func()) (failed bool) {
	if !s.reserveModifying(node) {
		return true
	}
	f()
	s.releaseModifying(node)
	return false
}

func (s *scheduleContext) modify(node *Node, f func()) bool {
	s.reserveModifyingSync(node)
	f()
	s.releaseModifying(node)
	return true
}

func (s *scheduleContext) reserveModifyingSync(node *Node) {
	for {
		if node.Modifying.CompareAndSwap(false, true) {
			return
		}
	}
}

func (s *scheduleContext) releaseModifying(node *Node) {
	node.Modifying.Store(false)
}

func (s *scheduleContext) isNodeModifying(node *Node) bool {
	return node.Modifying.Load().(bool)
}

func (s *scheduleContext) bestBenefitLocked(f func()) {
	s.bestBenefitMu.Lock()
	defer s.bestBenefitMu.Unlock()
	f()
}

func (s *scheduleContext) removeNodeCascade(node *Node) {
	if node == nil || node == s.RootNode {
		return
	}
	parent := node.Parent
	rmIdx := -1
	for idx, childNode := range parent.Children {
		if childNode == node {
			rmIdx = idx
			break
		}
	}
	left := parent.Children[:rmIdx]
	right := parent.Children[rmIdx+1:]
	parent.Children = append(left, right...)
	if len(parent.Children) == 0 {
		// 不释放锁的情况下，将parent删除
		s.removeNodeCascade(parent)
	}
}

func (s *scheduleContext) isNodeWorthSelect(node *Node) bool {
	return !node.NotWorthSelect
}

func (s *scheduleContext) markNodeNotWorthSelect(node *Node) {
	node.NotWorthSelect = true
}

func (s *scheduleContext) StartNormalizationRoutine() context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	s.normalizationRoutineCtx = ctx
	s.normalizationRoutineChan = make(chan map[interfaces2.Calculator][]float64, 1024)
	go func() {
		stubs := make(map[interfaces2.Calculator]interface{})
		for calculator := range s.BenefitCalculator2Weights {
			stubs[calculator] = s.normalizers[calculator].NewStub()
		}
		once := &sync.Once{}
		for {
			select {
			case <-ctx.Done():
				return
			case calculator2newBenefits := <-s.normalizationRoutineChan:
				for calculator, newBenefits := range calculator2newBenefits {
					s.normalizers[calculator].UpdateStub(stubs[calculator], newBenefits...)
				}
				clonedStubs := make(map[interfaces2.Calculator]interface{})
				for calculator, stub := range stubs {
					clonedStubs[calculator] = s.normalizers[calculator].CloneStub(stub, false)
				}
				s.normalizationFunc = func(calculator interfaces2.Calculator, benefit float64) float64 {
					return s.normalizers[calculator].Normalize(benefit, clonedStubs[calculator])
				}
				once.Do(func() {
					s.normalizationFuncReadyInformer <- true
				})
			}
		}
	}()
	return cancel
}
