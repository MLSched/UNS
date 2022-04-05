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
	"fmt"
	"log"
	"math"
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

	Parent   *Node
	Children []*Node

	Level int // 节点所在的层级
	// Modifying 表示当前节点正在被某个goroutine访问，不能被其他goroutine同时访问
	Modifying *atomic.Value

	// TotalSimulatedBenefitMu, TotalSimulatedBenefit, TotalVisitedCount
	// 记录了该节点所拥有的叶子节点的总共的benefit，以及从该节点向下模拟的访问次数。
	TotalSimulatedBenefitMu *sync.RWMutex
	TotalSimulatedBenefit   interfaces2.Benefit
	TotalVisitedCount       int

	// LeafBenefit 当该节点是叶子节点时，计算出一次benefit后，缓存在该字段。
	LeafBenefit *interfaces2.Benefit

	// 缓存
	PartialPredictResult interfaces.PredictResult

	// JCTBenefit 与 ConsolidationScore 用于在一个任务的一批allocation中，筛选最优的jobAllocation。贪婪地选取JCT最好的任务分配结果。当JCT一样时，选取Consolidation分数最高的。
	JCTBenefitStub         interface{}
	JCTBenefit             interfaces2.Benefit
	ConsolidationScoreStub interface{}
	ConsolidationScore     score.JobAllocationsScore
}

func BuildMCTSMethod(sche *base2.Scheduler, configuration *configs.UNSSchedulerConfiguration) *Method {
	method := &Method{
		Scheduler: sche,
		CommonMethodParams: &base2.CommonMethodParams{
			Predictor: predictorfacade.BuildPredictor(configuration.GetPredictorConfiguration()),
			AllocationsProvider: &base.AllocationsProviderImpl{
				RandomMode: true,
			},
			BenefitsCalculator: benefits.NewJCTCalculator(),
			ScoreCalculator:    score.NewConsolidationScoreCalculator(),
		},
		MaxLatency:            1 * time.Second,
		ResourceEfficientMode: true,
		MaxNodeChildrenCount:  10,
	}
	method.AllocationProvideTypeMode = base.ProvideTypeDefault
	if configuration.GetNonSpaceSharing() {
		method.AllocationProvideTypeMode |= base.ProvideTypeOnlyNonSpaceSharing
	}
	return method
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
		Method:                 s,
		PC:                     pc,
		MaxJobAllocationsCount: 10,
		Predictor:              s.Predictor,
		Provider:               s.AllocationsProvider,
		ProvideTypeMode:        s.AllocationProvideTypeMode,
		BenefitCalculator:      s.BenefitsCalculator,
		C:                      0.5,
	})
	jobAllocations := scheduleCtx.Search(s.MaxLatency, 1)
	filteredJobAllocations := jobAllocations
	if !s.Config.GetReturnAllScheduleDecisions() {
		filteredJobAllocations = s.FilterScheduleAbleJobAllocations(jobAllocations, pc)
	}
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

	FingerPrints  map[string]*Node
	FingerPrintMu *sync.Mutex

	InitialPC                    *partition.Context
	Predictor                    interfaces.Predictor
	JobID2Priority               map[string]int
	SortedJobIDWithPriorities    []jobIDWithPriority
	BenefitCalculator            interfaces2.Calculator
	JCTCalculator                interfaces2.Calculator
	ConsolidationScoreCalculator score.Calculator
	Provider                     base.AllocationsProvider
	AllocationProvideTypeMode    base.ProvideType
	// C 在普通的MCTS中，C作为经验参数是不变量，但是对于我们的应用来说，不同的benefit定义会造成C的最优取值发生变化。
	// 所以我们动态地计算出该参数的大小，它定义为：每当计算出一个叶子节点的Benefit时，该参数为C乘上曾经得到过的全部的Benefit的值的平均。
	C float64

	// bestBenefit, bestAllocations 当playOut遇到叶子节点时，更新最佳收益和allocations作为调度结果
	bestBenefitMu   *sync.Mutex
	bestBenefit     *atomic.Value
	bestAllocations []*pb_gen.JobAllocation
}

type scheduleContextParams struct {
	Method                 *Method
	PC                     *partition.Context
	MaxJobAllocationsCount int
	Predictor              interfaces.Predictor
	Provider               base.AllocationsProvider
	ProvideTypeMode        base.ProvideType
	BenefitCalculator      interfaces2.Calculator
	C                      float64
}

func newScheduleContext(params *scheduleContextParams) *scheduleContext {
	pc := params.PC
	unallocatedJobsCount := len(pc.AllocationViews.UnallocatedJobs)
	ctx := &scheduleContext{
		method:                       params.Method,
		FingerPrints:                 make(map[string]*Node),
		FingerPrintMu:                &sync.Mutex{},
		InitialPC:                    pc,
		MaxLevel:                     unallocatedJobsCount,
		PlayOutExpandMaxCount:        5,
		MaxJobAllocationsCount:       params.MaxJobAllocationsCount,
		Predictor:                    params.Predictor,
		Provider:                     params.Provider,
		AllocationProvideTypeMode:    params.ProvideTypeMode,
		BenefitCalculator:            params.BenefitCalculator,
		JCTCalculator:                JCT.NewCalculator(),
		ConsolidationScoreCalculator: score.NewConsolidationScoreCalculator(),
		bestBenefit:                  utils.NewAtomic(interfaces2.Benefit(math.Inf(-1))),
		bestBenefitMu:                &sync.Mutex{},
		bestAllocations:              nil,
		C:                            params.C,
	}
	ctx.JobID2Priority, ctx.SortedJobIDWithPriorities = ctx.PrioritySort(pc.AllocationViews.UnallocatedJobs)
	predictResult, err := params.Predictor.Predict(pc, pc.AllocationViews.AllocationsSlice)
	if err != nil {
		reason := fmt.Sprintf("[UNS Scheduler] MCTS base predict failed, err=[%v]", err)
		panic(reason)
	}
	ctx.RootNode = &Node{
		AllocContext: &types.AllocContext{
			PC:                pc,
			NewJobAllocations: make([]*pb_gen.JobAllocation, 0),
			PredictResult:     predictResult,
		},
		TotalSimulatedBenefitMu: &sync.RWMutex{},
		Level:                   0,
		Modifying:               utils.NewAtomic(false),
		JCTBenefitStub:          ctx.JCTCalculator.NewStub(),
		ConsolidationScoreStub:  ctx.ConsolidationScoreCalculator.NewStub(),
	}
	return ctx
}

func (s *scheduleContext) Search(timeBudget time.Duration, parallelRoutines int) []*pb_gen.JobAllocation {
	// 手动将第一层扩展出来
	s.Expand(s.RootNode)
	end := time.Now().Add(timeBudget)
	totalPlayOutCount := utils.NewAtomicInt(0)
	searchRoutine := func() {
		playOutCount := 0
		for time.Now().Before(end) {
			children := s.SelectNodes()
			s.playOutAndPropagateForChildren(children)
			playOutCount += len(children)
			//if leaves != nil {
			//	s.playOutAndPropagateForChildren(leaves)
			//	playOutCount += len(leaves)
			//} else if normalNode != nil {
			//	s.playOutAndPropagateForMonoNode(normalNode)
			//	playOutCount++
			//} else {
			//	panic("should not reach here.")
			//}
		}
		totalPlayOutCount.GetAndIncrement(playOutCount)
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < parallelRoutines; i++ {
		utils.GoWithWG(wg, 0, func(_ int) {
			searchRoutine()
		})
	}
	wg.Wait()
	log.Printf("total PlayOutCount %d", totalPlayOutCount.Get())
	return s.bestAllocations
}

// playOutAndPropagateForChildren 对一批孩子节点做playOut和BackPropagation。
// 保证这批孩子有公共的父亲节点
func (s *scheduleContext) playOutAndPropagateForChildren(children []*Node) interfaces2.Benefit {
	if len(children) == 0 {
		return 0
	}
	parent := children[0].Parent
	for _, child := range children {
		if child.Parent != parent {
			panic("playOutAndPropagateForChildren children don't share a parent.")
		}
	}
	totalBenefit := interfaces2.Benefit(0)
	for _, node := range children {
		benefit := s.PlayOut(node)
		totalBenefit += benefit
		s.addSimulatedBenefit(node, benefit, 1)
	}
	s.BackPropagation(children[0].Parent, totalBenefit, len(children))
	return totalBenefit
}

func (s *scheduleContext) playOutAndPropagateForMonoNode(node *Node) interfaces2.Benefit {
	benefit := s.PlayOut(node)
	s.addSimulatedBenefit(node, benefit, 1)
	s.BackPropagation(node, benefit, 1)
	return benefit
}

func (s *scheduleContext) Expand(node *Node) bool {
	if s.isLeafNode(node) {
		panic("leaf node cannot be expanded.")
	}
	if node.Children != nil {
		panic("node already expanded.")
	}
	unallocatedJobs := node.PC.AllocationViews.UnallocatedJobs
	predictResult := node.PredictResult
	nodeID2TaskAllocations := node.PC.AllocationViews.NodeID2TaskAllocations
	// 使用固定的JCTCalculator作为node选择标准
	// 当JCT分数一致时，使用consolidation评分
	JCTBenefitStub := node.JCTBenefitStub
	consolidationScoreStub := node.ConsolidationScoreStub
	wg := &sync.WaitGroup{}
	index := 0
	// 将扩展出的节点个数：未分配的任务数量*每个任务最多产生的分配个数。
	resultNodes := make([]*Node, len(unallocatedJobs)*s.MaxJobAllocationsCount)
	sortedUnallocatedJobs := s.sortJobsByPriority(unallocatedJobs)
	for _, job := range sortedUnallocatedJobs {
		innerIndex := index
		job := job
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
		utils.GoWithWG(wg, innerIndex, func(i int) {
			//cloned := node.PC.Clone(false)
			//expandedNodes := s.ExpandForJob(&expandContext{
			//	PC:                     cloned,
			//	Node:                   node,
			//	NodeID2TaskAllocations: nodeID2TaskAllocations,
			//	Job:                    job,
			//	JCTBenefitStub:         JCTBenefitStub,
			//	ConsolidationScoreStub: consolidationScoreStub,
			//	PredictResult:          predictResult,
			//	PlayOutMode:            false,
			//})
			//copy(resultNodes[i*s.MaxJobAllocationsCount:(i+1)*s.MaxJobAllocationsCount], expandedNodes)
		})
		index++
	}
	wg.Wait()
	node.Children = make([]*Node, 0, len(unallocatedJobs)*s.MaxJobAllocationsCount)
	for _, resultNode := range resultNodes {
		if resultNode != nil {
			node.Children = append(node.Children, resultNode)
		}
	}
	if len(node.Children) > 0 {
		return true
	}
	return false
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
	possibleAllocations := s.Provider.GetPossibleAllocations(&base.GetPossibleAllocationsParams{
		PC:            pc,
		PredictResult: ctx.PredictResult,
		Job:           ctx.Job,
		ProvideType:   s.AllocationProvideTypeMode,
		MaxCount:      maxCount,
	})
	if s.method.ResourceEfficientMode {
		filtered := s.method.FilterAllocationsForResourceEfficiency(s.InitialPC, possibleAllocations)
		if len(filtered) != 0 {
			// 只有在过滤后不为空时，考虑采取filter的结果
			possibleAllocations = filtered
		}
	}
	nodes := getPossibleNodes(possibleAllocations)
	nodes = s.sortAndFilterPossibleNodes(nodes)
	selectCount := utils.MinInt64(int64(s.MaxJobAllocationsCount), int64(len(nodes)))
	selectedNodes := nodes[:selectCount]
	// 为选中的节点赋予树上的信息，以及必要的初始化信息
	for _, selected := range selectedNodes {
		cloned := pc.Clone(false)
		cloned.TempAllocJob(selected.JobAllocation)
		selected.PC = cloned
		selected.Parent = node
		selected.Level = node.Level + 1
		selected.Modifying = utils.NewAtomic(false)
		selected.PredictResult = ctx.PredictResult.Merge(selected.PartialPredictResult)
		selected.NewJobAllocationsFingerPrint = s.method.GenJobAllocationsFingerPrint(selected.NewJobAllocations)
		selected.TotalSimulatedBenefitMu = &sync.RWMutex{}
	}
	if ctx.PlayOutMode {
		return selectedNodes
	}
	// 如果不是play-out模式，还需要过滤重复的节点。
	s.fingerPrintLocked(func() {
		for i, node := range selectedNodes {
			if _, ok := s.FingerPrints[node.NewJobAllocationsFingerPrint]; ok {
				selectedNodes[i] = nil
				//selectedNodes[i] = cached
			} else {
				s.FingerPrints[node.NewJobAllocationsFingerPrint] = node
			}
		}
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
	s.FingerPrintMu.Lock()
	defer s.FingerPrintMu.Unlock()
	f()
}

func (s *scheduleContext) addSimulatedBenefit(node *Node, newTotalBenefit interfaces2.Benefit, count int) {
	node.TotalSimulatedBenefitMu.Lock()
	defer node.TotalSimulatedBenefitMu.Unlock()
	node.TotalSimulatedBenefit += newTotalBenefit
	node.TotalVisitedCount += count
}

func (s *scheduleContext) getNodeTotalSimulatedBenefit(node *Node) (interfaces2.Benefit, int) {
	node.TotalSimulatedBenefitMu.RLock()
	defer node.TotalSimulatedBenefitMu.RUnlock()
	totalBenefit := node.TotalSimulatedBenefit
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
				for _, m := range pc.AllocationViews.AllocationsSlice {
					st, _ := utils.MarshalJsonPB(m)
					log.Printf("%s", st)
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
			possibleNodes = append(possibleNodes, &Node{
				AllocContext: &types.AllocContext{
					PC:                pc,
					Job:               job,
					JobAllocation:     jobAllocation,
					NewJobAllocations: newJobAllocations,
				},
				ConsolidationScore:     consolidationScore,
				ConsolidationScoreStub: consolidationScoreStub,
				JCTBenefit:             JCTBenefit,
				JCTBenefitStub:         JCTBenefitStub,
				PartialPredictResult:   partialPredictResult,
			})
			possibleNodes = append(possibleNodes)
		}
		attemptAlloc()
	}
	return possibleNodes
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

func (s *scheduleContext) filterPrioritizedChildren(node *Node, totalAvgBenefit float64) []*NodeWithUCT {
	children := node.Children
	nodesWithUCT := make([]*NodeWithUCT, 0, len(children))
	_, parentVisitedCount := s.getNodeTotalSimulatedBenefit(node)
	for _, childNode := range children {
		uct := s.UCT(childNode, parentVisitedCount, totalAvgBenefit)
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
func (s *scheduleContext) SelectNodes() []*Node {
	root := s.RootNode
	node := root
nextLevel:
	for {
		if s.isNodeBeforeLeaf(node) && node.Children != nil {
			// 当一个节点是叶子节点的父亲，并且已经被扩展过了之后，将它全部的孩子返回。
			return node.Children
		}
		totalAvgBenefit := s.totalAvgBenefit()
		nodesWithUCT := s.filterPrioritizedChildren(node, totalAvgBenefit)
		// 向下找一个节点进行延伸，按照UCT和优先级进行排查
		for _, childNodeWithUCT := range nodesWithUCT {
			childNode := childNodeWithUCT.Node
			if s.isLeafNode(childNode) {
				panic("should never select leaf node.")
			}
			if childNode.Modifying.Load().(bool) {
				// 当节点正在扩展时，不考虑它。
				continue
			}
			if childNode.Children == nil {
				// 当节点没有扩展儿子节点时
				if !s.ReserveModifying(childNode) {
					// 预定Expanding失败时，证明该节点被其他goroutine抢占。寻找其他节点。
					continue
				}
				// 预定成功，选择该节点，扩展它，然后修改Expanding状态。
				ok := s.Expand(childNode)
				if !ok {
					// 当扩展失败时，说明该节点没有孩子节点，即失去了存在的价值，直接级联删除该节点。
					s.removeNodeCascade(childNode)
					// 从root重新开始寻找
					node = s.RootNode
					continue nextLevel
				}
				s.ReleaseModifying(childNode)
				return childNode.Children
			} else {
				// 当节点已经扩展了儿子节点时，从该节点继续向下延伸。
				node = childNode
				continue nextLevel
			}
		}
		// 找不到可以向下延伸的节点，从root重新开始寻找
		node = root
	}
}

func (s *scheduleContext) totalAvgBenefit() float64 {
	root := s.RootNode
	totalBenefit, totalVisitedCount := s.getNodeTotalSimulatedBenefit(root)
	if totalVisitedCount == 0 {
		return 0
	}
	return s.C * float64(totalBenefit) / float64(totalVisitedCount)
}

func (s *scheduleContext) BackPropagation(node *Node, totalBenefit interfaces2.Benefit, count int) {
	for node != nil {
		s.addSimulatedBenefit(node, totalBenefit, count)
		node = node.Parent
	}
}

func (s *scheduleContext) PlayOut(node *Node) interfaces2.Benefit {
	for !s.isLeafNode(node) {
		unallocatedJobs := node.PC.AllocationViews.UnallocatedJobs
		predictResult := node.PredictResult
		nodeID2TaskAllocations := node.PC.AllocationViews.NodeID2TaskAllocations
		// 使用固定的JCTCalculator作为node选择标准
		// 当JCT分数一致时，使用consolidation评分
		JCTBenefitStub := node.JCTBenefitStub
		consolidationScoreStub := node.ConsolidationScoreStub
		sortedJobs := s.sortJobsByPriority(unallocatedJobs)
		for _, job := range sortedJobs {
			job := job
			expandedNodes := s.ExpandForJob(&expandContext{
				PC:                     node.PC,
				NodeID2TaskAllocations: nodeID2TaskAllocations,
				Job:                    job,
				Node:                   node,
				JCTBenefitStub:         JCTBenefitStub,
				ConsolidationScoreStub: consolidationScoreStub,
				PredictResult:          predictResult,
				PlayOutMode:            true,
			})
			if len(expandedNodes) > 0 {
				// 向下继续扩展
				node = expandedNodes[0]
				break
			}
		}
	}
	if node.LeafBenefit != nil {
		return *node.LeafBenefit
	}
	// 找到了叶子节点，获取他的Benefit
	benefit, _ := s.BenefitCalculator.ByPredict(node.PC, node.PredictResult)
	node.LeafBenefit = &benefit
	if benefit > s.bestBenefit.Load().(interfaces2.Benefit) {
		s.bestBenefitLocked(func() {
			if benefit > s.bestBenefit.Load().(interfaces2.Benefit) {
				// 并发安全，需要double check.
				s.bestBenefit.Store(benefit)
				s.bestAllocations = node.NewJobAllocations
			}
		})
	}
	return benefit
}

type jobIDWithPriority struct {
	JobID    string
	Priority int
}

// PrioritySort 当拓展一批未visited过的节点时，根据PrioritySort的结果选择。该排序是对不同benefit定制的。
func (s *scheduleContext) PrioritySort(jobs map[string]*objects.Job) (map[string]int, []jobIDWithPriority) {
	jobID2Priority := s.BenefitCalculator.PrioritySort(s.InitialPC, jobs, s.Predictor)
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
func (s *scheduleContext) UCT(node *Node, parentVisitedCount int, totalAvgBenefit float64) float64 {
	parentVisitedCountF := float64(parentVisitedCount)
	totalBenefit, totalVisitedCount := s.getNodeTotalSimulatedBenefit(node)
	//visitedCount := node.TotalVisitedCount.Load().(float64)
	//totalSimulatedBenefit := float64(node.TotalSimulatedBenefit.Load().(interfaces2.Benefit))
	avgSimulatedBenefit := float64(totalBenefit) / float64(totalVisitedCount)
	if totalVisitedCount == 0 {
		return math.Inf(1)
	} else {
		// (avg simulated benefit) + (explore C) * sqrt(2 * log(self.parent.N) / self.N)
		normalizedBenefit := avgSimulatedBenefit / totalAvgBenefit
		v := normalizedBenefit + math.Sqrt(math.Log(parentVisitedCountF)/float64(totalVisitedCount))
		return v
	}
}

func (s *scheduleContext) ReserveModifying(node *Node) bool {
	return node.Modifying.CompareAndSwap(false, true)
}

func (s *scheduleContext) ReserveModifyingSync(node *Node) {
	for {
		if node.Modifying.CompareAndSwap(false, true) {
			return
		}
	}
}

func (s *scheduleContext) ReleaseModifying(node *Node) {
	node.Modifying.Store(false)
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
	s.ReserveModifyingSync(node.Parent)
	defer s.ReleaseModifying(node.Parent)
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
