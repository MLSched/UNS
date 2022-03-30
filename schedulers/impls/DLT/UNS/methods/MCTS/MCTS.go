package MCTS

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor"
	"UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/UNS/benefits"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	base2 "UNS/schedulers/impls/DLT/UNS/methods/base"
	"UNS/schedulers/impls/DLT/UNS/sampler"
	"UNS/schedulers/impls/DLT/UNS/score"
	"UNS/schedulers/impls/DLT/UNS/types"
	"UNS/schedulers/impls/DLT/base"
	"UNS/schedulers/partition"
	"UNS/utils"
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
	Level int // 节点所在的层级
	Index int // 节点所在层级中的索引
	// VisitingMu 表示当前节点正在被某个goroutine访问，不能被其他goroutine同时访问
	VisitingMu       *sync.Mutex
	Visiting         *atomic.Value
	SimulatedBenefit interfaces2.Benefit
	VisitedCount     int

	// JCTBenefit 与 ConsolidationScore 用于在一个任务的一批allocation中，筛选最优的jobAllocation。贪婪地选取JCT最好的任务分配结果。当JCT一样时，选取Consolidation分数最高的。
	JCTBenefit         interfaces2.Benefit
	ConsolidationScore score.JobAllocationsScore
}

func BuildMCTSMethod(sche *base2.Scheduler, configuration *configs.UNSSchedulerConfiguration) *Method {
	method := &Method{
		Scheduler: sche,
		CommonMethodParams: &base2.CommonMethodParams{
			Predictor: predictor.BuildPredictor(configuration.GetPredictorConfiguration()),
			AllocationsProvider: &base.AllocationsProviderImpl{
				RandomMode: true,
			},
			BenefitsCalculator: benefits.NewJCTCalculator(),
			ScoreCalculator:    score.NewConsolidationScoreCalculator(),
		},
		MaxLatency:            10 * time.Second,
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
	jobAllocations := make([]*pb_gen.JobAllocation, 0)
	if !s.IfHasUnallocated(pc) {
		return nil
	}

	filteredJobAllocations := s.FilterScheduleAbleJobAllocations(jobAllocations, pc)
	return &eventobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(filteredJobAllocations)}
}

type scheduleContext struct {
	method *Method
	// 为了节省资源和加快节点访问速度，使用二维数组表示树。这种方法需要设置一个节点扩展出来的孩子节点的个数上限。
	// 由于未被分配的任务数量是已知且有限的，所以使用树表示是可能的。
	// 第一维表示树的层级，假设未被分配的任务有n个，则第一维的大小即为n+1。第0层表示当前没有任务被分配。
	// 第二维表示这个层级下的全部节点，节点数量可直接计算得出。假设当前层数为x，则表示已经有x个任务被分配了，设上一层的节点个数为u，那么剩余n-x个任务，假设每个任务最多扩展p个节点，则该层的节点个数即为u*p(n-x)。
	InitialPC *partition.Context
	Predictor interfaces.Predictor
	// MaxJobAllocationsCount 限制了一个任务在一个partition context下，最多可能的allocation的可能性。这个数字限制了每一层树的最大宽度。
	MaxJobAllocationsCount    int
	Level2Nodes               [][]*Node
	Provider                  base.AllocationsProvider
	AllocationProvideTypeMode base.ProvideType
	// C 在普通的MCTS中，C作为经验参数是不变量，但是对于我们的应用来说，不同的benefit定义会造成C的最优取值发生变化。
	// 所以我们动态地计算出C的大小，C的定义为：每当计算出一个叶子节点的Benefit时，将C更新为曾经得到过的全部的Benefit的值的平均。
	C float64
}

func newScheduleContext(pc *partition.Context, maxJobAllocationsCount int, predictor interfaces.Predictor, provider base.AllocationsProvider, provideTypeMode base.ProvideType) *scheduleContext {
	unallocatedJobsCount := len(pc.AllocationViews.UnallocatedJobs)
	ctx := &scheduleContext{
		InitialPC:                 pc,
		Level2Nodes:               make([][]*Node, unallocatedJobsCount+1),
		MaxJobAllocationsCount:    maxJobAllocationsCount,
		Provider:                  provider,
		AllocationProvideTypeMode: provideTypeMode,
	}
	// 直接为搜索树预分配全部的空间。
	ctx.Level2Nodes[0] = make([]*Node, 1)
	ctx.Level2Nodes[0][0] = &Node{
		AllocContext: &types.AllocContext{
			PC:                pc,
			NewJobAllocations: make([]*pb_gen.JobAllocation, 0),
		},
		Level:      0,
		Index:      0,
		VisitingMu: &sync.Mutex{},
		Visiting: func() *atomic.Value {
			v := &atomic.Value{}
			v.Store(false)
			return v
		}(),
		SimulatedBenefit: 0,
		VisitedCount:     0,
	}
	for i := 1; i <= len(ctx.Level2Nodes); i++ {
		level := i
		// 上一层的节点个数
		lastLevelNodeCount := len(ctx.Level2Nodes[level-1])
		// 上一层的每个节点所产生的孩子的个数
		lastLevelEachNodeChildrenCount := ctx.level2EachNodeChildrenCount(level - 1)
		ctx.Level2Nodes[level] = make([]*Node, lastLevelNodeCount*lastLevelEachNodeChildrenCount)
	}
	return ctx
}

// level2LeftJobsCount 树的层级对应剩余任务数量。如第0层，只有根节点，则剩余任务即为所有的未分配的任务。
// 第1层，已经分配了1个任务，则剩余所有未分配的任务数量减一。
func (s *scheduleContext) level2LeftJobsCount(level int) int {
	return len(s.Level2Nodes) - level - 1
}

func (s *scheduleContext) level2EachNodeChildrenCount(level int) int {
	leftJobsCount := s.level2LeftJobsCount(level)
	return s.MaxJobAllocationsCount * leftJobsCount
}

func (s *scheduleContext) NodeParent(node *Node) *Node {
	if node.Level == 0 {
		return nil
	}
	parentIndex := node.Index % s.level2EachNodeChildrenCount(node.Level-1)
	parentNode := s.Level2Nodes[node.Level-1][parentIndex]
	return parentNode
}

func (s *scheduleContext) NodeChildren(node *Node) []*Node {
	if node.Level == len(s.Level2Nodes) {
		return nil
	}
	eachNodeChildrenCount := s.level2EachNodeChildrenCount(node.Level)
	left := node.Index * eachNodeChildrenCount
	right := (node.Index + 1) * eachNodeChildrenCount
	children := s.Level2Nodes[node.Level+1][left:right]
	return children
}

func (s *scheduleContext) EmptyChildrenNodes(node *Node) bool {
	children := s.NodeChildren(node)
	if children == nil || children[0] == nil {
		return true
	}
	return false
}

func (s *scheduleContext) Expand(node *Node) {
	unallocatedJobs := node.PC.AllocationViews.UnallocatedJobs
	predictResult, err := s.Predictor.Predict(node.PC, node.PC.AllocationViews.AllocationsSlice)
	if err != nil {
		panic(err)
	}
	accID2SortedTaskAllocations := s.Provider.PrepareAccID2SortedTaskAllocations(node.PC, predictResult)
	nodeID2TaskAllocations := s.method.GetNodeID2TaskAllocations(node.PC)
	// 使用固定的JCTCalculator作为node选择标准
	JCTCalculator := benefits.NewJCTCalculator()
	// 当JCT分数一致时，使用consolidation评分
	consolidationScoreCalculator := score.NewConsolidationScoreCalculator()
	_, JCTBenefitStub := JCTCalculator.ByPredict(node.PC, predictResult)
	_, consolidationScoreStub := consolidationScoreCalculator.GetScore(node.PC, node.PC.GetAllocationsSlice())
	wg := &sync.WaitGroup{}
	index := 0
	resultNodes := make([]*Node, len(unallocatedJobs))
	for _, job := range unallocatedJobs {
		job := job
		innerIndex := index
		utils.GoWithWG(wg, innerIndex, func(i int) {
			pc := node.PC
			getPossibleNodes := func(possibleAllocations []*pb_gen.JobAllocation) []*Node {
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
							for _, m := range pc.GetAllocationsSlice() {
								st, _ := utils.MarshalJsonPB(m)
								log.Printf("%s", st)
							}
							log.Printf("[UNS Scheduler] MCTS find unproper job allocation, err=[%v]", err)
							panic("fast fail")
						}
						if job.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
							s.method.MarkGangJobStartTime(jobAllocation, *partialPredictResult.GetResult(jobAllocation.GetTaskAllocations()[0]).GetStartExecutionNanoTime())
						}
						consolidationScore, _ := consolidationScoreCalculator.GetScoreIncrementally(pc, []*pb_gen.JobAllocation{jobAllocation}, consolidationScoreStub)
						JCTBenefit, _ := JCTCalculator.ByPredictIncrementally(pc, partialPredictResult, JCTBenefitStub)
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
							ConsolidationScore: consolidationScore,
							JCTBenefit:         JCTBenefit,
						})
						possibleNodes = append(possibleNodes)
					}
					attemptAlloc()
				}
				return possibleNodes
			}
			possibleAllocations := s.Provider.GetPossibleAllocations(&base.GetPossibleAllocationsParams{
				PC:                          pc,
				AccID2SortedTaskAllocations: accID2SortedTaskAllocations,
				PredictResult:               predictResult,
				Job:                         job,
				ProvideType:                 s.AllocationProvideTypeMode,
				MaxCount:                    math.MaxInt64,
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
			selectedNodes := nodes[:s.MaxJobAllocationsCount]
			for idx, selected := range selectedNodes {
				selected.Level = node.Level + 1
				selected.Index = i*s.MaxJobAllocationsCount + idx
				selected.Visiting = s.newVisiting()
				selected.VisitingMu = &sync.Mutex{}
			}
			copy(resultNodes[i*s.MaxJobAllocationsCount:(i+1)*s.MaxJobAllocationsCount], selectedNodes)
		})
		index++
	}
	wg.Wait()
}

func (s *scheduleContext) newVisiting() *atomic.Value {
	visiting := &atomic.Value{}
	visiting.Store(false)
	return visiting
}

func (s *scheduleContext) sortAndFilterPossibleNodes(possibleNodes []*Node) []*Node {
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
			continue
		} else {
			result = append(result, node)
			lastNodeBenefit = node.JCTBenefit
		}
	}
	return result
}

func (s *scheduleContext) SelectNode() *Node {
	//for level := 0; level < len(s.Level2Nodes); level++ {
	//	nodes := s.Level2Nodes[level]
	//	for _, node := range nodes {
	//
	//	}
	//}
	panic("implement me.")
}

func (s *scheduleContext) BackPropagation() {
	panic("implement me.")
}

func (s *scheduleContext) PlayOut() {
	panic("implement me.")
}

func (s *scheduleContext) UCT(node *Node) float64 {
	if node.VisitedCount == 0 {

	}
	panic("implement me.")

}
