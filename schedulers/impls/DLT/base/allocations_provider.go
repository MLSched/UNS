package base

import (
	"UNS/pb_gen"
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"github.com/golang/protobuf/ptypes/wrappers"
	"math"
	"sort"
	"strings"
)

type GetPossibleAllocationsParams struct {
	PC            *partition.Context
	PredictResult interfaces.PredictResult
	Job           *objects.Job
	ProvideType   ProvideType
	MaxCount      int
}

type AllocationsProvider interface {
	GetPossibleAllocations(params *GetPossibleAllocationsParams) []*pb_gen.JobAllocation
}

type ProvideType int

const (
	ProvideTypeDefault             ProvideType = 0
	ProvideTypeOnlyUnoccupied      ProvideType = 0x0000_0010
	ProvideTypeOnlyNonSpaceSharing ProvideType = 0x0000_0100
	ProvideTypeResourceEfficient   ProvideType = 0x0000_1000
)

type AllocationsProviderImpl struct {
	RandomMode bool // RandomMode开启时，找出的Allocation并不具有可复现性。
}

func (a *AllocationsProviderImpl) GetPossibleAllocations(params *GetPossibleAllocationsParams) []*pb_gen.JobAllocation {
	m := map[objects.TaskGroupType]func(params *GetPossibleAllocationsParams) []*pb_gen.JobAllocation{
		objects.TaskGroupType_taskGroupTypeSingle: a.GetSingleTaskJobPossibleAllocations,
		objects.TaskGroupType_taskGroupTypeGang:   a.GetGangJobPossibleAllocations,
	}
	return m[params.Job.GetTaskGroup().GetTaskGroupType()](params)
}

func (a *AllocationsProviderImpl) isProvideTypeMode(mode ProvideType, provideType ProvideType) bool {
	return mode&provideType != 0
}

func (a *AllocationsProviderImpl) GetSingleTaskJobPossibleAllocations(params *GetPossibleAllocationsParams) []*pb_gen.JobAllocation {
	// 筛选出AcceleratorID，使得他们上面最多只有一个job在运行，并且运行的不是GangJob
	// 从每个accelerator上，考虑最后一个运行的taskAllocation，查看是否存在与它共同运行的可能性
	pc := params.PC
	job := params.Job
	provideTypeMode := params.ProvideType
	predictResult := params.PredictResult

	//accID2SortedTaskAllocations := a.PrepareAccID2SortedTaskAllocations(pc, predictResult)
	result := make([]*pb_gen.JobAllocation, 0)
	buildJobAllocation := func(accID string, startTime int64) *pb_gen.JobAllocation {
		return buildJobAllocation(pc, job, []string{accID}, &startTime, startTime, false, nil)
	}
	addJobAllocation := func(accID string, startTime int64) {
		result = append(result, buildJobAllocation(accID, startTime))
	}
	finishTime := func(taskAllocation *objects.TaskAllocation) int64 {
		return *predictResult.GetResult(taskAllocation).GetFinishNanoTime()
	}
	rangeAccIDs := func() func(func(accID string) bool) {
		// 如果是随机模式，利用map自带的随机性质即可
		if a.RandomMode {
			return func(f func(accID string) bool) {
				for accID := range pc.MetalViews.AcceleratorID2SocketID {
					if stop := f(accID); stop {
						return
					}
				}
			}
		} else {
			return func(f func(accID string) bool) {
				for _, accID := range pc.MetalViews.AcceleratorIDs {
					if stop := f(accID); stop {
						return
					}
				}
			}
		}
	}()
	enoughAllocations := func() bool {
		if len(result) >= params.MaxCount {
			return true
		}
		return false
	}
	rangeAccIDs(func(accID string) (stop bool) {
		if enoughAllocations() {
			return true
		}
		taskAllocations := pc.AllocationViews.AcceleratorID2TaskAllocations[accID]
		////taskAllocations := accID2SortedTaskAllocations[accID]
		//// TODO debug
		//{
		//	//cachedTaskAllocations := pc.AllocationViews.AcceleratorID2TaskAllocations[accID]
		//	//sort.Slice(cachedTaskAllocations, func(i, j int) bool {
		//	//	return cachedTaskAllocations[i].TaskID < cachedTaskAllocations[j].TaskID
		//	//})
		//	sort.Slice(taskAllocations, func(i, j int) bool {
		//		return taskAllocations[i].TaskID < taskAllocations[j].TaskID
		//	})
		//	//for i := 0; i < len(cachedTaskAllocations); i++ {
		//	//	if cachedTaskAllocations[i] != taskAllocations[i] {
		//	//		log.Printf("")
		//	//	}
		//	//}
		//}
		if a.isProvideTypeMode(provideTypeMode, ProvideTypeOnlyUnoccupied) && len(taskAllocations) != 0 {
			return false
		}
		if len(taskAllocations) == 0 {
			// 没有任务在运行，直接添加
			addJobAllocation(accID, pc.FixedNow())
			return false
		}
		_, beforeLastFinishTime, lastTaskAllocation, _ := a.getLastFinishTaskAllocationAndJobID(pc, taskAllocations, predictResult)
		if j := pc.GetJob(lastTaskAllocation.GetJobID()); j.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
			// 最后一个Task是gang的，不能与它并行执行，直接放在它的后面。
			addJobAllocation(accID, finishTime(lastTaskAllocation))
			return false
		}
		if a.isProvideTypeMode(provideTypeMode, ProvideTypeOnlyNonSpaceSharing) {
			t := finishTime(lastTaskAllocation)
			addJobAllocation(accID, t)
			return false
		}
		if len(taskAllocations) == 1 {
			// 如果仅有一个任务，并且已知它不是gang的任务，则必定可以与它并行
			// 挑选两个时间点，分别是从now开始运行，和从它结束后开始运行
			now := pc.FixedNow()
			addJobAllocation(accID, now)
			if lastFinish := finishTime(lastTaskAllocation); lastFinish != now {
				addJobAllocation(accID, lastFinish)
			}
			return false
		} else {
			// 如果多于一个任务，则从倒数第二个任务结束开始，可以与最后一个任务并行执行
			//beforeLast := taskAllocations[len(taskAllocations)-2]
			//beforeLastFinishTime := finishTime(beforeLast)
			addJobAllocation(accID, beforeLastFinishTime)
			if lastFinish := finishTime(lastTaskAllocation); lastFinish != beforeLastFinishTime {
				addJobAllocation(accID, lastFinish)
			}
			return false
		}
	})
	// TODO debug
	//c := 0
	//for _, allocation := range result {
	//	if allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID() == "inst-2-replica-0-cpusocket-0-acc-0" && !allocation.GetTaskAllocations()[0].GetPlaceholder() && allocation.GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue() == 0 {
	//		c++
	//		break
	//	}
	//}
	//predictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
	//	if allocation.GetAcceleratorAllocation().GetAcceleratorID() == "inst-2-replica-0-cpusocket-0-acc-0" && allocation.GetStartExecutionTimeNanoSecond().GetValue() == 0 {
	//		c++
	//	}
	//})
	//if c == 3 {
	//	log.Printf("f")
	//}
	return result
}

func (a *AllocationsProviderImpl) GetGangJobPossibleAllocations(params *GetPossibleAllocationsParams) []*pb_gen.JobAllocation {
	// gang job不允许与其他任务并发运行
	// 需要遍历同一类型的acc，且数量要等于该任务的task数量。
	// 按照consolidation的级别，从最紧密开始遍历。
	// 将同一级别的acc进行排序，使得最早空闲的acc能够排在前面。
	// 然后每次按照task数量的时间窗口大小，进行滑动遍历。
	pc := params.PC
	job := params.Job
	provideTypeMode := params.ProvideType
	predictResult := params.PredictResult
	maxCount := params.MaxCount

	workersCount := len(job.GetTaskGroup().GetTasks())
	getLastTaskFinishTimeAndJobID := func() func(accID string) (int64, string) {
		type timeAndID struct {
			Time  int64
			JobID string
		}
		cache := make(map[string]timeAndID)
		return func(accID string) (int64, string) {
			if c, ok := cache[accID]; ok {
				return c.Time, c.JobID
			}
			time, _, _, jobID := a.getLastFinishTaskAllocationAndJobID(pc, pc.AllocationViews.AcceleratorID2TaskAllocations[accID], predictResult)
			cache[accID] = timeAndID{Time: time, JobID: jobID}
			return time, jobID
		}
	}()
	sortAccsByFinishTime := func(accIDs []string) {
		sort.SliceStable(accIDs, func(i, j int) bool {
			iFinish, _ := getLastTaskFinishTimeAndJobID(accIDs[i])
			jFinish, _ := getLastTaskFinishTimeAndJobID(accIDs[j])
			if iFinish < jFinish {
				return true
			}
			if iFinish == jFinish {
				return accIDs[i] < accIDs[j]
			}
			return false
		})
	}
	resultAllocations := make([]*pb_gen.JobAllocation, 0)
	addNewJobAllocation := func() func(accIDs []string) bool {
		attemptedAccIDs := make(map[string]bool)
		return func(accIDs []string) bool {
			sort.Strings(accIDs)
			connectedAccIDs := strings.Join(accIDs, "")
			if _, ok := attemptedAccIDs[connectedAccIDs]; ok {
				return false
			}
			allocationTime, latest, latestWaitingAccIDs := func() (earliest int64, latest int64, latestWaitingAccIDs map[string]bool) {
				earliest = math.MaxInt64
				latest = pc.FixedNow()
				latestWaitingAccIDs = make(map[string]bool)
				for _, accID := range accIDs {
					lastFinishTime, _ := getLastTaskFinishTimeAndJobID(accID)
					if lastFinishTime < earliest {
						earliest = lastFinishTime
					}
					if lastFinishTime == latest {
						latestWaitingAccIDs[accID] = true
					}
					if lastFinishTime > latest {
						latestWaitingAccIDs = make(map[string]bool)
						latestWaitingAccIDs[accID] = true
						latest = lastFinishTime
					}
				}
				if earliest < pc.FixedNow() {
					earliest = pc.FixedNow()
				}
				return earliest, latest, latestWaitingAccIDs
			}()
			attemptedAccIDs[connectedAccIDs] = true
			startTime, placeholder, placeholderWaitingJobIDs := func() (*int64, bool, []string) {
				if latest == pc.FixedNow() {
					n := pc.FixedNow()
					return &n, false, nil
				}
				waitingJobIDs := make(map[string]bool)
				for accID := range latestWaitingAccIDs {
					_, jobID := getLastTaskFinishTimeAndJobID(accID)
					waitingJobIDs[jobID] = true
				}
				return nil, true, utils.StringSetToSlice(waitingJobIDs)
			}()
			if a.isProvideTypeMode(provideTypeMode, ProvideTypeOnlyUnoccupied) && (startTime == nil || *startTime != pc.FixedNow()) {
				return false
			}
			if a.isProvideTypeMode(provideTypeMode, ProvideTypeResourceEfficient) {
				for _, waitingJobID := range placeholderWaitingJobIDs {
					if pc.Allocations[waitingJobID].GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue() > pc.FixedNow() {
						// 当该allocation等待的一个任务的开始时间比pc的FixedNow更晚，这时该allocation不是ResourceEfficient类型的，
						// 因为此时它等待了一个还没有真正开始运行的任务。
						continue
					}
				}
			}
			na := buildJobAllocation(pc, job, accIDs, startTime, allocationTime, placeholder, placeholderWaitingJobIDs)
			resultAllocations = append(resultAllocations, na)
			if len(resultAllocations) >= maxCount {
				return true
			}
			return false
		}
	}()
	pickSortedAccsAsWindow := func(accIDs []string) bool {
		for i := 0; i <= len(accIDs)-workersCount; i++ {
			if enough := addNewJobAllocation(accIDs[i : i+workersCount]); enough {
				return true
			}
		}
		return false
	}
	accType2Node2Socket2Accs := pc.MetalViews.GroupedAccelerators
	accTypes := make([]string, 0, len(accType2Node2Socket2Accs))
	for accType := range accType2Node2Socket2Accs {
		accTypes = append(accTypes, accType)
	}
	sortIfNotRandom := func(s []string) {
		if !a.RandomMode {
			sort.Strings(s)
		}
	}
	sortIfNotRandom(accTypes)
out:
	for _, accType := range accTypes {
		nodeID2Socket2Accs := accType2Node2Socket2Accs[accType]
		sameTypeAccIDs := make([]string, 0)
		nodeIDs := make([]string, 0, len(nodeID2Socket2Accs))
		for nodeID := range nodeID2Socket2Accs {
			nodeIDs = append(nodeIDs, nodeID)
		}
		sortIfNotRandom(nodeIDs)
		for _, nodeID := range nodeIDs {
			socket2accs := nodeID2Socket2Accs[nodeID]
			// 首先从最紧密的排布开始选取
			sameNodeAccIDs := make([]string, 0)
			sockets := make([]string, 0, len(socket2accs))
			for s := range socket2accs {
				sockets = append(sockets, s)
			}
			sortIfNotRandom(sockets)
			for _, socket := range sockets {
				accs := socket2accs[socket]
				sameNodeAccIDs = append(sameNodeAccIDs, accs...)
				if len(accs) < workersCount {
					continue
				}
				sortAccsByFinishTime(accs)
				// 遍历时，按照结束时间顺序，从早到晚，选取worksCount个acc作为该任务的一个分配结果
				if enough := pickSortedAccsAsWindow(accs); enough {
					break out
				}
			}
			sameTypeAccIDs = append(sameTypeAccIDs, sameNodeAccIDs...)
			// 再从同一Node，多个Socket的角度去选取
			sortAccsByFinishTime(sameNodeAccIDs)
			if enough := pickSortedAccsAsWindow(sameNodeAccIDs); enough {
				break out
			}
		}
		sortAccsByFinishTime(sameTypeAccIDs)
		if enough := pickSortedAccsAsWindow(sameTypeAccIDs); enough {
			break out
		}
	}
	return resultAllocations
}

// groupAccelerators 将accelerators分组，获取acc类型 -> acc所在节点 -> acc所在Socket -> acc 的映射
func (a *AllocationsProviderImpl) groupAccelerators(pc *partition.Context) map[string]map[string]map[string][]string {
	result := make(map[string]map[string]map[string][]string)
	for accID, acc := range pc.MetalViews.AcceleratorID2Accelerator {
		t := acc.GetAcceleratorMetaInfo().GetBriefType()
		if _, ok := result[t]; !ok {
			result[t] = make(map[string]map[string][]string)
		}
		nodeID := pc.MetalViews.AcceleratorID2NodeID[accID]
		node := pc.MetalViews.NodeID2Node[nodeID]
		if _, ok := result[t][node.GetNodeID()]; !ok {
			result[t][node.GetNodeID()] = make(map[string][]string, 0)
		}
		accID2SocketID := make(map[string]string)
		for _, socket := range node.GetCPUSockets() {
			for _, acc := range socket.GetAccelerators() {
				accID2SocketID[acc.GetAcceleratorID()] = socket.GetCPUSocketID()
			}
		}
		socketID := accID2SocketID[accID]
		if _, ok := result[t][node.GetNodeID()][socketID]; !ok {
			result[t][node.GetNodeID()][socketID] = make([]string, 0)
		}
		result[t][node.GetNodeID()][socketID] = append(result[t][node.GetNodeID()][socketID], acc.GetAcceleratorID())
	}
	return result
}

func (a *AllocationsProviderImpl) PrepareAccID2SortedTaskAllocations(pc *partition.Context, predictResult interfaces.PredictResult) map[string][]*objects.TaskAllocation {
	result := make(map[string][]*objects.TaskAllocation)
	for accID := range pc.MetalViews.AcceleratorID2Accelerator {
		result[accID] = make([]*objects.TaskAllocation, 0)
	}
	getFinishTime := func(taskAllocation *objects.TaskAllocation) int64 {
		ptr := predictResult.GetResult(taskAllocation).GetFinishNanoTime()
		return *ptr
	}
	for _, jobAllocation := range pc.Allocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()

			finish := getFinishTime(taskAllocation)
			s := result[accID]
			insertIdx := len(s)
			for idx, a := range result[accID] {
				f := getFinishTime(a)
				if finish < f {
					insertIdx = idx
					break
				}
			}
			rear := append([]*objects.TaskAllocation{}, s[insertIdx:]...)
			s = append(s[:insertIdx], taskAllocation)
			s = append(s, rear...)
			if !sort.SliceIsSorted(s, func(i, j int) bool {
				return getFinishTime(s[i]) < getFinishTime(s[j])
			}) {
				panic("not sorted")
			}
			//sorter := &utils.Sorter{
			//	LenFunc: func() int {
			//		return len(s)
			//	},
			//	LessFunc: func(i, j int) bool {
			//		return getFinishTime(s[i]) < getFinishTime(s[j])
			//	},
			//	SwapFunc: func(i, j int) {
			//		t := s[i]
			//		s[i] = s[j]
			//		s[j] = t
			//	},
			//}
			//if !sort.IsSorted(sorter) {
			//	panic("not sorted")
			//}
			result[accID] = s
		}
	}
	return result
}

func buildJobAllocation(pc *partition.Context, job *objects.Job, accIDs []string, startTime *int64, allocationTime int64, placeholder bool, placeholderWaitingJobIDs []string) *pb_gen.JobAllocation {
	fixedNow := pc.FixedNow()
	var start *wrappers.Int64Value = nil
	if startTime != nil {
		if *startTime < pc.FixedNow() {
			startTime = &fixedNow
		}
		start = &wrappers.Int64Value{Value: *startTime}
	}
	buildTaskAllocation := func(taskID string, accID string) *objects.TaskAllocation {
		return &objects.TaskAllocation{
			NodeID:                       pc.MetalViews.AcceleratorID2NodeID[accID],
			JobID:                        job.GetJobID(),
			TaskID:                       taskID,
			StartExecutionTimeNanoSecond: start,
			AllocationTimeNanoSecond:     allocationTime,
			Placeholder:                  placeholder,
			AcceleratorAllocation: &objects.AcceleratorAllocation{
				AcceleratorID: accID,
			},
		}
	}
	taskAllocations := make([]*objects.TaskAllocation, 0, len(job.GetTaskGroup().GetTasks()))
	for i, task := range job.GetTaskGroup().GetTasks() {
		taskAllocations = append(taskAllocations, buildTaskAllocation(task.GetTaskID(), accIDs[i]))
	}
	return &pb_gen.JobAllocation{
		JobAllocation: &objects.JobAllocation{
			JobID:             job.GetJobID(),
			ResourceManagerID: pc.Meta.GetResourceManagerID(),
			PartitionID:       pc.Meta.GetPartitionID(),
			TaskAllocations:   taskAllocations,
			Extra:             nil,
		},
		PlaceholderWaitingJobIDs: placeholderWaitingJobIDs,
	}
}

func (a *AllocationsProviderImpl) getLastFinishTaskAllocationAndJobID(pc *partition.Context, taskAllocations []*objects.TaskAllocation, result interfaces.PredictResult) (int64, int64, *objects.TaskAllocation, string) {
	if len(taskAllocations) == 0 {
		return pc.FixedNow(), 0, nil, ""
	}
	max := int64(math.MinInt64)
	beforeMax := int64(math.MinInt64)
	maxJobID := ""
	var maxTaskAllocation *objects.TaskAllocation = nil
	for _, taskAllocation := range taskAllocations {
		t := *result.GetResult(taskAllocation).GetFinishNanoTime()
		if t >= max {
			beforeMax = max
			max = t
			maxJobID = taskAllocation.GetJobID()
			maxTaskAllocation = taskAllocation
		} else if t >= beforeMax {
			beforeMax = t
		}
	}
	return max, beforeMax, maxTaskAllocation, maxJobID
}
