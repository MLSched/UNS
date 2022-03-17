package base

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"github.com/golang/protobuf/ptypes/wrappers"
	"sort"
	"strings"
)

type AllocationsProvider interface {
	GetPossibleAllocations(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, predictResult interfaces.PredictResult, job *objects.Job) []*objects.JobAllocation
	// PrepareAccID2SortedTaskAllocations 在一个predictResult下，计算出每个加速器上，所有的jobAllocation的一个排序，该排序按照每个任务的结束时间进行排序。
	PrepareAccID2SortedTaskAllocations(pc *partition.Context, predictResult interfaces.PredictResult) map[string][]*objects.TaskAllocation
}

type AllocationsProviderImpl struct {
	MaxGangAllocations int
}

func (a *AllocationsProviderImpl) GetPossibleAllocations(pc *partition.Context, accID2SortedJobAllocations map[string][]*objects.TaskAllocation, predictResult interfaces.PredictResult, job *objects.Job) []*objects.JobAllocation {
	m := map[objects.TaskGroupType]func(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, predictResult interfaces.PredictResult, job *objects.Job) []*objects.JobAllocation{
		objects.TaskGroupType_taskGroupTypeSingle: a.GetSingleTaskJobPossibleAllocations,
		objects.TaskGroupType_taskGroupTypeGang:   a.GetGangJobPossibleAllocations,
	}
	return m[job.GetTaskGroup().GetTaskGroupType()](pc, accID2SortedJobAllocations, predictResult, job)
}

func (a *AllocationsProviderImpl) GetSingleTaskJobPossibleAllocations(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, predictResult interfaces.PredictResult, job *objects.Job) []*objects.JobAllocation {
	// 筛选出AcceleratorID，使得他们上面最多只有一个job在运行，并且运行的不是GangJob
	// 从每个accelerator上，考虑最后一个运行的taskAllocation，查看是否存在与它共同运行的可能性
	result := make([]*objects.JobAllocation, 0)
	buildJobAllocation := func(accID string, startTime int64) *objects.JobAllocation {
		return buildJobAllocation(pc, job, []string{accID}, &startTime, false)
	}
	finishTime := func(taskAllocation *objects.TaskAllocation) int64 {
		return *predictResult.GetResult(taskAllocation).GetFinishNanoTime()
	}
	for accID, taskAllocations := range accID2SortedTaskAllocations {
		if len(taskAllocations) == 0 {
			// 没有任务在运行，直接添加
			result = append(result, buildJobAllocation(accID, pc.Now()))
			continue
		}
		lastTaskAllocation := taskAllocations[len(taskAllocations)-1]
		if j := pc.GetUnfinishedJob(lastTaskAllocation.GetJobID()); j.GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
			// 最后一个Task是gang的，不能与它并行执行，直接放在它的后面。
			result = append(result, buildJobAllocation(accID, finishTime(lastTaskAllocation)))
			continue
		}
		if len(taskAllocations) == 1 {
			// 如果仅有一个任务，并且已知它不是gang的任务，则必定可以与它并行
			// 挑选两个时间点，分别是从now开始运行，和从它结束后开始运行
			result = append(result, buildJobAllocation(accID, pc.Now()))
			result = append(result, buildJobAllocation(accID, finishTime(lastTaskAllocation)))
			continue
		} else {
			// 如果多于一个任务，则从倒数第二个任务结束开始，可以与最后一个任务并行执行
			beforeLast := taskAllocations[len(taskAllocations)-2]
			result = append(result, buildJobAllocation(accID, finishTime(beforeLast)))
			result = append(result, buildJobAllocation(accID, finishTime(lastTaskAllocation)))
			continue
		}
	}
	return result
}

func (a *AllocationsProviderImpl) GetGangJobPossibleAllocations(pc *partition.Context, accID2SortedTaskAllocations map[string][]*objects.TaskAllocation, predictResult interfaces.PredictResult, job *objects.Job) []*objects.JobAllocation {
	// gang job不允许与其他任务并发运行
	// 需要遍历同一类型的acc，且数量要等于该任务的task数量。
	// 按照consolidation的级别，从最紧密开始遍历。
	// 将同一级别的acc进行排序，使得最早空闲的acc能够排在前面。
	// 然后每次按照task数量的时间窗口大小，进行滑动遍历。
	workersCount := len(job.GetTaskGroup().GetTasks())
	getLastTaskFinishTime := func(acc *objects.Accelerator) int64 {
		if sorted, ok := accID2SortedTaskAllocations[acc.GetAcceleratorID()]; ok && len(sorted) > 0 {
			return *predictResult.GetResult(sorted[len(sorted)-1]).GetFinishNanoTime()
		}
		return pc.Now()
	}
	sortAccsByFinishTime := func(accs []*objects.Accelerator) {
		sorter := &utils.Sorter{
			LenFunc: func() int {
				return len(accs)
			},
			LessFunc: func(i, j int) bool {
				return getLastTaskFinishTime(accs[i]) < getLastTaskFinishTime(accs[j])
			},
			SwapFunc: func(i, j int) {
				t := accs[i]
				accs[i] = accs[j]
				accs[j] = t
			},
		}
		if !sort.IsSorted(sorter) {
			sort.Sort(sorter)
		}
	}
	resultAllocations := make([]*objects.JobAllocation, 0)
	addNewJobAllocation := func() func(accs []*objects.Accelerator) {
		attemptedAccIDs := make(map[string]bool)
		return func(accs []*objects.Accelerator) {
			accIDs := make([]string, 0, len(accs))
			for _, acc := range accs {
				acc.GetAcceleratorID()
			}
			sort.Strings(accIDs)
			connectedAccIDs := strings.Join(accIDs, "")
			if _, ok := attemptedAccIDs[connectedAccIDs]; ok {
				return
			}
			attemptedAccIDs[connectedAccIDs] = true
			na := buildJobAllocation(pc, job, accIDs, nil, true)
			resultAllocations = append(resultAllocations, na)
		}
	}()
	pickSortedAccsAsWindow := func(accs []*objects.Accelerator) {
		for i := 0; i < len(accs)-workersCount; i++ {
			addNewJobAllocation(accs[i : i+workersCount])
		}
	}
	accType2Node2Socket2Accs := a.groupAccelerators(pc)
	for _, node2Socket2Accs := range accType2Node2Socket2Accs {
		sameTypeAccs := make([]*objects.Accelerator, 0)
		for _, socket2accs := range node2Socket2Accs {
			// 首先从最紧密的排布开始选取
			sameNodeAccs := make([]*objects.Accelerator, 0)
			for _, accs := range socket2accs {
				sameNodeAccs = append(sameNodeAccs, accs...)
				if len(accs) < workersCount {
					continue
				}
				sortAccsByFinishTime(accs)
				// 遍历时，按照结束时间顺序，从早到晚，选取worksCount个acc作为该任务的一个分配结果
				pickSortedAccsAsWindow(accs)
				if len(resultAllocations) > a.MaxGangAllocations {
					return resultAllocations
				}
			}
			sameTypeAccs = append(sameTypeAccs, sameNodeAccs...)
			// 再从同一Node，多个Socket的角度去选取
			sortAccsByFinishTime(sameNodeAccs)
			pickSortedAccsAsWindow(sameNodeAccs)
			if len(resultAllocations) > a.MaxGangAllocations {
				return resultAllocations
			}
		}
		sortAccsByFinishTime(sameTypeAccs)
		pickSortedAccsAsWindow(sameTypeAccs)
		if len(resultAllocations) > a.MaxGangAllocations {
			return resultAllocations
		}
	}
	return resultAllocations
}

// groupAccelerators 将accelerators分组，获取acc类型 -> acc所在节点 -> acc所在Socket -> acc 的映射
func (a *AllocationsProviderImpl) groupAccelerators(pc *partition.Context) map[string]map[*objects.Node]map[string][]*objects.Accelerator {
	result := make(map[string]map[*objects.Node]map[string][]*objects.Accelerator)
	for accID, acc := range pc.View.AcceleratorID2Accelerator {
		t := acc.GetAcceleratorMetaInfo().GetBriefType()
		if _, ok := result[t]; !ok {
			result[t] = make(map[*objects.Node]map[string][]*objects.Accelerator)
		}
		nodeID := pc.View.AcceleratorID2NodeID[accID]
		node := pc.View.NodeID2Node[nodeID]
		if _, ok := result[t][node]; !ok {
			result[t][node] = make(map[string][]*objects.Accelerator, 0)
		}
		accID2SocketID := make(map[string]string)
		for _, socket := range node.GetCPUSockets() {
			for _, acc := range socket.GetAccelerators() {
				accID2SocketID[acc.GetAcceleratorID()] = socket.GetCPUSocketID()
			}
		}
		socketID := accID2SocketID[accID]
		if _, ok := result[t][node][socketID]; !ok {
			result[t][node][socketID] = make([]*objects.Accelerator, 0)
		}
		result[t][node][socketID] = append(result[t][node][socketID], acc)
	}
	return result
}

func (a *AllocationsProviderImpl) PrepareAccID2SortedTaskAllocations(pc *partition.Context, predictResult interfaces.PredictResult) map[string][]*objects.TaskAllocation {
	result := make(map[string][]*objects.TaskAllocation)
	for accID := range pc.View.AcceleratorID2Accelerator {
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
			insertIdx := 0
			for idx, a := range result[accID] {
				f := getFinishTime(a)
				if finish > f {
					insertIdx = idx + 1
					break
				}
			}
			rear := append([]*objects.TaskAllocation{}, result[accID][insertIdx:]...)
			result[accID] = append(result[accID][:insertIdx], taskAllocation)
			result[accID] = append(result[accID], rear...)
		}
	}
	return result
}

func buildJobAllocation(pc *partition.Context, job *objects.Job, accIDs []string, startTime *int64, placeholder bool) *objects.JobAllocation {
	var start *wrappers.Int64Value = nil
	if startTime != nil {
		start = &wrappers.Int64Value{Value: *startTime}
	}
	buildTaskAllocation := func(taskID string, accID string) *objects.TaskAllocation {
		return &objects.TaskAllocation{
			NodeID:                       pc.View.AcceleratorID2NodeID[accID],
			JobID:                        job.GetJobID(),
			TaskID:                       job.GetTaskGroup().GetTasks()[0].GetTaskID(),
			StartExecutionTimeNanoSecond: start,
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
	return &objects.JobAllocation{
		JobID:             job.GetJobID(),
		ResourceManagerID: pc.Meta.GetResourceManagerID(),
		PartitionID:       pc.Meta.GetPartitionID(),
		TaskAllocations:   taskAllocations,
		Extra:             nil,
	}
}
