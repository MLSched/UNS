package partition

import (
	"UNS/pb_gen"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"log"
	"sort"
	"sync"
	"time"
)

type Context struct {
	Meta            *objects.Partition
	MetalViews      *MetalViews
	AllocationViews *AllocationViews
	mu              *sync.RWMutex

	Allocations    map[string]*pb_gen.JobAllocation
	UnfinishedJobs map[string]*objects.Job
	FinishedJobs   map[string]*objects.Job

	ExecutionHistoryManager *ExecutionHistoryManager

	*Util

	Time *int64
}

type MetalViews struct {
	NodeID2Node               map[string]*objects.Node
	NodeID2Accelerators       map[string][]*objects.Accelerator
	AcceleratorID2Accelerator map[string]*objects.Accelerator
	AcceleratorID2NodeID      map[string]string
	AcceleratorID2SocketID    map[string]string
	AcceleratorIDs            []string
	AcceleratorIDsSet         map[string]bool
	// GroupedAccelerators acc类型 -> acc所在节点 -> acc所在Socket -> acc 的映射
	GroupedAccelerators map[string]map[string]map[string][]string
}

type AllocationViews struct {
	UnallocatedAcceleratorIDs     map[string]bool
	UnallocatedJobs               map[string]*objects.Job
	AllocationsSlice              []*pb_gen.JobAllocation
	AcceleratorID2TaskAllocations map[string][]*objects.TaskAllocation
	NodeID2TaskAllocations        map[string][]*objects.TaskAllocation
}

func Build(partition *objects.Partition) (*Context, error) {
	ctx := &Context{
		Meta:                    partition,
		mu:                      &sync.RWMutex{},
		Util:                    &Util{},
		Allocations:             make(map[string]*pb_gen.JobAllocation),
		UnfinishedJobs:          make(map[string]*objects.Job),
		FinishedJobs:            make(map[string]*objects.Job),
		ExecutionHistoryManager: NewExecutionHistoryManager(),
	}
	ctx.refreshMetalViews()
	ctx.RefreshAllocationViews()
	return ctx, nil
}

func (c *Context) refreshMetalViews() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MetalViews = &MetalViews{
		NodeID2Node:               make(map[string]*objects.Node),
		NodeID2Accelerators:       make(map[string][]*objects.Accelerator),
		AcceleratorID2Accelerator: make(map[string]*objects.Accelerator),
		AcceleratorID2NodeID:      make(map[string]string),
		AcceleratorID2SocketID:    make(map[string]string),
		AcceleratorIDs:            make([]string, 0),
		AcceleratorIDsSet:         make(map[string]bool),
	}
	for _, node := range c.Meta.GetNodes() {
		c.MetalViews.NodeID2Node[node.GetNodeID()] = node
		accelerators := make([]*objects.Accelerator, 0)
		for _, CPUSocket := range node.GetCPUSockets() {
			for _, accelerator := range CPUSocket.GetAccelerators() {
				accelerators = append(accelerators, accelerator)
			}
			//accelerators = append(accelerators, CPUSocket.GetAccelerators()...)
			for _, accelerator := range CPUSocket.GetAccelerators() {
				c.MetalViews.AcceleratorID2Accelerator[accelerator.GetAcceleratorID()] = accelerator
				c.MetalViews.AcceleratorID2NodeID[accelerator.GetAcceleratorID()] = node.GetNodeID()
				c.MetalViews.AcceleratorID2SocketID[accelerator.GetAcceleratorID()] = CPUSocket.GetCPUSocketID()
				c.MetalViews.AcceleratorIDs = append(c.MetalViews.AcceleratorIDs, accelerator.GetAcceleratorID())
				c.MetalViews.AcceleratorIDsSet[accelerator.GetAcceleratorID()] = true
			}
		}
		c.MetalViews.NodeID2Accelerators[node.GetNodeID()] = accelerators
	}
	c.MetalViews.GroupedAccelerators = c.groupAccelerators()
	sort.Strings(c.MetalViews.AcceleratorIDs)
}

func (c *Context) UpdateAllocations(eo *eventobjs.RMUpdateAllocationsEvent) error {
	c.mu.Lock()
	defer func() {
		c.RefreshAllocationViews()
		c.mu.Unlock()
	}()
	c.UpdateCurrentTime(eo.GetCurrentNanoTime())
	for _, updatedJobAllocation := range eo.UpdatedJobAllocations {
		jobID := updatedJobAllocation.GetJobID()
		if _, ok := c.UnfinishedJobs[jobID]; !ok {
			reason := fmt.Sprintf("MockPartition Context ID = [%s] update allocations, encounter unkonwn job ID = [%s]", c.Meta.GetPartitionID(), jobID)
			log.Println(reason)
			return errors.New(reason)
		}
	}
	for _, finishedJobID := range eo.FinishedJobIDs {
		delete(c.Allocations, finishedJobID)
		j := c.UnfinishedJobs[finishedJobID]
		delete(c.UnfinishedJobs, finishedJobID)
		c.FinishedJobs[finishedJobID] = j
	}
	for _, unallocatedJobID := range eo.UnallocatedJobIDs {
		delete(c.Allocations, unallocatedJobID)
	}
	for _, updatedJobAllocation := range eo.UpdatedJobAllocations {
		jobID := updatedJobAllocation.GetJobID()
		c.Allocations[jobID] = pb_gen.WrapJobAllocation(updatedJobAllocation)
	}
	c.ExecutionHistoryManager.Add(eo.GetJobExecutionHistories()...)
	return nil
}

func (c *Context) UpdateJobs(eo *eventobjs.RMUpdateJobsEvent) error {
	c.mu.Lock()
	defer func() {
		c.RefreshAllocationViews()
		c.mu.Unlock()
	}()
	c.UpdateCurrentTime(eo.GetCurrentNanoTime())
	for _, job := range eo.GetNewJobs() {
		if _, duplicated := c.UnfinishedJobs[job.GetJobID()]; duplicated {
			reason := fmt.Sprintf("MockPartition Context ID = [%s] update jobs, add new jobs, encounter duplicated job ID = [%s]", c.Meta.GetPartitionID(), job.GetJobID())
			log.Println(reason)
			return errors.New(reason)
		}
	}
	for _, jobID := range eo.GetRemovedJobIDs() {
		if _, exists := c.UnfinishedJobs[jobID]; !exists {
			reason := fmt.Sprintf("MockPartition Context ID = [%s] update jobs, remove jobs, encounter unkown job ID = [%s]", c.Meta.GetPartitionID(), jobID)
			log.Println(reason)
			return errors.New(reason)
		}
	}
	for _, job := range eo.GetNewJobs() {
		c.UnfinishedJobs[job.GetJobID()] = job
	}
	for _, jobID := range eo.GetRemovedJobIDs() {
		delete(c.UnfinishedJobs, jobID)
		delete(c.Allocations, jobID)
	}
	return nil
}

func (c *Context) UpdateTime(eo *eventobjs.RMUpdateTimeEvent) error {
	t := eo.GetCurrentNanoTime()
	c.Time = &t
	return nil
}

func (c *Context) UpdateCurrentTime(currentNanoTime *wrappers.Int64Value) {
	if currentNanoTime == nil {
		return
	}
	v := currentNanoTime.GetValue()
	c.Time = &v
}

func (c *Context) Now() int64 {
	if c.Time == nil {
		return time.Now().UnixNano()
	}
	return *c.Time
}

func (c *Context) FixedNow() int64 {
	return *c.Time
}

// Clone 不需加锁，因为仅仅在调度算法中调用，不会同时进行写操作。
func (c *Context) Clone(cloneAllocations bool) *Context {
	cloned, _ := Build(c.Meta)
	if cloneAllocations {
		for jobID, allocation := range c.Allocations {
			clonedTaskAllocations := make([]*objects.TaskAllocation, 0, len(allocation.GetTaskAllocations()))
			for _, taskAllocation := range allocation.GetTaskAllocations() {
				clonedTaskAllocations = append(clonedTaskAllocations, &objects.TaskAllocation{
					NodeID:                       taskAllocation.GetNodeID(),
					TaskID:                       taskAllocation.GetTaskID(),
					HostMemoryAllocation:         taskAllocation.GetHostMemoryAllocation(),
					CPUSocketAllocations:         taskAllocation.GetCPUSocketAllocations(),
					AcceleratorAllocation:        taskAllocation.GetAcceleratorAllocation(),
					Extra:                        taskAllocation.GetExtra(),
					StartExecutionTimeNanoSecond: taskAllocation.GetStartExecutionTimeNanoSecond(),
					AllocationTimeNanoSecond:     taskAllocation.GetAllocationTimeNanoSecond(),
					Placeholder:                  taskAllocation.GetPlaceholder(),
				})
			}
			cloned.Allocations[jobID] = &pb_gen.JobAllocation{
				JobAllocation: &objects.JobAllocation{
					JobID:             allocation.GetJobID(),
					ResourceManagerID: allocation.GetResourceManagerID(),
					PartitionID:       allocation.GetPartitionID(),
					TaskAllocations:   clonedTaskAllocations,
					Extra:             allocation.GetExtra(),
				},
			}
		}
	} else {
		cloned.Allocations = make(map[string]*pb_gen.JobAllocation)
		for jobID, allocation := range c.Allocations {
			cloned.Allocations[jobID] = allocation
		}
	}
	//cloned.UnfinishedJobs = c.UnfinishedJobs
	cloned.UnfinishedJobs = make(map[string]*objects.Job)
	for ID, job := range c.UnfinishedJobs {
		cloned.UnfinishedJobs[ID] = job
	}
	//cloned.FinishedJobs = c.FinishedJobs
	cloned.FinishedJobs = make(map[string]*objects.Job)
	for ID, job := range c.FinishedJobs {
		cloned.FinishedJobs[ID] = job
	}
	cloned.ExecutionHistoryManager = c.ExecutionHistoryManager.Clone()
	cloned.Time = c.Time
	cloned.RefreshAllocationViews()
	return cloned
}

func (c *Context) GetUnfinishedJob(jobID string) *objects.Job {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnfinishedJobs[jobID]
}

func (c *Context) GetUnfinishedJobs() map[string]*objects.Job {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnfinishedJobs
}

func (c *Context) GetJob(jobID string) *objects.Job {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if j, ok := c.UnfinishedJobs[jobID]; ok {
		return j
	} else {
		return c.FinishedJobs[jobID]
	}
}

func (c *Context) getAllocationsSlice() []*pb_gen.JobAllocation {
	allocations := make([]*pb_gen.JobAllocation, 0, len(c.Allocations))
	for _, allocation := range c.Allocations {
		allocations = append(allocations, allocation)
	}
	return allocations
}

func (c *Context) getUnallocatedJobs() map[string]*objects.Job {
	jobs := make(map[string]*objects.Job)
	for jobID, job := range c.UnfinishedJobs {
		jobs[jobID] = job
	}
	for jobID := range c.Allocations {
		delete(jobs, jobID)
	}
	return jobs
}

func (c *Context) getUnallocatedAcceleratorIDs() map[string]bool {
	totalAcceleratorIDs := make(map[string]bool)
	for accID := range c.MetalViews.AcceleratorID2Accelerator {
		totalAcceleratorIDs[accID] = true
	}
	for _, jobAllocation := range c.Allocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			delete(totalAcceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
		}
	}
	return totalAcceleratorIDs
}

func (c *Context) GetJobExecutionHistories() map[string]*objects.JobExecutionHistory {
	return c.ExecutionHistoryManager.GetAll()
}

func (c *Context) RefreshAllocationViews() {
	c.AllocationViews = &AllocationViews{
		UnallocatedAcceleratorIDs:     nil,
		UnallocatedJobs:               nil,
		AllocationsSlice:              nil,
		AcceleratorID2TaskAllocations: nil,
		NodeID2TaskAllocations:        nil,
	}
	c.AllocationViews.UnallocatedAcceleratorIDs = c.getUnallocatedAcceleratorIDs()
	c.AllocationViews.UnallocatedJobs = c.getUnallocatedJobs()
	c.AllocationViews.AllocationsSlice = c.getAllocationsSlice()
	c.AllocationViews.AcceleratorID2TaskAllocations = c.getAccID2SortedTaskAllocations()
	// getNodeID2TaskAllocations 依赖了AllocationsSlice，一定要注意顺序，不能在GetAllocationsSlice之前调用GetNodeID2TaskAllocations
	c.AllocationViews.NodeID2TaskAllocations = c.getNodeID2TaskAllocations()
}

func (c *Context) getAccID2SortedTaskAllocations() map[string][]*objects.TaskAllocation {
	result := make(map[string][]*objects.TaskAllocation)
	for accID := range c.MetalViews.AcceleratorID2Accelerator {
		result[accID] = make([]*objects.TaskAllocation, 0)
	}
	for _, jobAllocation := range c.Allocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
			result[accID] = append(result[accID], taskAllocation)
		}
	}
	return result
}

// TempAllocJob 临时性的将jobAllocation添加，为了高性能，需要更新缓存。
func (c *Context) TempAllocJob(allocation *pb_gen.JobAllocation) func() {
	// Allocations
	c.Allocations[allocation.GetJobID()] = allocation

	// UnallocatedJobs
	job := c.AllocationViews.UnallocatedJobs[allocation.GetJobID()]
	delete(c.AllocationViews.UnallocatedJobs, allocation.GetJobID())

	// AllocationsSlice
	//beforeAllocationsSlice := make([]*pb_gen.JobAllocation, len(c.AllocationViews.AllocationsSlice))
	// TODO debug
	//copy(beforeAllocationsSlice, c.AllocationViews.AllocationsSlice)
	c.AllocationViews.AllocationsSlice = append(c.AllocationViews.AllocationsSlice, allocation)

	// AcceleratorID2TaskAllocations
	accIDs := make(map[string]bool)
	// TODO debug
	//copiedAccID2TaskAllocations := make(map[string][]*objects.TaskAllocation)
	//for accID, taskAllocations := range c.AllocationViews.AcceleratorID2TaskAllocations {
	//	copiedAccID2TaskAllocations[accID] = make([]*objects.TaskAllocation, len(taskAllocations))
	//	copy(copiedAccID2TaskAllocations[accID], taskAllocations)
	//}

	for _, taskAllocation := range allocation.GetTaskAllocations() {
		accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
		c.AllocationViews.AcceleratorID2TaskAllocations[accID] = append(c.AllocationViews.AcceleratorID2TaskAllocations[accID], taskAllocation)
		accIDs[accID] = true
	}

	// UnallocatedAcceleratorIDs
	deletedUnallocatedAccIDKeys := make(map[string]bool)
	for _, taskAllocation := range allocation.GetTaskAllocations() {
		accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
		if _, ok := c.AllocationViews.UnallocatedAcceleratorIDs[accID]; ok {
			deletedUnallocatedAccIDKeys[accID] = true
			delete(c.AllocationViews.UnallocatedAcceleratorIDs, accID)
		}
	}

	// NodeID2TaskAllocations

	// TODO Debug
	//copiedNodeID2TaskAllocations := make(map[string][]*objects.TaskAllocation)
	//for nodeID, taskAllocations := range c.AllocationViews.NodeID2TaskAllocations {
	//	copiedNodeID2TaskAllocations[nodeID] = make([]*objects.TaskAllocation, len(taskAllocations))
	//	copy(copiedNodeID2TaskAllocations[nodeID], taskAllocations)
	//}

	for _, taskAllocation := range allocation.GetTaskAllocations() {
		nodeID := taskAllocation.GetNodeID()
		c.AllocationViews.NodeID2TaskAllocations[nodeID] = append(c.AllocationViews.NodeID2TaskAllocations[nodeID], taskAllocation)
	}

	return func() {
		// Allocations
		delete(c.Allocations, allocation.GetJobID())

		// UnallocatedJobs
		c.AllocationViews.UnallocatedJobs[allocation.GetJobID()] = job

		// UnallocatedAcceleratorIDs
		for key := range deletedUnallocatedAccIDKeys {
			c.AllocationViews.UnallocatedAcceleratorIDs[key] = true
		}

		// AllocationsSlice
		c.AllocationViews.AllocationsSlice = c.AllocationViews.AllocationsSlice[:len(c.AllocationViews.AllocationsSlice)-1]
		//// TODO Debug
		//for idx, item := range c.AllocationViews.AllocationsSlice {
		//	if beforeAllocationsSlice[idx] != item {
		//		panic("error")
		//	}
		//}

		// AcceleratorID2TaskAllocations
		for accID := range accIDs {
			s := c.AllocationViews.AcceleratorID2TaskAllocations[accID]
			// 删除最后一项。
			c.AllocationViews.AcceleratorID2TaskAllocations[accID] = s[:len(s)-1]
		}
		//// TODO Debug
		//for accID, taskAllocations := range copiedAccID2TaskAllocations {
		//	copied := c.AllocationViews.AcceleratorID2TaskAllocations[accID]
		//	for idx, taskAllocation := range taskAllocations {
		//		if copied[idx] != taskAllocation {
		//			panic("error accID2taskAllocations")
		//		}
		//	}
		//}

		// NodeID2TaskAllocations
		for _, taskAllocation := range allocation.GetTaskAllocations() {
			nodeID := taskAllocation.GetNodeID()
			s := c.AllocationViews.NodeID2TaskAllocations[nodeID]
			// 删除最后一项。
			c.AllocationViews.NodeID2TaskAllocations[nodeID] = s[:len(s)-1]
		}

		// TODO debug
		//for nodeID, taskAllocations := range copiedNodeID2TaskAllocations {
		//	copied := c.AllocationViews.NodeID2TaskAllocations[nodeID]
		//	for idx, taskAllocation := range taskAllocations {
		//		if copied[idx] != taskAllocation {
		//			panic("error nodeID2taskAllocations")
		//		}
		//	}
		//}
	}
}

// groupAccelerators 将accelerators分组，获取acc类型 -> acc所在节点 -> acc所在Socket -> acc 的映射
func (c *Context) groupAccelerators() map[string]map[string]map[string][]string {
	pc := c
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

func (c *Context) getNodeID2TaskAllocations() map[string][]*objects.TaskAllocation {
	pc := c
	result := make(map[string][]*objects.TaskAllocation)
	for _, jobAllocation := range pc.AllocationViews.AllocationsSlice {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			nodeID := taskAllocation.GetNodeID()
			if _, ok := result[nodeID]; !ok {
				result[nodeID] = make([]*objects.TaskAllocation, 0)
			}
			result[nodeID] = append(result[nodeID], taskAllocation)
		}
	}
	return result
}
