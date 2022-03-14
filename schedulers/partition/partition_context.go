package partition

import (
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type Context struct {
	Meta *objects.Partition
	View *View
	mu   *sync.RWMutex

	Allocations    map[string]*objects.JobAllocation
	UnfinishedJobs map[string]*objects.Job
	FinishedJobs   map[string]*objects.Job

	ExecutionHistoryManager *ExecutionHistoryManager

	*Util

	Time *int64
}

type View struct {
	NodeID2Node               map[string]*objects.Node
	NodeID2Accelerators       map[string][]*objects.Accelerator
	AcceleratorID2Accelerator map[string]*objects.Accelerator
}

func Build(partition *objects.Partition) (*Context, error) {
	ctx := &Context{
		Meta:                    partition,
		mu:                      &sync.RWMutex{},
		Util:                    &Util{},
		Allocations:             make(map[string]*objects.JobAllocation),
		UnfinishedJobs:          make(map[string]*objects.Job),
		FinishedJobs:            make(map[string]*objects.Job),
		ExecutionHistoryManager: NewExecutionHistoryManager(),
	}
	ctx.refreshView()
	return ctx, nil
}

func (c *Context) refreshView() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.View = &View{
		NodeID2Node:               make(map[string]*objects.Node),
		NodeID2Accelerators:       make(map[string][]*objects.Accelerator),
		AcceleratorID2Accelerator: make(map[string]*objects.Accelerator),
	}
	for _, node := range c.Meta.GetNodes() {
		c.View.NodeID2Node[node.GetNodeID()] = node
		accelerators := make([]*objects.Accelerator, 0)
		for _, CPUSocket := range node.GetCPUSockets() {
			accelerators = append(accelerators, CPUSocket.GetAccelerators()...)
			for _, accelerator := range accelerators {
				c.View.AcceleratorID2Accelerator[accelerator.GetAcceleratorID()] = accelerator
			}
		}
		c.View.NodeID2Accelerators[node.GetNodeID()] = accelerators
	}
}

func (c *Context) UpdateAllocations(eo *eventobjs.RMUpdateAllocationsEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if eo.GetCurrentNanoTime() != 0 {
		c.updateCurrentTime(eo.GetCurrentNanoTime())
	}
	for _, updatedJobAllocation := range eo.UpdatedJobAllocations {
		jobID := updatedJobAllocation.GetJobID()
		if _, ok := c.UnfinishedJobs[jobID]; !ok {
			reason := fmt.Sprintf("Partition Context ID = [%s] update allocations, encounter unkonwn job ID = [%s]", c.Meta.GetPartitionID(), jobID)
			log.Println(reason)
			return errors.New(reason)
		}
	}
	for _, finishedJobID := range eo.FinishedJobIDs {
		delete(c.Allocations, finishedJobID)
		j := c.UnfinishedJobs[finishedJobID]
		c.FinishedJobs[finishedJobID] = j
	}
	for _, updatedJobAllocation := range eo.UpdatedJobAllocations {
		jobID := updatedJobAllocation.GetJobID()
		c.Allocations[jobID] = updatedJobAllocation
	}
	c.ExecutionHistoryManager.Add(eo.GetJobExecutionHistories()...)
	return nil
}

func (c *Context) UpdateJobs(eo *eventobjs.RMUpdateJobsEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if eo.GetCurrentNanoTime() != 0 {
		c.updateCurrentTime(eo.GetCurrentNanoTime())
	}
	for _, job := range eo.GetNewJobs() {
		if _, duplicated := c.UnfinishedJobs[job.GetJobID()]; duplicated {
			reason := fmt.Sprintf("Partition Context ID = [%s] update jobs, add new jobs, encounter duplicated job ID = [%s]", c.Meta.GetPartitionID(), job.GetJobID())
			log.Println(reason)
			return errors.New(reason)
		}
	}
	for _, jobID := range eo.GetRemovedJobIDs() {
		if _, exists := c.UnfinishedJobs[jobID]; !exists {
			reason := fmt.Sprintf("Partition Context ID = [%s] update jobs, remove jobs, encounter unkown job ID = [%s]", c.Meta.GetPartitionID(), jobID)
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
	c.updateCurrentTime(eo.GetCurrentNanoTime())
	return nil
}

func (c *Context) updateCurrentTime(currentNanoTime int64) {
	c.Time = &currentNanoTime
}

func (c *Context) Now() int64 {
	if c.Time == nil {
		return time.Now().UnixNano()
	}
	return *c.Time
}

func (c *Context) Clone() *Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cloned, _ := Build(c.Meta)
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
				Placeholder:                  taskAllocation.GetPlaceholder(),
			})
		}
		cloned.Allocations[jobID] = &objects.JobAllocation{
			JobID:             allocation.GetJobID(),
			ResourceManagerID: allocation.GetResourceManagerID(),
			PartitionID:       allocation.GetPartitionID(),
			TaskAllocations:   allocation.GetTaskAllocations(),
			Extra:             allocation.GetExtra(),
		}
	}
	cloned.UnfinishedJobs = c.UnfinishedJobs
	cloned.FinishedJobs = c.FinishedJobs
	cloned.ExecutionHistoryManager = c.ExecutionHistoryManager.Clone()
	return cloned
}

func (c *Context) GetUnfinishedJob(jobID string) *objects.Job {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnfinishedJobs[jobID]
}

func (c *Context) GetPendingAllocationsSlice() []*objects.JobAllocation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	allocations := make([]*objects.JobAllocation, 0, len(c.Allocations))
	for _, allocation := range c.Allocations {
		allocations = append(allocations, allocation)
	}
	return allocations
}
