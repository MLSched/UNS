package partition

import (
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
	AcceleratorID2NodeID      map[string]string
	AcceleratorID2SocketID    map[string]string
	AcceleratorIDs            []string
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
		AcceleratorID2NodeID:      make(map[string]string),
		AcceleratorID2SocketID:    make(map[string]string),
		AcceleratorIDs:            make([]string, 0),
	}
	for _, node := range c.Meta.GetNodes() {
		c.View.NodeID2Node[node.GetNodeID()] = node
		accelerators := make([]*objects.Accelerator, 0)
		for _, CPUSocket := range node.GetCPUSockets() {
			for _, accelerator := range CPUSocket.GetAccelerators() {
				accelerators = append(accelerators, accelerator)
			}
			//accelerators = append(accelerators, CPUSocket.GetAccelerators()...)
			for _, accelerator := range CPUSocket.GetAccelerators() {
				c.View.AcceleratorID2Accelerator[accelerator.GetAcceleratorID()] = accelerator
				c.View.AcceleratorID2NodeID[accelerator.GetAcceleratorID()] = node.GetNodeID()
				c.View.AcceleratorID2SocketID[accelerator.GetAcceleratorID()] = CPUSocket.GetCPUSocketID()
				c.View.AcceleratorIDs = append(c.View.AcceleratorIDs, accelerator.GetAcceleratorID())
			}
		}
		c.View.NodeID2Accelerators[node.GetNodeID()] = accelerators
	}
	sort.Strings(c.View.AcceleratorIDs)
}

func (c *Context) UpdateAllocations(eo *eventobjs.RMUpdateAllocationsEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateCurrentTime(eo.GetCurrentNanoTime())
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
		delete(c.UnfinishedJobs, finishedJobID)
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
	c.updateCurrentTime(eo.GetCurrentNanoTime())
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
	t := eo.GetCurrentNanoTime()
	c.Time = &t
	return nil
}

func (c *Context) updateCurrentTime(currentNanoTime *wrappers.Int64Value) {
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

func (c *Context) Clone(cloneAllocations bool) *Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
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
			cloned.Allocations[jobID] = &objects.JobAllocation{
				JobID:             allocation.GetJobID(),
				ResourceManagerID: allocation.GetResourceManagerID(),
				PartitionID:       allocation.GetPartitionID(),
				TaskAllocations:   clonedTaskAllocations,
				Extra:             allocation.GetExtra(),
			}
		}
	} else {
		cloned.Allocations = make(map[string]*objects.JobAllocation)
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
	return cloned
}

func (c *Context) GetUnfinishedJob(jobID string) *objects.Job {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnfinishedJobs[jobID]
}

func (c *Context) GetAllocationsSlice() []*objects.JobAllocation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	allocations := make([]*objects.JobAllocation, 0, len(c.Allocations))
	for _, allocation := range c.Allocations {
		allocations = append(allocations, allocation)
	}
	return allocations
}

func (c *Context) GetUnallocatedJobs() map[string]*objects.Job {
	c.mu.RLock()
	defer c.mu.RUnlock()
	jobs := make(map[string]*objects.Job)
	for jobID, job := range c.UnfinishedJobs {
		jobs[jobID] = job
	}
	for jobID := range c.Allocations {
		delete(jobs, jobID)
	}
	return jobs
}

func (c *Context) GetUnallocatedAcceleratorIDs() map[string]bool {
	totalAcceleratorIDs := make(map[string]bool)
	for accID := range c.View.AcceleratorID2Accelerator {
		totalAcceleratorIDs[accID] = true
	}
	for _, jobAllocation := range c.Allocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			delete(totalAcceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
		}
	}
	return totalAcceleratorIDs
}
