package partition

import (
	"UNS/events"
	eventobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"fmt"
	"log"
	"sync"
	"time"
)

type Context struct {
	Meta *objects.Partition
	mu   *sync.RWMutex

	PendingAllocations  map[string]*objects.JobAllocation
	UnfinishedJobs      map[string]*objects.Job
	FinishedAllocations map[string]*objects.JobAllocation

	Time *time.Time
}

func Build(partition *objects.Partition) (*Context, error) {
	return &Context{
		Meta:                partition,
		mu:                  &sync.RWMutex{},
		PendingAllocations:  make(map[string]*objects.JobAllocation),
		UnfinishedJobs:      make(map[string]*objects.Job),
		FinishedAllocations: make(map[string]*objects.JobAllocation),
	}, nil
}

func (c *Context) HandleEvent(event *events.Event) {
	switch eo := event.Data.(type) {
	case *eventobjs.RMUpdateAllocationsEvent:
		c.HandleUpdateAllocationsEvent(eo, event.ResultChan)
	case *eventobjs.RMUpdateJobsEvent:
		c.HandleUpdateJobsEvent(eo, event.ResultChan)
	default:
		log.Printf("Partition Context ID = [%s] received unknown event = [%v]", c.Meta.GetPartitionID(), event.Data)
	}
}

func (c *Context) HandleUpdateAllocationsEvent(eo *eventobjs.RMUpdateAllocationsEvent, resultChan chan *events.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, updatedJobAllocation := range eo.JobAllocations {
		jobID := updatedJobAllocation.GetJob().GetJobID()
		if _, ok := c.UnfinishedJobs[jobID]; !ok {
			reason := fmt.Sprintf("Partition Context ID = [%s] update allocations, encounter unkonwn job ID = [%s]", c.Meta.GetPartitionID(), jobID)
			log.Println(reason)
			resultChan <- &events.Result{
				Succeeded: false,
				Reason:    reason,
			}
		}
	}
	for _, updatedJobAllocation := range eo.JobAllocations {
		jobID := updatedJobAllocation.GetJob().GetJobID()
		c.PendingAllocations[jobID] = updatedJobAllocation
	}
}

func (c *Context) HandleUpdateJobsEvent(eo *eventobjs.RMUpdateJobsEvent, resultChan chan *events.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, job := range eo.GetNewJobs() {
		if _, duplicated := c.UnfinishedJobs[job.GetJobID()]; duplicated {
			reason := fmt.Sprintf("Partition Context ID = [%s] update jobs, add new jobs, encounter duplicated job ID = [%s]", c.Meta.GetPartitionID(), job.GetJobID())
			log.Println(reason)
			events.Reply(resultChan, &events.Result{
				Succeeded: false,
				Reason:    reason,
			})
		}
	}
	for _, jobID := range eo.GetRemovedJobIDs() {
		if _, exists := c.UnfinishedJobs[jobID]; !exists {
			reason := fmt.Sprintf("Partition Context ID = [%s] update jobs, remove jobs, encounter unkown job ID = [%s]", c.Meta.GetPartitionID(), jobID)
			log.Println(reason)
			events.Reply(resultChan, &events.Result{
				Succeeded: false,
				Reason:    reason,
			})
		}
	}
	for _, job := range eo.GetNewJobs() {
		c.UnfinishedJobs[job.GetJobID()] = job
	}
	for _, jobID := range eo.GetRemovedJobIDs() {
		delete(c.UnfinishedJobs, jobID)
		delete(c.PendingAllocations, jobID)
	}
}

func (c *Context) Now() time.Time {
	if c.Time == nil {
		return time.Now()
	}
	return *c.Time
}

func (c *Context) Clone() *Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cloned, _ := Build(c.Meta)
	for jobID, allocation := range c.PendingAllocations {
		clonedTaskAllocations := make([]*objects.TaskAllocation, 0, len(allocation.GetTaskAllocations()))
		for _, taskAllocation := range allocation.GetTaskAllocations() {
			clonedTaskAllocations = append(clonedTaskAllocations, &objects.TaskAllocation{
				TaskAllocationID:     taskAllocation.GetTaskAllocationID(),
				Node:                 taskAllocation.GetNode(),
				Task:                 taskAllocation.GetTask(),
				HostMemoryAllocation: taskAllocation.GetHostMemoryAllocation(),
				CPUSocketAllocations: taskAllocation.GetCPUSocketAllocations(),
				StartTimeStampSecond: taskAllocation.GetStartTimeStampSecond(),
				DurationSecond:       taskAllocation.GetDurationSecond(),
				Placeholder:          taskAllocation.GetPlaceholder(),
				Finished:             taskAllocation.GetFinished(),
				Extra:                taskAllocation.GetExtra(),
			})
		}
		cloned.PendingAllocations[jobID] = &objects.JobAllocation{
			Job:               allocation.GetJob(),
			ResourceManagerID: allocation.GetResourceManagerID(),
			PartitionID:       allocation.GetPartitionID(),
			TaskAllocations:   allocation.GetTaskAllocations(),
			Finished:          allocation.GetFinished(),
			Extra:             allocation.GetExtra(),
		}
	}
	cloned.UnfinishedJobs = c.UnfinishedJobs
	cloned.FinishedAllocations = c.FinishedAllocations
	return cloned
}
