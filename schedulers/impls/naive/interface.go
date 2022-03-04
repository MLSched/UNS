package naive

import (
	"UNS/events"
	"UNS/pb_gen/configs"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers"
	"UNS/schedulers/cluster"
	"UNS/schedulers/partition"
	"encoding/json"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

type Scheduler struct {
	Config *configs.NaiveSchedulerConfiguration

	taskAllocationID int64
	pusher schedulers.EventPusher
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configurationBytes []byte, pusher schedulers.EventPusher) (schedulers.Scheduler, error) {
	c := &configs.NaiveSchedulerConfiguration{}
	err := json.Unmarshal(configurationBytes, c)
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		Config: c,
		pusher: pusher,
		taskAllocationID: 1,
	}, nil
}

func (s *Scheduler) StartService() {
	go func() {
		for {
			s.Schedule()
			time.Sleep(time.Second)
		}
	}()
}

func (s *Scheduler) Schedule() {
	partitionContext := s.GetPartitionContext().Clone()
	unscheduledJobs := make([]*objects.Job, 0)
	for _, job := range partitionContext.UnfinishedJobs {
		if _, ok := partitionContext.PendingAllocations[job.GetJobID()]; !ok {
			unscheduledJobs = append(unscheduledJobs, job)
		}
	}
	if len(unscheduledJobs) == 0 {
		return
	}

	unoccupiedAccelerators := make(map[*objects.Node]map[string]*objects.Accelerator)
	for _, node := range partitionContext.Meta.GetNodes() {
		for _, CPUSocket := range node.GetCPUSockets() {
			accelerators := make(map[string]*objects.Accelerator)
			ls := CPUSocket.GetAccelerators()
			for _, acc := range ls {
				accelerators[acc.GetAcceleratorID()] = acc
			}
		}
	}
	for _, pendingAllocation := range partitionContext.PendingAllocations {
		for _, taskAllocation := range pendingAllocation.GetTaskAllocations() {
			node := taskAllocation.GetNode()
			acceleratorAllocation := taskAllocation.GetAcceleratorAllocation()
			delete(unoccupiedAccelerators[node], acceleratorAllocation.GetAcceleratorID())
		}
	}

	newAllocations := make([]*objects.JobAllocation, 0)
	for _, job := range unscheduledJobs {
		taskGroup := job.GetTaskGroup()
		switch taskGroup.GetTaskGroupType() {
		case objects.TaskGroupType_single, objects.TaskGroupType_gang:
		default:
			log.Printf("Naive Scheduler support task groups [%v, %v], but received task groupd of [%v]", objects.TaskGroupType_single, objects.TaskGroupType_gang, taskGroup.GetTaskGroupType())
			continue
		}
		taskGroupLen := len(taskGroup.GetTasks())
		for node, accelerators := range unoccupiedAccelerators {
			if len(accelerators) < taskGroupLen {
				continue
			}
			chosenAccelerators := make([]*objects.Accelerator, 0, taskGroupLen)
			for _, acc := range accelerators {
				chosenAccelerators = append(chosenAccelerators, acc)
			}
			for _, acc := range chosenAccelerators {
				delete(accelerators, acc.GetAcceleratorID())
			}
			taskAllocations := make([]*objects.TaskAllocation, 0, taskGroupLen)
			for i, task := range taskGroup.GetTasks() {
				acc := chosenAccelerators[i]
				acceleratorAllocation := &objects.AcceleratorAllocation{
					AcceleratorID: acc.GetAcceleratorID(),
				}
				taskAllocation := &objects.TaskAllocation{
					TaskAllocationID:       s.getNextTaskAllocationID(),
					Node:                   node,
					Task:                   task,
					AcceleratorAllocation:  acceleratorAllocation,
					Placeholder:            false,
				}
				taskAllocations = append(taskAllocations, taskAllocation)
			}
			newAllocation := &objects.JobAllocation{
				Job:               job,
				ResourceManagerID: s.Config.GetResourceManagerID(),
				PartitionID:       s.Config.GetPartitionID(),
				TaskAllocations:   taskAllocations,
			}
			newAllocations = append(newAllocations, newAllocation)
			break
		}
	}
	s.pusher(s.GetSchedulerID(), &events.Event{
		Data:       &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: newAllocations},
	})
}

func (s *Scheduler) GetPartitionContext() *partition.Context {
	return cluster.GetManagerInstance().GetPartitionContext(s.Config.GetResourceManagerID(), s.Config.GetPartitionID())
}

func (s *Scheduler) getNextTaskAllocationID() string {
	r := atomic.AddInt64(&s.taskAllocationID, 1)
	return strconv.Itoa(int(r))
}