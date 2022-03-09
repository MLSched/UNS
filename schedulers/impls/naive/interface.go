package naive

import (
	"UNS/events"
	"UNS/pb_gen/configs"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers/cluster"
	"UNS/schedulers/impls/base"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"encoding/json"
	"log"
)

type Scheduler struct {
	*base.IntervalSchedulerTemplate

	Config *configs.NaiveSchedulerConfiguration

	pusher interfaces.EventPusher
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func (s *Scheduler) HandleEvent(event *events.Event) {
	s.WaitSchedule()
}

func Build(configurationBytes []byte, pusher interfaces.EventPusher) (interfaces.Scheduler, error) {
	c := &configs.NaiveSchedulerConfiguration{}
	err := json.Unmarshal(configurationBytes, c)
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		IntervalSchedulerTemplate: base.NewIntervalSchedulerTemplate(1e9),
		Config:                    c,
		pusher:                    pusher,
	}, nil
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
			nodeID := taskAllocation.GetNodeID()
			acceleratorAllocation := taskAllocation.GetAcceleratorAllocation()
			delete(unoccupiedAccelerators[partitionContext.View.NodeID2Node[nodeID]], acceleratorAllocation.GetAcceleratorID())
		}
	}

	if len(unoccupiedAccelerators) == 0 {
		return
	}

	newAllocations := make([]*objects.JobAllocation, 0)
	for _, job := range unscheduledJobs {
		taskGroup := job.GetTaskGroup()
		switch taskGroup.GetTaskGroupType() {
		case objects.TaskGroupType_taskGroupTypeSingle, objects.TaskGroupType_taskGroupTypeGang:
		default:
			log.Printf("Naive Scheduler support task groups [%v, %v], but received task groupd of [%v]", objects.TaskGroupType_taskGroupTypeSingle, objects.TaskGroupType_taskGroupTypeGang, taskGroup.GetTaskGroupType())
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
					NodeID:                node.GetNodeID(),
					TaskID:                task.GetTaskID(),
					AcceleratorAllocation: acceleratorAllocation,
				}
				taskAllocations = append(taskAllocations, taskAllocation)
			}
			newAllocation := &objects.JobAllocation{
				JobID:             job.GetJobID(),
				ResourceManagerID: s.Config.GetResourceManagerID(),
				PartitionID:       s.Config.GetPartitionID(),
				TaskAllocations:   taskAllocations,
			}
			newAllocations = append(newAllocations, newAllocation)
			break
		}
	}
	s.pusher(s.GetSchedulerID(), &events.Event{
		Data: &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: newAllocations},
	})
}

func (s *Scheduler) GetPartitionContext() *partition.Context {
	return cluster.GetManagerInstance().GetPartitionContext(s.Config.GetResourceManagerID(), s.Config.GetPartitionID())
}
