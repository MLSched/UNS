package naive

import (
	"UNS/pb_gen/configs"
	eventsobjs "UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers/impls/base"
	"UNS/schedulers/interfaces"
	"log"
)

type Scheduler struct {
	*base.IntervalSchedulerTemplate

	Config *configs.NaiveSchedulerConfiguration

	pusher base.EventPusher
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base.EventPusher, partitionContextAware base.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.NaiveSchedulerConfiguration)
	sche := &Scheduler{
		Config: c,
	}
	sche.IntervalSchedulerTemplate = base.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventsobjs.SSUpdateAllocationsEvent {
	partitionContext := s.GetPartitionContext().Clone()
	unscheduledJobs := make([]*objects.Job, 0)
	for _, job := range partitionContext.UnfinishedJobs {
		if _, ok := partitionContext.PendingAllocations[job.GetJobID()]; !ok {
			unscheduledJobs = append(unscheduledJobs, job)
		}
	}
	if len(unscheduledJobs) == 0 {
		return nil
	}
	nodesMap := make(map[string]*objects.Node)
	for _, node := range partitionContext.Meta.GetNodes() {
		nodesMap[node.GetNodeID()] = node
	}
	acceleratorID2NodeID := make(map[string]string)
	for nodeID, accelerators := range partitionContext.View.NodeID2Accelerators {
		for _, accelerator := range accelerators {
			acceleratorID2NodeID[accelerator.GetAcceleratorID()] = nodeID
		}
	}

	unoccupiedAcceleratorIDsMap := make(map[string]bool)
	for acceleratorID := range acceleratorID2NodeID {
		unoccupiedAcceleratorIDsMap[acceleratorID] = true
	}
	for _, pendingAllocation := range partitionContext.PendingAllocations {
		for _, taskAllocation := range pendingAllocation.GetTaskAllocations() {
			acceleratorAllocation := taskAllocation.GetAcceleratorAllocation()
			delete(unoccupiedAcceleratorIDsMap, acceleratorAllocation.GetAcceleratorID())
		}
	}

	if len(unoccupiedAcceleratorIDsMap) == 0 {
		return nil
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
		if len(unoccupiedAcceleratorIDsMap) < taskGroupLen {
			break
		}
		chosenAcceleratorIDs := make([]string, 0, taskGroupLen)
		for acceleratorID := range unoccupiedAcceleratorIDsMap {
			chosenAcceleratorIDs = append(chosenAcceleratorIDs, acceleratorID)
			if len(chosenAcceleratorIDs) == taskGroupLen {
				break
			}
		}
		for _, acceleratorID := range chosenAcceleratorIDs {
			delete(unoccupiedAcceleratorIDsMap, acceleratorID)
		}
		taskAllocations := make([]*objects.TaskAllocation, 0, taskGroupLen)
		for i, task := range taskGroup.GetTasks() {
			accID := chosenAcceleratorIDs[i]
			acceleratorAllocation := &objects.AcceleratorAllocation{
				AcceleratorID: accID,
			}
			taskAllocation := &objects.TaskAllocation{
				NodeID:                acceleratorID2NodeID[accID],
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
	}
	return &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: newAllocations}
}