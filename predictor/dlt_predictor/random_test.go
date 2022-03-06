package dlt_predictor

import (
	"UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers/partition"
	"encoding/json"
	"testing"
)

func TestRandomPredictor(t *testing.T) {
	p := NewRandomPredictor()
	partitionContext := genPartitionContext(t)
	allocations := make([]*objects.JobAllocation, 0, len(partitionContext.PendingAllocations))
	for _, allocation := range partitionContext.PendingAllocations {
		allocations = append(allocations, allocation)
	}
	result, err := p.Predict(partitionContext, allocations)
	if err != nil {
		t.Fatal(err)
	}
	for _, allocation := range allocations {
		each := result.GetResult(allocation)
		t.Logf("allocation job ID = %s, startExecutionTime = %f, finishTime = %f", allocation.GetJobID(), each.GetStartExecutionTime(), each.GetFinishTime())
	}
}

func genPartitionContext(t *testing.T) *partition.Context {
	partitionContext, err := partition.Build(&objects.Partition{
		PartitionID: "PARTITION_ID",
		Nodes: []*objects.Node{
			{
				NodeID: "NODE_1",
				CPUSockets: []*objects.CPUSocket{
					{
						CPUSocketID: "CPUSOCKET_1_1",
						Accelerators: []*objects.Accelerator{
							{
								AcceleratorID: "ACCELERATOR_1_1_1",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "A100",
								},
							},
							{
								AcceleratorID: "ACCELERATOR_1_1_2",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "A100",
								},
							},
						},
					},
					{
						CPUSocketID: "CPUSOCKET_1_2",
						Accelerators: []*objects.Accelerator{
							{
								AcceleratorID: "ACCELERATOR_1_2_1",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "V100",
								},
							},
							{
								AcceleratorID: "ACCELERATOR_1_2_2",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "V100",
								},
							},
						},
					},
				},
			},
			{
				NodeID: "NODE_2",
				CPUSockets: []*objects.CPUSocket{
					{
						CPUSocketID: "CPUSOCKET_2_1",
						Accelerators: []*objects.Accelerator{
							{
								AcceleratorID: "ACCELERATOR_2_1_1",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "GTX 2080",
								},
							},
						},
					},
					{
						CPUSocketID: "CPUSOCKET_2_2",
						Accelerators: []*objects.Accelerator{
							{
								AcceleratorID: "ACCELERATOR_2_2_1",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "V100",
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return partitionContext
}

func injectJobAllocations(t *testing.T, partitionContext *partition.Context) {
	job1TaskGroupDLTExtra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
	job1TaskGroupDLTExtraBytes, _ := json.Marshal(job1TaskGroupDLTExtra)
	job1TaskGroupInfo := &objects.GangTaskGroup{
		TaskGroupType: 0,
		Extra:         job1TaskGroupDLTExtraBytes,
	}
	job1TaskGroupInfoBytes, _ := json.Marshal(job1TaskGroupInfo)
	job1 := &objects.Job{
		JobID:   "JOB_1",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
			Tasks: []*objects.Task{
				{
					TaskID: "JOB_1_TASK_1",
				},
				{
					TaskID: "JOB_1_TASK_2",
				},
			},
			TaskGroupInfoBytes: job1TaskGroupInfoBytes,
		},
	}
	job2 := &objects.Job{
		JobID:   "JOB_2",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "JOB_2_TASK_1",
				},
			},
			TaskGroupInfoBytes: nil,
		},
	}
	job3 := &objects.Job{
		JobID:   "JOB_3",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "JOB_3_TASK_1",
				},
			},
			TaskGroupInfoBytes: nil,
		},
	}

	partitionContext.HandleUpdateJobsEvent(&events.RMUpdateJobsEvent{
		NewJobs: []*objects.Job{
			job1, job2, job3,
		},
	}, nil)
	partitionContext.HandleUpdateAllocationsEvent(&events.RMUpdateAllocationsEvent{JobAllocations: []*objects.JobAllocation{
		{
			JobID:                    job1.GetJobID(),
			StartExecutionTimeSecond: 1,
			Placeholder:              false,
			TaskAllocations: []*objects.TaskAllocation{
				{
					TaskAllocationID: "JOB_1_TASK_ALLOC_1",
					NodeID:           "NODE_1",
					TaskID:           "JOB_1_TASK_1",
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_1",
					},
				},
				{
					TaskAllocationID: "JOB_1_TASK_ALLOC_2",
					NodeID:           "NODE_1",
					TaskID:           "JOB_1_TASK_2",
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_2",
					},
					Extra: nil,
				},
			},
			Finished: false,
		},
		{
			JobID:                    job2.GetJobID(),
			StartExecutionTimeSecond: 2,
			Placeholder:              false,
			TaskAllocations: []*objects.TaskAllocation{
				{
					TaskAllocationID: "JOB_2_TASK_ALLOC_1",
					NodeID:           "NODE_1",
					TaskID:           "JOB_2_TASK_1",
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_2_1",
					},
				},
			},
			Finished: false,
		},
		{
			JobID:                    job3.GetJobID(),
			StartExecutionTimeSecond: 2.5,
			Placeholder:              false,
			TaskAllocations: []*objects.TaskAllocation{
				{
					TaskAllocationID: "JOB_3_TASK_ALLOC_1",
					NodeID:           "NODE_1",
					TaskID:           "JOB_3_TASK_1",
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_2_1",
					},
				},
			},
			Finished: false,
		},
	}}, nil)

}
