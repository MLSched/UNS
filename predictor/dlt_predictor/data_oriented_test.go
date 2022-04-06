package dlt_predictor

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	"UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
	"UNS/utils"
	"github.com/golang/protobuf/ptypes/wrappers"
	"io/ioutil"
	"math/rand"
	"testing"
)

var dataSource = "/Users/purchaser/go/src/UNS/cases/sync_predictor_data.json"
var syncConfigPath = "/Users/purchaser/go/src/UNS/cases/sync_simulator_configuration.json"

func TestDataOrientedCase1(t *testing.T) {
	rand.Seed(1)
	p := NewDataOrientedPredictor(&configs.DLTPredictorDataOrientedConfiguration{DataSourcePath: dataSource})
	partitionContext := getPartitionContext(t)

	jobs := getJobs(t)

	job1, job2, job3, job4, job5, job6 := jobs[0], jobs[1], jobs[2], jobs[3], jobs[4], jobs[5]
	_ = partitionContext.UpdateJobs(&events.RMUpdateJobsEvent{
		NewJobs: []*objects.Job{
			job1, job2, job3, job4, job5, job6,
		},
	})
	_ = partitionContext.UpdateAllocations(&events.RMUpdateAllocationsEvent{UpdatedJobAllocations: []*objects.JobAllocation{
		{
			JobID: job1.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       job1.GetTaskGroup().GetTasks()[0].GetTaskID(),
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 455572514055759},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_1",
					},
				},
			},
		},
		{
			JobID: job2.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       job2.GetTaskGroup().GetTasks()[0].GetTaskID(),
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 683542914715180},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_1",
					},
				},
			},
		},
		{
			JobID: job3.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       job3.GetTaskGroup().GetTasks()[0].GetTaskID(),
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 0},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_1",
					},
				},
			},
		},
		{
			JobID: job4.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       job4.GetTaskGroup().GetTasks()[0].GetTaskID(),
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 543262022306400},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_1",
					},
				},
			},
		},
		{
			JobID: job5.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       job5.GetTaskGroup().GetTasks()[0].GetTaskID(),
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 4910000000000},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_1",
					},
				},
			},
		},
		{
			JobID: job6.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       job6.GetTaskGroup().GetTasks()[0].GetTaskID(),
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 153470000000000},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_1_2",
					},
				},
			},
		},
	}})
	allocations := make([]*pb_gen.JobAllocation, 0, len(partitionContext.Allocations))
	for _, allocation := range partitionContext.Allocations {
		allocations = append(allocations, allocation)
	}

	for _, allocation := range allocations {
		t.Logf("job ID = %s, total mini batches = %d, solely mini batch duration second = %f", allocation.GetJobID(), p.getJobTotalMiniBatches(allocation.GetJobID()), float64(p.getMiniBatchDurationNanoSecond(nil, allocation.GetJobID(), partitionContext.MetalViews.AcceleratorID2Accelerator[allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()].GetAcceleratorMetaInfo().GetBriefType()))/1e9)
	}
	j2j3Shared := p.getSpaceSharingMiniBatchDurationNanoSecond(nil, []*objects.Accelerator{partitionContext.MetalViews.AcceleratorID2Accelerator["ACCELERATOR_1_2_1"]}, []*objects.Job{job2, job3})
	fj2j3Shared := make(map[string]float64)
	for j, t := range j2j3Shared {
		fj2j3Shared[j] = float64(t) / 1e9
	}
	t.Logf("job2, job3 space sharing mini batch duration second = %v", fj2j3Shared)

	j1j4Shared := p.getSpaceSharingMiniBatchDurationNanoSecond(nil, []*objects.Accelerator{partitionContext.MetalViews.AcceleratorID2Accelerator["ACCELERATOR_1_1_1"], partitionContext.MetalViews.AcceleratorID2Accelerator["ACCELERATOR_1_1_2"]}, []*objects.Job{job1, job4})
	fj1j4Shared := make(map[string]float64)
	for j, t := range j1j4Shared {
		fj1j4Shared[j] = float64(t) / 1e9
	}
	t.Logf("job1, job4 space sharing mini batch duration second = %v", fj1j4Shared)

	// result, err := p.PredictByEndTime(partitionContext, allocations, 2292958*1e7)
	result, err := p.Predict(partitionContext, allocations)
	if err != nil {
		t.Fatal(err)
	}
	for _, allocation := range allocations {
		each := result.GetResult(p.extractRepresentTaskAllocation(allocation))
		t.Logf("allocation job ID = %s, startExecutionTime = %f, finishTime = %f", allocation.GetJobID(), float64(*each.GetStartExecutionNanoTime())/1e9, float64(*each.GetFinishNanoTime())/1e9)
	}

	r := p.getSpaceSharingMiniBatchDurationNanoSecond(nil, []*objects.Accelerator{partitionContext.MetalViews.AcceleratorID2Accelerator["ACCELERATOR_2_1_1"], partitionContext.MetalViews.AcceleratorID2Accelerator["ACCELERATOR_2_2_1"]}, []*objects.Job{job1})
	t.Log(float64(r["JOB_1"]) / 1e9)

	result.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		s, _ := utils.MarshalJsonPB(allocation)
		t.Log(s)
		t.Log(*result.GetStartExecutionNanoTime())
		t.Log(*result.GetFinishNanoTime())
	})

}

func getJobs(t *testing.T) []*objects.Job {
	//job1TaskGroupDLTExtra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
	//s, err := json.Marshal(job1TaskGroupDLTExtra)
	//if err != nil {
	//	panic(err)
	//}
	//job1TaskGroupDLTExtraBytes := s
	//job1and4TaskGroupInfo := &objects.TaskGroup_GangTaskGroupInfo{GangTaskGroupInfo: &objects.GangTaskGroup{
	//	TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
	//	Extra:         job1TaskGroupDLTExtraBytes,
	//}}
	job1 := &objects.Job{
		JobID:   "0763f5964484b5acffb99e89",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "0763f5964484b5acffb99e89_TASK_0",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	job2 := &objects.Job{
		JobID:   "27d29764ca70365e92cb1ccb",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "27d29764ca70365e92cb1ccb_TASK_0",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	job3 := &objects.Job{
		JobID:   "81749c2e708fc2cd18918846",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "81749c2e708fc2cd18918846_TASK_0",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	job4 := &objects.Job{
		JobID:   "10ff84e15b2c42a736f4e3e5",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "10ff84e15b2c42a736f4e3e5_TASK_0",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	job5 := &objects.Job{
		JobID:   "6d942b15622d001d9edacb8e",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "6d942b15622d001d9edacb8e_TASK_0",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}

	job6 := &objects.Job{
		JobID:   "51d01202d231f9fae85bceab",
		JobType: objects.JobType_jobTypeDLT,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "51d01202d231f9fae85bceab_TASK_0",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	return []*objects.Job{job1, job2, job3, job4, job5, job6}
}

func getPartitionContext(t *testing.T) *partition.Context {
	partitionContext, err := partition.Build(&objects.Partition{
		PartitionID: "PARTITION_ID",
		Nodes: []*objects.Node{
			{
				NodeID: "NODE_1",
				CPUSockets: []*objects.CPUSocket{
					{
						CPUSocketID: "CPUSOCKET_1_1",
						Accelerators: map[string]*objects.Accelerator{
							"ACCELERATOR_1_1_1": {
								AcceleratorID: "ACCELERATOR_1_1_1",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "GTX 2080Ti",
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 17179869184,
									},
								},
							},
						},
					},
					{
						CPUSocketID: "CPUSOCKET_1_2",
						Accelerators: map[string]*objects.Accelerator{
							"ACCELERATOR_1_1_2": {
								AcceleratorID: "ACCELERATOR_1_1_2",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "GTX 2080Ti",
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 17179869184,
									},
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

func TestDataOrientedPredictorDataFormat(t *testing.T) {
	df := &configs.DLTPredictorDataOrientedDataFormat{
		JobID2DLTJobData: make(map[string]*configs.DLTJobData),
	}

	jobs := getJobs(t)

	job1DLTData := &configs.DLTJobData{
		Job:              jobs[0],
		TotalMiniBatches: 1000,
		AcceleratorType2MiniBatchDuration: &configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{AccType2Duration: map[string]int64{
			"GTX 2080": 1e9,
			"A100":     0.7 * 1e9,
			"V100":     0.5 * 1e9,
		}},
		MinSpaceSharingPenalty: 1.1,
		MaxSpaceSharingPenalty: 3.,
		ConsolidationLevel2Penalties: map[int64]float32{
			int64(configs.ConsolidationLevel_SameCPUSocket): 1.2,
			int64(configs.ConsolidationLevel_DiffCPUSocket): 1.5,
			int64(configs.ConsolidationLevel_DiffNode):      1.7,
		},
		MaximumAcceleratorMemoryCostBytes: 4 * 1024 * 1024,
	}

	job2DLTData := &configs.DLTJobData{
		Job:              jobs[1],
		TotalMiniBatches: 1000,
		AcceleratorType2MiniBatchDuration: &configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{AccType2Duration: map[string]int64{
			"GTX 2080": 2 * 1e9,
			"A100":     1e9,
			"V100":     0.5 * 1e9,
		}},
		MinSpaceSharingPenalty: 1.1,
		MaxSpaceSharingPenalty: 3.,
		ConsolidationLevel2Penalties: map[int64]float32{
			int64(configs.ConsolidationLevel_SameCPUSocket): 1.2,
			int64(configs.ConsolidationLevel_DiffCPUSocket): 1.5,
			int64(configs.ConsolidationLevel_DiffNode):      1.7,
		},
		MaximumAcceleratorMemoryCostBytes: 2 * 1024 * 1024,
	}

	job3DLTData := &configs.DLTJobData{
		Job:              jobs[2],
		TotalMiniBatches: 1000,
		AcceleratorType2MiniBatchDuration: &configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{AccType2Duration: map[string]int64{
			"GTX 2080": 2.7 * 1e9,
			"A100":     1e9,
			"V100":     0.3 * 1e9,
		}},
		MinSpaceSharingPenalty: 1.1,
		MaxSpaceSharingPenalty: 3.,
		ConsolidationLevel2Penalties: map[int64]float32{
			int64(configs.ConsolidationLevel_SameCPUSocket): 1.2,
			int64(configs.ConsolidationLevel_DiffCPUSocket): 1.5,
			int64(configs.ConsolidationLevel_DiffNode):      1.7,
		},
		MaximumAcceleratorMemoryCostBytes: 6 * 1024 * 1024,
	}

	job4DLTData := &configs.DLTJobData{
		Job:              jobs[3],
		TotalMiniBatches: 1000,
		AcceleratorType2MiniBatchDuration: &configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{AccType2Duration: map[string]int64{
			"GTX 2080": 1e9,
			"A100":     0.9 * 1e9,
			"V100":     0.3 * 1e9,
		}},
		MinSpaceSharingPenalty: 1.1,
		MaxSpaceSharingPenalty: 3.,
		ConsolidationLevel2Penalties: map[int64]float32{
			int64(configs.ConsolidationLevel_SameCPUSocket): 1.2,
			int64(configs.ConsolidationLevel_DiffCPUSocket): 1.5,
			int64(configs.ConsolidationLevel_DiffNode):      1.7,
		},
		MaximumAcceleratorMemoryCostBytes: 7 * 1024 * 1024,
	}

	DLTDatas := []*configs.DLTJobData{job1DLTData, job2DLTData, job3DLTData, job4DLTData}
	for _, d := range DLTDatas {
		df.JobID2DLTJobData[d.GetJob().GetJobID()] = d
	}

	bytes, err := utils.MarshalJsonPB(df)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(dataSource, []byte(bytes), 0666)
	if err != nil {
		t.Fatal(err)
	}
}
