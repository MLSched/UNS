package dlt_predictor

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/events"
	"UNS/pb_gen/objects"
	"UNS/schedulers/partition"
	"UNS/utils"
	"encoding/json"
	"github.com/golang/protobuf/ptypes/wrappers"
	"io/ioutil"
	"math/rand"
	"testing"
)

var dataSource = "/Users/purchaser/go/src/UNS/cases/case_test.json"

func TestDataOrientedCase1(t *testing.T) {
	rand.Seed(1)
	p := NewDataOrientedPredictor(&configs.DLTPredictorDataOrientedConfiguration{DataSourcePath: dataSource})
	partitionContext := getPartitionContext(t)

	jobs := getJobs(t)

	job1, job2, job3, job4 := jobs[0], jobs[1], jobs[2], jobs[3]
	_ = partitionContext.UpdateJobs(&events.RMUpdateJobsEvent{
		NewJobs: []*objects.Job{
			job1, job2, job3, job4,
		},
	})
	_ = partitionContext.UpdateAllocations(&events.RMUpdateAllocationsEvent{UpdatedJobAllocations: []*objects.JobAllocation{
		{
			JobID: job1.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       "JOB_1_TASK_1",
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
					Placeholder:                  true,
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_2_1",
					},
				},
				{
					NodeID:                       "NODE_1",
					TaskID:                       "JOB_1_TASK_2",
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_1_2_2",
					},
					Extra: nil,
				},
			},
		},
		{
			JobID: job2.GetJobID(),
			TaskAllocations: []*objects.TaskAllocation{
				{
					NodeID:                       "NODE_1",
					TaskID:                       "JOB_2_TASK_1",
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
					Placeholder:                  false,
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
					NodeID:                       "NODE_2",
					TaskID:                       "JOB_3_TASK_1",
					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
					Placeholder:                  false,
					AcceleratorAllocation: &objects.AcceleratorAllocation{
						AcceleratorID: "ACCELERATOR_2_1_1",
					},
				},
			},
		},
	}})
	allocations := make([]*objects.JobAllocation, 0, len(partitionContext.Allocations))
	for _, allocation := range partitionContext.Allocations {
		allocations = append(allocations, allocation)
	}

	for _, allocation := range allocations {
		t.Logf("job ID = %s, total mini batches = %d, solely mini batch duration second = %f", allocation.GetJobID(), p.getJobTotalMiniBatches(nil, allocation.GetJobID()), float64(p.getMiniBatchDurationNanoSecond(nil, partitionContext.GetUnfinishedJob(allocation.GetJobID()), partitionContext.View.AcceleratorID2Accelerator[allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()].GetAcceleratorMetaInfo().GetBriefType()))/1e9)
	}
	j2j3Shared := p.getSpaceSharingMiniBatchDurationNanoSecond(nil, []*objects.Accelerator{partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_1_2_1"]}, []*objects.Job{job2, job3})
	fj2j3Shared := make(map[string]float64)
	for j, t := range j2j3Shared {
		fj2j3Shared[j] = float64(t) / 1e9
	}
	t.Logf("job2, job3 space sharing mini batch duration second = %v", fj2j3Shared)

	j1j4Shared := p.getSpaceSharingMiniBatchDurationNanoSecond(nil, []*objects.Accelerator{partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_1_1_1"], partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_1_1_2"]}, []*objects.Job{job1, job4})
	fj1j4Shared := make(map[string]float64)
	for j, t := range j1j4Shared {
		fj1j4Shared[j] = float64(t) / 1e9
	}
	t.Logf("job1, job4 space sharing mini batch duration second = %v", fj1j4Shared)

	// result, err := p.PredictByEndTime(partitionContext, allocations, 2292958*1e7)
	result, err := p.PredictByEndTime(partitionContext, allocations, 1e15)
	if err != nil {
		t.Fatal(err)
	}
	for _, allocation := range allocations {
		each, complete := result.GetResult(p.extractRepresentTaskAllocation(allocation))
		t.Logf("allocation job ID = %s, startExecutionTime = %f, finishTime = %f, complete = %v", allocation.GetJobID(), float64(*each.GetStartExecutionNanoTime())/1e9, float64(*each.GetFinishNanoTime())/1e9, complete)
	}

	r := p.getSpaceSharingMiniBatchDurationNanoSecond(nil, []*objects.Accelerator{partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_2_1_1"], partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_2_2_1"]}, []*objects.Job{job1})
	t.Log(float64(r["JOB_1"]) / 1e9)
}

func getJobs(t *testing.T) []*objects.Job {
	job1TaskGroupDLTExtra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
	s, err := json.Marshal(job1TaskGroupDLTExtra)
	if err != nil {
		panic(err)
	}
	job1TaskGroupDLTExtraBytes := s
	job1and4TaskGroupInfo := &objects.TaskGroup_GangTaskGroupInfo{GangTaskGroupInfo: &objects.GangTaskGroup{
		TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
		Extra:         job1TaskGroupDLTExtraBytes,
	}}
	job1 := &objects.Job{
		JobID:                "JOB_1",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 1e9,
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
			TaskGroupInfo: job1and4TaskGroupInfo,
		},
	}
	job2 := &objects.Job{
		JobID:                "JOB_2",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 1e9,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "JOB_2_TASK_1",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	job3 := &objects.Job{
		JobID:                "JOB_3",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 1e9,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
			Tasks: []*objects.Task{
				{
					TaskID: "JOB_3_TASK_1",
				},
			},
			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
		},
	}
	job4 := &objects.Job{
		JobID:                "JOB_4",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 3 * 1e9,
		TaskGroup: &objects.TaskGroup{
			TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
			Tasks: []*objects.Task{
				{
					TaskID: "JOB_4_TASK_1",
				},
				{
					TaskID: "JOB_4_TASK_2",
				},
			},
			TaskGroupInfo: job1and4TaskGroupInfo,
		},
	}
	return []*objects.Job{job1, job2, job3, job4}
}

func getPartitionContext(t *testing.T) *partition.Context {
	GiB := int64(1024 * 1024)
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
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 16 * GiB,
									},
								},
							},
							{
								AcceleratorID: "ACCELERATOR_1_1_2",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "A100",
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 16 * GiB,
									},
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
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 16 * GiB,
									},
								},
							},
							{
								AcceleratorID: "ACCELERATOR_1_2_2",
								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
									BriefType: "V100",
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 16 * GiB,
									},
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
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 16 * GiB,
									},
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
									AcceleratorMemory: &objects.AcceleratorMemory{
										BytesCapacity: 16 * GiB,
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
		AcceleratorType2MiniBatchDurationNanoSecond: map[string]int64{
			"GTX 2080": 1e9,
			"A100":     0.7 * 1e9,
			"V100":     0.5 * 1e9,
		},
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
		AcceleratorType2MiniBatchDurationNanoSecond: map[string]int64{
			"GTX 2080": 2 * 1e9,
			"A100":     1e9,
			"V100":     0.5 * 1e9,
		},
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
		AcceleratorType2MiniBatchDurationNanoSecond: map[string]int64{
			"GTX 2080": 2.7 * 1e9,
			"A100":     1e9,
			"V100":     0.3 * 1e9,
		},
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
		AcceleratorType2MiniBatchDurationNanoSecond: map[string]int64{
			"GTX 2080": 1e9,
			"A100":     0.9 * 1e9,
			"V100":     0.3 * 1e9,
		},
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
