package dlt_predictor

//
//import (
//	"UNS/pb_gen/events"
//	"UNS/pb_gen/objects"
//	"UNS/schedulers/partition"
//	"encoding/json"
//	"github.com/golang/protobuf/ptypes/wrappers"
//	"hash/crc32"
//	"testing"
//)
//
//func TestCase5(t *testing.T) {
//	p := NewRandomPredictor(nil)
//	partitionContext, err := partition.Build(&objects.Partition{
//		PartitionID: "PARTITION_ID",
//		Nodes: []*objects.Node{
//			{
//				NodeID: "NODE_1",
//				CPUSockets: []*objects.CPUSocket{
//					{
//						CPUSocketID: "CPUSOCKET_1_1",
//						Accelerators: map[string]*objects.Accelerator{
//							"ACCELERATOR_1_1_1": {
//								AcceleratorID: "ACCELERATOR_1_1_1",
//								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
//									BriefType: "A100",
//								},
//							},
//							"ACCELERATOR_1_1_2": {
//								AcceleratorID: "ACCELERATOR_1_1_2",
//								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
//									BriefType: "A100",
//								},
//							},
//						},
//					},
//					{
//						CPUSocketID: "CPUSOCKET_1_2",
//						Accelerators: []*objects.Accelerator{
//							{
//								AcceleratorID: "ACCELERATOR_1_2_1",
//								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
//									BriefType: "V100",
//								},
//							},
//							{
//								AcceleratorID: "ACCELERATOR_1_2_2",
//								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
//									BriefType: "V100",
//								},
//							},
//						},
//					},
//				},
//			},
//			{
//				NodeID: "NODE_2",
//				CPUSockets: []*objects.CPUSocket{
//					{
//						CPUSocketID: "CPUSOCKET_2_1",
//						Accelerators: []*objects.Accelerator{
//							{
//								AcceleratorID: "ACCELERATOR_2_1_1",
//								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
//									BriefType: "GTX 2080",
//								},
//							},
//						},
//					},
//					{
//						CPUSocketID: "CPUSOCKET_2_2",
//						Accelerators: []*objects.Accelerator{
//							{
//								AcceleratorID: "ACCELERATOR_2_2_1",
//								AcceleratorMetaInfo: &objects.AcceleratorMetaInfo{
//									BriefType: "V100",
//								},
//							},
//						},
//					},
//				},
//			},
//		},
//	})
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	job1TaskGroupDLTExtra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
//	s, err := json.Marshal(job1TaskGroupDLTExtra)
//	if err != nil {
//		panic(err)
//	}
//	job1TaskGroupDLTExtraBytes := s
//	//job1TaskGroupDLTExtraBytes, _ := json.Marshal(job1TaskGroupDLTExtra)
//	job1and4TaskGroupInfo := &objects.TaskGroup_GangTaskGroupInfo{GangTaskGroupInfo: &objects.GangTaskGroup{
//		TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
//		Extra:         job1TaskGroupDLTExtraBytes,
//	}}
//
//	job1 := &objects.Job{
//		JobID:                "JOB_1",
//		JobType:              objects.JobType_jobTypeDLT,
//		SubmitTimeNanoSecond: 1e9,
//		TaskGroup: &objects.TaskGroup{
//			TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
//			Tasks: []*objects.Task{
//				{
//					TaskID: "JOB_1_TASK_1",
//				},
//				{
//					TaskID: "JOB_1_TASK_2",
//				},
//			},
//			TaskGroupInfo: job1and4TaskGroupInfo,
//		},
//	}
//	job2 := &objects.Job{
//		JobID:                "JOB_2",
//		JobType:              objects.JobType_jobTypeDLT,
//		SubmitTimeNanoSecond: 1e9,
//		TaskGroup: &objects.TaskGroup{
//			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
//			Tasks: []*objects.Task{
//				{
//					TaskID: "JOB_2_TASK_1",
//				},
//			},
//			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
//		},
//	}
//	job3 := &objects.Job{
//		JobID:                "JOB_3",
//		JobType:              objects.JobType_jobTypeDLT,
//		SubmitTimeNanoSecond: 1e9,
//		TaskGroup: &objects.TaskGroup{
//			TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
//			Tasks: []*objects.Task{
//				{
//					TaskID: "JOB_3_TASK_1",
//				},
//			},
//			TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
//		},
//	}
//	job4 := &objects.Job{
//		JobID:                "JOB_4",
//		JobType:              objects.JobType_jobTypeDLT,
//		SubmitTimeNanoSecond: 3 * 1e9,
//		TaskGroup: &objects.TaskGroup{
//			TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
//			Tasks: []*objects.Task{
//				{
//					TaskID: "JOB_4_TASK_1",
//				},
//				{
//					TaskID: "JOB_4_TASK_2",
//				},
//			},
//			TaskGroupInfo: job1and4TaskGroupInfo,
//		},
//	}
//
//	_ = partitionContext.UpdateJobs(&events.RMUpdateJobsEvent{
//		NewJobs: []*objects.Job{
//			job1, job2, job3, job4,
//		},
//	})
//	_ = partitionContext.UpdateAllocations(&events.RMUpdateAllocationsEvent{UpdatedJobAllocations: []*objects.JobAllocation{
//		{
//			JobID: job1.GetJobID(),
//			TaskAllocations: []*objects.TaskAllocation{
//				{
//					NodeID:                       "NODE_2",
//					TaskID:                       "JOB_1_TASK_1",
//					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
//					Placeholder:                  true,
//					AcceleratorAllocation: &objects.AcceleratorAllocation{
//						AcceleratorID: "ACCELERATOR_2_1_1",
//					},
//				},
//				{
//					NodeID: "NODE_2",
//					TaskID: "JOB_1_TASK_2",
//					AcceleratorAllocation: &objects.AcceleratorAllocation{
//						AcceleratorID: "ACCELERATOR_2_2_1",
//					},
//					Extra: nil,
//				},
//			},
//		},
//		{
//			JobID: job2.GetJobID(),
//			TaskAllocations: []*objects.TaskAllocation{
//				{
//					NodeID:                       "NODE_1",
//					TaskID:                       "JOB_2_TASK_1",
//					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
//					Placeholder:                  false,
//					AcceleratorAllocation: &objects.AcceleratorAllocation{
//						AcceleratorID: "ACCELERATOR_1_1_1",
//					},
//				},
//			},
//		},
//		{
//			JobID: job3.GetJobID(),
//			TaskAllocations: []*objects.TaskAllocation{
//				{
//					NodeID:                       "NODE_1",
//					TaskID:                       "JOB_3_TASK_1",
//					StartExecutionTimeNanoSecond: &wrappers.Int64Value{Value: 1e9},
//					Placeholder:                  false,
//					AcceleratorAllocation: &objects.AcceleratorAllocation{
//						AcceleratorID: "ACCELERATOR_1_2_2",
//					},
//				},
//			},
//		},
//	}})
//	allocations := make([]*objects.JobAllocation, 0, len(partitionContext.Allocations))
//	for _, allocation := range partitionContext.Allocations {
//		allocations = append(allocations, allocation)
//	}
//
//	for _, allocation := range allocations {
//		t.Logf("job ID = %s, total mini batches = %d, solely mini batch duration second = %f", allocation.GetJobID(), p.getJobTotalMiniBatches(nil, allocation.GetJobID()), float64(p.getMiniBatchDurationNanoSecond(nil, partitionContext.GetUnfinishedJob(allocation.GetJobID()), partitionContext.View.AcceleratorID2Accelerator[allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID()].GetAcceleratorMetaInfo().GetBriefType()))/1e9)
//	}
//	j2j3Shared := p.getSpaceSharingMiniBatchDuration(nil, []*objects.Accelerator{partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_1_2_1"]}, []*objects.Job{job2, job3})
//	fj2j3Shared := make(map[string]float64)
//	for j, t := range j2j3Shared {
//		fj2j3Shared[j] = float64(t) / 1e9
//	}
//	t.Logf("job2, job3 space sharing mini batch duration second = %v", fj2j3Shared)
//
//	j1j4Shared := p.getSpaceSharingMiniBatchDuration(nil, []*objects.Accelerator{partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_1_1_1"], partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_1_1_2"]}, []*objects.Job{job1, job4})
//	fj1j4Shared := make(map[string]float64)
//	for j, t := range j1j4Shared {
//		fj1j4Shared[j] = float64(t) / 1e9
//	}
//	t.Logf("job1, job4 space sharing mini batch duration second = %v", fj1j4Shared)
//
//	// result, err := p.PredictByEndTime(partitionContext, allocations, 2292958*1e7)
//	result, err := p.PredictByEndTime(partitionContext, allocations, 1e15)
//	if err != nil {
//		t.Fatal(err)
//	}
//	for _, allocation := range allocations {
//		each := result.GetResult(p.extractRepresentTaskAllocation(allocation))
//		t.Logf("allocation job ID = %s, startExecutionTime = %f, finishTime = %f", allocation.GetJobID(), float64(*each.GetStartExecutionNanoTime())/1e9, float64(*each.GetFinishNanoTime())/1e9)
//	}
//
//	r := p.getSpaceSharingMiniBatchDuration(nil, []*objects.Accelerator{partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_2_1_1"], partitionContext.View.AcceleratorID2Accelerator["ACCELERATOR_2_2_1"]}, []*objects.Job{job1})
//	t.Log(float64(r["JOB_1"]) / 1e9)
//}
//
//func TestCRC(t *testing.T) {
//	c := crc32.ChecksumIEEE([]byte("GTX aaaa"))
//	t.Log(c)
//	t.Log(float64(c%400) / 100)
//}
