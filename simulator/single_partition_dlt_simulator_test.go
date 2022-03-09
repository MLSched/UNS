package simulator

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
)

var configPath = "/Users/purchaser/go/src/UNS/simulator/configs/single_partition_dlt_simulator_config.json"

func TestSinglePartitionDLTSimulator(t *testing.T) {
	simulator := NewSinglePartitionDLTSimulator(configPath)
	schedulerConfigurationBytes := simulator.config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["SINGLE_PARTITION_DLT_SIMULATOR_PARTITION_ID"].GetConfigurationBytes()
	schedulerConfiguration := &configs.NaiveSchedulerConfiguration{}
	err := json.Unmarshal(schedulerConfigurationBytes, schedulerConfiguration)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", schedulerConfiguration)
	bytes, err := json.MarshalIndent(simulator.config, "", "\t")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(bytes))
}

func TestSinglePartitionDLTSimulatorConfiguration(t *testing.T) {
	rmID := "SINGLE_PARTITION_DLT_SIMULATOR_RESOURCE_MANAGER_ID"
	partitionID := "SINGLE_PARTITION_DLT_SIMULATOR_PARTITION_ID"
	partition := &objects.Partition{
		PartitionID: partitionID,
		Description: "SINGLE_PARTITION_DLT_SIMULATOR_PARTITION",
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
	}
	schedulerConfiguration := &configs.NaiveSchedulerConfiguration{
		IntervalNano:      10 * 1e9,
		SchedulerID:       "SINGLE_PARTITION_DLT_SIMULATOR_SCHEDULER",
		ResourceManagerID: rmID,
		PartitionID:       partitionID,
	}
	schedulerConfigurationBytes := mustMarshal(schedulerConfiguration)

	schedulersConfiguration := &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
		partitionID: {
			SchedulerType:      configs.SchedulerType_schedulerTypeNaive,
			ConfigurationBytes: schedulerConfigurationBytes,
		},
	}}
	rmConfig := &configs.RMConfiguration{
		Cluster: &objects.Cluster{
			ResourceManagerID: rmID,
			Description:       "SINGLE_PARTITION_DLT_SIMULATOR_CLUSTER",
			Partitions:        []*objects.Partition{partition},
		},
		SchedulersConfiguration: schedulersConfiguration,
	}
	predictorConfiguration := &configs.DLTPredictorRandomConfiguration{}
	predictorConfigurationBytes := mustMarshal(predictorConfiguration)

	job1TaskGroupDLTExtra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
	job1TaskGroupDLTExtraBytes, _ := json.Marshal(job1TaskGroupDLTExtra)
	job1TaskGroupInfo := &objects.GangTaskGroup{
		TaskGroupType: 0,
		Extra:         job1TaskGroupDLTExtraBytes,
	}
	job1TaskGroupInfoBytes, _ := json.Marshal(job1TaskGroupInfo)
	job4TaskGroupInfoBytes, _ := json.Marshal(job1TaskGroupInfo)

	job1 := &objects.Job{
		JobID:                "JOB_1",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 0,
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
		JobID:                "JOB_2",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 0,
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
		JobID:                "JOB_3",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 0,
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
	job4 := &objects.Job{
		JobID:                "JOB_4",
		JobType:              objects.JobType_jobTypeDLT,
		SubmitTimeNanoSecond: 0,
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
			TaskGroupInfoBytes: job4TaskGroupInfoBytes,
		},
	}
	simulatorConfig := &configs.SinglePartitionDLTSimulatorConfiguration{
		ResourceManagerID: "SINGLE_PARTITION_DLT_SIMULATOR_RESOURCE_MANAGER_ID",
		PartitionID:       "SINGLE_PARTITION_DLT_SIMULATOR_PARTITION_ID",
		RmConfiguration:   rmConfig,
		PredictorConfiguration: &configs.PredictorConfiguration{
			PredictorType:      configs.PredictorType_predictorTypeDLTRandom,
			ConfigurationBytes: predictorConfigurationBytes,
		},
		Jobs: []*objects.Job{
			job1, job2, job3, job4,
		},
	}
	bytes, err := json.MarshalIndent(simulatorConfig, "", "\t")
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(configPath, bytes, 0666)
	if err != nil {
		panic(err)
	}
}

func mustMarshal(i interface{}) []byte {
	bytes, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return bytes
}
