package simulator

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"UNS/utils"
	"encoding/json"
	"io/ioutil"
	"testing"
)

var configPath = "/Users/purchaser/go/src/UNS/simulator/configs/single_partition_dlt_simulator_config.json"

func TestSinglePartitionDLTSimulator(t *testing.T) {
	simulator := NewSinglePartitionDLTSimulator(configPath)
	// schedulerConfigurationBytes := simulator.config.GetRmConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()["SINGLE_PARTITION_DLT_SIMULATOR_PARTITION_ID"].GetConfigurationBytes()
	simulator.StartSimulation()
}

func TestUnmarshalConfiguration(t *testing.T) {
	config := &configs.SinglePartitionDLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	err = utils.Unmarshal(string(bytes), config)
	if err != nil {
		panic(err)
	}
	t.Log(config)
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
	naiveSchedulerConfiguration := &configs.SchedulerConfiguration_NaiveSchedulerConfiguration{
		NaiveSchedulerConfiguration: &configs.NaiveSchedulerConfiguration{
			IntervalNano:      1e18,
			SchedulerID:       "SINGLE_PARTITION_DLT_SIMULATOR_SCHEDULER",
			ResourceManagerID: rmID,
			PartitionID:       partitionID,
			SyncMode:          true,
		},
	}
	schedulerConfiguration := &configs.SchedulerConfiguration{
		SchedulerType: configs.SchedulerType_schedulerTypeNaive,
		Configuration: naiveSchedulerConfiguration,
	}

	schedulersConfiguration := &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
		partitionID: schedulerConfiguration,
	}}
	rmConfig := &configs.RMConfiguration{
		Cluster: &objects.Cluster{
			ResourceManagerID: rmID,
			Description:       "SINGLE_PARTITION_DLT_SIMULATOR_CLUSTER",
			Partitions:        []*objects.Partition{partition},
		},
		SchedulersConfiguration: schedulersConfiguration,
	}

	job1TaskGroupDLTExtra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
	s, err := json.Marshal(job1TaskGroupDLTExtra)
	if err != nil {
		panic(err)
	}
	job1TaskGroupDLTExtraBytes := s
	//job1TaskGroupDLTExtraBytes, _ := json.Marshal(job1TaskGroupDLTExtra)
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
	simulatorConfig := &configs.SinglePartitionDLTSimulatorConfiguration{
		ResourceManagerID: "SINGLE_PARTITION_DLT_SIMULATOR_RESOURCE_MANAGER_ID",
		PartitionID:       "SINGLE_PARTITION_DLT_SIMULATOR_PARTITION_ID",
		RmConfiguration:   rmConfig,
		PredictorConfiguration: &configs.PredictorConfiguration{
			PredictorType: configs.PredictorType_predictorTypeDLTRandom,
			Configuration: &configs.PredictorConfiguration_DLTPredictorRandomConfiguration{DLTPredictorRandomConfiguration: &configs.DLTPredictorRandomConfiguration{}},
		},
		Jobs: []*objects.Job{
			job1, job2, job3, job4,
			//job1, job4,
		},
	}
	str, err := utils.MarshalJsonPB(simulatorConfig)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(configPath, []byte(str), 0666)
	if err != nil {
		panic(err)
	}
}
