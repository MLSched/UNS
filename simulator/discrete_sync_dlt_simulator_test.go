package simulator

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"UNS/utils"
	"encoding/json"
	"io/ioutil"
	"testing"
)

var syncConfigPath = "/Users/purchaser/go/src/UNS/cases/sync_simulator_configuration.json"

var GiB = int64(1024 * 1024 * 1024)

func init() {
	//f, err := os.OpenFile("/Users/purchaser/go/src/UNS/logs/simulator_test.log", os.O_CREATE, 0666)
	//if err != nil {
	//	panic(err)
	//}
	//log.SetOutput(f)
}

func TestSimulator(t *testing.T) {
	simulator := NewDiscreteSyncDLTSimulator(syncConfigPath)
	simulator.StartSimulation()
}

func TestUnmarshalConfiguration(t *testing.T) {
	config := &configs.DLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(syncConfigPath)
	if err != nil {
		panic(err)
	}
	err = utils.Unmarshal(string(bytes), config)
	if err != nil {
		panic(err)
	}
	t.Log(config)
}

func TestSimulatorConfiguration(t *testing.T) {
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
									BriefType: "GTX 2080Ti",
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
	simulatorConfig := &configs.DLTSimulatorConfiguration{
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
	err = ioutil.WriteFile(syncConfigPath, []byte(str), 0666)
	if err != nil {
		panic(err)
	}
}

func TestDataOrientedPredictorSimulator(t *testing.T) {
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
									BriefType: "GTX 2080Ti",
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

	casePath := "/Users/purchaser/go/src/UNS/cases/predictor_data.json"
	bytes, err := ioutil.ReadFile(casePath)
	if err != nil {
		panic(err)
	}
	df := &configs.DLTPredictorDataOrientedDataFormat{}
	err = utils.Unmarshal(string(bytes), df)
	if err != nil {
		panic(err)
	}
	jobs := make([]*objects.Job, 0, len(df.GetJobID2DLTJobData()))
	for _, jobDLTData := range df.GetJobID2DLTJobData() {
		jobs = append(jobs, jobDLTData.GetJob())
	}

	simulatorConfig := &configs.DLTSimulatorConfiguration{
		ResourceManagerID: "SINGLE_PARTITION_DLT_SIMULATOR_RESOURCE_MANAGER_ID",
		PartitionID:       "SINGLE_PARTITION_DLT_SIMULATOR_PARTITION_ID",
		RmConfiguration:   rmConfig,
		PredictorConfiguration: &configs.PredictorConfiguration{
			PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
			Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{DataSourcePath: casePath}},
		},
		Jobs: jobs,
	}
	str, err := utils.MarshalJsonPB(simulatorConfig)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(syncConfigPath, []byte(str), 0666)
	if err != nil {
		panic(err)
	}
}
