package main

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"UNS/utils"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"hash/crc32"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
)

var resourceManagerID = "RESOURCE_MANAGER_ID"
var partitionID = "PARTITION_ID"
var schedulerID = "SCHEDULER_ID"

var dataDir = "/Users/purchaser/datasets/ali-cluster/cluster-trace-gpu-v2020/data"

//var predictorDataPath = "/Users/purchaser/go/src/UNS/cases/async_predictor_data.json"
//var simulatorConfigurationPath = "/Users/purchaser/go/src/UNS/cases/async_simulator_configuration.json"

var predictorDataPath = "/Users/purchaser/go/src/UNS/cases/sync_predictor_data.json"
var simulatorConfigurationPath = "/Users/purchaser/go/src/UNS/cases/sync_simulator_configuration.json"

//var predictorDataPath = "/Users/purchaser/go/src/UNS/cases/sync_predictor_data_test.json"
//var simulatorConfigurationPath = "/Users/purchaser/go/src/UNS/cases/sync_simulator_configuration_test.json"

var gpuTypes = []string{A100, V100, GTX2080Ti}

var jobCount = 100
var miniBatchDurationNanoSecondDistribution = []int{0.1 * 1e9, 3 * 1e9}
var BaseGPU = A100
var GPUEfficiencyRatio = map[string][]float64{
	V100:      {1.5, 2.1},
	GTX2080Ti: {2.2, 2.9},
}
var minSpaceSharingPenaltyDistribution = []float64{1, 1.3}
var maxSpaceSharingPenaltyDistribution = []float64{1.5, 4}

//var submitTimeScaleFactor = float64(0.001)

//var submitTimeScaleFactor = float64(10)
//var submitTimeScaleFactor = float64(5)

//var submitTimeScaleFactor = float64(0)

var submitTimeScaleFactor = float64(5)

//var jobExecutionTimeScaleFactor = float64(0.00001)

var jobExecutionTimeScaleFactor = float64(1)

//var syncMode = false
//
var syncMode = true

var deadlineProb = float64(0.3)
var deadlineDistribution = []float64{1.2, 2}

var onlySingleTaskJob = false

const (
	A100      = "A100"
	V100      = "V100"
	GTX2080Ti = "GTX 2080Ti"

	GiB = 1024 * 1024 * 1024
)

var GPUType2Meta = map[string]*objects.AcceleratorMetaInfo{
	GTX2080Ti: {
		BriefType: GTX2080Ti,
		AcceleratorMemory: &objects.AcceleratorMemory{
			BytesCapacity: 16 * GiB,
		},
	},
	V100: {
		BriefType: V100,
		AcceleratorMemory: &objects.AcceleratorMemory{
			BytesCapacity: 32 * GiB,
		},
	},
	A100: {
		BriefType: A100,
		AcceleratorMemory: &objects.AcceleratorMemory{
			BytesCapacity: 40 * GiB,
		},
	},
}

var consolidationLevel2PenaltyDistributions = map[configs.ConsolidationLevel][]float64{
	configs.ConsolidationLevel_NVLink:        {1, 1.05},
	configs.ConsolidationLevel_SameCPUSocket: {1, 1.1},
	configs.ConsolidationLevel_DiffCPUSocket: {1.1, 1.3},
	configs.ConsolidationLevel_DiffNode:      {1.1, 1.6},
}
var consolidationLevels = []configs.ConsolidationLevel{configs.ConsolidationLevel_NVLink, configs.ConsolidationLevel_SameCPUSocket, configs.ConsolidationLevel_DiffCPUSocket, configs.ConsolidationLevel_DiffNode}

var gpuMemoryCostDistributions = []int64{int64(0.5 * float64(GiB)), int64(16 * GiB)}

var instance2Count = map[*Instance]int64{
	//NewInstance(map[int64][]string{
	//	0: {GTX2080Ti},
	//}): 4,
	NewInstance(map[int64][]string{
		0: {V100, V100},
	}): 2,
	NewInstance(map[int64][]string{
		0: {A100, A100},
	}): 2,
	NewInstance(map[int64][]string{
		0: {GTX2080Ti, GTX2080Ti},
	}): 2,
	NewInstance(map[int64][]string{
		0: {GTX2080Ti, GTX2080Ti},
		1: {GTX2080Ti, GTX2080Ti},
	}): 2,
	NewInstance(map[int64][]string{
		0: {V100, V100},
		1: {V100, V100},
	}): 2,
	NewInstance(map[int64][]string{
		0: {V100, V100, A100, A100},
	}): 2,
	NewInstance(map[int64][]string{
		0: {A100, A100},
		1: {A100, A100},
	}): 2,
	//NewInstance(map[int64][]string{
	//	0: {A100, A100, A100, A100},
	//}): 1,
	NewInstance(map[int64][]string{
		0: {A100, A100, A100, A100},
	}): 2,
}

var naiveSchedulerConfiguration = &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
	partitionID: {
		SchedulerType: configs.SchedulerType_schedulerTypeNaive,
		Configuration: &configs.SchedulerConfiguration_NaiveSchedulerConfiguration{NaiveSchedulerConfiguration: &configs.NaiveSchedulerConfiguration{
			SchedulerID:       schedulerID,
			ResourceManagerID: resourceManagerID,
			PartitionID:       partitionID,
			IntervalNano:      1e16,
			SyncMode:          syncMode,
			PredictorConfiguration: &configs.PredictorConfiguration{
				PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
				Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{
					DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{
						DataSourcePath: predictorDataPath,
					},
				},
			},
			NonSpaceSharing: false,
		}},
	},
}}

var hydraSchedulerConfiguration = &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
	partitionID: {
		SchedulerType: configs.SchedulerType_schedulerTypeHydra,
		Configuration: &configs.SchedulerConfiguration_HydraSchedulerConfiguration{HydraSchedulerConfiguration: &configs.HydraSchedulerConfiguration{
			SchedulerID:       schedulerID,
			ResourceManagerID: resourceManagerID,
			PartitionID:       partitionID,
			IntervalNano:      1e16,
			SyncMode:          syncMode,
			PredictorConfiguration: &configs.PredictorConfiguration{
				PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
				Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{
					DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{
						DataSourcePath: predictorDataPath,
					},
				},
			},
			NonSpaceSharing: false,
		}},
	},
}}

var sjfSchedulerConfiguration = &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
	partitionID: {
		SchedulerType: configs.SchedulerType_schedulerTypeSJF,
		Configuration: &configs.SchedulerConfiguration_SjfSchedulerConfiguration{SjfSchedulerConfiguration: &configs.SJFSchedulerConfiguration{
			SchedulerID:       schedulerID,
			ResourceManagerID: resourceManagerID,
			PartitionID:       partitionID,
			IntervalNano:      1e16,
			SyncMode:          syncMode,
			PredictorConfiguration: &configs.PredictorConfiguration{
				PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
				Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{
					DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{
						DataSourcePath: predictorDataPath,
					},
				},
			},
		}},
	},
}}

var edfSchedulerConfiguration = &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
	partitionID: {
		SchedulerType: configs.SchedulerType_schedulerTypeEDF,
		Configuration: &configs.SchedulerConfiguration_EdfSchedulerConfiguration{EdfSchedulerConfiguration: &configs.EDFSchedulerConfiguration{
			SchedulerID:       schedulerID,
			ResourceManagerID: resourceManagerID,
			PartitionID:       partitionID,
			IntervalNano:      1e16,
			SyncMode:          syncMode,
			PredictorConfiguration: &configs.PredictorConfiguration{
				PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
				Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{
					DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{
						DataSourcePath: predictorDataPath,
					},
				},
			},
		}},
	},
}}

var unsSchedulerConfiguration = &configs.SchedulersConfiguration{PartitionID2SchedulerConfiguration: map[string]*configs.SchedulerConfiguration{
	partitionID: {
		SchedulerType: configs.SchedulerType_schedulerTypeUNS,
		Configuration: &configs.SchedulerConfiguration_UnsSchedulerConfiguration{UnsSchedulerConfiguration: &configs.UNSSchedulerConfiguration{
			SchedulerID:       schedulerID,
			ResourceManagerID: resourceManagerID,
			PartitionID:       partitionID,
			IntervalNano:      1e16,
			SyncMode:          syncMode,
			PredictorConfiguration: &configs.PredictorConfiguration{
				PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
				Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{
					DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{
						DataSourcePath: predictorDataPath,
					},
				},
			},
			NonSpaceSharing: false,
		}},
	},
}}

//var schedulerConfiguration = naiveSchedulerConfiguration

var schedulerConfiguration = unsSchedulerConfiguration

//var schedulerConfiguration = hydraSchedulerConfiguration

//var schedulerConfiguration = sjfSchedulerConfiguration

//var schedulerConfiguration = edfSchedulerConfiguration

func init() {
	rand.Seed(3)
}

func main() {
	generator := &CaseGenerator{}
	jobHeader, jobRecords := generator.ReadTable("pai_job_table.header", "pai_job_table.csv")
	taskHeader, taskRecords := generator.ReadTable("pai_task_table.header", "pai_task_table.csv")
	taskHeader, taskRecords = generator.PreprocessTaskTable(taskHeader, taskRecords)
	mergedHeader, mergedRecords := generator.MergeTaskAndJob(taskHeader, taskRecords, jobHeader, jobRecords)
	jobID2DLTJobData, jobs := generator.GenerateJobsData(mergedHeader, mergedRecords)
	predictorData := &configs.DLTPredictorDataOrientedDataFormat{JobID2DLTJobData: jobID2DLTJobData}
	cluster := generator.GenerateCluster()

	schedulersConfiguration := schedulerConfiguration

	simulatorConfiguration := &configs.DLTSimulatorConfiguration{
		ResourceManagerID: resourceManagerID,
		PartitionID:       partitionID,
		RmConfiguration: &configs.RMConfiguration{
			Cluster:                 cluster,
			SchedulersConfiguration: schedulersConfiguration,
		},
		PredictorConfiguration: &configs.PredictorConfiguration{
			PredictorType: configs.PredictorType_predictorTypeDLTDataOriented,
			Configuration: &configs.PredictorConfiguration_DLTPredictorDataOrientedConfiguration{
				DLTPredictorDataOrientedConfiguration: &configs.DLTPredictorDataOrientedConfiguration{
					DataSourcePath: predictorDataPath,
				},
			},
		},
		Jobs: jobs,
	}

	write(predictorDataPath, predictorData)
	write(simulatorConfigurationPath, simulatorConfiguration)
}

func write(path string, message proto.Message) {
	s, err := utils.MarshalJsonPB(message)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(path, []byte(s), 0666)
	if err != nil {
		panic(err)
	}
}

type CaseGenerator struct {
}

func (g *CaseGenerator) ReadHeader(headerFileName string) []string {
	headerPath := path.Join(dataDir, headerFileName)
	tt, err := os.Open(headerPath)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(tt)
	header, err := reader.Read()
	if err != nil {
		panic(err)
	}
	return header
}

func (g *CaseGenerator) ReadTable(headerFileName string, dataFilename string) ([]string, [][]string) {
	header := g.ReadHeader(headerFileName)
	dataPath := path.Join(dataDir, dataFilename)
	tt, err := os.Open(dataPath)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(tt)
	var records [][]string
	records, err = reader.ReadAll()
	if err != nil {
		panic(err)
	}
	log.Printf("ReadTable headerFileName %s, dataFileName %s, records len = %d\n", headerFileName, dataFilename, len(records))
	return header, records
}

func (g *CaseGenerator) PreprocessTaskTable(header []string, records [][]string) ([]string, [][]string) {
	taskNameIdx := utils.IndexOf(header, "task_name")
	statusIdx := utils.IndexOf(header, "status")
	gpuTypeIdx := utils.IndexOf(header, "gpu_type")
	planGPUIdx := utils.IndexOf(header, "plan_gpu")
	startTimeIdx := utils.IndexOf(header, "start_time")
	endTimeIdx := utils.IndexOf(header, "end_time")
	filterTaskName := func(record []string) bool {
		return -1 == utils.IndexOf([]string{"tensorflow", "PyTorchWorker", "worker"}, record[taskNameIdx])
	}
	filterStatus := func(record []string) bool {
		return record[statusIdx] != "Terminated"
	}
	filterGPU := func(record []string) bool {
		gpuType := record[gpuTypeIdx]
		if -1 == utils.IndexOf([]string{"T4", "V100", "V100M32", "A100", "MISC"}, gpuType) {
			return true
		}
		if gpuType == "T4" || gpuType == "MISC" {
			record[gpuTypeIdx] = "GTX 2080Ti"
		}
		if gpuType == "V100M32" {
			record[gpuTypeIdx] = "V100"
		}
		if gpuType == "MISC" {
			c := int(crc32.ChecksumIEEE([]byte(record[0])))
			record[gpuTypeIdx] = gpuTypes[c%len(gpuTypes)]
		}
		return false
	}
	filterPlanGPU := func(record []string) bool {
		planGPU := record[planGPUIdx]
		planGPUFloat, err := strconv.ParseFloat(planGPU, 64)
		if err != nil {
			return true
		}
		planGPUInt64 := int64(planGPUFloat)
		if planGPUInt64%100 != 0 {
			planGPUInt64 = planGPUInt64/100 + 1
		} else {
			planGPUInt64 = planGPUInt64 / 100
		}
		if onlySingleTaskJob && planGPUInt64 > 1 {
			return true
		}
		record[planGPUIdx] = strconv.FormatInt(planGPUInt64, 10)
		return false
	}
	filterStartEndTime := func(record []string) bool {
		startTime, err := strconv.ParseFloat(record[startTimeIdx], 64)
		if err != nil {
			return true
		}
		endTime, err := strconv.ParseFloat(record[endTimeIdx], 64)
		if err != nil {
			return true
		}
		if startTime == 0. || endTime == 0. || startTime > endTime {
			return true
		}
		return false
	}
	filters := map[string]func(record []string) bool{
		"filterTaskName":     filterTaskName,
		"filterStatus":       filterStatus,
		"filterGPU":          filterGPU,
		"filterPlanGPU":      filterPlanGPU,
		"filterStartEndTime": filterStartEndTime,
	}
	resultRecords := make([][]string, 0, len(records))
	filterCount := make(map[string]int)
	filterNames := make([]string, 0, len(filters))
	for filterName := range filters {
		filterCount[filterName] = 0
		filterNames = append(filterNames, filterName)
	}
nextRecord:
	for _, record := range records {
		for _, filterName := range filterNames {
			filter := filters[filterName]
			if filter(record) {
				filterCount[filterName] += 1
				continue nextRecord
			}
		}
		resultRecords = append(resultRecords, record)
	}
	log.Printf("PreprocessTaskTable, after filter, task table records len = %d, filterCount = %+v", len(resultRecords), filterCount)
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(resultRecords)
		},
		LessFunc: func(i, j int) bool {
			return resultRecords[i][startTimeIdx] < resultRecords[j][startTimeIdx]
		},
		SwapFunc: func(i, j int) {
			t := resultRecords[i]
			resultRecords[i] = resultRecords[j]
			resultRecords[j] = t
		},
	}
	sort.Stable(sorter)
	return header, resultRecords
}

func (g *CaseGenerator) MergeTaskAndJob(taskHeader []string, taskRecords [][]string, jobHeader []string, jobRecords [][]string) ([]string, [][]string) {
	jobNameCol := "job_name"
	colTaskHeaderIdx := utils.IndexOf(taskHeader, jobNameCol)
	colJobHeaderIdx := utils.IndexOf(jobHeader, jobNameCol)
	if colTaskHeaderIdx == -1 || colJobHeaderIdx == -1 {
		panic("MergeTaskAndJob onCol jobNameCol not exists.")
	}
	mergedHeader := []string{
		"jobID",
		"startTime",
		"endTime",
		"GPUCount",
		"GPUType",
		"user",
	}
	taskHeaderJobNameIdx := utils.IndexOf(taskHeader, "job_name")
	taskHeaderStartTimeIdx := utils.IndexOf(taskHeader, "start_time")
	taskHeaderEndTimeIdx := utils.IndexOf(taskHeader, "end_time")
	taskHeaderPlanGPUIdx := utils.IndexOf(taskHeader, "plan_gpu")
	taskHeaderGPUTypeIdx := utils.IndexOf(taskHeader, "gpu_type")

	jobHeaderJobNameIdx := utils.IndexOf(taskHeader, "job_name")
	jobHeaderUserIdx := utils.IndexOf(jobHeader, "user")

	jobName2JobRecord := make(map[string][]string)
	for _, jobRecord := range jobRecords {
		jobName2JobRecord[jobRecord[jobHeaderJobNameIdx]] = jobRecord
	}
	mergedRecords := make([][]string, 0, len(taskRecords))
	for _, taskRecord := range taskRecords {
		jobName := taskRecord[taskHeaderJobNameIdx]
		if _, ok := jobName2JobRecord[jobName]; !ok {
			continue
		}
		jobRecord := jobName2JobRecord[jobName]
		mergedRecords = append(mergedRecords, []string{
			jobName,
			taskRecord[taskHeaderStartTimeIdx],
			taskRecord[taskHeaderEndTimeIdx],
			taskRecord[taskHeaderPlanGPUIdx],
			taskRecord[taskHeaderGPUTypeIdx],
			jobRecord[jobHeaderUserIdx],
		})
	}
	return mergedHeader, mergedRecords
}

func (g *CaseGenerator) GenerateJobsData(mergedHeader []string, mergedRecords [][]string) (map[string]*configs.DLTJobData, []*objects.Job) {
	rand.Seed(1)
	for i := 0; i < 10; i++ {
		log.Println(rand.Float64())
	}
	mergedRecords = mergedRecords[:jobCount]
	//mergedHeader := []string{
	//	"jobID",
	//	"startTime",
	//	"endTime",
	//	"GPUCount",
	//	"GPUType",
	//	"user",
	//}
	jobIDIdx := utils.IndexOf(mergedHeader, "jobID")
	startTimeIdx := utils.IndexOf(mergedHeader, "startTime")
	endTimeIdx := utils.IndexOf(mergedHeader, "endTime")
	GPUCountIdx := utils.IndexOf(mergedHeader, "GPUCount")
	GPUTypeIdx := utils.IndexOf(mergedHeader, "GPUType")
	userIDIdx := utils.IndexOf(mergedHeader, "user")

	generatorJob := func(record []string) *configs.DLTJobData {
		jobID := record[jobIDIdx]
		startTimeSecond, _ := strconv.ParseFloat(record[startTimeIdx], 64)
		endTimeSecond, _ := strconv.ParseFloat(record[endTimeIdx], 64)
		executionDurationSecond := jobExecutionTimeScaleFactor * (endTimeSecond - startTimeSecond)
		min := float64(miniBatchDurationNanoSecondDistribution[0])
		max := float64(miniBatchDurationNanoSecondDistribution[1])
		miniBatchDurationNanoSecond := int64(g.randomUniform([]float64{min, max}))
		log.Println(miniBatchDurationNanoSecond)
		totalMiniBatches := int64(executionDurationSecond * 1e9 / float64(miniBatchDurationNanoSecond))
		if totalMiniBatches < 10 {
			totalMiniBatches = 10
		}
		submitTimeNanoSecond := int64(submitTimeScaleFactor * startTimeSecond * 1e9)
		executionDurationNanoSecond := totalMiniBatches * miniBatchDurationNanoSecond
		GPUCount, _ := strconv.ParseInt(record[GPUCountIdx], 10, 64)
		GPUType := record[GPUTypeIdx]
		var taskGroup *objects.TaskGroup
		if GPUCount > 1 {
			tasks := make([]*objects.Task, 0, GPUCount)
			for i := 0; i < int(GPUCount); i++ {
				tasks = append(tasks, &objects.Task{
					TaskID: jobID + "_TASK_" + strconv.Itoa(i),
				})
			}
			extra := &objects.GangTaskGroupDLTExtra{DLTGangType: objects.DLTGangType_DLTGangTypeDataParallel}
			bytes, _ := json.Marshal(extra)
			gangTaskGroupInfo := &objects.TaskGroup_GangTaskGroupInfo{GangTaskGroupInfo: &objects.GangTaskGroup{
				TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
				Extra:         bytes,
			}}
			taskGroup = &objects.TaskGroup{
				TaskGroupType: objects.TaskGroupType_taskGroupTypeGang,
				Tasks:         tasks,
				TaskGroupInfo: gangTaskGroupInfo,
			}
		} else {
			tasks := make([]*objects.Task, 0, GPUCount)
			tasks = append(tasks, &objects.Task{
				TaskID: jobID + "_TASK_0",
			})
			taskGroup = &objects.TaskGroup{
				TaskGroupType: objects.TaskGroupType_taskGroupTypeSingle,
				Tasks:         tasks,
				TaskGroupInfo: &objects.TaskGroup_SingleTaskGroupInfo{SingleTaskGroupInfo: &objects.SingleTaskGroup{}},
			}
		}
		job := &objects.Job{
			JobID:                jobID,
			JobType:              objects.JobType_jobTypeDLT,
			TaskGroup:            taskGroup,
			SubmitTimeNanoSecond: submitTimeNanoSecond,
			UserGroup: &objects.UserGroup{
				User:  record[userIDIdx],
				Group: "", // TODO
			},
			Deadline: g.generateDeadline(submitTimeNanoSecond, executionDurationNanoSecond),
		}
		AcceleratorType2MiniBatchDurationNanoSecond := g.generateGPUType2MiniBatchDurationNanoSecond(GPUType, miniBatchDurationNanoSecond)
		MaxSpaceSharingPenalty := float32(g.randomUniform(maxSpaceSharingPenaltyDistribution))
		MinSpaceSharingPenalty := float32(g.randomUniform(minSpaceSharingPenaltyDistribution))
		ConsolidationLevel2Penalties := g.generateConsolidationLevel2Penalties()
		MaximumAcceleratorMemoryCostBytes := int64(g.randomUniform([]float64{float64(gpuMemoryCostDistributions[0]), float64(gpuMemoryCostDistributions[1])}))
		return &configs.DLTJobData{
			Job:                                   job,
			TotalMiniBatches:                      totalMiniBatches,
			AcceleratorType2MiniBatchDuration:     &configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{AccType2Duration: AcceleratorType2MiniBatchDurationNanoSecond},
			SpaceSharingMiniBatchDurations:        map[string]*configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{},
			MaxSpaceSharingPenalty:                MaxSpaceSharingPenalty,
			MinSpaceSharingPenalty:                MinSpaceSharingPenalty,
			ConsolidationLevel2Penalties:          ConsolidationLevel2Penalties,
			ConsolidationLevel2MiniBatchDurations: map[int64]*configs.DLTJobData_AcceleratorType2MiniBatchDurationNanoSecond{},
			MaximumAcceleratorMemoryCostBytes:     MaximumAcceleratorMemoryCostBytes,
		}
	}
	data := make(map[string]*configs.DLTJobData)
	gangJobsCount := 0
	for _, record := range mergedRecords {
		d := generatorJob(record)
		s, _ := utils.MarshalJsonPB(d)
		log.Printf("%v", s)
		data[d.GetJob().GetJobID()] = d
		if d.GetJob().GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
			gangJobsCount++
		}
	}
	normalizeSubmitTime := func(data map[string]*configs.DLTJobData) {
		minSubmitTime := int64(math.MaxInt64)
		for _, d := range data {
			if d.GetJob().GetSubmitTimeNanoSecond() < minSubmitTime {
				minSubmitTime = d.GetJob().GetSubmitTimeNanoSecond()
			}
		}
		for _, d := range data {
			d.GetJob().SubmitTimeNanoSecond -= minSubmitTime
			if d.GetJob().GetDeadline() != math.MaxInt64 {
				d.GetJob().Deadline -= minSubmitTime
			}
		}
	}
	normalizeSubmitTime(data)
	log.Printf("GenerateJobsData finished, generated jobs count = %d, gangJobsCount = %d, singleJobsCount = %d", len(data), gangJobsCount, len(data)-gangJobsCount)
	jobs := make([]*objects.Job, 0, len(data))
	for _, d := range data {
		jobs = append(jobs, d.GetJob())
	}
	return data, jobs
}

func (g *CaseGenerator) generateConsolidationLevel2Penalties() map[int64]float32 {
	result := make(map[int64]float32)
	for _, level := range consolidationLevels {
		dis := consolidationLevel2PenaltyDistributions[level]
		result[int64(level.Number())] = float32(g.randomUniform(dis))
	}
	return result
}

func (g *CaseGenerator) generateGPUType2MiniBatchDurationNanoSecond(fromGPU string, originalDuration int64) map[string]int64 {
	result := make(map[string]int64)
	for _, gpuType := range gpuTypes {
		result[gpuType] = int64(float64(originalDuration) * g.generateGPURatio(fromGPU, gpuType))
	}
	return result
}

func (g *CaseGenerator) generateGPURatio(fromGPU, toGPU string) float64 {
	if fromGPU == toGPU {
		return 1
	}
	base := 1.0
	if fromGPU != BaseGPU {
		base = 1. / g.randomUniform(GPUEfficiencyRatio[fromGPU])
	}
	if toGPU == BaseGPU {
		return base
	} else {
		return base * g.randomUniform(GPUEfficiencyRatio[toGPU])
	}
}

func (g *CaseGenerator) randomUniform(minAndMax []float64) float64 {
	size := minAndMax[1] - minAndMax[0]
	return minAndMax[0] + size*rand.Float64()
}

func (g *CaseGenerator) GenerateCluster() *objects.Cluster {
	partition := &objects.Partition{
		PartitionID: partitionID,
		Nodes:       make([]*objects.Node, 0),
	}
	cluster := &objects.Cluster{
		ResourceManagerID: resourceManagerID,
		Partitions: []*objects.Partition{
			partition,
		},
	}
	instanceType := 0
	for instance, count := range instance2Count {
		instanceType += 1
		for i := 0; i < int(count); i++ {
			nodeID := fmt.Sprintf("inst-%d-replica-%d", instanceType, i)
			CPUSockets := make([]*objects.CPUSocket, 0, len(instance.CPUSocket2GPUs))
			for CPUSocketIdx, GPUs := range instance.CPUSocket2GPUs {
				CPUSocket := &objects.CPUSocket{}
				CPUSocket.CPUSocketID = fmt.Sprintf("%s-cpusocket-%d", nodeID, CPUSocketIdx)
				CPUSocket.Accelerators = make(map[string]*objects.Accelerator)
				for GPUIdx, GPUType := range GPUs {
					acc := &objects.Accelerator{
						AcceleratorID:       fmt.Sprintf("%s-acc-%d", CPUSocket.CPUSocketID, GPUIdx),
						AcceleratorMetaInfo: GPUType2Meta[GPUType],
					}
					CPUSocket.Accelerators[acc.GetAcceleratorID()] = acc
				}
				CPUSockets = append(CPUSockets, CPUSocket)
			}
			node := &objects.Node{
				NodeID:     fmt.Sprintf("inst-%d-replica-%d", instanceType, i),
				CPUSockets: CPUSockets,
			}
			partition.Nodes = append(partition.Nodes, node)
		}
	}
	accType2Count := make(map[string]int)
	for _, partition := range cluster.Partitions {
		for _, node := range partition.GetNodes() {
			for _, socket := range node.GetCPUSockets() {
				for _, acc := range socket.GetAccelerators() {
					accType2Count[acc.GetAcceleratorMetaInfo().GetBriefType()]++
				}
			}
		}
	}
	log.Printf("acceleratorTypeToCount: %+v", accType2Count)
	return cluster
}

func (g *CaseGenerator) generateDeadline(submitNanoTime int64, executionDurationNanoTime int64) int64 {
	if rand.Float64() > deadlineProb {
		return math.MaxInt64
	}
	factor := g.randomUniform(deadlineDistribution)
	return submitNanoTime + int64(float64(executionDurationNanoTime)*factor)
}

type Instance struct {
	CPUSocket2GPUs map[int64][]string
}

func NewInstance(CPUSocket2GPUs map[int64][]string) *Instance {
	return &Instance{CPUSocket2GPUs: CPUSocket2GPUs}
}
