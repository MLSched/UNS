package main

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"UNS/utils"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
)

var dataDir = "/Users/purchaser/datasets/ali-cluster/cluster-trace-gpu-v2020/data"
var maxReadLines = -1
var jobCount = 50
var miniBatchDurationNanoSecondDistribution = []int{0.1 * 1e9, 3 * 1e9}
var GPUEfficiencyRatio = map[string][]float64{
	"A100_to_V100":       {1.78, 2.42},
	"A100_to_GTX_2080Ti": {2.36, 3.42},
}
var minSpaceSharingPenaltyDistribution = []float64{1, 1.5}
var maxSpaceSharingPenaltyDistribution = []float64{2, 5}
var gpuTypes = []string{"A100", "V100", "GTX 2080Ti"}
var consolidationLevel2PenaltyDistributions = map[configs.ConsolidationLevel][]float64{
	configs.ConsolidationLevel_NVLink:        {1, 1.05},
	configs.ConsolidationLevel_SameCPUSocket: {1, 1.2},
	configs.ConsolidationLevel_DiffCPUSocket: {1.1, 1.5},
	configs.ConsolidationLevel_DiffNode:      {1.3, 1.8},
}
var GiB = 1024 * 1024
var gpuMemoryCostDistributions = []int64{int64(0.5 * float64(GiB)), int64(8 * GiB)}

func init() {
	rand.Seed(1)
}

func main() {
	generator := &CaseGenerator{}
	jobHeader, jobRecords := generator.ReadTable("pai_job_table.header", "pai_job_table.csv")
	taskHeader, taskRecords := generator.ReadTable("pai_task_table.header", "pai_task_table.csv")
	taskHeader, taskRecords = generator.PreprocessTaskTable(taskHeader, taskRecords)
	mergedHeader, mergedRecords := generator.MergeTaskAndJob(taskHeader, taskRecords, jobHeader, jobRecords)
	jobID2DLTJobData := generator.GenerateJobsData(mergedHeader, mergedRecords)
	predictorData := &configs.DLTPredictorDataOrientedDataFormat{JobID2DLTJobData: jobID2DLTJobData}
	s, err := utils.MarshalJsonPB(predictorData)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("/Users/purchaser/go/src/UNS/cases/case_test.json", []byte(s), 0666)
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
	if maxReadLines < 0 {
		records, err = reader.ReadAll()
		if err != nil {
			panic(err)
		}
	} else {
		records = make([][]string, 0, maxReadLines)
		for i := 0; i < maxReadLines; i++ {
			record, err := reader.Read()
			if err != nil {
				panic(err)
			}
			records = append(records, record)
		}
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
		if record[taskNameIdx] == "TVMTuneMain" {
			//log.Println(record)
		}
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
			record[gpuTypeIdx] = gpuTypes[rand.Intn(len(gpuTypes))]
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
		if planGPUInt64 > 1 {
			// log.Printf("planGPUInt64 > 1\n")
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
	for filterName := range filters {
		filterCount[filterName] = 0
	}
nextRecord:
	for _, record := range records {
		for filterName, filter := range filters {
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
	sort.Sort(sorter)
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

func (g *CaseGenerator) GenerateJobsData(mergedHeader []string, mergedRecords [][]string) map[string]*configs.DLTJobData {
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
		executionDurationSecond := endTimeSecond - startTimeSecond
		min := float64(miniBatchDurationNanoSecondDistribution[0])
		max := float64(miniBatchDurationNanoSecondDistribution[1])
		miniBatchDurationNanoSecond := int64(g.randomUniform([]float64{min, max}))
		totalMiniBatches := int64(executionDurationSecond * 1e9 / float64(miniBatchDurationNanoSecond))
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
			SubmitTimeNanoSecond: int64(startTimeSecond * 1e9),
			UserGroup: &objects.UserGroup{
				User:  record[userIDIdx],
				Group: "", // TODO
			},
		}
		return &configs.DLTJobData{
			Job:              job,
			TotalMiniBatches: totalMiniBatches,
			AcceleratorType2MiniBatchDurationNanoSecond: g.generateGPUType2MiniBatchDurationNanoSecond(GPUType, miniBatchDurationNanoSecond),
			MaxSpaceSharingPenalty:                      float32(g.randomUniform(maxSpaceSharingPenaltyDistribution)),
			MinSpaceSharingPenalty:                      float32(g.randomUniform(minSpaceSharingPenaltyDistribution)),
			ConsolidationLevel2Penalties:                g.generateConsolidationLevel2Penalties(),
			MaximumAcceleratorMemoryCostBytes:           int64(g.randomUniform([]float64{float64(gpuMemoryCostDistributions[0]), float64(gpuMemoryCostDistributions[1])})),
		}
	}
	data := make(map[string]*configs.DLTJobData)
	gangJobsCount := 0
	for _, record := range mergedRecords {
		d := generatorJob(record)
		data[d.GetJob().GetJobID()] = d
		if d.GetJob().GetTaskGroup().GetTaskGroupType() == objects.TaskGroupType_taskGroupTypeGang {
			gangJobsCount++
		}
	}
	log.Printf("GenerateJobsData finished, generated jobs count = %d, gangJobsCount = %d, singleJobsCount = %d", len(data), gangJobsCount, len(data)-gangJobsCount)
	return data
}

func (g *CaseGenerator) generateConsolidationLevel2Penalties() map[int64]float32 {
	result := make(map[int64]float32)
	for cl, dis := range consolidationLevel2PenaltyDistributions {
		result[int64(cl.Number())] = float32(g.randomUniform(dis))
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
	a100Base := 1.0
	if fromGPU != "A100" {
		if fromGPU == "V100" {
			a100Base = 1. / g.randomUniform(GPUEfficiencyRatio["A100_to_V100"])
		} else if fromGPU == "GTX 2080Ti" {
			a100Base = 1. / g.randomUniform(GPUEfficiencyRatio["A100_to_GTX_2080Ti"])
		} else {
			panic("unsupported fromGPU")
		}
	}
	if toGPU == "A100" {
		return a100Base
	} else if toGPU == "V100" {
		return a100Base * g.randomUniform(GPUEfficiencyRatio["A100_to_V100"])
	} else if toGPU == "GTX 2080Ti" {
		return a100Base * g.randomUniform(GPUEfficiencyRatio["A100_to_GTX_2080Ti"])
	} else {
		panic("unsupported toGPU")
	}
}

func (g *CaseGenerator) randomUniform(minAndMax []float64) float64 {
	size := minAndMax[1] - minAndMax[0]
	return minAndMax[0] + size*rand.Float64()
}
